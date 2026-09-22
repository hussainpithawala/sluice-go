// Package main demonstrates how to implement a distributed, periodic DLQ processor
// using Hibiken's Asynq library to schedule task executions across Go microservices,
// backed by AWS DynamoDB.
//
// It highlights the "Payload Healing" pattern:
// 1. Ingest records that violate the WriteContract (routed to DLQ).
// 2. Activate a "healing" flag in the contract.
// 3. Trigger an Asynq task to re-process the DLQ.
// 4. The contract corrects the bad data on the fly, allowing the write to succeed.
//
// Run against DynamoDB Local:
//
//	DYNAMODB_ENDPOINT=http://localhost:8000 REDIS_ADDR=localhost:6379 go run ./examples/asynq_dlq_dynamodb/main.go
//
// Run against AWS DynamoDB:
//
//	AWS_REGION=us-east-1 REDIS_ADDR=localhost:6379 go run ./examples/asynq_dlq_dynamodb/main.go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/hibiken/asynq"
	sluice "github.com/hussainpithawala/sluice-go"
	"github.com/hussainpithawala/sluice-go/internal/localjournal"
	dynsink "github.com/hussainpithawala/sluice-go/sink/dynamodb"
)

const (
	// TaskSluiceProcessDLQ defines the unique Asynq task type identifier.
	TaskSluiceProcessDLQ = "sluice:process_dlq"
	// SluiceNamespace isolates our Redis database boundaries.
	SluiceNamespace = "nudge_inventory_dynamo"
)

var (
	// healBadRecords allows the contract to dynamically correct and accept previously
	// quarantined payloads during the recovery phase.
	healBadRecords atomic.Bool
)

type NudgeInventoryPayload struct {
	NudgeMasterID string    `json:"nudge_master_id"`
	Channel       string    `json:"channel"`
	Priority      int       `json:"priority"`
	CampaignID    string    `json:"campaign_id"`
	ExpiresAt     time.Time `json:"expires_at"`
	LastUpdated   time.Time `json:"last_updated"`
}

type ProcessDLQPayload struct {
	Namespace string `json:"namespace"`
	Strategy  string `json:"strategy"` // "upsert", "ignore", "reinsert"
	BatchSize int    `json:"batch_size"`
}

// asynqLogger bridges the standard slog.Logger to satisfy the asynq.Logger interface.
type asynqLogger struct {
	inner *slog.Logger
}

func (l *asynqLogger) Debug(args ...interface{}) { l.inner.Debug(fmt.Sprint(args...)) }
func (l *asynqLogger) Info(args ...interface{})  { l.inner.Info(fmt.Sprint(args...)) }
func (l *asynqLogger) Warn(args ...interface{})  { l.inner.Warn(fmt.Sprint(args...)) }
func (l *asynqLogger) Error(args ...interface{}) { l.inner.Error(fmt.Sprint(args...)) }
func (l *asynqLogger) Fatal(args ...interface{}) {
	l.inner.Error(fmt.Sprint(args...))
	os.Exit(1)
}

// nudgeWriteContract validates incoming payloads.
// For DynamoDB, we must return the FULL item map (PutItem semantics).
func nudgeWriteContract(correlationKey string, rawPayload []byte) (*sluice.WriteModel, error) {
	var p NudgeInventoryPayload
	if err := json.Unmarshal(rawPayload, &p); err != nil {
		return nil, fmt.Errorf("nudge contract: invalid payload for correlation_key %s: %w", correlationKey, err)
	}

	// Structural Contract Rule: We simulate ingestion errors on certain records.
	if p.Channel == "REJECT" || (len(correlationKey) >= 4 && correlationKey[:4] == "bad_") {
		// If healing is enabled during recovery, correct the payload instead of rejecting it!
		if healBadRecords.Load() {
			p.Channel = "email" // Heal bad channel payload to a safe default
			slog.Info("dlq-healing: corrected bad payload field during recovery", "correlationKey", correlationKey)
		} else {
			return nil, fmt.Errorf("contract violation: invalid channel type on key %s", correlationKey)
		}
	}

	// DynamoDB requires the full item map for a PutItem operation.
	item := map[string]any{
		"PK":              correlationKey,
		"nudge_master_id": p.NudgeMasterID,
		"channel":         p.Channel,
		"priority":        p.Priority,
		"campaign_id":     p.CampaignID,
		"expires_at":      p.ExpiresAt.Format(time.RFC3339),
		"last_updated":    p.LastUpdated.Format(time.RFC3339),
	}

	return &sluice.WriteModel{
		Update: item,
		Upsert: true,
	}, nil
}

// DLQTaskHandler executes the Sluice DLQ process upon receiving signals from the Asynq worker server.
type DLQTaskHandler struct {
	sl  *sluice.Sluice
	log *slog.Logger
}

func NewDLQTaskHandler(sl *sluice.Sluice, log *slog.Logger) *DLQTaskHandler {
	return &DLQTaskHandler{sl: sl, log: log}
}

func (h *DLQTaskHandler) ProcessTask(ctx context.Context, t *asynq.Task) error {
	var payload ProcessDLQPayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		return fmt.Errorf("parse task payload: %w", err)
	}

	h.log.Info("asynq: executing scheduled DLQ processing task",
		"namespace", payload.Namespace,
		"strategy", payload.Strategy,
	)

	// Map string strategy parameters back to Sluice typed constants.
	var strategy sluice.DLQStrategy
	switch payload.Strategy {
	case "upsert":
		strategy = sluice.DLQUpsert
	case "ignore":
		strategy = sluice.DLQIgnore
	case "reinsert":
		strategy = sluice.DLQReInsert
	default:
		return fmt.Errorf("unknown DLQ strategy: %s", payload.Strategy)
	}

	// Execute ProcessDLQ inside the worker task context
	res, err := h.sl.ProcessDLQ(ctx, strategy,
		sluice.WithDLQBatchSize(payload.BatchSize),
		sluice.WithDLQLogger(h.log),
	)
	if err != nil {
		h.log.Error("asynq: ProcessDLQ execution failed", "err", err)
		return err
	}

	h.log.Info("asynq: DLQ recovery completed successfully",
		"processed", res.Processed,
		"succeeded", res.Succeeded,
		"failed", res.Failed,
	)
	return nil
}

// logMetrics registers trace counters.
type logMetrics struct{ log *slog.Logger }

func (m *logMetrics) RecordBroadcastLag(namespace string, lag time.Duration) {
	m.log.Info("broadcast-lag", "ns", namespace, "lag", lag)
}
func (m *logMetrics) RecordWarmUp(namespace string, duration time.Duration, err error) {
	m.log.Info("warm-up", "ns", namespace, "duration", duration.Milliseconds(), "error", err)
}
func (m *logMetrics) RecordRead(namespace string, duration time.Duration, isHot bool, err error) {
	m.log.Info("read", "ns", namespace, "duration", duration.Milliseconds(), "isHot", isHot, "error", err)
}
func (m *logMetrics) RecordHotSetSize(namespace string, size int) {
	m.log.Info("hot-set-size", "ns", namespace, "size", size)
}
func (m *logMetrics) RecordWrite(_ string) {}
func (m *logMetrics) RecordDegradedWrite(ns string, err error) {
	m.log.Warn("degraded write ", "ns ", ns, "err ", err)
}
func (m *logMetrics) RecordRedisOp(ns, op string, d time.Duration, err error) {
	if err != nil {
		m.log.Error("redis op ", "ns ", ns, "op ", op, "ms ", d.Milliseconds(), "err ", err)
	}
}
func (m *logMetrics) RecordFlush(ns, band string, batch int, d time.Duration, err error) {
	m.log.Info("flush ", "ns ", ns, "band ", band, "batch ", batch, "ms ", d.Milliseconds(), "err ", err)
}
func (m *logMetrics) RecordDirtyQueueDepth(ns, band string, depth int) {
	if depth > 100 {
		m.log.Warn("dirty queue depth ", "ns ", ns, "band ", band, "depth ", depth)
	}
}
func (m *logMetrics) RecordContractError(ns, correlationKey string, err error) {
	m.log.Error("contract error ", "ns ", ns, "correlationKey ", correlationKey, "err ", err)
}
func (m *logMetrics) RecordDeadLetter(ns, band string, count int) {
	m.log.Warn("dead-letter record written ", "ns ", ns, "band ", band, "count ", count)
}
func (m *logMetrics) RecordDLQProcess(ns, strategy string, processed, succeeded, failed int) {
	m.log.Info("dlq-process-complete ", "ns ", ns, "strategy ", strategy, "processed ", processed, "succeeded ", succeeded, "failed ", failed)
}
func (m *logMetrics) RecordLocalCacheHit(namespace string) {
	m.log.Info("record-local-cache-hit ", "ns ", namespace)
}
func (m *logMetrics) RecordLocalCacheMiss(namespace string, reason localjournal.MissReason) {
	m.log.Info("record-local-cache-miss", "ns", namespace, "reason", reason)
}
func (m *logMetrics) RecordLocalSetSize(namespace string, size int) {
	m.log.Info("record-local-set-size", "ns", namespace, "size", size)
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(log); err != nil {
		log.Error("application run failed", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	endpoint := os.Getenv("DYNAMODB_ENDPOINT")

	// 1. Initialize DynamoDB Client
	var cfg aws.Config
	var err error

	// Custom HTTP client with a longer timeout for DynamoDB Local stability
	httpClient := &http.Client{Timeout: 30 * time.Second}

	if endpoint != "" {
		log.Info("using local dynamodb endpoint", "endpoint", endpoint)
		cfg, err = config.LoadDefaultConfig(ctx,
			config.WithRegion("us-east-1"),
			config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: endpoint}, nil
			})),
			config.WithHTTPClient(httpClient),
		)
	} else {
		log.Info("using default aws config (production mode)")
		cfg, err = config.LoadDefaultConfig(ctx, config.WithHTTPClient(httpClient))
	}
	if err != nil {
		return fmt.Errorf("load aws config: %w", err)
	}

	client := dynamodb.NewFromConfig(cfg)
	tableName := "NudgeInventoryDLQ"

	// Ensure table exists for local testing
	if err := ensureTableExists(ctx, client, tableName, log); err != nil {
		return fmt.Errorf("ensure table exists: %w", err)
	}

	// 2. Initialize Sluice Sink
	sk := dynsink.NewSink(client, tableName)

	// 3. Build Sluice write pipeline instance
	sl, err := sluice.New(SluiceNamespace).
		WithRedis(sluice.RedisConfig{Addrs: []string{redisAddr}, PoolSize: 20}).
		WithSink(sk).
		WithWriteContract(nudgeWriteContract).
		WithFlushWindow(250 * time.Millisecond).
		WithMaxBatchSize(500).
		WithBandCount(4).
		WithKeyTTL(30 * time.Second).
		WithMetrics(&logMetrics{log: log}).
		Build(ctx)
	if err != nil {
		return fmt.Errorf("failed to build Sluice: %w", err)
	}

	// Setup Asynq connection parameters
	redisConnOpt := asynq.RedisClientOpt{Addr: redisAddr}

	// 4. Initialize Asynq Scheduler (Distributed Cron)
	scheduler := asynq.NewScheduler(redisConnOpt, &asynq.SchedulerOpts{Location: time.UTC})

	// Define our scheduled task payload configuration (Runs every 2 minutes for demo)
	dlqPayload, _ := json.Marshal(ProcessDLQPayload{
		Namespace: SluiceNamespace,
		Strategy:  "upsert",
		BatchSize: 10,
	})
	task := asynq.NewTask(TaskSluiceProcessDLQ, dlqPayload)
	cronExpr := "*/2 * * * *"
	entryID, err := scheduler.Register(cronExpr, task)
	if err != nil {
		return fmt.Errorf("failed to register cron task: %w", err)
	}
	log.Info("registered periodic DLQ task in cron registry", "entry_id", entryID, "schedule", cronExpr)

	// 5. Initialize Asynq Server (Worker) and Client (Manual validation trigger)
	asynqServer := asynq.NewServer(redisConnOpt, asynq.Config{
		Concurrency: 2,
		Logger:      &asynqLogger{inner: log},
	})
	asynqClient := asynq.NewClient(redisConnOpt)
	defer func() {
		_ = asynqClient.Close()
	}()

	mux := asynq.NewServeMux()
	mux.Handle(TaskSluiceProcessDLQ, NewDLQTaskHandler(sl, log))

	// 6. Spin up background workers
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := scheduler.Run(); err != nil {
			log.Error("asynq scheduler crash", "err", err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := asynqServer.Run(mux); err != nil {
			log.Error("asynq server crash", "err", err)
		}
	}()

	// ── Controlled Functional Validation Flow ──
	log.Info("starting controlled validation sequence: Ingesting 10 payloads (60% success, 40% failure expected)")

	// Ingest exactly 10 requests (6 good, 4 bad)
	for i := 1; i <= 10; i++ {
		var correlationKey string
		var channel string
		if i <= 6 {
			correlationKey = fmt.Sprintf("correlationKey_good_%d", i)
			channel = "push"
		} else {
			correlationKey = fmt.Sprintf("bad_correlationKey_failed_%d", i)
			channel = "REJECT" // This will violate the contract
		}
		payload, _ := json.Marshal(NudgeInventoryPayload{
			NudgeMasterID: "nm_spring_retarget_2026",
			Channel:       channel,
			Priority:      3,
			CampaignID:    "camp_9999",
			ExpiresAt:     time.Now().Add(24 * time.Hour),
			LastUpdated:   time.Now().UTC(),
		})
		if err := sl.Write(ctx, correlationKey, payload); err != nil {
			log.Error("validation ingest failed", "correlationKey", correlationKey, "err", err)
		}
	}

	// Wait 600ms for Sluice's background flush window (250ms) to trigger and quarantine bad payloads
	log.Info("waiting for Sluice's normal flush window to process inputs...")
	select {
	case <-ctx.Done():
		log.Info("aborted during wait")
		return nil
	case <-time.After(600 * time.Millisecond):
	}

	log.Info("Sluice flush phase completed. Initiating recovery phase...")

	// Activate payload healing logic in the contract
	healBadRecords.Store(true)
	log.Info("asynq: Healing flag activated! Enqueuing task for immediate execution...")

	// Enqueue the task immediately to bypass the 2-minute cron wait during functional verification
	info, err := asynqClient.Enqueue(task)
	if err != nil {
		log.Error("asynq: failed to enqueue immediate verification task", "err", err)
	} else {
		log.Info("asynq: task enqueued successfully", "task_id", info.ID, "queue", info.Queue)
	}

	// Wait 1.5 seconds for the worker server to pick up the task and execute the healing process
	select {
	case <-ctx.Done():
	case <-time.After(1500 * time.Millisecond):
	}

	// 7. Graceful Shutdown
	log.Info("stopping background cron and asynq processes")
	scheduler.Shutdown()
	asynqServer.Shutdown()
	wg.Wait()

	// Drain Sluice and clean up connections
	log.Info("draining remaining Sluice buffers...")
	shutCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = sl.DrainAndClose(shutCtx)
	log.Info("validation script finished cleanly")
	return nil
}

// ensureTableExists creates the table if it doesn't exist and waits for it to become ACTIVE.
func ensureTableExists(ctx context.Context, client *dynamodb.Client, tableName string, log *slog.Logger) error {
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("PK"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("PK"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
	})

	if err != nil {
		// Check if table already exists
		var alreadyExists *types.ResourceInUseException
		if errors.As(err, &alreadyExists) {
			log.Info("table already exists, skipping creation", "table", tableName)
		} else if strings.Contains(err.Error(), "ResourceInUseException") {
			log.Info("table already exists (string match), skipping creation", "table", tableName)
		} else {
			return fmt.Errorf("create table: %w", err)
		}
	} else {
		log.Info("table creation initiated", "table", tableName)
	}

	log.Info("waiting for table to become active...", "table", tableName)
	waiter := dynamodb.NewTableExistsWaiter(client)
	err = waiter.Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}, 2*time.Minute)

	if err != nil {
		return fmt.Errorf("wait for table active: %w", err)
	}

	log.Info("table is active", "table", tableName)
	return nil
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
