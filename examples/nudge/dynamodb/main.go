// Package main demonstrates a production-grade nudge inventory consumer
// using sluice as the write buffer between Kafka/SQS and AWS DynamoDB.
//
// This example highlights the "Velocity Shield" pattern for DynamoDB:
// 1. Absorbs high-velocity writes into Redis.
// 2. Coalesces duplicate updates for the same correlation key.
// 3. Drains to DynamoDB in efficient BatchWriteItem calls (max 25 items/batch).
//
// Prerequisites:
// - A running Redis instance (local or cluster).
// - A running DynamoDB Local instance on port 8000 OR valid AWS credentials.
//
// Run against DynamoDB Local:
//
//	DYNAMODB_ENDPOINT=http://localhost:8000 REDIS_ADDRS=localhost:6379 go run ./examples/nudge/dynamodb/main.go
//
// Run against AWS DynamoDB:
//
//	AWS_REGION=us-east-1 REDIS_ADDRS=localhost:6379 go run ./examples/nudge/dynamodb/main.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	sluice "github.com/hussainpithawala/sluice-go"
	"github.com/hussainpithawala/sluice-go/internal/localjournal"
	dynsink "github.com/hussainpithawala/sluice-go/sink/dynamodb"
)

var (
	version   = "dev"
	commit    = "none"
	buildDate = "unknown"
)

type NudgeInventoryPayload struct {
	NudgeMasterID string    `json:"nudge_master_id"`
	Channel       string    `json:"channel"`
	Priority      int       `json:"priority"`
	CampaignID    string    `json:"campaign_id"`
	ExpiresAt     time.Time `json:"expires_at"`
	LastUpdated   time.Time `json:"last_updated"`
}

// nudgeWriteContract translates the raw payload into a DynamoDB PutItem format.
func nudgeWriteContract(correlationKey string, rawPayload []byte) (*sluice.WriteModel, error) {
	var p NudgeInventoryPayload
	if err := json.Unmarshal(rawPayload, &p); err != nil {
		return nil, fmt.Errorf("nudge contract: invalid payload for correlation_key %s: %w", correlationKey, err)
	}

	// Construct the full item to be stored in DynamoDB.
	// 'PK' is our Partition Key.
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

type logMetrics struct{ log *slog.Logger }

func (m *logMetrics) RecordBroadcastLag(namespace string, lag time.Duration) {
	m.log.Info("broadcast-lag", "ns", namespace, "lag_ms", lag.Milliseconds())
}
func (m *logMetrics) RecordWarmUp(namespace string, duration time.Duration, err error) {
	m.log.Info("warm-up", "ns", namespace, "duration_ms", duration.Milliseconds(), "error", err)
}
func (m *logMetrics) RecordRead(namespace string, duration time.Duration, isHot bool, err error) {
	m.log.Info("read", "ns", namespace, "duration_ms", duration.Milliseconds(), "isHot", isHot, "error", err)
}
func (m *logMetrics) RecordHotSetSize(namespace string, size int) {
	m.log.Info("hot-set-size", "ns", namespace, "size", size)
}
func (m *logMetrics) RecordWrite(_ string) {}
func (m *logMetrics) RecordDegradedWrite(ns string, err error) {
	m.log.Warn("degraded write", "ns", ns, "err", err)
}
func (m *logMetrics) RecordRedisOp(ns, op string, d time.Duration, err error) {
	if err != nil {
		m.log.Error("redis op", "ns", ns, "op", op, "ms", d.Milliseconds(), "err", err)
	}
}
func (m *logMetrics) RecordFlush(ns, band string, batch int, d time.Duration, err error) {
	m.log.Info("flush", "ns", ns, "band", band, "batch", batch, "ms", d.Milliseconds(), "err", err)
}
func (m *logMetrics) RecordDirtyQueueDepth(ns, band string, depth int) {
	if depth > 100 {
		m.log.Warn("dirty queue depth", "ns", ns, "band", band, "depth", depth)
	}
}
func (m *logMetrics) RecordContractError(ns, correlationKey string, err error) {
	m.log.Error("contract error", "ns", ns, "correlationKey", correlationKey, "err", err)
}
func (m *logMetrics) RecordDeadLetter(ns, band string, count int) {
	m.log.Warn("dead-letter", "ns", ns, "band", band, "count", count)
}
func (m *logMetrics) RecordDLQProcess(ns, strategy string, processed, succeeded, failed int) {
	m.log.Info("dlq-process", "ns", ns, "strategy", strategy, "processed", processed, "succeeded", succeeded, "failed", failed)
}
func (m *logMetrics) RecordLocalCacheHit(namespace string)                                  {}
func (m *logMetrics) RecordLocalCacheMiss(namespace string, reason localjournal.MissReason) {}
func (m *logMetrics) RecordLocalSetSize(namespace string, size int)                         {}

// simulatedConsumer generates events at a sustainable rate for functional testing.
// Reduced load: 4 workers * 2 events * 10 ticks/sec = ~80 events/sec.
// This is enough to demonstrate coalescing and batching without overwhelming DynamoDB Local.
func simulatedConsumer(ctx context.Context, workerID int, sl *sluice.Sluice, log *slog.Logger, written *atomic.Int64, wg *sync.WaitGroup) {
	defer wg.Done()
	nudgeMasters := []string{"nm_spring_retarget_2026", "nm_cart_abandonment", "nm_win_back_q2", "nm_first_purchase", "nm_loyalty_upgrade"}
	channels := []string{"push", "email", "sms", "in_app"}

	// Increased ticker interval from 1ms to 100ms to reduce pressure on DynamoDB Local
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Reduced burst size from 10 to 2
			for burst := 0; burst < 2; burst++ {
				seq := written.Add(1)
				// Use a limited set of keys to demonstrate coalescing in Redis
				correlationKey := fmt.Sprintf("user#%09d", (workerID*1_000_000)+int(seq%2_000_000))
				payload, _ := json.Marshal(NudgeInventoryPayload{
					NudgeMasterID: nudgeMasters[seq%int64(len(nudgeMasters))],
					Channel:       channels[seq%int64(len(channels))],
					Priority:      int(seq%5) + 1,
					CampaignID:    fmt.Sprintf("camp_%04d", seq%100),
					ExpiresAt:     time.Now().Add(24 * time.Hour),
					LastUpdated:   time.Now().UTC(),
				})
				if err := sl.Write(ctx, correlationKey, payload); err != nil {
					log.Error("write error", "correlationKey", correlationKey, "err", err)
				}
			}
		}
	}
}

const bandCount = 16

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) (err error) {
	log.Info("sluice dynamodb nudge example", "version", version, "commit", commit, "built", buildDate)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// --- 1. Initialize DynamoDB Client ---
	var cfg aws.Config
	endpoint := os.Getenv("DYNAMODB_ENDPOINT")

	// Custom HTTP client with a longer timeout for DynamoDB Local stability
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
	}

	if endpoint != "" {
		log.Info("using local dynamodb endpoint", "endpoint", endpoint)
		cfg, err = config.LoadDefaultConfig(ctx,
			config.WithRegion("us-east-1"),
			config.WithHTTPClient(httpClient),
		)
	} else {
		log.Info("using default aws config (production mode)")
		cfg, err = config.LoadDefaultConfig(ctx,
			config.WithHTTPClient(httpClient),
		)
	}
	if err != nil {
		return fmt.Errorf("load aws config: %w", err)
	}

	// Use BaseEndpoint on the service client options instead of the deprecated global resolver
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	})

	tableName := "NudgeInventory"

	// --- 2. Ensure Table Exists and is Active ---
	if err := ensureTableExists(ctx, client, tableName, log); err != nil {
		return fmt.Errorf("ensure table exists: %w", err)
	}

	// --- 3. Initialize Sluice Sink ---
	sk := dynsink.NewSink(client, tableName)

	// --- 4. Configure Redis ---
	redisAddrsRaw := getEnv("REDIS_ADDRS", "localhost:6379")
	redisAddrs := strings.Split(redisAddrsRaw, ",")
	for i := range redisAddrs {
		redisAddrs[i] = strings.TrimSpace(redisAddrs[i])
	}

	clusterModeRaw := getEnv("REDIS_CLUSTER_MODE", "false")
	clusterMode, err := strconv.ParseBool(clusterModeRaw)
	if err != nil {
		return fmt.Errorf("invalid REDIS_CLUSTER_MODE %q: %w", clusterModeRaw, err)
	}

	// --- 5. Build Sluice ---
	sl, err := sluice.New("nudge_inventory_dynamodb").
		WithRedis(sluice.RedisConfig{
			Addrs:        redisAddrs,
			ClusterMode:  clusterMode,
			PoolSize:     30,
			DialTimeout:  5 * time.Second,
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 3 * time.Second,
		}).
		WithSink(sk).
		WithWriteContract(nudgeWriteContract).
		WithFlushWindow(250 * time.Millisecond).
		WithMaxBatchSize(1000).
		WithBandCount(bandCount).
		WithKeyTTL(30 * time.Second).
		WithDegradedModeDirect(true).
		WithMetrics(&logMetrics{log: log}).
		OnFlush(func(correlationKeys []string, result *sluice.BulkWriteResult, err error) {
			if err != nil {
				log.Error("flush failed", "keys_count", len(correlationKeys), "err", err)
				return
			}
			for _, se := range result.Errors {
				log.Warn("partial write failure", "correlationKey", se.CorrelationKey, "err", se.Err)
			}
			log.Debug("flush complete", "keys_count", len(correlationKeys), "upserted", result.UpsertedCount)
		}).
		Build(ctx)
	if err != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if cerr := sk.Close(closeCtx); cerr != nil {
			log.Warn("closing dynamodb sink after failed build", "err", cerr)
		}
		return fmt.Errorf("build sluice: %w", err)
	}

	log.Info("sluice ready", "redis_addrs", redisAddrs, "cluster_mode", clusterMode, "sink", "DynamoDB")

	defer func() {
		log.Info("draining sluice...")
		shutCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if derr := sl.DrainAndClose(shutCtx); derr != nil {
			log.Error("drain error", "err", derr)
			if err == nil {
				err = fmt.Errorf("drain and close: %w", derr)
			}
			return
		}
		log.Info("sluice drained and closed")
	}()

	// --- 6. Start Simulation ---
	// Reduced worker count from 8 to 4 for functional testing
	const workerCount = 4
	var wg sync.WaitGroup
	var written atomic.Int64

	log.Info("starting consumer workers", "count", workerCount)
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go simulatedConsumer(ctx, i, sl, log, &written, &wg)
	}

	statsTicker := time.NewTicker(5 * time.Second)
	defer statsTicker.Stop()
	start := time.Now()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-statsTicker.C:
				total := written.Load()
				elapsed := time.Since(start).Seconds()
				log.Info("throughput", "events", total, "elapsed_s", fmt.Sprintf("%.1f", elapsed), "rate", fmt.Sprintf("%.0f/s", float64(total)/elapsed))
			}
		}
	}()

	<-ctx.Done()
	log.Info("shutdown signal received")
	wg.Wait()

	total := written.Load()
	elapsed := time.Since(start)
	log.Info("final summary", "total_events", total, "elapsed", elapsed.Round(time.Second), "avg_rate", fmt.Sprintf("%.0f events/sec", float64(total)/elapsed.Seconds()))
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
		if !strings.Contains(err.Error(), "ResourceInUseException") {
			return fmt.Errorf("create table: %w", err)
		}
		log.Info("table already exists, skipping creation", "table", tableName)
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
