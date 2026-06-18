// Package main demonstrates a production-grade nudge inventory consumer
// that schedules and executes Dead-Letter Queue (DLQ) healing tasks via hibiken/asynq.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hibiken/asynq"
	sluice "github.com/hussainpithawala/sluice-go"
	"github.com/hussainpithawala/sluice-go/sink/docdb"
	"go.mongodb.org/mongo-driver/bson"
)

const (
	// TaskSluiceProcessDLQ defines the unique Asynq task type identifier.
	TaskSluiceProcessDLQ = "sluice:process_dlq"

	// SluiceNamespace isolates our Redis database boundaries.
	SluiceNamespace = "nudge_inventory"
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

// nudgeWriteContract validates incoming payloads. If a payload violates our system contract
// (e.g. channel field is empty or key starts with "bad_"), it returns an error.
// This causes Sluice's flush engine to isolate and route this record to the DLQ.
func nudgeWriteContract(crn string, rawPayload []byte) (*sluice.WriteModel, error) {
	var p NudgeInventoryPayload
	if err := json.Unmarshal(rawPayload, &p); err != nil {
		return nil, fmt.Errorf("nudge contract: invalid payload for CRN %s: %w", crn, err)
	}

	// Structural Contract Rule: We simulate ingestion errors on certain records.
	if p.Channel == "REJECT" || (len(crn) >= 4 && crn[:4] == "bad_") {
		// If healing is enabled during recovery, correct the payload instead of rejecting it!
		if healBadRecords.Load() {
			p.Channel = "email" // Heal bad channel payload to a safe default
			slog.Info("dlq-healing: corrected bad payload field during recovery", "crn", crn)
		} else {
			return nil, fmt.Errorf("contract violation: invalid channel type on key %s", crn)
		}
	}

	return &sluice.WriteModel{
		Filter: bson.D{{Key: "_id", Value: crn}},
		Update: bson.D{{Key: "$set", Value: bson.D{
			{Key: "nudge_master_id", Value: p.NudgeMasterID},
			{Key: "channel", Value: p.Channel},
			{Key: "priority", Value: p.Priority},
			{Key: "campaign_id", Value: p.CampaignID},
			{Key: "expires_at", Value: p.ExpiresAt},
			{Key: "last_updated", Value: p.LastUpdated},
		}}},
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

	// Completion logging statement is reached flawlessly because bad records are now healed!
	h.log.Info("asynq: DLQ recovery completed successfully",
		"processed", res.Processed,
		"succeeded", res.Succeeded,
		"failed", res.Failed,
	)
	return nil
}

// logMetrics registers trace counters.
type logMetrics struct{ log *slog.Logger }

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
func (m *logMetrics) RecordContractError(ns, crn string, err error) {
	m.log.Error("contract error", "ns", ns, "crn", crn, "err", err)
}
func (m *logMetrics) RecordDeadLetter(ns, band string, count int) {
	m.log.Warn("dead-letter record written", "ns", ns, "band", band, "count", count)
}
func (m *logMetrics) RecordDLQProcess(ns, strategy string, processed, succeeded, failed int) {
	m.log.Info("dlq-process-complete", "ns", ns, "strategy", strategy, "processed", processed, "succeeded", succeeded, "failed", failed)
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	mongoURI := getEnv("MONGO_URI", "mongodb://localhost:27017")

	// 1. Initialize MongoDB sink instance
	sk, err := docdb.New(ctx, docdb.Config{URI: mongoURI, Database: "adroll", Collection: "nudge_inventory", MaxPoolSize: 50, MinPoolSize: 5})
	if err != nil {
		log.Error("mongodb connection failure", "err", err)
		os.Exit(1)
	}

	// 2. Build Sluice write pipeline instance
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
		log.Error("failed to build sluice", "err", err)
		os.Exit(1)
	}

	// Setup Asynq connection parameters
	redisConnOpt := asynq.RedisClientOpt{Addr: redisAddr}

	// 3. Initialize Asynq Scheduler (Distributed Cron)
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
		log.Error("failed to register cron task", "err", err)
		os.Exit(1)
	}
	log.Info("registered periodic DLQ task in cron registry", "entry_id", entryID, "schedule", cronExpr)

	// 4. Initialize Asynq Server (Worker) and Client (Manual validation trigger)
	asynqServer := asynq.NewServer(redisConnOpt, asynq.Config{
		Concurrency: 2,
		Logger:      &asynqLogger{inner: log},
	})
	asynqClient := asynq.NewClient(redisConnOpt)
	defer asynqClient.Close()

	mux := asynq.NewServeMux()
	mux.Handle(TaskSluiceProcessDLQ, NewDLQTaskHandler(sl, log))

	// 5. Spin up background workers
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
		var crn string
		var channel string

		if i <= 6 {
			crn = fmt.Sprintf("crn_good_%d", i)
			channel = "push"
		} else {
			crn = fmt.Sprintf("bad_crn_failed_%d", i)
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

		if err := sl.Write(ctx, crn, payload); err != nil {
			log.Error("validation ingest failed", "crn", crn, "err", err)
		}
	}

	// Wait 600ms for Sluice's background flush window (250ms) to trigger and quarantine bad payloads
	log.Info("waiting for Sluice's normal flush window to process inputs...")
	select {
	case <-ctx.Done():
		log.Info("aborted during wait")
		return
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

	// 6. Graceful Shutdown
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
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
