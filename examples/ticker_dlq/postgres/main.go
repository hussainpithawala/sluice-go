// Package main demonstrates a production-grade nudge inventory consumer
// that functionally validates the Dead-Letter Queue (DLQ) processing and healing flow
// using a simple inline execution (no external scheduler), backed by PostgreSQL.
//
// It highlights the "Payload Healing" pattern:
// 1. Ingest records that violate the WriteContract (routed to DLQ).
// 2. Activate a "healing" flag in the contract.
// 3. Trigger ProcessDLQ inline to re-process the DLQ.
// 4. The contract corrects the bad data on the fly, allowing the write to succeed.
//
// Run against local PostgreSQL + Redis:
//
//	POSTGRES_URI=postgres://sluice:sluice@localhost:5432/sluice_test?sslmode=disable
//	REDIS_ADDR=localhost:6379
//	go run ./examples/ticker_dlq_postgres/main.go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	sluice "github.com/hussainpithawala/sluice-go"
	"github.com/hussainpithawala/sluice-go/internal/localjournal"
	pgsink "github.com/hussainpithawala/sluice-go/sink/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
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

// nudgeWriteContract validates incoming payloads.
// For PostgreSQL, Update must be the full row map, and Filter is the ON CONFLICT target.
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

	// PostgreSQL requires the full row map for an upsert.
	item := map[string]any{
		"id":              correlationKey,
		"nudge_master_id": p.NudgeMasterID,
		"channel":         p.Channel,
		"priority":        p.Priority,
		"campaign_id":     p.CampaignID,
		"expires_at":      p.ExpiresAt,
		"last_updated":    p.LastUpdated,
	}

	return &sluice.WriteModel{
		Filter: "id", // ON CONFLICT (id)
		Update: item,
		Upsert: true,
	}, nil
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
	m.log.Warn("dead-letter record written", "ns", ns, "band", band, "count", count)
}
func (m *logMetrics) RecordDLQProcess(ns, strategy string, processed, succeeded, failed int) {
	m.log.Info("dlq-process-complete", "ns", ns, "strategy", strategy, "processed", processed, "succeeded", succeeded, "failed", failed)
}
func (m *logMetrics) RecordLocalCacheHit(namespace string) {
	m.log.Info("record-local-cache-hit", "ns", namespace)
}
func (m *logMetrics) RecordLocalCacheMiss(namespace string, reason localjournal.MissReason) {
	m.log.Info("record-local-cache-miss", "ns", namespace, "reason", reason)
}
func (m *logMetrics) RecordLocalSetSize(namespace string, size int) {
	m.log.Info("record-local-set-size", "ns", namespace, "size", size)
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	// Delegating to run() avoids calling os.Exit after defer setup, satisfying the 'exitAfterDefer' linter rule.
	if err := run(log); err != nil {
		log.Error("application run failed", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	postgresURI := getEnv("POSTGRES_URI", "postgres://sluice:sluice@localhost:5432/sluice_test?sslmode=disable")
	tableName := "nudge_inventory_dlq"

	// 1. Initialize PostgreSQL Sink
	sk, err := pgsink.New(ctx, pgsink.Config{
		ConnString: postgresURI,
		TableName:  tableName,
		MaxConns:   20,
		MinConns:   5,
	})
	if err != nil {
		return fmt.Errorf("postgres connection failure: %w", err)
	}

	// Ensure table exists for local testing (Operator's responsibility in production)
	if err := ensureTableExists(ctx, sk.Pool(), tableName, log); err != nil {
		return fmt.Errorf("ensure table exists: %w", err)
	}

	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")

	// 2. Build Sluice write pipeline instance
	sl, err := sluice.New("nudge_inventory_postgres").
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
	log.Info("dlq-recovery: Healing flag activated! Re-running ProcessDLQ...")

	// Execute ProcessDLQ inline
	runCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	res, err := sl.ProcessDLQ(runCtx, sluice.DLQUpsert,
		sluice.WithDLQBatchSize(10),
		sluice.WithDLQLogger(log),
	)
	cancel()

	if err != nil {
		if errors.Is(err, context.Canceled) {
			log.Info("DLQ recovery execution gracefully aborted due to application shutdown")
		} else {
			log.Error("DLQ recovery execution failed", "err", err)
		}
	} else {
		// Because bad records are now corrected by the healing contract,
		// DLQUpsert succeeds, and this completion statement is reached flawlessly!
		log.Info("DLQ recovery completed successfully",
			"processed", res.Processed,
			"succeeded", res.Succeeded,
			"failed", res.Failed,
		)
	}

	// Drain Sluice and clean up connections
	log.Info("draining remaining Sluice buffers...")
	shutCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := sl.DrainAndClose(shutCtx); err != nil {
		log.Error("drain error during shutdown", "err", err)
	}
	log.Info("validation script finished cleanly")
	return nil
}

// ensureTableExists creates the table if it doesn't exist.
// In production, this is strictly the operator's responsibility via migration tooling.
func ensureTableExists(ctx context.Context, pool *pgxpool.Pool, tableName string, log *slog.Logger) error {
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id TEXT PRIMARY KEY,
			nudge_master_id TEXT NOT NULL,
			channel TEXT NOT NULL,
			priority INTEGER NOT NULL,
			campaign_id TEXT NOT NULL,
			expires_at TIMESTAMPTZ NOT NULL,
			last_updated TIMESTAMPTZ NOT NULL
		)
	`, tableName))
	if err != nil {
		return fmt.Errorf("create table: %w", err)
	}
	log.Info("ensured table exists", "table", tableName)
	return nil
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
