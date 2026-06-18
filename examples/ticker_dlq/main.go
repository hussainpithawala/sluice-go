// Package main demonstrates a production-grade nudge inventory consumer
// that functionally validates the Dead-Letter Queue (DLQ) processing and healing flow.
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
	"github.com/hussainpithawala/sluice-go/sink/docdb"
	"go.mongodb.org/mongo-driver/bson"
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

	mongoURI := getEnv("MONGO_URI", "mongodb://localhost:27017")
	sk, err := docdb.New(ctx, docdb.Config{URI: mongoURI, Database: "adroll", Collection: "nudge_inventory", MaxPoolSize: 50, MinPoolSize: 5})
	if err != nil {
		log.Error("failed to connect to MongoDB", "err", err)
		os.Exit(1)
	}

	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	sl, err := sluice.New("nudge_inventory").
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

	// ── Controlled Functional Validation Flow ──

	log.Info("starting controlled validation sequence: Ingesting 10 payloads (60% success, 40% failure expected)")

	// Ingest exactly 10 requests
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

	// Wait 500ms for Sluice's background flush window (250ms) to trigger and clean up active bands
	log.Info("waiting for Sluice's normal flush window to process inputs...")
	select {
	case <-ctx.Done():
		log.Info("aborted during wait")
		return
	case <-time.After(600 * time.Millisecond):
	}

	log.Info("Sluice flush phase completed. Initializing recovery phase...")

	// Activate payload healing logic in the contract
	healBadRecords.Store(true)
	log.Info("dlq-recovery: Healing flag activated! Re-running ProcessDLQ...")

	// Execute ProcessDLQ
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
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
