// Command writer is "Pod A" in the Local Journal PushPull demo.
//
// It writes a sequence of payloads for a single correlation key and verifies
// same-pod read-your-own-write on each. A separate "reader" process (Pod B)
// observes cross-pod convergence via the broadcast stream.
//
// Run the reader first, then this writer:
//
//	# Terminal 1
//	go run ./examples/localjournal_pushpull/reader
//
//	# Terminal 2
//	go run ./examples/localjournal_pushpull/writer
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	sluice "github.com/hussainpithawala/sluice-go"
	"github.com/hussainpithawala/sluice-go/internal/localjournal"
	"github.com/hussainpithawala/sluice-go/internal/shield"
	"github.com/hussainpithawala/sluice-go/sink/docdb"
	"github.com/hussainpithawala/sluice-go/source"
	sourcedocdb "github.com/hussainpithawala/sluice-go/source/docdb"
	"go.mongodb.org/mongo-driver/bson"
)

// ─── Shared constants (must match reader) ───────────────────────────────────
const (
	namespace      = "nudge_inventory"
	correlationKey = "pushpull_demo_session_001"
	bandCount      = 16
)

type NudgeInventoryPayload struct {
	NudgeMasterID string    `json:"nudge_master_id" bson:"nudge_master_id"`
	Channel       string    `json:"channel" bson:"channel"`
	Priority      int       `json:"priority" bson:"priority"`
	CampaignID    string    `json:"campaign_id" bson:"campaign_id"`
	ExpiresAt     time.Time `json:"expires_at" bson:"expires_at"`
	LastUpdated   time.Time `json:"last_updated" bson:"last_updated"`
}

func makePayload(priority int) []byte {
	p, _ := json.Marshal(NudgeInventoryPayload{
		NudgeMasterID: "nm_welcome_bonus",
		Channel:       "push",
		Priority:      priority,
		CampaignID:    "camp_pushpull_demo",
		ExpiresAt:     time.Now().Add(48 * time.Hour),
		LastUpdated:   time.Now().UTC(),
	})
	return p
}

// ─── Contracts ──────────────────────────────────────────────────────────────
func nudgeWriteContract(ck string, raw []byte) (*sluice.WriteModel, error) {
	var p NudgeInventoryPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("write contract: %w", err)
	}
	return &sluice.WriteModel{
		Filter: bson.D{{Key: "_id", Value: ck}},
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

func nudgeReadContract(ck string) (*source.ReadModel, error) {
	return &source.ReadModel{Filter: bson.M{"_id": ck}}, nil
}

func nudgeIndexContract(ck string, payload []byte) (map[string]interface{}, error) {
	var p NudgeInventoryPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("index contract: %w", err)
	}
	return map[string]interface{}{
		"channel":  p.Channel,
		"campaign": p.CampaignID,
		"priority": float64(p.Priority),
		"expires":  float64(p.ExpiresAt.UnixMilli()),
	}, nil
}

// ─── Metrics ────────────────────────────────────────────────────────────────
type logMetrics struct{ log *slog.Logger }

func (m *logMetrics) RecordWrite(string) {}
func (m *logMetrics) RecordDegradedWrite(ns string, err error) {
	m.log.Warn("degraded write", "ns", ns, "err", err)
}
func (m *logMetrics) RecordRedisOp(ns, op string, d time.Duration, err error) {
	if err != nil {
		m.log.Error("redis op", "ns", ns, "op", op, "err", err)
	}
}
func (m *logMetrics) RecordFlush(ns, band string, b int, d time.Duration, err error) {
	m.log.Info("flush", "ns", ns, "band", band, "batch", b, "err", err)
}
func (m *logMetrics) RecordDirtyQueueDepth(ns, band string, depth int) {}
func (m *logMetrics) RecordContractError(ns, ck string, err error) {
	m.log.Error("contract error", "ns", ns, "ck", ck, "err", err)
}
func (m *logMetrics) RecordDeadLetter(ns, band string, count int) {
	m.log.Warn("dead-letter", "ns", ns, "band", band, "count", count)
}
func (m *logMetrics) RecordDLQProcess(ns, s string, p, ok, f int)                {}
func (m *logMetrics) RecordWarmUp(ns string, d time.Duration, err error)         {}
func (m *logMetrics) RecordRead(ns string, d time.Duration, hot bool, err error) {}
func (m *logMetrics) RecordHotSetSize(ns string, size int)                       {}
func (m *logMetrics) RecordLocalCacheHit(ns string)                              { m.log.Info("local-cache-hit", "ns", ns) }
func (m *logMetrics) RecordLocalCacheMiss(ns string, r localjournal.MissReason) {
	m.log.Info("local-cache-miss", "ns", ns, "reason", r)
}
func (m *logMetrics) RecordLocalSetSize(ns string, size int) {}
func (m *logMetrics) RecordBroadcastLag(ns string, lag time.Duration) {
	m.log.Info("broadcast-lag", "ns", ns, "lag_ms", lag.Milliseconds())
}

// ─── Main ───────────────────────────────────────────────────────────────────
func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	mongoURI := getEnv("MONGO_URI", "mongodb://localhost:27017")

	sk, err := docdb.New(ctx, docdb.Config{
		URI: mongoURI, Database: "adroll", Collection: "nudge_inventory",
		MaxPoolSize: 50, MinPoolSize: 5,
	})
	if err != nil {
		return fmt.Errorf("connect MongoDB: %w", err)
	}
	src := sourcedocdb.NewSourceWithClient(sk.Client(), "adroll", "nudge_inventory")

	redisAddrs := splitAddrs(getEnv("REDIS_ADDRS", "localhost:7001,localhost:7002,localhost:7003,localhost:7004"))
	clusterMode, _ := strconv.ParseBool(getEnv("REDIS_CLUSTER_MODE", "true"))

	sl, err := sluice.New(namespace).
		WithRedis(sluice.RedisConfig{
			Addrs: redisAddrs, ClusterMode: clusterMode, PoolSize: 30,
			DialTimeout: 5 * time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second,
		}).
		WithSink(sk).
		WithWriteContract(nudgeWriteContract).
		WithSource(src).
		WithReadContract(nudgeReadContract).
		WithIndexContract(nudgeIndexContract).
		WithFlushWindow(250 * time.Millisecond).
		WithMaxBatchSize(1000).
		WithBandCount(bandCount).
		WithKeyTTL(30 * time.Second).
		WithActivityWindow(4 * time.Hour).
		WithHotAwareFlush(true).
		WithContentDedup(true).
		WithDegradedModeDirect(true).
		WithMetrics(&logMetrics{log: log}).
		WithLocalCache(localjournal.LocalCacheConfig{
			Mode:       localjournal.LocalCachePushPull,
			MaxEntries: 200_000,          // 256 shards × ~782 entries each
			LocalTTL:   60 * time.Second, // max staleness bound
			Broadcast:  shield.BroadcastPayload,
			Retention:  200_000, // stream MAXLEN ~ N
		}).
		Build(ctx)
	if err != nil {
		return fmt.Errorf("build sluice: %w", err)
	}
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = sl.DrainAndClose(shutCtx)
		log.Info("writer drained and closed")
	}()

	log.Info("═══ WRITER (Pod A) ready ═══", "namespace", namespace, "key", correlationKey)

	// Give the reader a moment to start and subscribe.
	initialWait, _ := time.ParseDuration(getEnv("INITIAL_WAIT", "3s"))
	log.Info("waiting for reader to start", "duration", initialWait)
	select {
	case <-ctx.Done():
		return nil
	case <-time.After(initialWait):
	}

	// Write a sequence of values; the reader should converge to each.
	sequence := []struct {
		priority int
		hold     time.Duration
	}{
		{5, 8 * time.Second},
		{1, 8 * time.Second},
		{9, 4 * time.Second},
	}

	for i, step := range sequence {
		payload := makePayload(step.priority)

		if err := sl.Write(ctx, correlationKey, payload); err != nil {
			return fmt.Errorf("write priority=%d: %w", step.priority, err)
		}
		log.Info("▶▶▶ WROTE", "step", i+1, "priority", step.priority)

		// Same-pod read-your-own-write: must be an L1 hit with the new value.
		readStart := time.Now()
		got, err := sl.Read(ctx, correlationKey)
		if err != nil {
			return fmt.Errorf("read-after-write priority=%d: %w", step.priority, err)
		}
		var r NudgeInventoryPayload
		_ = json.Unmarshal(got, &r)
		log.Info("same-pod read-after-write",
			"priority", r.Priority,
			"latency_us", time.Since(readStart).Microseconds(),
			"match", r.Priority == step.priority)

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(step.hold):
		}
	}

	log.Info("═══ WRITER complete ═══")
	return nil
}

func splitAddrs(raw string) []string {
	parts := strings.Split(raw, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
