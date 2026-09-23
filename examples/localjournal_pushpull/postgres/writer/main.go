// Command writer is "Pod A" in the Local Journal PushPull demo for PostgreSQL.
//
// It writes a sequence of payloads for a single correlation key and verifies
// same-pod read-your-own-write on each. A separate "reader" process (Pod B)
// observes cross-pod convergence via the broadcast stream.
//
// Run the reader first, then this writer:
//
//	# Terminal 1
//	go run ./examples/localjournal_pushpull_postgres/reader.go
//
//	# Terminal 2
//	go run ./examples/localjournal_pushpull_postgres/writer.go
package main

import (
	"context"
	"encoding/json"
	"errors"
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
	pgsink "github.com/hussainpithawala/sluice-go/sink/postgres"
	"github.com/hussainpithawala/sluice-go/source"
	pgsource "github.com/hussainpithawala/sluice-go/source/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── Shared constants (must match reader) ───────────────────────────────────
const (
	namespace      = "nudge_inventory_postgres"
	correlationKey = "pushpull_demo_session_001"
	bandCount      = 16
	tableName      = "nudge_inventory_pushpull"
)

type NudgeInventoryPayload struct {
	NudgeMasterID string    `json:"nudge_master_id"`
	Channel       string    `json:"channel"`
	Priority      int       `json:"priority"`
	CampaignID    string    `json:"campaign_id"`
	ExpiresAt     time.Time `json:"expires_at"`
	LastUpdated   time.Time `json:"last_updated"`
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
		Filter: "id",
		Update: map[string]any{
			"id":              ck,
			"nudge_master_id": p.NudgeMasterID,
			"channel":         p.Channel,
			"priority":        p.Priority,
			"campaign_id":     p.CampaignID,
			"expires_at":      p.ExpiresAt,
			"last_updated":    p.LastUpdated,
		},
		Upsert: true,
	}, nil
}

func nudgeReadContract(ck string) (*source.ReadModel, error) {
	return &source.ReadModel{
		Filter: pgsource.QueryParams{
			Query: `
				SELECT json_build_object(
					'nudge_master_id', nudge_master_id, 'channel', channel,
					'priority', priority, 'campaign_id', campaign_id,
					'expires_at', expires_at, 'last_updated', last_updated
				) FROM nudge_inventory_pushpull WHERE id = $1
			`,
			Args: []any{ck},
			Projector: func(row pgx.Row) ([]byte, error) {
				var payload []byte
				if err := row.Scan(&payload); err != nil {
					if errors.Is(err, pgx.ErrNoRows) {
						return nil, source.ErrRecordNotFound
					}
					return nil, err
				}
				return payload, nil
			},
		},
	}, nil
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
func (m *logMetrics) RecordDeadLetter(ns, band string, count int)                {}
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

	postgresURI := getEnv("POSTGRES_URI", "postgres://sluice:sluice@localhost:5432/sluice_test?sslmode=disable")

	sk, err := pgsink.New(ctx, pgsink.Config{ConnString: postgresURI, TableName: tableName, MaxConns: 20, MinConns: 5})
	if err != nil {
		return fmt.Errorf("connect PostgreSQL: %w", err)
	}
	if err := ensureTableExists(ctx, sk.Pool(), tableName, log); err != nil {
		return fmt.Errorf("ensure table: %w", err)
	}

	src := pgsource.NewSourceWithPool(sk.Pool())
	redisAddrs := splitAddrs(getEnv("REDIS_ADDRS", "localhost:7001,localhost:7002,localhost:7003,localhost:7004"))
	clusterMode, _ := strconv.ParseBool(getEnv("REDIS_CLUSTER_MODE", "true"))

	sl, err := sluice.New(namespace).
		WithRedis(sluice.RedisConfig{
			Addrs: redisAddrs, ClusterMode: clusterMode, PoolSize: 30,
			DialTimeout: 5 * time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second,
		}).
		WithSink(sk).
		WithSource(src).
		WithWriteContract(nudgeWriteContract).
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
			Mode: localjournal.LocalCachePushPull, MaxEntries: 200_000, LocalTTL: 60 * time.Second,
			Broadcast: shield.BroadcastPayload, Retention: 200_000,
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

	initialWait, _ := time.ParseDuration(getEnv("INITIAL_WAIT", "3s"))
	log.Info("waiting for reader to start", "duration", initialWait)
	select {
	case <-ctx.Done():
		return nil
	case <-time.After(initialWait):
	}

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

func ensureTableExists(ctx context.Context, pool *pgxpool.Pool, tableName string, log *slog.Logger) error {
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id TEXT PRIMARY KEY, nudge_master_id TEXT NOT NULL, channel TEXT NOT NULL,
			priority INTEGER NOT NULL, campaign_id TEXT NOT NULL,
			expires_at TIMESTAMPTZ NOT NULL, last_updated TIMESTAMPTZ NOT NULL
		)`, tableName))
	if err != nil {
		return fmt.Errorf("create table: %w", err)
	}
	log.Info("ensured table exists", "table", tableName)
	return nil
}
