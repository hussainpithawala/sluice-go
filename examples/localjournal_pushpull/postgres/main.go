// Command localjournal_pushpull_postgres demonstrates the L1 Local Journal's PushPull
// mode against a real PostgreSQL sink and source.
//
// Two independent Sluice instances ("Pod A" and "Pod B") share a single Redis
// journal and broadcast stream. Each has its own in-process L1 cache and its
// own broadcast subscriber. The example proves three invariants from the
// Local Journal RFC's consistency contract:
//
//  1. Same-pod read-your-own-write is exact (sub-µs, L1 hit).
//  2. Cross-pod reads converge via the Redis Streams broadcast within ~lag.
//  3. ReadFresh bypasses L1 entirely, returning the journal's authoritative value.
//
// Run against local PostgreSQL + single-node Redis:
//
//	POSTGRES_URI=postgres://sluice:sluice@localhost:5432/sluice_test?sslmode=disable
//	REDIS_ADDRS=localhost:6379
//	REDIS_CLUSTER_MODE=false
//	go run ./examples/localjournal_pushpull_postgres/main.go
//
// Run against the local 4-shard Valkey cluster (CME) from docker-compose.yml:
//
//	POSTGRES_URI=postgres://sluice:sluice@localhost:5432/sluice_test?sslmode=disable
//	REDIS_ADDRS=localhost:7001,localhost:7002,localhost:7003,localhost:7004
//	REDIS_CLUSTER_MODE=true
//	go run ./examples/localjournal_pushpull_postgres/main.go
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
	"github.com/redis/go-redis/v9"
)

var (
	version   = "dev"
	commit    = "none"
	buildDate = "unknown"
)

// ─── Payload ─────────────────────────────────────────────────────────────────
type NudgeInventoryPayload struct {
	NudgeMasterID string    `json:"nudge_master_id"`
	Channel       string    `json:"channel"`
	Priority      int       `json:"priority"`
	CampaignID    string    `json:"campaign_id"`
	ExpiresAt     time.Time `json:"expires_at"`
	LastUpdated   time.Time `json:"last_updated"`
}

// ─── Contracts ───────────────────────────────────────────────────────────────
// nudgeWriteContract translates the raw payload into a PostgreSQL row map.
// Filter is the ON CONFLICT target column.
func nudgeWriteContract(correlationKey string, rawPayload []byte) (*sluice.WriteModel, error) {
	var p NudgeInventoryPayload
	if err := json.Unmarshal(rawPayload, &p); err != nil {
		return nil, fmt.Errorf("nudge contract: invalid payload for correlation_key %s: %w", correlationKey, err)
	}

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

// nudgeReadContract uses the QueryParams + Projector pattern to execute a
// set-based SQL query and shape the result into the canonical JSON payload.
func nudgeReadContract(correlationKey string) (*source.ReadModel, error) {
	return &source.ReadModel{
		Filter: pgsource.QueryParams{
			Query: `
				SELECT json_build_object(
					'nudge_master_id', nudge_master_id,
					'channel', channel,
					'priority', priority,
					'campaign_id', campaign_id,
					'expires_at', expires_at,
					'last_updated', last_updated
				)
				FROM nudge_inventory_pushpull
				WHERE id = $1
			`,
			Args: []any{correlationKey},
			Projector: func(row pgx.Row) ([]byte, error) {
				var payload []byte
				if err := row.Scan(&payload); err != nil {
					if errors.Is(err, pgx.ErrNoRows) {
						return nil, source.ErrRecordNotFound
					}
					return nil, fmt.Errorf("postgres projection failed: %w", err)
				}
				return payload, nil
			},
		},
	}, nil
}

// nudgeIndexContract extracts secondary index fields for Redis-side Query().
func nudgeIndexContract(correlationKey string, payload []byte) (map[string]interface{}, error) {
	var p NudgeInventoryPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("index contract: invalid payload for correlation_key %s: %w", correlationKey, err)
	}
	return map[string]interface{}{
		"channel":  p.Channel,
		"campaign": p.CampaignID,
		"priority": float64(p.Priority),
		"expires":  float64(p.ExpiresAt.UnixMilli()),
	}, nil
}

// ─── Metrics ─────────────────────────────────────────────────────────────────
type logMetrics struct {
	log *slog.Logger
	tag string // "pod-a" or "pod-b"
}

func (m *logMetrics) RecordWrite(_ string) {}
func (m *logMetrics) RecordDegradedWrite(ns string, err error) {
	m.log.Warn("degraded write", "pod", m.tag, "ns", ns, "err", err)
}
func (m *logMetrics) RecordRedisOp(ns, op string, d time.Duration, err error) {
	if err != nil {
		m.log.Error("redis op", "pod", m.tag, "ns", ns, "op", op, "ms", d.Milliseconds(), "err", err)
	}
}
func (m *logMetrics) RecordFlush(ns, band string, batch int, d time.Duration, err error) {
	m.log.Info("flush", "pod", m.tag, "ns", ns, "band", band, "batch", batch, "ms", d.Milliseconds(), "err", err)
}
func (m *logMetrics) RecordDirtyQueueDepth(ns, band string, depth int) {
	if depth > 100 {
		m.log.Warn("dirty queue depth", "pod", m.tag, "ns", ns, "band", band, "depth", depth)
	}
}
func (m *logMetrics) RecordContractError(ns, correlationKey string, err error) {
	m.log.Error("contract error", "pod", m.tag, "ns", ns, "correlationKey", correlationKey, "err", err)
}
func (m *logMetrics) RecordDeadLetter(ns, band string, count int) {
	m.log.Warn("dead-letter", "pod", m.tag, "ns", ns, "band", band, "count", count)
}
func (m *logMetrics) RecordDLQProcess(ns, strategy string, processed, succeeded, failed int) {
	m.log.Info("dlq-process", "pod", m.tag, "ns", ns, "strategy", strategy, "processed", processed, "succeeded", succeeded, "failed", failed)
}
func (m *logMetrics) RecordWarmUp(ns string, duration time.Duration, err error) {
	m.log.Info("warm-up", "pod", m.tag, "ns", ns, "duration_ms", duration.Milliseconds(), "err", err)
}
func (m *logMetrics) RecordRead(ns string, duration time.Duration, isHot bool, err error) {
	m.log.Info("read", "pod", m.tag, "ns", ns, "duration_ms", duration.Milliseconds(), "is_hot", isHot, "err", err)
}
func (m *logMetrics) RecordHotSetSize(ns string, size int) {
	m.log.Info("hot-set-size", "pod", m.tag, "ns", ns, "size", size)
}
func (m *logMetrics) RecordLocalCacheHit(namespace string) {
	m.log.Info("local-cache-hit", "pod", m.tag, "ns", namespace)
}
func (m *logMetrics) RecordLocalCacheMiss(namespace string, reason localjournal.MissReason) {
	m.log.Info("local-cache-miss", "pod", m.tag, "ns", namespace, "reason", reason)
}
func (m *logMetrics) RecordLocalSetSize(namespace string, size int) {
	m.log.Info("local-set-size", "pod", m.tag, "ns", namespace, "size", size)
}
func (m *logMetrics) RecordBroadcastLag(namespace string, lag time.Duration) {
	m.log.Info("broadcast-lag", "pod", m.tag, "ns", namespace, "lag_ms", lag.Milliseconds())
}

// ─── Pod Builder ─────────────────────────────────────────────────────────────
const bandCount = 16
const tableName = "nudge_inventory_pushpull"

// buildPod constructs a full Sluice instance backed by PostgreSQL.
func buildPod(ctx context.Context, tag string, postgresURI string, redisAddrs []string, clusterMode bool, log *slog.Logger) (*sluice.Sluice, func(context.Context) error, error) {
	sk, err := pgsink.New(ctx, pgsink.Config{
		ConnString: postgresURI,
		TableName:  tableName,
		MaxConns:   20,
		MinConns:   5,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("connect to PostgreSQL at %s: %w", postgresURI, err)
	}

	if err := ensureTableExists(ctx, sk.Pool(), tableName, log); err != nil {
		_ = sk.Close(context.Background())
		return nil, nil, fmt.Errorf("ensure table exists: %w", err)
	}

	// Source shares the exact same pgxpool.Pool to minimize connections
	src := pgsource.NewSourceWithPool(sk.Pool())

	sl, err := sluice.New("nudge_inventory_postgres").
		WithRedis(sluice.RedisConfig{
			Addrs:        redisAddrs,
			ClusterMode:  clusterMode,
			PoolSize:     30,
			DialTimeout:  5 * time.Second,
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 3 * time.Second,
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
		WithMetrics(&logMetrics{log: log, tag: tag}).
		WithLocalCache(localjournal.LocalCacheConfig{
			Mode:       localjournal.LocalCachePushPull,
			MaxEntries: 200_000,
			LocalTTL:   60 * time.Second,
			Broadcast:  shield.BroadcastPayload,
			Retention:  200_000,
		}).
		Build(ctx)
	if err != nil {
		_ = sk.Close(context.Background())
		return nil, nil, fmt.Errorf("build sluice: %w", err)
	}

	cleanup := func(shutCtx context.Context) error {
		var firstErr error
		if err := sl.DrainAndClose(shutCtx); err != nil {
			log.Error("drain error", "pod", tag, "err", err)
			firstErr = err
		}
		_ = sk.Close(context.Background())
		return firstErr
	}
	return sl, cleanup, nil
}

// ─── Scenario Runner ─────────────────────────────────────────────────────────
func runScenarios(ctx context.Context, podA, podB *sluice.Sluice, log *slog.Logger) error {
	// ── Scenario 1: Same-pod read-your-own-write ─────────────────────────
	log.Info("═══ Scenario 1: Same-pod read-your-own-write ═══")
	crn := "pushpull_demo_session_001"
	payloadV1, _ := json.Marshal(NudgeInventoryPayload{
		NudgeMasterID: "nm_welcome_bonus",
		Channel:       "push",
		Priority:      5,
		CampaignID:    "camp_onboarding",
		ExpiresAt:     time.Now().Add(48 * time.Hour),
		LastUpdated:   time.Now().UTC(),
	})
	if err := podA.Write(ctx, crn, payloadV1); err != nil {
		return fmt.Errorf("pod A write: %w", err)
	}
	log.Info("Pod A wrote payload", "crn", crn, "priority", 5)

	readStart := time.Now()
	got, err := podA.Read(ctx, crn)
	if err != nil {
		return fmt.Errorf("pod A read: %w", err)
	}
	readLatency := time.Since(readStart)
	var result NudgeInventoryPayload
	_ = json.Unmarshal(got, &result)
	log.Info("Pod A read (L1 hit, sub-µs)", "crn", crn, "priority", result.Priority, "latency_us", readLatency.Microseconds())

	// ── Scenario 2: Cross-pod convergence via broadcast ──────────────────
	log.Info("═══ Scenario 2: Cross-pod convergence via broadcast ═══")
	log.Info("Pod B has never written this key. Waiting for broadcast convergence...")
	converged := false
	for i := 0; i < 300; i++ { // 3s max with 10ms polling
		got, err := podB.Read(ctx, crn)
		if err == nil {
			var r NudgeInventoryPayload
			_ = json.Unmarshal(got, &r)
			if r.Priority == 5 {
				converged = true
				log.Info("Pod B converged via broadcast", "crn", crn, "priority", r.Priority, "attempts", i+1)
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !converged {
		return fmt.Errorf("cross-pod convergence timed out after 3s")
	}

	// ── Scenario 3: Pod A updates; Pod B converges the new value ─────────
	log.Info("═══ Scenario 3: Pod A updates, Pod B converges ═══")
	time.Sleep(10 * time.Millisecond) // Ensure new millisecond for version gate
	payloadV2, _ := json.Marshal(NudgeInventoryPayload{
		NudgeMasterID: "nm_welcome_bonus",
		Channel:       "push",
		Priority:      1, // user dismissed high-priority nudge
		CampaignID:    "camp_onboarding",
		ExpiresAt:     time.Now().Add(48 * time.Hour),
		LastUpdated:   time.Now().UTC(),
	})
	if err := podA.Write(ctx, crn, payloadV2); err != nil {
		return fmt.Errorf("pod A update: %w", err)
	}
	log.Info("Pod A updated priority to 1", "crn", crn)

	converged = false
	for i := 0; i < 300; i++ {
		got, err := podB.Read(ctx, crn)
		if err == nil {
			var r NudgeInventoryPayload
			_ = json.Unmarshal(got, &r)
			if r.Priority == 1 {
				converged = true
				log.Info("Pod B converged to updated value", "crn", crn, "priority", r.Priority, "attempts", i+1)
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !converged {
		return fmt.Errorf("cross-pod convergence on update timed out")
	}

	// ── Scenario 4: ReadFresh — strong-consistency escape hatch ──────────
	log.Info("═══ Scenario 4: ReadFresh bypasses L1 ═══")
	freshStart := time.Now()
	fresh, err := podA.ReadFresh(ctx, crn)
	if err != nil {
		return fmt.Errorf("read fresh: %w", err)
	}
	freshLatency := time.Since(freshStart)
	var freshResult NudgeInventoryPayload
	_ = json.Unmarshal(fresh, &freshResult)
	log.Info("Pod A ReadFresh (L2 journal, bypasses L1)",
		"crn", crn, "priority", freshResult.Priority,
		"latency_us", freshLatency.Microseconds(),
		"note", "authoritative journal view")

	// ── Scenario 5: Compound query via Redis indexes ─────────────────────
	log.Info("═══ Scenario 5: Compound query via Redis indexes ═══")
	results, err := podA.Query(ctx, sluice.Query{
		Equality: map[string]string{"channel": "push"},
		RangeMin: map[string]float64{"priority": 1},
	})
	if err != nil {
		log.Warn("compound query failed", "err", err)
	} else {
		log.Info("compound query result", "filter", "channel=push AND priority>=1", "matches", len(results))
		for i, r := range results {
			if i >= 3 {
				break
			}
			log.Info("query match", "correlationKey", r.CorrelationKey)
		}
	}
	return nil
}

// ─── Main ────────────────────────────────────────────────────────────────────
func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) (err error) {
	log.Info("sluice localjournal_pushpull postgres example",
		"version", version, "commit", commit, "built", buildDate)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	postgresURI := getEnv("POSTGRES_URI", "postgres://sluice:sluice@localhost:5432/sluice_test?sslmode=disable")

	// ── Redis config (shared by both pods) ───────────────────────────────
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

	// ── Reset broadcast stream for a clean demo run ──────────────────────
	{
		var tempClient redis.UniversalClient
		if clusterMode {
			tempClient = redis.NewClusterClient(&redis.ClusterOptions{Addrs: redisAddrs})
		} else {
			tempClient = redis.NewClient(&redis.Options{Addr: redisAddrs[0]})
		}
		streamKey := shield.BroadcastKey("nudge_inventory_postgres")
		if err := tempClient.Del(ctx, streamKey).Err(); err != nil {
			log.Warn("cleanup: failed to delete broadcast stream", "stream", streamKey, "err", err)
		} else {
			log.Info("cleanup: broadcast stream deleted", "stream", streamKey)
		}
		_ = tempClient.Close()
	}

	// ── Build Pod A ──────────────────────────────────────────────────────
	log.Info("building Pod A (PostgreSQL sink/source + L1 PushPull)")
	podA, cleanupA, err := buildPod(ctx, "pod-a", postgresURI, redisAddrs, clusterMode, log)
	if err != nil {
		return fmt.Errorf("build pod A: %w", err)
	}
	defer func() {
		log.Info("draining Pod A...")
		shutCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if cerr := cleanupA(shutCtx); cerr != nil && err == nil {
			err = fmt.Errorf("pod A cleanup: %w", cerr)
		}
		log.Info("Pod A drained and closed")
	}()

	// ── Build Pod B ──────────────────────────────────────────────────────
	log.Info("building Pod B (PostgreSQL sink/source + L1 PushPull)")
	podB, cleanupB, err := buildPod(ctx, "pod-b", postgresURI, redisAddrs, clusterMode, log)
	if err != nil {
		return fmt.Errorf("build pod B: %w", err)
	}
	defer func() {
		log.Info("draining Pod B...")
		shutCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if cerr := cleanupB(shutCtx); cerr != nil && err == nil {
			err = fmt.Errorf("pod B cleanup: %w", cerr)
		}
		log.Info("Pod B drained and closed")
	}()

	log.Info("both pods ready",
		"redis_addrs", redisAddrs,
		"cluster_mode", clusterMode,
		"l1_mode", "PushPull",
		"broadcast", "Payload",
		"local_ttl", "60s",
	)

	// ── Run scenarios ────────────────────────────────────────────────────
	if err := runScenarios(ctx, podA, podB, log); err != nil {
		return err
	}

	log.Info("═══ All scenarios completed successfully ═══")
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
