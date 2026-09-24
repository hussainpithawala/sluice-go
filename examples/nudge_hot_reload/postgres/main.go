// Package main demonstrates a production-grade nudge inventory consumer
// using sluice as the write buffer between Kafka/SQS and PostgreSQL.
//
// It exercises BOTH regimes:
//   - Cold regime: high-velocity bulk writes via Write() → time-drain flush.
//   - Hot regime:  user-login HotLoad() → sub-ms Read() → sync HTTP responses.
//
// It also demonstrates IndexContract (secondary indexes) and Query() (compound lookups).
//
// Run against local PostgreSQL + Redis:
//
//	POSTGRES_URI=postgres://sluice:sluice@localhost:5432/sluice_test?sslmode=disable
//	REDIS_ADDRS=localhost:6379
//	go run ./examples/nudge_hot_reload_postgres/main.go
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
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	sluice "github.com/hussainpithawala/sluice-go"
	"github.com/hussainpithawala/sluice-go/internal/localjournal"
	pgsink "github.com/hussainpithawala/sluice-go/sink/postgres"
	"github.com/hussainpithawala/sluice-go/source"
	pgsource "github.com/hussainpithawala/sluice-go/source/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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

// ─── WriteContract ───────────────────────────────────────────────────────────
// PostgreSQL requires the full row map for an upsert.
// Filter is repurposed as the ON CONFLICT target column.
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

// ─── ReadContract ────────────────────────────────────────────────────────────
// Demonstrates the QueryParams + Projector pattern.
// Uses Postgres-native json_build_object to return a single, fully-formed
// JSON payload directly from the database, which the Projector simply scans.
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
				FROM nudge_inventory_hot_cold
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

// ─── IndexContract ───────────────────────────────────────────────────────────
func nudgeIndexContract(correlationKey string, payload []byte) (map[string]interface{}, error) {
	var p NudgeInventoryPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("index contract: invalid payload for correlation_key %s: %w", correlationKey, err)
	}
	return map[string]interface{}{
		"channel":  p.Channel,                        // equality: SET
		"campaign": p.CampaignID,                     // equality: SET
		"priority": float64(p.Priority),              // range:    ZSET
		"expires":  float64(p.ExpiresAt.UnixMilli()), // range:    ZSET
	}, nil
}

// ─── Metrics ─────────────────────────────────────────────────────────────────
type logMetrics struct{ log *slog.Logger }

func (m *logMetrics) RecordBroadcastLag(namespace string, lag time.Duration) {
	m.log.Info("broadcast-lag", "ns", namespace, "lag", lag)
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
func (m *logMetrics) RecordWarmUp(ns string, duration time.Duration, err error) {
	m.log.Info("warm-up", "ns", ns, "duration_ms", duration.Milliseconds(), "err", err)
}
func (m *logMetrics) RecordRead(ns string, duration time.Duration, isHot bool, err error) {
	m.log.Info("read", "ns", ns, "duration_ms", duration.Milliseconds(), "is_hot", isHot, "err", err)
}
func (m *logMetrics) RecordHotSetSize(ns string, size int) {
	m.log.Info("hot-set-size", "ns", ns, "size", size)
}
func (m *logMetrics) RecordLocalCacheHit(namespace string)                                  {}
func (m *logMetrics) RecordLocalCacheMiss(namespace string, reason localjournal.MissReason) {}
func (m *logMetrics) RecordLocalSetSize(namespace string, size int)                         {}

// ─── Cold Regime: Simulated Bulk Consumer ────────────────────────────────────
// Kept at a sustainable functional load (~80 events/sec) to prevent local Postgres timeouts.
func simulatedConsumer(ctx context.Context, workerID int, sl *sluice.Sluice, log *slog.Logger, written *atomic.Int64, wg *sync.WaitGroup) {
	defer wg.Done()
	nudgeMasters := []string{"nm_spring_retarget_2026", "nm_cart_abandonment", "nm_win_back_q2", "nm_first_purchase", "nm_loyalty_upgrade"}
	channels := []string{"push", "email", "sms", "in_app"}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for burst := 0; burst < 2; burst++ {
				seq := written.Add(1)
				correlationKey := fmt.Sprintf("correlationKey_%09d", (workerID*1_000_000)+int(seq%2_000_000))
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

// ─── Hot Regime: Simulated User Sessions ─────────────────────────────────────
func hotRegimeSimulator(ctx context.Context, sl *sluice.Sluice, log *slog.Logger, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	sessionCount := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sessionCount++
			correlationKey := fmt.Sprintf("correlationKey_hot_%06d", sessionCount)
			log.Info("── hot regime: simulating user login ──", "correlationKey", correlationKey, "session", sessionCount)

			isHot, err := sl.IsHot(ctx, correlationKey)
			if err != nil {
				log.Error("hot regime: IsHot failed", "correlationKey", correlationKey, "err", err)
				continue
			}
			log.Info("hot regime: pre-login state", "correlationKey", correlationKey, "is_hot", isHot)

			if err := sl.HotLoad(ctx, correlationKey); err != nil {
				log.Error("hotload failed", "err", err)
			}
			// If you need the payload immediately, use Read() instead:
			payload, err := sl.Read(ctx, correlationKey)

			if err != nil {
				log.Info("hot regime: HotLoad (new correlation_key, writing directly)", "correlationKey", correlationKey, "err", err)
				newPayload, _ := json.Marshal(NudgeInventoryPayload{
					NudgeMasterID: "nm_welcome_bonus",
					Channel:       "push",
					Priority:      5,
					CampaignID:    "camp_onboarding",
					ExpiresAt:     time.Now().Add(48 * time.Hour),
					LastUpdated:   time.Now().UTC(),
				})
				if writeErr := sl.Write(ctx, correlationKey, newPayload); writeErr != nil {
					log.Error("hot regime: Write failed", "correlationKey", correlationKey, "err", writeErr)
				}
				payload = newPayload
			} else {
				log.Info("hot regime: HotLoad success (warmed from PostgreSQL)", "correlationKey", correlationKey, "payload_bytes", len(payload))
			}

			isHot, err = sl.IsHot(ctx, correlationKey)
			if err != nil {
				log.Error("hot regime: IsHot after load failed", "correlationKey", correlationKey, "err", err)
				continue
			}
			log.Info("hot regime: post-login state", "correlationKey", correlationKey, "is_hot", isHot)

			var current NudgeInventoryPayload
			_ = json.Unmarshal(payload, &current)
			current.Priority = 1
			current.LastUpdated = time.Now().UTC()
			updatedPayload, _ := json.Marshal(current)
			if err := sl.Write(ctx, correlationKey, updatedPayload); err != nil {
				log.Error("hot regime: user action Write failed", "correlationKey", correlationKey, "err", err)
				continue
			}

			readStart := time.Now()
			readPayload, err := sl.Read(ctx, correlationKey)
			readLatency := time.Since(readStart)
			if err != nil {
				log.Error("hot regime: Read failed", "correlationKey", correlationKey, "err", err)
				continue
			}
			var readResult NudgeInventoryPayload
			_ = json.Unmarshal(readPayload, &readResult)
			log.Info("hot regime: sync HTTP response served from journal",
				"correlationKey", correlationKey,
				"read_latency_us", readLatency.Microseconds(),
				"priority", readResult.Priority,
				"channel", readResult.Channel,
			)

			if sessionCount%3 == 0 {
				results, queryErr := sl.Query(ctx, sluice.Query{
					Equality: map[string]string{"channel": "push"},
					RangeMin: map[string]float64{"priority": 3},
				})
				if queryErr != nil {
					log.Warn("hot regime: Query failed", "err", queryErr)
				} else {
					log.Info("hot regime: compound query result",
						"filter", "channel=push AND priority>=3",
						"matches", len(results),
					)
					for i, r := range results {
						if i >= 3 {
							break
						}
						log.Info("hot regime: query match", "correlationKey", r.CorrelationKey, "payload_bytes", len(r.Payload))
					}
				}
			}
		}
	}
}

// ─── Main ────────────────────────────────────────────────────────────────────
const bandCount = 16

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) (err error) {
	log.Info("sluice postgres hot/cold example", "version", version, "commit", commit, "built", buildDate)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	postgresURI := getEnv("POSTGRES_URI", "postgres://sluice:sluice@localhost:5432/sluice_test?sslmode=disable")
	tableName := "nudge_inventory_hot_cold"

	// ── Initialize PostgreSQL Sink ───────────────────────────────────────
	sk, err := pgsink.New(ctx, pgsink.Config{
		ConnString: postgresURI,
		TableName:  tableName,
		MaxConns:   20,
		MinConns:   5,
	})
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}

	// ── Ensure Table Exists (operator's responsibility in production) ────
	if err := ensureTableExists(ctx, sk.Pool(), tableName, log); err != nil {
		return fmt.Errorf("ensure table exists: %w", err)
	}

	// ── Initialize PostgreSQL Source (shares connection pool) ────────────
	src := pgsource.NewSourceWithPool(sk.Pool())

	// ── Redis config ─────────────────────────────────────────────────────
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

	// ── Build Sluice ─────────────────────────────────────────────────────
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
		_ = sk.Close(closeCtx)
		return fmt.Errorf("build sluice: %w", err)
	}

	log.Info("sluice ready", "sink", "PostgreSQL", "source", "PostgreSQL (shared pool)")

	defer func() {
		log.Info("draining sluice...")
		shutCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if derr := sl.DrainAndClose(shutCtx); derr != nil {
			log.Error("drain error", "err", derr)
			if err == nil {
				err = fmt.Errorf("drain and close: %w", derr)
			}
		}
		log.Info("sluice drained and closed")
	}()

	// ── Start Cold Regime ────────────────────────────────────────────────
	const workerCount = 4
	var wg sync.WaitGroup
	var written atomic.Int64

	log.Info("starting cold regime: consumer workers", "count", workerCount)
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go simulatedConsumer(ctx, i, sl, log, &written, &wg)
	}

	// ── Start Hot Regime ─────────────────────────────────────────────────
	log.Info("starting hot regime: user session simulator")
	wg.Add(1)
	go hotRegimeSimulator(ctx, sl, log, &wg)

	// ── Stats ────────────────────────────────────────────────────────────
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
				log.Info("throughput", "events", total, "rate", fmt.Sprintf("%.0f/s", float64(total)/elapsed))
			}
		}
	}()

	<-ctx.Done()
	log.Info("shutdown signal received")
	wg.Wait()

	total := written.Load()
	elapsed := time.Since(start)
	log.Info("final summary", "total_events", total, "elapsed", elapsed.Round(time.Second))
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
