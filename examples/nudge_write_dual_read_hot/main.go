// Package main demonstrates the full sluice-go data platform lifecycle:
//
//   - COLD WRITE: High-velocity stream ingest (Kafka/SQS) → Write() → time-drain flush
//   - HOT WRITE:  Synchronous API calls on active users → Write() → immediate flush signal
//   - READ:       Unified sub-ms read with lazy TTL refresh (hot) or Source fallback (cold)
//   - QUERY:      Compound lookups via band-scoped SINTER + ZRANGEBYSCORE
//
// The distinction between cold and hot writes is NOT a different API call.
// Both use sl.Write(). The difference is whether the CRN has been HotLoad()'d:
//
//   - Cold: Write() → dirty queue → flush after FlushWindow (250ms)
//   - Hot:  Write() → dirty queue → HotAwareFlush detects IsHot() → SignalVolume → flush NOW
//
// Run:
//
//	MONGO_URI=mongodb://localhost:27017 REDIS_ADDRS=localhost:6379
//	go run ./examples/nudge_hot_reload/main.go
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
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	sluice "github.com/hussainpithawala/sluice-go"
	"github.com/hussainpithawala/sluice-go/sink/docdb"
	"github.com/hussainpithawala/sluice-go/source"
	sourcedocdb "github.com/hussainpithawala/sluice-go/source/docdb"
	"go.mongodb.org/mongo-driver/bson"
)

var (
	version   = "dev"
	commit    = "none"
	buildDate = "unknown"
)

type NudgeInventoryPayload struct {
	NudgeMasterID string    `json:"nudge_master_id" bson:"nudge_master_id"`
	Channel       string    `json:"channel" bson:"channel"`
	Priority      int       `json:"priority" bson:"priority"`
	CampaignID    string    `json:"campaign_id" bson:"campaign_id"`
	ExpiresAt     time.Time `json:"expires_at" bson:"expires_at"`
	LastUpdated   time.Time `json:"last_updated" bson:"last_updated"`
}

// ─── WriteContract (shared by both cold and hot writes) ──────────────────────

func nudgeWriteContract(crn string, rawPayload []byte) (*sluice.WriteModel, error) {
	var p NudgeInventoryPayload
	if err := json.Unmarshal(rawPayload, &p); err != nil {
		return nil, fmt.Errorf("nudge contract: invalid payload for CRN %s: %w", crn, err)
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

// ─── ReadContract (unified read — used by both HotLoad and cold fallback) ────

func nudgeReadContract(crn string) (*source.ReadModel, error) {
	return &source.ReadModel{
		Filter: bson.D{{Key: "_id", Value: crn}},
	}, nil
}

// ─── IndexContract (enables Query() for compound lookups) ────────────────────

func nudgeIndexContract(crn string, payload []byte) (map[string]interface{}, error) {
	var p NudgeInventoryPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("index contract: invalid payload for CRN %s: %w", crn, err)
	}
	return map[string]interface{}{
		"channel":  p.Channel,                        // equality SET
		"campaign": p.CampaignID,                     // equality SET
		"priority": float64(p.Priority),              // range ZSET
		"expires":  float64(p.ExpiresAt.UnixMilli()), // range ZSET
	}, nil
}

// ─── Metrics ─────────────────────────────────────────────────────────────────

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

// ─── COLD WRITE PATH: Simulated Stream Consumer ──────────────────────────────
// These writes come from Kafka/SQS. The CRN is NOT hot.
// Write() → dirty queue → flush after FlushWindow (250ms) → DocumentDB.
// No immediate flush signal. No TTL extension. Pure velocity shielding.

func coldWriteSimulator(ctx context.Context, workerID int, sl *sluice.Sluice, log *slog.Logger, written *atomic.Int64, wg *sync.WaitGroup) {
	defer wg.Done()
	nudgeMasters := []string{"nm_spring_retarget", "nm_cart_abandonment", "nm_win_back_q2", "nm_first_purchase", "nm_loyalty_upgrade"}
	channels := []string{"push", "email", "sms", "in_app"}
	ticker := time.NewTicker(1 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for burst := 0; burst < 10; burst++ {
				seq := written.Add(1)
				crn := fmt.Sprintf("crn_cold_%09d", (workerID*1_000_000)+int(seq%2_000_000))
				payload, _ := json.Marshal(NudgeInventoryPayload{
					NudgeMasterID: nudgeMasters[seq%int64(len(nudgeMasters))],
					Channel:       channels[seq%int64(len(channels))],
					Priority:      int(seq%5) + 1,
					CampaignID:    fmt.Sprintf("camp_%04d", seq%100),
					ExpiresAt:     time.Now().Add(24 * time.Hour),
					LastUpdated:   time.Now().UTC(),
				})
				// COLD WRITE: sl.Write on a CRN that has NOT been HotLoad()'d.
				// Because HotAwareFlush=true, the engine checks IsHot(crn).
				// IsHot returns false → no immediate flush signal → time-drain only.
				if err := sl.Write(ctx, crn, payload); err != nil {
					log.Error("cold write error", "crn", crn, "err", err)
				}
			}
		}
	}
}

// ─── HOT WRITE PATH: Simulated Synchronous API ───────────────────────────────
// These writes come from sync HTTP handlers (e.g., user dismisses a nudge).
// The CRN IS hot because HotLoad() was called on login.
// Write() → dirty queue → HotAwareFlush detects IsHot() → SignalVolume → flush NOW.
// After flush, RefreshHotTTL extends the ActivityWindow.

func hotWriteSimulator(ctx context.Context, sl *sluice.Sluice, log *slog.Logger, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	sessionCount := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sessionCount++
			crn := fmt.Sprintf("crn_hot_%06d", sessionCount)
			log.Info("═══ HOT SESSION START ═══", "crn", crn, "session", sessionCount)

			// ─── Step 1: User logs in → HotLoad warms the journal ───────────
			// This is the ONLY difference between hot and cold.
			// HotLoad() writes the payload AND sets the hot marker.
			// After this, IsHot(crn) == true for ActivityWindow duration.
			payload, err := sl.HotLoad(ctx, crn)
			if err != nil {
				// New CRN not in DocumentDB yet — write directly to establish state.
				log.Info("hot session: new CRN, seeding via direct write", "crn", crn)
				payload, _ = json.Marshal(NudgeInventoryPayload{
					NudgeMasterID: "nm_welcome_bonus",
					Channel:       "push",
					Priority:      5,
					CampaignID:    "camp_onboarding",
					ExpiresAt:     time.Now().Add(48 * time.Hour),
					LastUpdated:   time.Now().UTC(),
				})
				if writeErr := sl.Write(ctx, crn, payload); writeErr != nil {
					log.Error("hot session: seed write failed", "crn", crn, "err", writeErr)
					continue
				}
			} else {
				log.Info("hot session: HotLoad success (warmed from Source)", "crn", crn, "bytes", len(payload))
			}

			// ─── Step 2: Verify CRN is hot ───────────────────────────────────
			isHot, _ := sl.IsHot(ctx, crn)
			log.Info("hot session: post-login state", "crn", crn, "is_hot", isHot)

			// ─── Step 3: HOT WRITE — user action via sync API ────────────────
			// This is the SAME sl.Write() call as the cold path.
			// But because IsHot(crn) == true AND HotAwareFlush == true,
			// the engine immediately signals a flush for this band.
			// Result: persistence latency drops from 250ms to <10ms.
			var current NudgeInventoryPayload
			_ = json.Unmarshal(payload, &current)
			current.Priority = 1 // user dismissed high-priority nudge
			current.LastUpdated = time.Now().UTC()
			hotPayload, _ := json.Marshal(current)

			writeStart := time.Now()
			if err := sl.Write(ctx, crn, hotPayload); err != nil {
				log.Error("hot session: write failed", "crn", crn, "err", err)
				continue
			}
			log.Info("hot session: HOT WRITE committed to journal",
				"crn", crn,
				"write_latency_us", time.Since(writeStart).Microseconds(),
				"note", "HotAwareFlush will trigger immediate band flush",
			)

			// ─── Step 4: Unified READ — sub-ms from Redis journal ────────────
			// Read() does NOT care whether the CRN is hot or cold.
			// - Hot:  returns from Redis in <1ms, lazy-refreshes TTL if <20% remaining
			// - Cold: falls back to Source (DocumentDB) via ReadContract
			readStart := time.Now()
			readPayload, err := sl.Read(ctx, crn)
			readLatency := time.Since(readStart)
			if err != nil {
				log.Error("hot session: read failed", "crn", crn, "err", err)
				continue
			}

			var readResult NudgeInventoryPayload
			_ = json.Unmarshal(readPayload, &readResult)
			log.Info("hot session: READ served from journal",
				"crn", crn,
				"read_latency_us", readLatency.Microseconds(),
				"priority", readResult.Priority,
				"channel", readResult.Channel,
			)

			// ─── Step 5: QUERY — compound lookup via indexes ─────────────────
			// Every Write() (cold or hot) maintains secondary indexes via
			// IndexContract → Go-side pipeline (SADD/ZADD).
			// Query() resolves via band-scoped SINTER + ZRANGEBYSCORE.
			if sessionCount%3 == 0 {
				results, queryErr := sl.Query(ctx, sluice.Query{
					Equality: map[string]string{"channel": "push"},
					RangeMin: map[string]float64{"priority": 3},
				})
				if queryErr != nil {
					log.Warn("hot session: query failed", "err", queryErr)
				} else {
					log.Info("hot session: QUERY result",
						"filter", "channel=push AND priority>=3",
						"matches", len(results),
					)
				}
			}

			log.Info("═══ HOT SESSION END ═══", "crn", crn)
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
	log.Info("sluice nudge_hot_reload example",
		"version", version, "commit", commit, "built", buildDate,
		"purpose", "Demonstrates COLD writes (stream) vs HOT writes (sync API) + unified READ + QUERY",
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	mongoURI := getEnv("MONGO_URI", "mongodb://localhost:27017")
	databaseName := "adroll"
	collectionName := "nudge_inventory"

	// ── Sink (write path → DocumentDB) ─────────────────────────────────────
	sk, err := docdb.New(ctx, docdb.Config{
		URI: mongoURI, Database: databaseName, Collection: collectionName,
		MaxPoolSize: 100, MinPoolSize: 10,
	})
	if err != nil {
		return fmt.Errorf("connect to MongoDB at %s: %w", mongoURI, err)
	}
	log.Info("connected to MongoDB (sink)", "uri", mongoURI)

	// ── Source (read path ← DocumentDB, shares connection pool) ────────────
	src := sourcedocdb.NewSourceWithClient(sk.Client(), databaseName, collectionName)
	log.Info("initialized Source (read path)", "database", databaseName, "collection", collectionName)

	// ── Redis config ───────────────────────────────────────────────────────
	redisAddrsRaw := getEnv("REDIS_ADDRS", "localhost:6379")
	redisAddrs := strings.Split(redisAddrsRaw, ",")
	for i := range redisAddrs {
		redisAddrs[i] = strings.TrimSpace(redisAddrs[i])
	}
	clusterMode, _ := strconv.ParseBool(getEnv("REDIS_CLUSTER_MODE", "false"))

	// ── Build Sluice ───────────────────────────────────────────────────────
	sl, err := sluice.New("nudge_inventory").
		WithRedis(sluice.RedisConfig{
			Addrs:        redisAddrs,
			ClusterMode:  clusterMode,
			PoolSize:     30,
			DialTimeout:  5 * time.Second,
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 3 * time.Second,
		}).
		WithSink(sk).
		WithSource(src). // ← Read path (Source abstraction)
		WithWriteContract(nudgeWriteContract).
		WithReadContract(nudgeReadContract).   // ← Translates CRN → ReadModel
		WithIndexContract(nudgeIndexContract). // ← Maintains SET/ZSET indexes
		WithFlushWindow(250 * time.Millisecond).
		WithMaxBatchSize(1000).
		WithBandCount(bandCount).
		WithKeyTTL(30 * time.Second).
		WithActivityWindow(4 * time.Hour). // ← Hot CRN session TTL
		WithHotAwareFlush(true).           // ← KEY: hot writes trigger immediate flush
		WithContentDedup(true).            // ← xxHash64 deduplication
		WithIdempotencyTTL(4 * time.Hour).
		WithDegradedModeDirect(true).
		WithMetrics(&logMetrics{log: log}).
		OnFlush(func(crns []string, result *sluice.BulkWriteResult, err error) {
			if err != nil {
				log.Error("flush failed", "crns", len(crns), "err", err)
				return
			}
			for _, se := range result.Errors {
				log.Warn("partial write failure", "crn", se.CorrelationKey, "err", se.Err)
			}
			log.Debug("flush complete", "crns", len(crns), "upserted", result.UpsertedCount, "modified", result.ModifiedCount)
		}).
		Build(ctx)
	if err != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if cerr := sk.Close(closeCtx); cerr != nil {
			log.Warn("closing mongo sink after failed build", "err", cerr)
		}
		return fmt.Errorf("build sluice: %w", err)
	}

	log.Info("sluice ready",
		"redis_addrs", redisAddrs,
		"cluster_mode", clusterMode,
		"bands", bandCount,
		"flush_window", "250ms",
		"activity_window", "4h",
		"hot_aware_flush", true,
		"content_dedup", true,
	)

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

	// ── Start COLD write path: stream consumers ────────────────────────────
	const coldWorkers = 8
	var wg sync.WaitGroup
	var coldWritten atomic.Int64
	log.Info("starting COLD write path: stream consumer workers", "count", coldWorkers)
	for i := 0; i < coldWorkers; i++ {
		wg.Add(1)
		go coldWriteSimulator(ctx, i, sl, log, &coldWritten, &wg)
	}

	// ── Start HOT write path: synchronous API simulator ────────────────────
	log.Info("starting HOT write path: sync API session simulator")
	wg.Add(1)
	go hotWriteSimulator(ctx, sl, log, &wg)

	// ── Stats reporter ─────────────────────────────────────────────────────
	statsTicker := time.NewTicker(5 * time.Second)
	defer statsTicker.Stop()
	start := time.Now()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-statsTicker.C:
				total := coldWritten.Load()
				elapsed := time.Since(start).Seconds()
				log.Info("throughput",
					"cold_writes", total,
					"elapsed_s", fmt.Sprintf("%.1f", elapsed),
					"cold_rate", fmt.Sprintf("%.0f/s", float64(total)/elapsed),
				)
			}
		}
	}()

	<-ctx.Done()
	log.Info("shutdown signal received")
	wg.Wait()

	total := coldWritten.Load()
	elapsed := time.Since(start)
	log.Info("final summary",
		"total_cold_writes", total,
		"elapsed", elapsed.Round(time.Second),
		"avg_cold_rate", fmt.Sprintf("%.0f writes/sec", float64(total)/elapsed.Seconds()),
	)
	return nil
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
