// Package main demonstrates a production-grade nudge inventory consumer
// using sluice as the write buffer between Kafka/SQS and DocumentDB.
//
// It exercises BOTH regimes:
//   - Cold regime: high-velocity bulk writes via Write() → time-drain flush.
//   - Hot regime:  user-login HotLoad() → sub-ms Read() → sync HTTP responses.
//
// It also demonstrates IndexContract (secondary indexes) and Query() (compound lookups).
//
// Run against a single-node / CMD Redis (unchanged behavior):
//
//	MONGO_URI=mongodb://localhost:27017 REDIS_ADDRS=localhost:6379
//	go run ./examples/nudge/main.go
//
// Run against the local 4-shard Valkey cluster (CME) from docker-compose.yml:
//
//	MONGO_URI=mongodb://localhost:27017
//	REDIS_ADDRS=localhost:7001,localhost:7002,localhost:7003,localhost:7004
//	REDIS_CLUSTER_MODE=true
//	go run ./examples/nudge/main.go
//
// Run against a real AWS ElastiCache CME cluster:
//
//	REDIS_ADDRS=my-cluster.xxxxx.clustercfg.use1.cache.amazonaws.com:6379
//	REDIS_CLUSTER_MODE=true
//	go run ./examples/nudge/main.go
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
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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

// ─── WriteContract ───────────────────────────────────────────────────────────

func nudgeWriteContract(correlationKey string, rawPayload []byte) (*sluice.WriteModel, error) {
	var p NudgeInventoryPayload
	if err := json.Unmarshal(rawPayload, &p); err != nil {
		return nil, fmt.Errorf("nudge contract: invalid payload for correlation_key %s: %w", correlationKey, err)
	}
	return &sluice.WriteModel{
		Filter: bson.D{{Key: "_id", Value: correlationKey}},
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

// ─── ReadContract ────────────────────────────────────────────────────────────
// ReadContract loads the current state of a correlation_key from DocumentDB.
// Used by HotLoad() to warm the Redis journal on user login,
// and by Read() as a cold-path fallback.

func nudgeReadContract(correlationKey string) (*source.ReadModel, error) {
	return &source.ReadModel{
		Filter: bson.M{"_id": correlationKey},
	}, nil
}

// ─── IndexContract ───────────────────────────────────────────────────────────
// IndexContract extracts secondary index fields from the payload.
//
//	string values       → equality SET index   (O(1) lookup by value)
//	int / float64       → range ZSET index     (scored for ZRANGEBYSCORE)
//	time.Time           → range ZSET index     (Unix ms score)
//
// These indexes enable Query() for compound lookups without touching DocumentDB.

func nudgeIndexContract(correlationKey string, payload []byte) (map[string]interface{}, error) {
	var p NudgeInventoryPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("index contract: invalid payload for correlation_key %s: %w", correlationKey, err)
	}
	return map[string]interface{}{
		"channel":  p.Channel,                        // equality: SET sl:{ns}:idx:{band}:channel:push
		"campaign": p.CampaignID,                     // equality: SET sl:{ns}:idx:{band}:campaign:camp_0042
		"priority": float64(p.Priority),              // range:    ZSET sl:{ns}:ridx:{band}:priority
		"expires":  float64(p.ExpiresAt.UnixMilli()), // range:    ZSET sl:{ns}:ridx:{band}:expires
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
func (m *logMetrics) RecordContractError(ns, correlationKey string, err error) {
	m.log.Error("contract error", "ns", ns, "correlationKey", correlationKey, "err", err)
}
func (m *logMetrics) RecordDeadLetter(ns, band string, count int) {
	m.log.Warn("dead-letter", "ns", ns, "band", band, "count", count)
}
func (m *logMetrics) RecordDLQProcess(ns, strategy string, processed, succeeded, failed int) {
	m.log.Info("dlq-process", "ns", ns, "strategy", strategy, "processed", processed, "succeeded", succeeded, "failed", failed)
}

// Hot/Cold regime metrics.
func (m *logMetrics) RecordWarmUp(ns string, duration time.Duration, err error) {
	m.log.Info("warm-up", "ns", ns, "duration_ms", duration.Milliseconds(), "err", err)
}
func (m *logMetrics) RecordRead(ns string, duration time.Duration, isHot bool, err error) {
	m.log.Info("read", "ns", ns, "duration_ms", duration.Milliseconds(), "is_hot", isHot, "err", err)
}
func (m *logMetrics) RecordHotSetSize(ns string, size int) {
	m.log.Info("hot-set-size", "ns", ns, "size", size)
}

// ─── Cold Regime: Simulated Bulk Consumer ────────────────────────────────────

func simulatedConsumer(ctx context.Context, workerID int, sl *sluice.Sluice, log *slog.Logger, written *atomic.Int64, wg *sync.WaitGroup) {
	defer wg.Done()
	nudgeMasters := []string{"nm_spring_retarget_2026", "nm_cart_abandonment", "nm_win_back_q2", "nm_first_purchase", "nm_loyalty_upgrade"}
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
// Demonstrates the full hot correlation_key lifecycle:
//   1. User logs in → HotLoad() warms the journal from DocumentDB.
//   2. User action  → Write() updates the live journal.
//   3. Sync HTTP    → Read() returns immediately from Redis (sub-ms).
//   4. Observability → IsHot() confirms the correlation_key is in the hot set.

func hotRegimeSimulator(ctx context.Context, sl *sluice.Sluice, log *slog.Logger, wg *sync.WaitGroup) {
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
			correlationKey := fmt.Sprintf("correlationKey_hot_%06d", sessionCount)
			log.Info("── hot regime: simulating user login ──", "correlationKey", correlationKey, "session", sessionCount)

			// Step 1: Check if correlation_key is hot before login.
			isHot, err := sl.IsHot(ctx, correlationKey)
			if err != nil {
				log.Error("hot regime: IsHot failed", "correlationKey", correlationKey, "err", err)
				continue
			}
			log.Info("hot regime: pre-login state", "correlationKey", correlationKey, "is_hot", isHot)

			// Step 2: HotLoad — simulates user login.
			// Loads collections from DocumentDB into the Redis journal.
			payload, err := sl.HotLoad(ctx, correlationKey)
			if err != nil {
				// Expected for brand-new correlation_keys that don't exist in DocumentDB yet.
				log.Info("hot regime: HotLoad (new correlation_key, writing directly)", "correlationKey", correlationKey, "err", err)

				// Write directly for new users (cold → hot transition).
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
				log.Info("hot regime: HotLoad success (warmed from DocumentDB)", "correlationKey", correlationKey, "payload_bytes", len(payload))
			}

			// Step 3: Confirm correlation_key is now hot.
			isHot, err = sl.IsHot(ctx, correlationKey)
			if err != nil {
				log.Error("hot regime: IsHot after load failed", "correlationKey", correlationKey, "err", err)
				continue
			}
			log.Info("hot regime: post-login state", "correlationKey", correlationKey, "is_hot", isHot)

			// Step 4: User action — update the live journal.
			// This simulates a banner dismissal or in-app action.
			var current NudgeInventoryPayload
			_ = json.Unmarshal(payload, &current)
			current.Priority = 1 // user dismissed high-priority nudge
			current.LastUpdated = time.Now().UTC()
			updatedPayload, _ := json.Marshal(current)

			if err := sl.Write(ctx, correlationKey, updatedPayload); err != nil {
				log.Error("hot regime: user action Write failed", "correlationKey", correlationKey, "err", err)
				continue
			}

			// Step 5: Sync HTTP response — Read() returns immediately from Redis.
			// This is the key insight: the app reads from the journal, not DocumentDB.
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

			// Step 6: Demonstrate Query() — find all hot correlation_keys with channel=push AND priority >= 3.
			if sessionCount%5 == 0 {
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
							break // only log first 3 matches
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
	log.Info("sluice nudge example", "version", version, "commit", commit, "built", buildDate)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	mongoURI := getEnv("MONGO_URI", "mongodb://localhost:27017")

	databaseName := "adroll"
	collectionName := "nudge_inventory"

	// ── Sink (write path) ──────────────────────────────────────────────────
	sk, err := docdb.New(ctx, docdb.Config{
		URI: mongoURI, Database: databaseName, Collection: collectionName,
		MaxPoolSize: 100, MinPoolSize: 10,
	})

	src := sourcedocdb.NewSourceWithClient(sk.Client(), databaseName, collectionName)

	if err != nil {
		return fmt.Errorf("connect to MongoDB at %s: %w", mongoURI, err)
	}
	log.Info("connected to MongoDB", "uri", mongoURI)

	// ── Direct MongoDB client (read path for ReadContract) ─────────────────
	readClient, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return fmt.Errorf("connect read client: %w", err)
	}
	defer func() { _ = readClient.Disconnect(context.Background()) }()

	// ── Redis config ───────────────────────────────────────────────────────
	redisAddrsRaw := getEnv("REDIS_ADDRS", "localhost:7001,localhost:7002,localhost:7003,localhost:7004")
	redisAddrs := strings.Split(redisAddrsRaw, ",")
	for i := range redisAddrs {
		redisAddrs[i] = strings.TrimSpace(redisAddrs[i])
	}

	clusterModeRaw := getEnv("REDIS_CLUSTER_MODE", "true")
	clusterMode, err := strconv.ParseBool(clusterModeRaw)
	if err != nil {
		return fmt.Errorf("invalid REDIS_CLUSTER_MODE %q, expected true/false: %w", clusterModeRaw, err)
	}

	// ── Build Sluice with all contracts ────────────────────────────────────
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
		WithWriteContract(nudgeWriteContract).
		WithSource(src).
		WithReadContract(nudgeReadContract).   // ← ReadContract for HotLoad/Read
		WithIndexContract(nudgeIndexContract). // ← IndexContract for Query
		WithFlushWindow(250 * time.Millisecond).
		WithMaxBatchSize(1000).
		WithBandCount(bandCount).
		WithKeyTTL(30 * time.Second).
		WithActivityWindow(4 * time.Hour). // ← Hot correlation_key session TTL
		WithHotAwareFlush(true).           // ← Extend TTL after successful flush
		WithContentDedup(true).            // ← xxHash64 deduplication
		WithDegradedModeDirect(true).
		WithMetrics(&logMetrics{log: log}).
		OnFlush(func(correlation_keys []string, result *sluice.BulkWriteResult, err error) {
			if err != nil {
				log.Error("flush failed", "correlation_keys", len(correlation_keys), "err", err)
				return
			}
			for _, se := range result.Errors {
				log.Warn("partial write failure", "correlationKey", se.CorrelationKey, "err", se.Err)
			}
			log.Debug("flush complete", "correlation_keys", len(correlation_keys), "upserted", result.UpsertedCount, "modified", result.ModifiedCount)
		}).
		Build(ctx)
	if err != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if cerr := sk.Close(closeCtx); cerr != nil {
			log.Warn("closing mongo sink after failed build", "err", cerr)
		}
		return fmt.Errorf("build sluice (redis_addrs=%v cluster_mode=%t): %w", redisAddrs, clusterMode, err)
	}

	log.Info("sluice ready",
		"redis_addrs", redisAddrs,
		"cluster_mode", clusterMode,
		"flush_window", "250ms",
		"bands", bandCount,
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

	// ── Start Cold Regime: bulk consumer workers ───────────────────────────
	const workerCount = 8
	var wg sync.WaitGroup
	var written atomic.Int64
	log.Info("starting cold regime: consumer workers", "count", workerCount)
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go simulatedConsumer(ctx, i, sl, log, &written, &wg)
	}

	// ── Start Hot Regime: simulated user sessions ──────────────────────────
	log.Info("starting hot regime: user session simulator")
	wg.Add(1)
	go hotRegimeSimulator(ctx, sl, log, &wg)

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
				total := written.Load()
				elapsed := time.Since(start).Seconds()
				log.Info("throughput",
					"cold_events", total,
					"elapsed_s", fmt.Sprintf("%.1f", elapsed),
					"rate", fmt.Sprintf("%.0f/s", float64(total)/elapsed),
				)
			}
		}
	}()

	<-ctx.Done()
	log.Info("shutdown signal received")
	wg.Wait()

	total := written.Load()
	elapsed := time.Since(start)
	log.Info("final summary",
		"total_cold_events", total,
		"elapsed", elapsed.Round(time.Second),
		"avg_rate", fmt.Sprintf("%.0f events/sec", float64(total)/elapsed.Seconds()),
	)
	return nil
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
