// Package main demonstrates a production-grade nudge inventory consumer
// using sluice as the write buffer between Kafka/SQS and AWS DocumentDB.
//
// It exercises BOTH regimes:
//   - Cold regime: high-velocity bulk writes via Write() → time-drain flush.
//   - Hot regime:  user-login HotLoad() → sub-ms Read() → sync HTTP responses.
//
// It also demonstrates IndexContract (secondary indexes) and Query() (compound lookups).
//
// Run against DocumentDB Local (MongoDB):
//
//	MONGODB_URI=mongodb://localhost:27017 REDIS_ADDRS=localhost:6379 go run ./examples/nudge_hot_reload_documentdb/main.go
//
// Run against AWS DocumentDB:
//
//	MONGODB_URI=mongodb://<username>:<password>@<cluster-endpoint>:27017/?tls=true&tlsCaFile=global-bundle.pem REDIS_ADDRS=localhost:6379 go run ./examples/nudge_hot_reload_documentdb/main.go
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

	"github.com/hussainpithawala/sluice-go"
	"github.com/hussainpithawala/sluice-go/internal/localjournal"
	docdbsink "github.com/hussainpithawala/sluice-go/sink/docdb"
	"github.com/hussainpithawala/sluice-go/source"
	docdbsource "github.com/hussainpithawala/sluice-go/source/docdb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
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
// DocumentDB requires MongoDB-style update operations.
func nudgeWriteContract(correlationKey string, rawPayload []byte) (*sluice.WriteModel, error) {
	var p NudgeInventoryPayload
	if err := json.Unmarshal(rawPayload, &p); err != nil {
		return nil, fmt.Errorf("nudge contract: invalid payload for correlation_key %s: %w", correlationKey, err)
	}

	// MongoDB update operation with $set
	update := bson.M{
		"$set": bson.M{
			"_id":             correlationKey,
			"nudge_master_id": p.NudgeMasterID,
			"channel":         p.Channel,
			"priority":        p.Priority,
			"campaign_id":     p.CampaignID,
			"expires_at":      p.ExpiresAt,
			"last_updated":    p.LastUpdated,
		},
	}

	return &sluice.WriteModel{
		Filter: bson.M{"_id": correlationKey},
		Update: update,
		Upsert: true,
	}, nil
}

// ─── ReadContract ────────────────────────────────────────────────────────────
// ReadContract translates a correlation key into a MongoDB filter.
func nudgeReadContract(correlationKey string) (*source.ReadModel, error) {
	return &source.ReadModel{
		Filter: bson.M{"_id": correlationKey},
	}, nil
}

// ─── IndexContract ───────────────────────────────────────────────────────────
// IndexContract extracts secondary index fields from the payload for Redis-side Query().
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
func (m *logMetrics) RecordLocalCacheHit(namespace string) {
	m.log.Info("record-local-cache-hit", "ns", namespace)
}
func (m *logMetrics) RecordLocalCacheMiss(namespace string, reason localjournal.MissReason) {
	m.log.Info("record-local-cache-miss", "ns", namespace, "reason", reason)
}
func (m *logMetrics) RecordLocalSetSize(namespace string, size int) {
	m.log.Info("record-local-set-size", "ns", namespace, "size", size)
}
func (m *logMetrics) RecordUnflushedExpiry(ns, band string, count int) {
	m.log.Error("unflushed payload lost", "ns", ns, "band", band, "count", count)
}

// ─── Cold Regime: Simulated Bulk Consumer ────────────────────────────────────
// Kept at a sustainable functional load (~80 events/sec) to prevent MongoDB timeouts.
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

	ticker := time.NewTicker(3 * time.Second) // Slowed down slightly for MongoDB stability
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

			// Step 2: HotLoad — simulates user login. Loads from DocumentDB into Redis.
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
	log.Info("sluice documentdb hot/cold example", "version", version, "commit", commit, "built", buildDate)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ── Initialize MongoDB Client ───────────────────────────────────────
	mongoURI := getEnv("MONGODB_URI", "mongodb://localhost:27017")
	database := getEnv("MONGODB_DATABASE", "nudge_inventory")
	collection := getEnv("MONGODB_COLLECTION", "nudge_hot_cold")

	log.Info("connecting to documentdb", "uri", mongoURI, "database", database, "collection", collection)

	clientOpts := options.Client().
		ApplyURI(mongoURI).
		SetMaxPoolSize(100).
		SetMinPoolSize(10).
		SetConnectTimeout(10 * time.Second).
		SetServerSelectionTimeout(5 * time.Second)

	mongoClient, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return fmt.Errorf("connect to documentdb: %w", err)
	}

	// Verify connection
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := mongoClient.Ping(pingCtx, readpref.Primary()); err != nil {
		return fmt.Errorf("ping documentdb: %w", err)
	}
	log.Info("documentdb connection established")

	// ── Ensure Collection Exists ──────────────────────────────────────────
	if err := ensureCollectionExists(ctx, mongoClient, database, collection, log); err != nil {
		return fmt.Errorf("ensure collection exists: %w", err)
	}

	// ── Sink (write path) & Source (read path) ───────────────────────────
	sk, err := docdbsink.New(ctx, docdbsink.Config{
		URI:                    mongoURI,
		Database:               database,
		Collection:             collection,
		MaxPoolSize:            100,
		MinPoolSize:            10,
		ConnectTimeout:         10 * time.Second,
		ServerSelectionTimeout: 5 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("create documentdb sink: %w", err)
	}

	cfg := docdbsource.Config{
		URI:        mongoURI,
		Database:   database,
		Collection: collection,
	}

	src, err := docdbsource.NewSource(ctx, cfg)

	// ── Redis config ─────────────────────────────────────────────────────
	redisAddrsRaw := getEnv("REDIS_ADDRS", "localhost:6379")
	redisAddrs := strings.Split(redisAddrsRaw, ",")
	for i := range redisAddrs {
		redisAddrs[i] = strings.TrimSpace(redisAddrs[i])
	}

	clusterModeRaw := getEnv("REDIS_CLUSTER_MODE", "false")
	clusterMode, err := strconv.ParseBool(clusterModeRaw)
	if err != nil {
		return fmt.Errorf("invalid REDIS_CLUSTER_MODE %q, expected true/false: %w", clusterModeRaw, err)
	}

	// ── Build Sluice with all contracts ──────────────────────────────────
	sl, err := sluice.New("nudge_inventory_documentdb").
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
			log.Debug("flush complete", "correlation_keys", len(correlation_keys), "upserted", result.UpsertedCount)
		}).
		Build(ctx)
	if err != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if cerr := sk.Close(closeCtx); cerr != nil {
			log.Warn("closing documentdb sink after failed build", "err", cerr)
		}
		return fmt.Errorf("build sluice: %w", err)
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

	// ── Start Cold Regime: bulk consumer workers ─────────────────────────
	const workerCount = 4
	var wg sync.WaitGroup
	var written atomic.Int64

	log.Info("starting cold regime: consumer workers", "count", workerCount)
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go simulatedConsumer(ctx, i, sl, log, &written, &wg)
	}

	// ── Start Hot Regime: simulated user sessions ────────────────────────
	log.Info("starting hot regime: user session simulator")
	wg.Add(1)
	go hotRegimeSimulator(ctx, sl, log, &wg)

	// ── Stats reporter ───────────────────────────────────────────────────
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

// ensureCollectionExists creates the collection and indexes if they don't exist.
func ensureCollectionExists(ctx context.Context, client *mongo.Client, database, collection string, log *slog.Logger) error {
	coll := client.Database(database).Collection(collection)

	// Create indexes for common query patterns
	indexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "channel", Value: 1}},
			Options: options.Index().SetName("idx_channel"),
		},
		{
			Keys:    bson.D{{Key: "campaign_id", Value: 1}},
			Options: options.Index().SetName("idx_campaign_id"),
		},
		{
			Keys:    bson.D{{Key: "priority", Value: 1}},
			Options: options.Index().SetName("idx_priority"),
		},
		{
			Keys:    bson.D{{Key: "expires_at", Value: 1}},
			Options: options.Index().SetName("idx_expires_at"),
		},
	}

	_, err := coll.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		return fmt.Errorf("create indexes: %w", err)
	}

	log.Info("documentdb collection and indexes ready", "database", database, "collection", collection)
	return nil
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
