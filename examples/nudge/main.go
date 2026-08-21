// Package main demonstrates a production-grade nudge inventory consumer
// using sluice as the write buffer between Kafka/SQS and DocumentDB.
//
// Run against a single-node / CMD Redis (unchanged behavior):
//
//	MONGO_URI=mongodb://localhost:27017 REDIS_ADDRS=localhost:6379 \
//	go run ./examples/nudge/main.go
//
// Run against the local 4-shard Valkey cluster (CME) from docker-compose.yml.
// Note the ports are 7001-7004 (valkey-node-0 listens on 7001, node-1 on 7002,
// and so on — the service names are 0-indexed, the ports are not):
//
//	MONGO_URI=mongodb://localhost:27017 \
//	REDIS_ADDRS=localhost:7001,localhost:7002,localhost:7003,localhost:7004 \
//	REDIS_CLUSTER_MODE=true \
//	go run ./examples/nudge/main.go
//
// Run against a real AWS ElastiCache CME cluster (single config endpoint —
// still requires REDIS_CLUSTER_MODE=true explicitly; address count alone
// no longer determines client type, see shield.RedisConfig.ClusterMode):
//
//	REDIS_ADDRS=my-cluster.xxxxx.clustercfg.use1.cache.amazonaws.com:6379 \
//	REDIS_CLUSTER_MODE=true \
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
	"go.mongodb.org/mongo-driver/bson"
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
				crn := fmt.Sprintf("crn_%09d", (workerID*1_000_000)+int(seq%2_000_000))
				payload, _ := json.Marshal(NudgeInventoryPayload{
					NudgeMasterID: nudgeMasters[seq%int64(len(nudgeMasters))],
					Channel:       channels[seq%int64(len(channels))],
					Priority:      int(seq%5) + 1,
					CampaignID:    fmt.Sprintf("camp_%04d", seq%100),
					ExpiresAt:     time.Now().Add(24 * time.Hour),
					LastUpdated:   time.Now().UTC(),
				})
				if err := sl.Write(ctx, crn, payload); err != nil {
					log.Error("write error", "crn", crn, "err", err)
				}
			}
		}
	}
}

const bandCount = 16

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

// run holds the real body of the example. main() is kept to just the
// os.Exit(1) so that every defer registered below still runs on the way out —
// calling os.Exit inline after a defer would skip it (and trips the
// exitAfterDefer linter).
//
// The error returns here are load-bearing, not decoration: an earlier version
// logged setup failures and fell through, which left sl == nil and produced a
// nil-pointer panic inside Sluice.Write on the first event from every worker
// goroutine. Any setup step that fails must abort.
func run(log *slog.Logger) (err error) {
	log.Info("sluice nudge example", "version", version, "commit", commit, "built", buildDate)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	mongoURI := getEnv("MONGO_URI", "mongodb://localhost:27017")
	sk, err := docdb.New(ctx, docdb.Config{URI: mongoURI, Database: "adroll", Collection: "nudge_inventory", MaxPoolSize: 100, MinPoolSize: 10})
	if err != nil {
		return fmt.Errorf("connect to MongoDB at %s: %w", mongoURI, err)
	}
	log.Info("connected to MongoDB", "uri", mongoURI)

	// REDIS_ADDRS: comma-separated. One address = one node (CMD) OR one
	// cluster config endpoint (CME) — address count alone can't tell these
	// apart, which is exactly the bug this rewrite fixes. REDIS_CLUSTER_MODE
	// is the actual switch; set it explicitly rather than relying on
	// how many addresses happen to be listed.
	redisAddrsRaw := getEnv("REDIS_ADDRS", "localhost:7001,localhost:7002,localhost:7003,localhost:7004")
	redisAddrs := strings.Split(redisAddrsRaw, ",")
	for i := range redisAddrs {
		redisAddrs[i] = strings.TrimSpace(redisAddrs[i])
	}
	// Deliberately fatal rather than defaulting on a parse error. The whole
	// point of ClusterMode being explicit is that guessing it wrong fails in a
	// confusing way later (MOVED redirects a standalone client won't follow),
	// so a typo here must not silently resolve to a guess.
	clusterModeRaw := getEnv("REDIS_CLUSTER_MODE", "true")
	clusterMode, err := strconv.ParseBool(clusterModeRaw)
	if err != nil {
		return fmt.Errorf("invalid REDIS_CLUSTER_MODE %q, expected true/false: %w", clusterModeRaw, err)
	}

	sl, err := sluice.New("nudge_inventory").
		WithRedis(sluice.RedisConfig{
			Addrs:        redisAddrs,
			ClusterMode:  clusterMode,
			PoolSize:     30,
			DialTimeout:  5 * time.Second,
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 3 * time.Second,
		}).
		WithSink(sk).WithWriteContract(nudgeWriteContract).
		WithFlushWindow(250 * time.Millisecond).WithMaxBatchSize(1000).
		WithBandCount(bandCount).WithKeyTTL(30 * time.Second).WithDegradedModeDirect(true).
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
		}).Build(ctx)
	if err != nil {
		// Build did not take ownership of the sink, so close it here. On the
		// success path DrainAndClose closes the sink for us — doing both would
		// double-close.
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if cerr := sk.Close(closeCtx); cerr != nil {
			log.Warn("closing mongo sink after failed build", "err", cerr)
		}
		return fmt.Errorf("build sluice (redis_addrs=%v cluster_mode=%t): %w", redisAddrs, clusterMode, err)
	}
	log.Info("sluice ready", "redis_addrs", redisAddrs, "cluster_mode", clusterMode, "flush_window", "250ms", "bands", bandCount)

	defer func() {
		log.Info("draining sluice...")
		shutCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if derr := sl.DrainAndClose(shutCtx); derr != nil {
			log.Error("drain error", "err", derr)
			// Surface a drain failure as the run's exit status, but never
			// clobber an earlier, more proximate error.
			if err == nil {
				err = fmt.Errorf("drain and close: %w", derr)
			}
			return
		}
		log.Info("sluice drained and closed")
	}()

	const workerCount = 8
	var wg sync.WaitGroup
	var written atomic.Int64
	log.Info("starting consumer workers", "count", workerCount)
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go simulatedConsumer(ctx, i, sl, log, &written, &wg)
	}

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
				log.Info("throughput", "events", total, "elapsed_s", fmt.Sprintf("%.1f", elapsed), "rate", fmt.Sprintf("%.0f/s", float64(total)/elapsed))
			}
		}
	}()

	<-ctx.Done()
	log.Info("shutdown signal received")
	wg.Wait()
	total := written.Load()
	elapsed := time.Since(start)
	log.Info("final summary", "total_events", total, "elapsed", elapsed.Round(time.Second), "avg_rate", fmt.Sprintf("%.0f events/sec", float64(total)/elapsed.Seconds()))
	return nil
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
