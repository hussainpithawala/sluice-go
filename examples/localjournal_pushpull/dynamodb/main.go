// Command localjournal_pushpull_dynamodb demonstrates the L1 Local Journal's PushPull
// mode against a real DynamoDB sink and source.
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
// Run against DynamoDB Local + single-node Redis:
//
//	DYNAMODB_ENDPOINT=http://localhost:8000 REDIS_ADDRS=localhost:6379 REDIS_CLUSTER_MODE=false
//	go run ./examples/localjournal_pushpull_dynamodb/main.go
//
// Run against the local 4-shard Valkey cluster (CME) from docker-compose.yml:
//
//	DYNAMODB_ENDPOINT=http://localhost:8000
//	REDIS_ADDRS=localhost:7001,localhost:7002,localhost:7003,localhost:7004
//	REDIS_CLUSTER_MODE=true
//	go run ./examples/localjournal_pushpull_dynamodb/main.go
//
// Run against real AWS infrastructure:
//
//	AWS_REGION=us-east-1
//	REDIS_ADDRS=my-cluster.xxxxx.clustercfg.use1.cache.amazonaws.com:6379
//	REDIS_CLUSTER_MODE=true
//	go run ./examples/localjournal_pushpull_dynamodb/main.go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	sluice "github.com/hussainpithawala/sluice-go"
	"github.com/hussainpithawala/sluice-go/internal/localjournal"
	"github.com/hussainpithawala/sluice-go/internal/shield"
	dynsink "github.com/hussainpithawala/sluice-go/sink/dynamodb"
	"github.com/hussainpithawala/sluice-go/source"
	dynsource "github.com/hussainpithawala/sluice-go/source/dynamodb"
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
// nudgeWriteContract translates the raw payload into a DynamoDB PutItem format.
func nudgeWriteContract(correlationKey string, rawPayload []byte) (*sluice.WriteModel, error) {
	var p NudgeInventoryPayload
	if err := json.Unmarshal(rawPayload, &p); err != nil {
		return nil, fmt.Errorf("nudge contract: invalid payload for correlation_key %s: %w", correlationKey, err)
	}

	// DynamoDB requires the full item map for a PutItem operation.
	item := map[string]any{
		"PK":              correlationKey,
		"nudge_master_id": p.NudgeMasterID,
		"channel":         p.Channel,
		"priority":        p.Priority,
		"campaign_id":     p.CampaignID,
		"expires_at":      p.ExpiresAt.Format(time.RFC3339),
		"last_updated":    p.LastUpdated.Format(time.RFC3339),
	}

	return &sluice.WriteModel{
		Update: item,
		Upsert: true,
	}, nil
}

// nudgeReadContract translates a correlation key into a DynamoDB GetItem Key.
func nudgeReadContract(correlationKey string) (*source.ReadModel, error) {
	return &source.ReadModel{
		Filter: map[string]any{"PK": correlationKey},
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
	tag string // "pod-a" or "pod-b" — prepended to distinguish the two pods
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

// buildPod constructs a full Sluice instance with:
//   - Real DynamoDB sink (writes via BatchWriteItem)
//   - Real DynamoDB source (reads via strongly consistent GetItem)
//   - L1 Local Journal in PushPull mode (sharded in-memory cache + broadcast)
//   - Broadcast stream for cross-pod convergence
func buildPod(ctx context.Context, tag string, endpoint string, redisAddrs []string, clusterMode bool, log *slog.Logger) (*sluice.Sluice, func(context.Context) error, error) {
	var cfg aws.Config
	var err error
	httpClient := &http.Client{Timeout: 30 * time.Second}

	if endpoint != "" {
		log.Info("using local dynamodb endpoint", "endpoint", endpoint)
		cfg, err = config.LoadDefaultConfig(ctx,
			config.WithRegion("us-east-1"),
			config.WithHTTPClient(httpClient),
		)
	} else {
		log.Info("using default aws config (production mode)")
		cfg, err = config.LoadDefaultConfig(ctx, config.WithHTTPClient(httpClient))
	}
	if err != nil {
		return nil, nil, fmt.Errorf("load aws config: %w", err)
	}

	// Use BaseEndpoint on the service client options instead of the deprecated global resolver
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	})

	tableName := "NudgeInventoryPushPull"

	if err := ensureTableExists(ctx, client, tableName, log); err != nil {
		return nil, nil, fmt.Errorf("ensure table exists: %w", err)
	}

	sk := dynsink.NewSink(client, tableName)
	src := dynsource.NewSource(client, tableName)

	sl, err := sluice.New("nudge_inventory_dynamodb").
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
		// ── L1 Local Journal: PushPull mode ──────────────────────────────
		WithLocalCache(localjournal.LocalCacheConfig{
			Mode:       localjournal.LocalCachePushPull,
			MaxEntries: 200_000,          // 256 shards × ~782 entries each
			LocalTTL:   60 * time.Second, // max staleness bound
			Broadcast:  shield.BroadcastPayload,
			Retention:  200_000, // stream MAXLEN ~ N
		}).
		Build(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("build sluice (redis_addrs=%v cluster_mode=%t): %w", redisAddrs, clusterMode, err)
	}

	cleanup := func(shutCtx context.Context) error {
		var firstErr error
		if err := sl.DrainAndClose(shutCtx); err != nil {
			log.Error("drain error", "pod", tag, "err", err)
			firstErr = err
		}
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
	for i := range 300 { // 3s max with 10ms polling
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
	}
	if !converged {
		return fmt.Errorf("cross-pod convergence timed out after 3s")
	}

	// ── Scenario 3: Pod A updates; Pod B converges the new value ─────────
	log.Info("═══ Scenario 3: Pod A updates, Pod B converges ═══")
	// Guarantee the v2 write lands in a different millisecond than v1.
	// The L1 version gate is strict '>' on UnixMilli ts; two writes in the
	// same millisecond tie and the resident value wins (RFC §8.5). Without
	// this boundary the demo can flake on fast hardware.
	time.Sleep(10 * time.Millisecond)
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

	// Dump stream tail for diagnostics (using the correct namespace)
	redisAddrsRaw := getEnv("REDIS_ADDRS", "localhost:7001,localhost:7002,localhost:7003,localhost:7004")
	redisAddrs := strings.Split(redisAddrsRaw, ",")
	clusterMode := true
	dumpStreamTail(ctx, redisAddrs, clusterMode, "sl:nudge_inventory_dynamodb:bcast", 5, log)

	converged = false
	for i := 0; i < 300; i++ {
		got, err := podB.Read(ctx, crn)
		if err == nil {
			var r NudgeInventoryPayload
			_ = json.Unmarshal(got, &r)
			// DIAGNOSTIC: log the actual payload and priority returned by Pod B
			log.Debug("SCENARIO3-POLL", "priority", r.Priority, "payload_bytes", len(got), "attempt", i)
			if r.Priority == 1 {
				converged = true
				log.Info("Pod B converged to updated value", "crn", crn, "priority", r.Priority, "attempts", i+1)
				break
			}
		}
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
		log.Warn("compound query failed (expected if no other keys indexed)", "err", err)
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
	log.Info("sluice localjournal_pushpull dynamodb example",
		"version", version, "commit", commit, "built", buildDate)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	endpoint := os.Getenv("DYNAMODB_ENDPOINT")

	// ── Redis config (shared by both pods) ───────────────────────────────
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

	// ── Reset broadcast stream for a clean demo run ──────────────────────
	// The stream persists across runs and contains messages from previous
	// debug sessions. Replaying this backlog delays convergence and causes
	// scenario timeouts. We delete the stream to ensure this run starts
	// with an empty history.
	{
		var tempClient redis.UniversalClient
		if clusterMode {
			tempClient = redis.NewClusterClient(&redis.ClusterOptions{Addrs: redisAddrs})
		} else {
			tempClient = redis.NewClient(&redis.Options{Addr: redisAddrs[0]})
		}
		streamKey := shield.BroadcastKey("nudge_inventory_dynamodb")
		if err := tempClient.Del(ctx, streamKey).Err(); err != nil {
			log.Warn("cleanup: failed to delete broadcast stream", "stream", streamKey, "err", err)
		} else {
			log.Info("cleanup: broadcast stream deleted", "stream", streamKey)
		}
		_ = tempClient.Close()
	}

	// ── Build Pod A ──────────────────────────────────────────────────────
	log.Info("building Pod A (DynamoDB sink/source + L1 PushPull)")
	podA, cleanupA, err := buildPod(ctx, "pod-a", endpoint, redisAddrs, clusterMode, log)
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
	log.Info("building Pod B (DynamoDB sink/source + L1 PushPull)")
	podB, cleanupB, err := buildPod(ctx, "pod-b", endpoint, redisAddrs, clusterMode, log)
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
		"stream_retention", 200_000,
	)

	// ── Run scenarios ────────────────────────────────────────────────────
	if err := runScenarios(ctx, podA, podB, log); err != nil {
		return err
	}

	log.Info("═══ All scenarios completed successfully ═══")
	return nil
}

// ensureTableExists creates the table if it doesn't exist and waits for it to become ACTIVE.
func ensureTableExists(ctx context.Context, client *dynamodb.Client, tableName string, log *slog.Logger) error {
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("PK"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("PK"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
	})

	if err != nil {
		// Rewritten as a switch statement to satisfy gocritic's ifElseChain rule
		var alreadyExists *types.ResourceInUseException
		switch {
		case errors.As(err, &alreadyExists):
			log.Info("table already exists, skipping creation", "table", tableName)
		case strings.Contains(err.Error(), "ResourceInUseException"):
			log.Info("table already exists (string match), skipping creation", "table", tableName)
		default:
			return fmt.Errorf("create table: %w", err)
		}
	} else {
		log.Info("table creation initiated", "table", tableName)
	}

	log.Info("waiting for table to become active...", "table", tableName)
	waiter := dynamodb.NewTableExistsWaiter(client)
	err = waiter.Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}, 2*time.Minute)

	if err != nil {
		return fmt.Errorf("wait for table active: %w", err)
	}

	log.Info("table is active", "table", tableName)
	return nil
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func dumpStreamTail(ctx context.Context, redisAddrs []string, clusterMode bool, stream string, n int, log *slog.Logger) {
	var c redis.UniversalClient
	if clusterMode {
		c = redis.NewClusterClient(&redis.ClusterOptions{Addrs: redisAddrs})
	} else {
		c = redis.NewClient(&redis.Options{Addr: redisAddrs[0]})
	}
	defer func(c redis.UniversalClient) {
		err := c.Close()
		if err != nil {
			slog.Debug("Unable to close the redis-universal-client")
		}
	}(c)
	entries, err := c.XRange(ctx, stream, "-", "+").Result()
	if err != nil {
		log.Error("dump stream: xrange", "err", err)
		return
	}
	log.Info("dump-stream", "stream", stream, "total_entries", len(entries))
	start := len(entries) - n
	if start < 0 {
		start = 0
	}
	for _, e := range entries[start:] {
		log.Info("stream-entry",
			"id", e.ID,
			"crn", e.Values["crn"],
			"ts", e.Values["ts"],
			"kind", e.Values["kind"],
		)
	}
}
