package documentdb

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hussainpithawala/sluice-go"
	"github.com/hussainpithawala/sluice-go/internal/localjournal"
	"github.com/hussainpithawala/sluice-go/internal/shield"
	"github.com/hussainpithawala/sluice-go/sink/docdb"
	"github.com/hussainpithawala/sluice-go/source"
	sourcedocdb "github.com/hussainpithawala/sluice-go/source/docdb"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const defaultMongoUrl = "mongodb://localhost:27017"

// ── Helpers ─────────────────────────────────────────────────────────────────

// buildRealTestSluice wires a REAL Sluice instance with a real DocumentDB sink/source
// and an L1 cache. It uses the production Builder path to ensure no assumptions
// are invalidated by test-specific wiring.
//
// IMPORTANT: 'sluiceNamespace' is used for Redis keys and the broadcast stream.
// 'dbSuffix' is used to isolate the MongoDB database name per pod/test.
// For cross-pod tests (like PushPull), both pods MUST share the same 'sluiceNamespace'
// but use different 'dbSuffix' values.
func buildRealTestSluice(t *testing.T, sluiceNamespace string, dbSuffix string, mode localjournal.LocalCacheMode, ttl time.Duration) (*sluice.Sluice, *countingRecorder) {
	t.Helper()

	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		mongoURI = defaultMongoUrl
	}

	dbName := "sluice_phase2_test_" + dbSuffix
	collName := "nudge_inventory"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Setup Real DocumentDB Sink
	sk, err := docdb.New(ctx, docdb.Config{
		URI:         mongoURI,
		Database:    dbName,
		Collection:  collName,
		MaxPoolSize: 10,
		MinPoolSize: 2,
	})
	require.NoError(t, err, "docdb.New sink")

	// 2. Setup Real DocumentDB Source (sharing the same client)
	src := sourcedocdb.NewSourceWithClient(sk.Client(), dbName, collName)

	// 3. Define Real Contracts
	writeContract := func(correlationKey string, payload []byte) (*sluice.WriteModel, error) {
		var doc map[string]any
		if err := json.Unmarshal(payload, &doc); err != nil {
			// Fallback for raw byte payloads used in tests
			doc = map[string]any{"raw": string(payload)}
		}
		return &sluice.WriteModel{
			Filter: bson.D{{Key: "_id", Value: correlationKey}},
			Update: bson.D{{Key: "$set", Value: doc}},
			Upsert: true,
		}, nil
	}

	readContract := func(correlationKey string) (*source.ReadModel, error) {
		return &source.ReadModel{
			Filter: bson.D{{Key: "_id", Value: correlationKey}},
		}, nil
	}

	// 4. Setup Metrics Recorder (reused from localjournal_l1_test.go)
	rec := newCountingRecorder()

	// 5. Isolate Redis DB before building
	redisClient := redis.NewClient(&redis.Options{Addr: l1RedisAddr(), DB: l1TestDB})
	require.NoError(t, redisClient.FlushDB(ctx).Err())
	_ = redisClient.Close()

	// 6. Build Sluice using the PRODUCTION Builder path
	builder := sluice.New(sluiceNamespace).
		WithRedis(sluice.RedisConfig{
			Addrs:        []string{l1RedisAddr()},
			ClusterMode:  false,
			DB:           l1TestDB,
			DialTimeout:  5 * time.Second,
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 3 * time.Second,
			PoolSize:     10,
		}).
		WithSink(sk).
		WithSource(src).
		WithWriteContract(writeContract).
		WithReadContract(readContract).
		WithFlushWindow(250 * time.Millisecond).
		WithMaxBatchSize(1000).
		WithBandCount(16).
		WithKeyTTL(30 * time.Second).
		WithActivityWindow(4 * time.Hour).
		WithMetrics(rec)

	if mode != localjournal.LocalCacheOff {
		builder = builder.WithLocalCache(localjournal.LocalCacheConfig{
			Mode:       mode,
			MaxEntries: 1024,
			LocalTTL:   ttl,
		})
	}

	sl, err := builder.Build(ctx)
	require.NoError(t, err, "sluice.Build")

	// Cleanup
	t.Cleanup(func() {
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutCancel()
		_ = sl.DrainAndClose(shutCtx)

		// Drop the test MongoDB database
		_ = sk.Client().Database(dbName).Drop(context.Background())
		_ = sk.Close(context.Background())
	})

	return sl, rec
}

// ── 2.4 ─ ReadFresh bypasses L1 ─────────────────────────────────────────────

// TestReadFresh_BypassesL1 proves the strong-consistency escape hatch:
// after L1 is seeded with a stale value and the journal advances, Read()
// keeps serving the stale L1 value while ReadFresh() returns the journal's
// authoritative value.
func TestReadFresh_BypassesL1(t *testing.T) {
	const namespace = "test_readfresh"
	// Note: We pass a unique DB suffix to isolate the MongoDB database
	s, rec := buildRealTestSluice(t, namespace, "readfresh_db", localjournal.LocalCacheLazy, 60*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const key = "readfresh_key"
	stalePayload := []byte(`{"v":1,"state":"stale"}`)
	freshPayload := []byte(`{"v":2,"state":"fresh"}`)

	// Write via Sluice: journals to Redis AND write-through seeds L1.
	require.NoError(t, s.Write(ctx, key, stalePayload))

	// Read() hits L1 → stale payload.
	got, err := s.Read(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, stalePayload, got)

	hits, _ := rec.snapshot()
	assert.Equal(t, 1, hits, "first Read must hit L1")

	// Advance the journal directly (simulates another pod's write landing).
	// We use a temporary shield instance purely for test assertions.
	sh, err := shield.New(shield.RedisConfig{
		Addrs: []string{l1RedisAddr()}, ClusterMode: false, DB: l1TestDB,
	}, namespace, 16, 30*time.Second, 4*time.Hour)
	require.NoError(t, err)
	defer func(sh *shield.Shield) {
		err := sh.Close()
		if err != nil {
			slog.Error(fmt.Sprintf("Problem while closing shield %e", err))
		}
	}(sh)

	require.NoError(t, sh.Write(ctx, key, freshPayload))

	// Read() STILL hits L1 → returns the stale value (bounded staleness).
	got, err = s.Read(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, stalePayload, got, "Read() must serve the L1 (stale) value")

	hits, _ = rec.snapshot()
	assert.Equal(t, 2, hits, "second Read must also hit L1")

	// ReadFresh() bypasses L1 → returns the journal's authoritative value.
	got, err = s.ReadFresh(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, freshPayload, got, "ReadFresh must return the journal's fresh value")
}

// ── 2.5 ─ HotLoad emits kind=seed ──────────────────────────────────────────

// TestHotLoad_EmitsSeedBroadcast proves HotLoad emits exactly one broadcast
// message with kind=seed (not a duplicate upsert), carrying the payload and
// a valid UnixMilli ts.
func TestHotLoad_EmitsSeedBroadcast(t *testing.T) {
	const namespace = "test_hotload_seed"

	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		mongoURI = defaultMongoUrl
	}
	dbName := "sluice_phase2_test_hotload_db"
	collName := "nudge_inventory"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// ── Clean slate: drop the database before seeding ──────────────────────
	// This ensures the test is idempotent even if a previous run crashed
	// before cleanup, or if tests are re-run without dropping the DB.
	tempClient, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	require.NoError(t, err)
	require.NoError(t, tempClient.Database(dbName).Drop(ctx))
	_ = tempClient.Disconnect(ctx)

	sk, err := docdb.New(ctx, docdb.Config{URI: mongoURI, Database: dbName, Collection: collName, MaxPoolSize: 10, MinPoolSize: 2})
	require.NoError(t, err)
	src := sourcedocdb.NewSourceWithClient(sk.Client(), dbName, collName)

	// Pre-seed the document in DocumentDB so HotLoad can successfully read it
	const key = "hotload_seed_key"
	_, err = sk.Client().Database(dbName).Collection(collName).InsertOne(ctx, bson.M{"_id": key})
	require.NoError(t, err)

	writeContract := func(correlationKey string, payload []byte) (*sluice.WriteModel, error) {
		return &sluice.WriteModel{
			Filter: bson.D{{Key: "_id", Value: correlationKey}},
			Update: bson.D{{Key: "$set", Value: bson.M{"data": payload}}},
			Upsert: true,
		}, nil
	}
	readContract := func(correlationKey string) (*source.ReadModel, error) {
		return &source.ReadModel{Filter: bson.D{{Key: "_id", Value: correlationKey}}}, nil
	}

	redisClient := redis.NewClient(&redis.Options{Addr: l1RedisAddr(), DB: l1TestDB})
	require.NoError(t, redisClient.FlushDB(ctx).Err())
	_ = redisClient.Close()

	s, err := sluice.New(namespace).
		WithRedis(sluice.RedisConfig{Addrs: []string{l1RedisAddr()}, ClusterMode: false, DB: l1TestDB}).
		WithSink(sk).
		WithSource(src).
		WithWriteContract(writeContract).
		WithReadContract(readContract).
		WithLocalCache(localjournal.LocalCacheConfig{Mode: localjournal.LocalCachePushPull, MaxEntries: 1024, LocalTTL: 60 * time.Second}).
		Build(ctx)
	require.NoError(t, err)

	defer func() {
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutCancel()
		_ = s.DrainAndClose(shutCtx)
		_ = sk.Client().Database(dbName).Drop(context.Background())
		err = sk.Close(context.Background())
		if err != nil {
			slog.Error(fmt.Sprintf("Problem while closing sink %e", err))
		}
	}()

	// Verify broadcast stream using a separate shield client
	sh, err := shield.New(shield.RedisConfig{Addrs: []string{l1RedisAddr()}, ClusterMode: false, DB: l1TestDB}, namespace, 16, 30*time.Second, 4*time.Hour)
	require.NoError(t, err)
	defer func(sh *shield.Shield) {
		err := sh.Close()
		if err != nil {
			if err != nil {
				slog.Error(fmt.Sprintf("Problem while closing shield %e", err))
			}
		}
	}(sh)

	// HotLoad is now a fire-and-forget command (returns only error)
	err = s.HotLoad(ctx, key)
	require.NoError(t, err)

	// Hot marker must be set synchronously.
	isHot, err := sh.IsHot(ctx, key)
	require.NoError(t, err)
	assert.True(t, isHot, "HotLoad must set the hot marker")

	// The broadcast is emitted asynchronously as part of the background hydration.
	// We use Eventually to poll for the broadcast to appear in the stream.
	streamKey := shield.BroadcastKey(namespace)
	var entries []redis.XMessage
	require.Eventually(t, func() bool {
		var err error
		entries, err = sh.Client().XRange(ctx, streamKey, "-", "+").Result()
		return err == nil && len(entries) >= 1
	}, 3*time.Second, 50*time.Millisecond, "HotLoad must emit a broadcast")

	require.Len(t, entries, 1, "HotLoad must emit exactly one broadcast (no duplicate upsert)")

	entry := entries[0]
	assert.Equal(t, key, entry.Values["crn"])

	// The payload will be the ExtJSON of the document we inserted
	expectedPayload, _ := bson.MarshalExtJSON(bson.M{"_id": key}, false, false)
	assert.Equal(t, expectedPayload, []byte(entry.Values["payload"].(string)))

	ts, err := strconv.ParseInt(entry.Values["ts"].(string), 10, 64)
	require.NoError(t, err)
	assert.Positive(t, ts, "ts must be a valid UnixMilli timestamp")
}

// ── 2.7 ─ PushPull end-to-end, two pods ────────────────────────────────────

// TestPushPull_EndToEnd_TwoPods is the RFC §12 exit criterion for Phase 2:
// Pod A writes, Pod B (which never wrote) converges via the broadcast stream,
// and Pod B's subsequent Read() is served from L1 with zero Redis reads.
func TestPushPull_EndToEnd_TwoPods(t *testing.T) {
	const namespace = "test_pushpull_e2e"

	// Isolate: flush the test DB before building any pod.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	redisClient := redis.NewClient(&redis.Options{Addr: l1RedisAddr(), DB: l1TestDB})
	require.NoError(t, redisClient.FlushDB(ctx).Err())
	_ = redisClient.Close()

	// Build Pod A and Pod B using the production builder.
	// CRITICAL FIX: Both pods MUST share the same 'namespace' so they publish/subscribe
	// to the same Redis broadcast stream. We use different DB suffixes ("pod_a_db", "pod_b_db")
	// to isolate their MongoDB databases.
	podA, _ := buildRealTestSluice(t, namespace, "pod_a_db", localjournal.LocalCachePushPull, 60*time.Second)
	podB, recB := buildRealTestSluice(t, namespace, "pod_b_db", localjournal.LocalCachePushPull, 60*time.Second)

	const key = "pushpull_session"
	payload := []byte(`{"cross":"pod","v":1}`)

	// Pod A writes. This journals to Redis, write-through seeds Pod A's L1,
	// and emits a broadcast to the shared stream.
	require.NoError(t, podA.Write(ctx, key, payload))

	// Pod B (which never wrote) must converge via the broadcast stream.
	// We verify convergence by calling Read() and checking that it returns the payload.
	require.Eventually(t, func() bool {
		got, err := podB.Read(ctx, key)
		return err == nil && string(got) == string(payload)
	}, 3*time.Second, 10*time.Millisecond, "Pod B's L1 must converge from Pod A's broadcast")

	// Install a Redis-read spy on Pod B BEFORE the final read.
	var redisReads int64
	recB.redisOpHook = func(op string) {
		if op == OpRead || op == OpReadJournal || op == OpReadFresh {
			atomic.AddInt64(&redisReads, 1)
		}
	}

	// Pod B Read() must be served from L1 — zero Redis reads.
	got, err := podB.Read(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, payload, got, "Pod B Read must return the converged payload")
	assert.Zero(t, atomic.LoadInt64(&redisReads), "Pod B Read must be served from L1, not Redis")

	hitsB, _ := recB.snapshot()
	assert.GreaterOrEqual(t, hitsB, 1, "Pod B must record an L1 hit")
}
