package unit_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"

	sluice "github.com/hussainpithawala/sluice-go"
	"github.com/hussainpithawala/sluice-go/sink/docdb"
)

const (
	testRedisAddr  = "localhost:6379"
	testMongoURI   = "mongodb://localhost:27017"
	testDatabase   = "sluice_unit_test"
	testCollection = "test_docs"
)

type testPayload struct {
	Value     string    `json:"value"`
	UpdatedAt time.Time `json:"updated_at"`
}

func testContract(key string, payload []byte) (*sluice.WriteModel, error) {
	var p testPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("invalid payload: %w", err)
	}
	return &sluice.WriteModel{
		Filter: bson.D{{Key: "_id", Value: key}},
		Update: bson.D{{Key: "$set", Value: bson.D{{Key: "value", Value: p.Value}}}},
		Upsert: true,
	}, nil
}

func mustPayload(t *testing.T, value string) []byte {
	t.Helper()
	b, err := json.Marshal(testPayload{Value: value, UpdatedAt: time.Now()})
	require.NoError(t, err)
	return b
}

func buildSluice(t *testing.T, opts ...func(*sluice.Builder)) (*sluice.Sluice, *docdb.Sink) {
	t.Helper()
	ctx := context.Background()
	sk, err := docdb.New(ctx, docdb.Config{
		URI:         testMongoURI,
		Database:    testDatabase,
		Collection:  testCollection,
		MaxPoolSize: 10,
		MinPoolSize: 1,
	})
	require.NoError(t, err)

	b := sluice.New("unit_test").
		WithRedis(sluice.RedisConfig{Addrs: []string{testRedisAddr}}).
		WithSink(sk).
		WithWriteContract(testContract).
		WithFlushWindow(50 * time.Millisecond).
		WithMaxBatchSize(100).
		WithBandCount(4).
		WithKeyTTL(5 * time.Second)
	for _, opt := range opts {
		opt(b)
	}
	sl, err := b.Build(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = sl.DrainAndClose(ctx)
	})
	return sl, sk
}

func TestWrite_SingleKey(t *testing.T) {
	sl, _ := buildSluice(t)
	require.NoError(t, sl.Write(context.Background(), "key_001", mustPayload(t, "hello")))
}

func TestWrite_UniqueKeys(t *testing.T) {
	const n = 500
	sl, _ := buildSluice(t)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("crn_%06d", i)
		require.NoError(t, sl.Write(context.Background(), key, mustPayload(t, key)))
	}
}

func TestWrite_DeduplicatesSameKey(t *testing.T) {
	sl, _ := buildSluice(t)
	for i := 0; i < 50; i++ {
		require.NoError(t, sl.Write(context.Background(), "crn_same", mustPayload(t, fmt.Sprintf("v%d", i))))
	}
}

func TestWrite_ConcurrentSafety(t *testing.T) {
	const goroutines, writesEach = 50, 20
	sl, _ := buildSluice(t)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gID int) {
			defer wg.Done()
			for i := 0; i < writesEach; i++ {
				_ = sl.Write(context.Background(), fmt.Sprintf("crn_g%d_i%d", gID, i), mustPayload(t, "v"))
			}
		}(g)
	}
	wg.Wait()
}

func TestWrite_EmptyKeyRejected(t *testing.T) {
	sl, _ := buildSluice(t)
	assert.ErrorIs(t, sl.Write(context.Background(), "", mustPayload(t, "v")), sluice.ErrEmptyCorrelationKey)
}

func TestWrite_AfterClose(t *testing.T) {
	sl, _ := buildSluice(t)
	ctx := context.Background()
	require.NoError(t, sl.DrainAndClose(ctx))
	assert.ErrorIs(t, sl.Write(ctx, "key", mustPayload(t, "v")), sluice.ErrLibraryClosed)
}

func TestDrainAndClose_Idempotent(t *testing.T) {
	sl, _ := buildSluice(t)
	ctx := context.Background()
	assert.NoError(t, sl.DrainAndClose(ctx))
	assert.NoError(t, sl.DrainAndClose(ctx))
}

func TestBuild_MissingSink(t *testing.T) {
	_, err := sluice.New("test").
		WithRedis(sluice.RedisConfig{Addrs: []string{testRedisAddr}}).
		WithWriteContract(testContract).
		Build(context.Background())
	assert.ErrorIs(t, err, sluice.ErrMissingSink)
}

func TestBuild_MissingContract(t *testing.T) {
	ctx := context.Background()
	sk, _ := docdb.New(ctx, docdb.Config{URI: testMongoURI, Database: testDatabase, Collection: testCollection})
	_, err := sluice.New("test").
		WithRedis(sluice.RedisConfig{Addrs: []string{testRedisAddr}}).
		WithSink(sk).
		Build(context.Background())
	assert.ErrorIs(t, err, sluice.ErrMissingContract)
}

func TestBuild_MissingNamespace(t *testing.T) {
	ctx := context.Background()
	sk, _ := docdb.New(ctx, docdb.Config{URI: testMongoURI, Database: testDatabase, Collection: testCollection})
	_, err := sluice.New("").
		WithRedis(sluice.RedisConfig{Addrs: []string{testRedisAddr}}).
		WithSink(sk).
		WithWriteContract(testContract).
		Build(context.Background())
	assert.ErrorIs(t, err, sluice.ErrMissingNamespace)
}

func TestBuild_MissingRedis(t *testing.T) {
	ctx := context.Background()
	sk, _ := docdb.New(ctx, docdb.Config{URI: testMongoURI, Database: testDatabase, Collection: testCollection})
	_, err := sluice.New("test").
		WithSink(sk).
		WithWriteContract(testContract).
		Build(context.Background())
	assert.ErrorIs(t, err, sluice.ErrMissingRedis)
}

func TestWrite_VolumeTrigger(t *testing.T) {
	const batchSize = 20
	sl, _ := buildSluice(t, func(b *sluice.Builder) {
		b.WithFlushWindow(60 * time.Second).
			WithMaxBatchSize(batchSize).
			WithBandCount(1).
			WithKeyTTL(10 * time.Second)
	})
	for i := 0; i < batchSize+1; i++ {
		require.NoError(t, sl.Write(context.Background(), fmt.Sprintf("vol_%d", i), mustPayload(t, "v")))
	}
}

func TestOnFlushCallback(t *testing.T) {
	var mu sync.Mutex
	var flushedKeys []string
	sl, _ := buildSluice(t, func(b *sluice.Builder) {
		b.WithFlushWindow(30 * time.Millisecond).
			WithBandCount(4).
			WithKeyTTL(5 * time.Second).
			OnFlush(func(keys []string, _ *sluice.BulkWriteResult, _ error) {
				mu.Lock()
				flushedKeys = append(flushedKeys, keys...)
				mu.Unlock()
			})
	})
	const n = 10
	for i := 0; i < n; i++ {
		require.NoError(t, sl.Write(context.Background(), fmt.Sprintf("cb_%d", i), mustPayload(t, "v")))
	}
	// Verify keys are eventually flushed via callback.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(flushedKeys) >= n
	}, 3*time.Second, 20*time.Millisecond, "expected %d flushed keys via callback", n)
}

// redisClient returns a test Redis client pointing at the unit-test Redis.
func redisClient(t *testing.T) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: testRedisAddr})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestFlush_TwoPhaseCommit_DirtySetEmptied verifies that after a successful
// flush cycle the dirty sorted set is empty — i.e. CommitKeys ran.
func TestFlush_TwoPhaseCommit_DirtySetEmptied(t *testing.T) {
	const ns = "tpc_test"
	rc := redisClient(t)
	ctx := context.Background()

	// Clean up any leftover keys from previous runs.
	cleanRedisKeys(t, rc, ns)

	sk, err := docdb.New(ctx, docdb.Config{
		URI: testMongoURI, Database: testDatabase, Collection: "tpc_docs",
		MaxPoolSize: 10, MinPoolSize: 1,
	})
	require.NoError(t, err)

	sl, err := sluice.New(ns).
		WithRedis(sluice.RedisConfig{Addrs: []string{testRedisAddr}}).
		WithSink(sk).
		WithWriteContract(testContract).
		WithFlushWindow(50 * time.Millisecond).
		WithMaxBatchSize(100).
		WithBandCount(2).
		WithKeyTTL(10 * time.Second).
		Build(ctx)
	require.NoError(t, err)

	// Write several keys.
	for i := 0; i < 10; i++ {
		require.NoError(t, sl.Write(ctx, fmt.Sprintf("tpc_%d", i), mustPayload(t, "v")))
	}

	// Wait for flush to drain all bands.
	require.Eventually(t, func() bool {
		total := int64(0)
		for band := 0; band < 2; band++ {
			n, _ := rc.ZCard(ctx, fmt.Sprintf("sl:%s:dirty:%d", ns, band)).Result()
			total += n
		}
		return total == 0
	}, 3*time.Second, 20*time.Millisecond, "dirty set should be empty after flush")

	require.NoError(t, sl.DrainAndClose(ctx))
}

// TestContractError_MovesToDeadLetter verifies that a key whose contract
// rejects the payload ends up in the dead-letter sorted set, not stuck
// in the dirty set forever.
func TestContractError_MovesToDeadLetter(t *testing.T) {
	const ns = "dlq_test"
	rc := redisClient(t)
	ctx := context.Background()

	cleanRedisKeys(t, rc, ns)

	sk, err := docdb.New(ctx, docdb.Config{
		URI: testMongoURI, Database: testDatabase, Collection: "dlq_docs",
		MaxPoolSize: 10, MinPoolSize: 1,
	})
	require.NoError(t, err)

	// Contract that rejects keys starting with "bad_".
	selectiveContract := func(key string, payload []byte) (*sluice.WriteModel, error) {
		if len(key) >= 4 && key[:4] == "bad_" {
			return nil, fmt.Errorf("rejected key: %s", key)
		}
		return testContract(key, payload)
	}

	sl, err := sluice.New(ns).
		WithRedis(sluice.RedisConfig{Addrs: []string{testRedisAddr}}).
		WithSink(sk).
		WithWriteContract(selectiveContract).
		WithFlushWindow(50 * time.Millisecond).
		WithMaxBatchSize(100).
		WithBandCount(1). // single band for deterministic DLQ key
		WithKeyTTL(10 * time.Second).
		Build(ctx)
	require.NoError(t, err)

	// Write one good key and two bad keys.
	require.NoError(t, sl.Write(ctx, "good_1", mustPayload(t, "ok")))
	require.NoError(t, sl.Write(ctx, "bad_1", mustPayload(t, "nope")))
	require.NoError(t, sl.Write(ctx, "bad_2", mustPayload(t, "nope")))

	dlqKey := fmt.Sprintf("sl:%s:dlq:0", ns)

	// Bad keys should appear in the DLQ.
	require.Eventually(t, func() bool {
		n, _ := rc.ZCard(ctx, dlqKey).Result()
		return n >= 2
	}, 3*time.Second, 20*time.Millisecond, "expected 2 keys in dead-letter set")

	// Bad keys should NOT remain in the dirty set.
	require.Eventually(t, func() bool {
		dirtyKey := fmt.Sprintf("sl:%s:dirty:0", ns)
		n, _ := rc.ZCard(ctx, dirtyKey).Result()
		return n == 0
	}, 3*time.Second, 20*time.Millisecond, "dirty set should be empty after flush")

	// Verify DLQ payload hash is annotated with reason.
	vals, err := rc.HGetAll(ctx, fmt.Sprintf("sl:%s:payload:bad_1", ns)).Result()
	require.NoError(t, err)
	assert.Equal(t, "contract_violation", vals["dlq_reason"])
	assert.NotEmpty(t, vals["dlq_at"])

	require.NoError(t, sl.DrainAndClose(ctx))
}

// TestOnFlushCallback_SuccessReceivesNilError verifies that the callback
// receives a nil error (not the raw flush error) when BulkWrite succeeds.
func TestOnFlushCallback_SuccessReceivesNilError(t *testing.T) {
	var mu sync.Mutex
	var callbackErrors []error

	sl, _ := buildSluice(t, func(b *sluice.Builder) {
		b.WithFlushWindow(50 * time.Millisecond).
			WithBandCount(1).
			WithKeyTTL(5 * time.Second).
			OnFlush(func(_ []string, _ *sluice.BulkWriteResult, err error) {
				mu.Lock()
				callbackErrors = append(callbackErrors, err)
				mu.Unlock()
			})
	})

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		require.NoError(t, sl.Write(ctx, fmt.Sprintf("nilcb_%d", i), mustPayload(t, "v")))
	}

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(callbackErrors) > 0
	}, 3*time.Second, 20*time.Millisecond, "callback should have been invoked")

	mu.Lock()
	defer mu.Unlock()
	for i, e := range callbackErrors {
		assert.Nil(t, e, "callback invocation %d should have nil error", i)
	}
}

// TestOnFlushCallback_ContractErrorCallsBack verifies that the callback
// is invoked for contract-rejected keys with a non-nil error and the
// offending correlation key in the result Errors slice.
func TestOnFlushCallback_ContractErrorCallsBack(t *testing.T) {
	var mu sync.Mutex
	var cbErrors []sluice.SinkError

	rejectContract := func(key string, payload []byte) (*sluice.WriteModel, error) {
		if key == "reject_me" {
			return nil, fmt.Errorf("nope")
		}
		return testContract(key, payload)
	}

	ctx := context.Background()
	sk, err := docdb.New(ctx, docdb.Config{
		URI: testMongoURI, Database: testDatabase, Collection: "cb_contract_docs",
		MaxPoolSize: 10, MinPoolSize: 1,
	})
	require.NoError(t, err)

	sl, err := sluice.New("cb_contract_test").
		WithRedis(sluice.RedisConfig{Addrs: []string{testRedisAddr}}).
		WithSink(sk).
		WithWriteContract(rejectContract).
		WithFlushWindow(50 * time.Millisecond).
		WithMaxBatchSize(100).
		WithBandCount(1).
		WithKeyTTL(5 * time.Second).
		OnFlush(func(_ []string, result *sluice.BulkWriteResult, _ error) {
			mu.Lock()
			if result != nil {
				cbErrors = append(cbErrors, result.Errors...)
			}
			mu.Unlock()
		}).
		Build(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = sl.DrainAndClose(cctx)
	})

	require.NoError(t, sl.Write(ctx, "reject_me", mustPayload(t, "v")))

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(cbErrors) > 0
	}, 3*time.Second, 20*time.Millisecond, "callback should report contract error")

	mu.Lock()
	defer mu.Unlock()
	found := false
	for _, se := range cbErrors {
		if se.CorrelationKey == "reject_me" {
			found = true
			assert.NotNil(t, se.Err)
		}
	}
	assert.True(t, found, "expected SinkError for 'reject_me' in callback")
}

// TestWrite_BatchedMode verifies that with batched writes enabled, all writes
// eventually land in Redis (dirty sorted set + payload hash) after DrainAndClose.
func TestWrite_BatchedMode(t *testing.T) {
	const ns = "batched_write_test"
	const n = 50
	rc := redisClient(t)
	ctx := context.Background()

	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	sk, err := docdb.New(ctx, docdb.Config{
		URI: testMongoURI, Database: testDatabase, Collection: "batched_docs",
		MaxPoolSize: 10, MinPoolSize: 1,
	})
	require.NoError(t, err)

	sl, err := sluice.New(ns).
		WithRedis(sluice.RedisConfig{Addrs: []string{testRedisAddr}}).
		WithSink(sk).
		WithWriteContract(testContract).
		WithFlushWindow(60*time.Second). // long window — rely on batcher, not timer flush
		WithMaxBatchSize(100).
		WithBandCount(4).
		WithKeyTTL(10*time.Second).
		WithBatchedWrites(20, 5*time.Millisecond).
		Build(ctx)
	require.NoError(t, err)

	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = fmt.Sprintf("bw_%04d", i)
		require.NoError(t, sl.Write(ctx, keys[i], mustPayload(t, keys[i])))
	}

	// StopBatcher (inside DrainAndClose) must flush all buffered entries before drain.
	require.NoError(t, sl.DrainAndClose(ctx))

	// After DrainAndClose the dirty sets should be empty (engine drained them).
	totalDirty := int64(0)
	for band := 0; band < 4; band++ {
		n, _ := rc.ZCard(ctx, fmt.Sprintf("sl:%s:dirty:%d", ns, band)).Result()
		totalDirty += n
	}
	assert.Equal(t, int64(0), totalDirty, "all dirty keys should be committed after drain")
}

// TestWrite_BatchedMode_FlushesToMongo verifies that batched writes are
// ultimately flushed to MongoDB through the normal engine drain cycle.
func TestWrite_BatchedMode_FlushesToMongo(t *testing.T) {
	const ns = "batched_mongo_test"
	const n = 30
	rc := redisClient(t)
	ctx := context.Background()

	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	sk, err := docdb.New(ctx, docdb.Config{
		URI: testMongoURI, Database: testDatabase, Collection: "batched_mongo_docs",
		MaxPoolSize: 10, MinPoolSize: 1,
	})
	require.NoError(t, err)

	sl, err := sluice.New(ns).
		WithRedis(sluice.RedisConfig{Addrs: []string{testRedisAddr}}).
		WithSink(sk).
		WithWriteContract(testContract).
		WithFlushWindow(50*time.Millisecond).
		WithMaxBatchSize(100).
		WithBandCount(2).
		WithKeyTTL(10*time.Second).
		WithBatchedWrites(10, 5*time.Millisecond).
		Build(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = sl.DrainAndClose(cctx)
	})

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("bm_%04d", i)
		require.NoError(t, sl.Write(ctx, key, mustPayload(t, key)))
	}

	// All dirty sets should empty once the engine has flushed.
	require.Eventually(t, func() bool {
		total := int64(0)
		for band := 0; band < 2; band++ {
			c, _ := rc.ZCard(ctx, fmt.Sprintf("sl:%s:dirty:%d", ns, band)).Result()
			total += c
		}
		return total == 0
	}, 3*time.Second, 20*time.Millisecond, "dirty sets should be empty after batched flush")
}

// cleanRedisKeys removes all keys matching the given namespace pattern
// to ensure test isolation.
func cleanRedisKeys(t *testing.T, rc *redis.Client, ns string) {
	t.Helper()
	ctx := context.Background()
	var cursor uint64
	for {
		keys, next, err := rc.Scan(ctx, cursor, fmt.Sprintf("sl:%s:*", ns), 100).Result()
		require.NoError(t, err)
		if len(keys) > 0 {
			rc.Del(ctx, keys...)
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
}
