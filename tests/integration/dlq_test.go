//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"

	sluice "github.com/hussainpithawala/sluice-go"
	"github.com/hussainpithawala/sluice-go/sink/docdb"
)

// dlqRedisClient returns a test Redis client with cleanup.
func dlqRedisClient(t *testing.T) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: redisAddr()})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// cleanRedisKeys removes all keys matching the given namespace pattern.
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

// buildDLQSluice creates a sluice instance with a contract that rejects keys
// starting with "bad_", causing them to be routed to the dead-letter queue.
func buildDLQSluice(t *testing.T, ns string) *sluice.Sluice {
	t.Helper()
	ctx := context.Background()

	sk, err := docdb.New(ctx, docdb.Config{
		URI: mongoURI(), Database: "sluice_integration", Collection: ns,
		MaxPoolSize: 10, MinPoolSize: 1,
	})
	require.NoError(t, err)

	selectiveContract := func(key string, payload []byte) (*sluice.WriteModel, error) {
		if len(key) >= 4 && key[:4] == "bad_" {
			return nil, fmt.Errorf("rejected key: %s", key)
		}
		return inventoryContract(key, payload)
	}

	sl, err := sluice.New(ns).
		WithRedis(sluice.RedisConfig{Addrs: []string{redisAddr()}, PoolSize: 20}).
		WithSink(sk).
		WithWriteContract(selectiveContract).
		WithFlushWindow(100 * time.Millisecond).
		WithMaxBatchSize(500).
		WithBandCount(1). // single band for deterministic DLQ key
		WithKeyTTL(30 * time.Second).
		Build(ctx)
	require.NoError(t, err)

	t.Cleanup(func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = sl.DrainAndClose(shutCtx)
	})
	return sl
}

// seedDLQ writes good and bad keys, waits for bad keys to land in the DLQ,
// and returns the list of bad keys.
func seedDLQ(t *testing.T, sl *sluice.Sluice, rc *redis.Client, ns string, badCount int) []string {
	t.Helper()
	ctx := context.Background()

	// Write good keys that will flush successfully.
	for i := 0; i < 3; i++ {
		require.NoError(t, sl.Write(ctx, fmt.Sprintf("good_%d", i), makePayload("nm_good")))
	}
	// Write bad keys that will be rejected by the contract.
	badKeys := make([]string, badCount)
	for i := 0; i < badCount; i++ {
		badKeys[i] = fmt.Sprintf("bad_%d", i)
		require.NoError(t, sl.Write(ctx, badKeys[i], makePayload("nm_bad")))
	}

	// Wait for bad keys to land in the DLQ.
	dlqKey := fmt.Sprintf("sl:%s:dlq:0", ns)
	require.Eventually(t, func() bool {
		n, _ := rc.ZCard(ctx, dlqKey).Result()
		return n >= int64(badCount)
	}, 5*time.Second, 50*time.Millisecond,
		"expected %d keys in dead-letter set", badCount)

	return badKeys
}

// TestProcessDLQ_Ignore verifies that the Ignore strategy drains the DLQ
// by logging and discarding all records.
func TestProcessDLQ_Ignore(t *testing.T) {
	const ns = "dlq_ignore_test"
	rc := dlqRedisClient(t)
	ctx := context.Background()
	cleanRedisKeys(t, rc, ns)

	sl := buildDLQSluice(t, ns)
	badKeys := seedDLQ(t, sl, rc, ns, 5)

	// Process DLQ with Ignore strategy.
	result, err := sl.ProcessDLQ(ctx, sluice.DLQIgnore)
	require.NoError(t, err)

	assert.Equal(t, len(badKeys), result.Processed)
	assert.Equal(t, len(badKeys), result.Succeeded)
	assert.Equal(t, 0, result.Failed)

	// DLQ should be empty now.
	dlqKey := fmt.Sprintf("sl:%s:dlq:0", ns)
	n, err := rc.ZCard(ctx, dlqKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), n, "DLQ should be empty after Ignore")

	// Payload hashes for bad keys should be deleted.
	for _, k := range badKeys {
		exists, _ := rc.Exists(ctx, fmt.Sprintf("sl:%s:payload:%s", ns, k)).Result()
		assert.Equal(t, int64(0), exists, "payload hash for %s should be deleted", k)
	}
}

// TestProcessDLQ_Upsert verifies that the Upsert strategy re-runs the
// WriteContract with Upsert=true and persists the records to MongoDB.
func TestProcessDLQ_Upsert(t *testing.T) {
	const ns = "dlq_upsert_test"
	rc := dlqRedisClient(t)
	ctx := context.Background()
	cleanRedisKeys(t, rc, ns)

	// For the upsert test we need a contract that accepts all keys during
	// DLQ processing. Build a sluice where the contract rejects "bad_" keys
	// during normal flush (to populate the DLQ), but we'll process DLQ with
	// a sluice whose contract accepts everything.
	sk, err := docdb.New(ctx, docdb.Config{
		URI: mongoURI(), Database: "sluice_integration", Collection: ns,
		MaxPoolSize: 10, MinPoolSize: 1,
	})
	require.NoError(t, err)

	// Contract that rejects bad_ keys during normal flush.
	selectiveContract := func(key string, payload []byte) (*sluice.WriteModel, error) {
		if len(key) >= 4 && key[:4] == "bad_" {
			return nil, fmt.Errorf("rejected key: %s", key)
		}
		return inventoryContract(key, payload)
	}

	sl, err := sluice.New(ns).
		WithRedis(sluice.RedisConfig{Addrs: []string{redisAddr()}, PoolSize: 20}).
		WithSink(sk).
		WithWriteContract(selectiveContract).
		WithFlushWindow(100 * time.Millisecond).
		WithMaxBatchSize(500).
		WithBandCount(1).
		WithKeyTTL(30 * time.Second).
		Build(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = sl.DrainAndClose(shutCtx)
	})

	badKeys := seedDLQ(t, sl, rc, ns, 3)

	// Close the first sluice and build a new one with an accepting contract.
	// DrainAndClose also closes the sink, so we need a fresh one.
	require.NoError(t, sl.DrainAndClose(ctx))

	sk2, err := docdb.New(ctx, docdb.Config{
		URI: mongoURI(), Database: "sluice_integration", Collection: ns,
		MaxPoolSize: 10, MinPoolSize: 1,
	})
	require.NoError(t, err)

	sl2, err := sluice.New(ns).
		WithRedis(sluice.RedisConfig{Addrs: []string{redisAddr()}, PoolSize: 20}).
		WithSink(sk2).
		WithWriteContract(inventoryContract). // accepts all keys
		WithFlushWindow(100 * time.Millisecond).
		WithMaxBatchSize(500).
		WithBandCount(1).
		WithKeyTTL(30 * time.Second).
		Build(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = sl2.DrainAndClose(shutCtx)
	})

	// Process DLQ with Upsert strategy.
	result, err := sl2.ProcessDLQ(ctx, sluice.DLQUpsert)
	require.NoError(t, err)

	assert.Equal(t, len(badKeys), result.Processed)
	assert.Equal(t, len(badKeys), result.Succeeded)
	assert.Equal(t, 0, result.Failed)

	// DLQ should be empty.
	dlqKey := fmt.Sprintf("sl:%s:dlq:0", ns)
	n, err := rc.ZCard(ctx, dlqKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), n, "DLQ should be empty after Upsert")

	// Documents should now exist in MongoDB.
	coll := mongoCollection(t, ns)
	for _, k := range badKeys {
		n := countDocs(t, coll, bson.M{"_id": k})
		assert.Equal(t, int64(1), n, "document %s should exist in MongoDB after upsert", k)
	}
}

// TestProcessDLQ_ReInsert verifies that the ReInsert strategy generates new
// keys and pushes records back to the dirty queue for normal processing.
func TestProcessDLQ_ReInsert(t *testing.T) {
	const ns = "dlq_reinsert_test"
	rc := dlqRedisClient(t)
	ctx := context.Background()
	cleanRedisKeys(t, rc, ns)

	// Build sluice with an accepting contract so reinserted keys flush successfully.
	sk, err := docdb.New(ctx, docdb.Config{
		URI: mongoURI(), Database: "sluice_integration", Collection: ns,
		MaxPoolSize: 10, MinPoolSize: 1,
	})
	require.NoError(t, err)

	// Contract that rejects bad_ keys during normal flush.
	selectiveContract := func(key string, payload []byte) (*sluice.WriteModel, error) {
		if len(key) >= 4 && key[:4] == "bad_" {
			return nil, fmt.Errorf("rejected key: %s", key)
		}
		return inventoryContract(key, payload)
	}

	sl, err := sluice.New(ns).
		WithRedis(sluice.RedisConfig{Addrs: []string{redisAddr()}, PoolSize: 20}).
		WithSink(sk).
		WithWriteContract(selectiveContract).
		WithFlushWindow(100 * time.Millisecond).
		WithMaxBatchSize(500).
		WithBandCount(1).
		WithKeyTTL(30 * time.Second).
		Build(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = sl.DrainAndClose(shutCtx)
	})

	seedDLQ(t, sl, rc, ns, 3)

	// Close first sluice. DrainAndClose also closes the sink, so create fresh ones.
	require.NoError(t, sl.DrainAndClose(ctx))

	sk2, err := docdb.New(ctx, docdb.Config{
		URI: mongoURI(), Database: "sluice_integration", Collection: ns,
		MaxPoolSize: 10, MinPoolSize: 1,
	})
	require.NoError(t, err)

	sl2, err := sluice.New(ns).
		WithRedis(sluice.RedisConfig{Addrs: []string{redisAddr()}, PoolSize: 20}).
		WithSink(sk2).
		WithWriteContract(inventoryContract). // accepts all keys
		WithFlushWindow(100 * time.Millisecond).
		WithMaxBatchSize(500).
		WithBandCount(1).
		WithKeyTTL(30 * time.Second).
		Build(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = sl2.DrainAndClose(shutCtx)
	})

	// Use a deterministic key mutator for test assertions.
	mutator := func(oldKey string) string {
		return fmt.Sprintf("reinserted_%s", oldKey)
	}

	// Process DLQ with ReInsert strategy.
	result, err := sl2.ProcessDLQ(ctx, sluice.DLQReInsert, sluice.WithKeyMutator(mutator))
	require.NoError(t, err)

	assert.Equal(t, 3, result.Processed)
	assert.Equal(t, 3, result.Succeeded)
	assert.Equal(t, 0, result.Failed)

	// DLQ should be empty.
	dlqKey := fmt.Sprintf("sl:%s:dlq:0", ns)
	n, err := rc.ZCard(ctx, dlqKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), n, "DLQ should be empty after ReInsert")

	// Wait for reinserted keys to be flushed to MongoDB.
	coll := mongoCollection(t, ns)
	for i := 0; i < 3; i++ {
		newKey := fmt.Sprintf("reinserted_bad_%d", i)
		require.Eventually(t, func() bool {
			return countDocs(t, coll, bson.M{"_id": newKey}) >= 1
		}, 5*time.Second, 100*time.Millisecond,
			"document %s should exist in MongoDB after reinsert", newKey)
	}
}

// TestProcessDLQ_EmptyDLQ verifies that processing an empty DLQ returns
// a zero result without error.
func TestProcessDLQ_EmptyDLQ(t *testing.T) {
	const ns = "dlq_empty_test"
	rc := dlqRedisClient(t)
	ctx := context.Background()
	cleanRedisKeys(t, rc, ns)

	sk, err := docdb.New(ctx, docdb.Config{
		URI: mongoURI(), Database: "sluice_integration", Collection: ns,
		MaxPoolSize: 10, MinPoolSize: 1,
	})
	require.NoError(t, err)

	sl, err := sluice.New(ns).
		WithRedis(sluice.RedisConfig{Addrs: []string{redisAddr()}, PoolSize: 20}).
		WithSink(sk).
		WithWriteContract(inventoryContract).
		WithFlushWindow(100 * time.Millisecond).
		WithMaxBatchSize(500).
		WithBandCount(1).
		WithKeyTTL(30 * time.Second).
		Build(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = sl.DrainAndClose(shutCtx)
	})

	result, err := sl.ProcessDLQ(ctx, sluice.DLQIgnore)
	require.NoError(t, err)
	assert.Equal(t, 0, result.Processed)
	assert.Equal(t, 0, result.Succeeded)
	assert.Equal(t, 0, result.Failed)
}

// TestProcessDLQ_AfterClose verifies that ProcessDLQ returns ErrLibraryClosed
// when called on a closed sluice instance.
func TestProcessDLQ_AfterClose(t *testing.T) {
	const ns = "dlq_closed_test"
	rc := dlqRedisClient(t)
	ctx := context.Background()
	cleanRedisKeys(t, rc, ns)

	sk, err := docdb.New(ctx, docdb.Config{
		URI: mongoURI(), Database: "sluice_integration", Collection: ns,
		MaxPoolSize: 10, MinPoolSize: 1,
	})
	require.NoError(t, err)

	sl, err := sluice.New(ns).
		WithRedis(sluice.RedisConfig{Addrs: []string{redisAddr()}, PoolSize: 20}).
		WithSink(sk).
		WithWriteContract(inventoryContract).
		WithFlushWindow(100 * time.Millisecond).
		WithMaxBatchSize(500).
		WithBandCount(1).
		WithKeyTTL(30 * time.Second).
		Build(ctx)
	require.NoError(t, err)

	require.NoError(t, sl.DrainAndClose(ctx))

	_, err = sl.ProcessDLQ(ctx, sluice.DLQIgnore)
	assert.ErrorIs(t, err, sluice.ErrLibraryClosed)
}

// TestProcessDLQ_ReInsert_DefaultMutator verifies that the default key
// mutator produces unique keys with a recognizable suffix pattern.
func TestProcessDLQ_ReInsert_DefaultMutator(t *testing.T) {
	key1 := sluice.DefaultKeyMutator("crn_123")
	key2 := sluice.DefaultKeyMutator("crn_123")

	assert.Contains(t, key1, "crn_123_dlq_")
	assert.Contains(t, key2, "crn_123_dlq_")
	assert.NotEqual(t, key1, key2, "default mutator should produce unique keys")
}
