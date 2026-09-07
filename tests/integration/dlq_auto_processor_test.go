//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"

	sluice "github.com/hussainpithawala/sluice-go"
	"github.com/hussainpithawala/sluice-go/internal/shield"
	"github.com/hussainpithawala/sluice-go/sink/docdb"
)

// TestDLQAutoProcess_DrainsDLQAutomatically verifies that when WithDLQAutoProcess
// is configured, dead-letter records are automatically processed without manual
// intervention.
func TestDLQAutoProcess_DrainsDLQAutomatically(t *testing.T) {
	const ns = "dlq_auto_drain_integ"
	rc := dlqRedisClient(t)
	ctx := context.Background()
	cleanRedisKeys(t, rc, ns)

	sk, err := docdb.New(ctx, docdb.Config{
		URI: mongoURI(), Database: "sluice_integration", Collection: ns,
		MaxPoolSize: 10, MinPoolSize: 1,
	})
	require.NoError(t, err)

	// Contract rejects "bad_" keys during normal flush.
	selectiveContract := func(key string, payload []byte) (*sluice.WriteModel, error) {
		if len(key) >= 4 && key[:4] == "bad_" {
			return nil, fmt.Errorf("rejected key: %s", key)
		}
		return inventoryContract(key, payload)
	}

	// Build with DLQ auto-processor using DLQIgnore strategy (just discards).
	sl, err := sluice.New(ns).
		WithRedis(sluice.RedisConfig{Addrs: []string{redisAddr()}, PoolSize: 20}).
		WithSink(sk).
		WithWriteContract(selectiveContract).
		WithFlushWindow(100*time.Millisecond).
		WithMaxBatchSize(500).
		WithBandCount(1).
		WithKeyTTL(30*time.Second).
		WithDLQAutoProcess(200*time.Millisecond, sluice.DLQIgnore).
		Build(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = sl.DrainAndClose(shutCtx)
	})

	// Write good and bad keys.
	for i := 0; i < 3; i++ {
		require.NoError(t, sl.Write(ctx, fmt.Sprintf("good_%d", i), makePayload("nm_good")))
	}
	for i := 0; i < 5; i++ {
		require.NoError(t, sl.Write(ctx, fmt.Sprintf("bad_%d", i), makePayload("nm_bad")))
	}

	dlqKey := shield.DLQKey(ns, 0)

	// Wait for bad keys to land in DLQ.
	require.Eventually(t, func() bool {
		n, _ := rc.ZCard(ctx, dlqKey).Result()
		return n >= 5
	}, 5*time.Second, 50*time.Millisecond, "expected 5 keys in DLQ")

	// Auto-processor should drain DLQ within a few intervals.
	require.Eventually(t, func() bool {
		n, _ := rc.ZCard(ctx, dlqKey).Result()
		return n == 0
	}, 5*time.Second, 100*time.Millisecond, "DLQ should be auto-drained")
}

// TestDLQAutoProcess_UpsertStrategyWritesToMongo verifies that the auto-processor
// with DLQUpsert strategy re-runs the contract and writes documents to MongoDB.
func TestDLQAutoProcess_UpsertStrategyWritesToMongo(t *testing.T) {
	const ns = "dlq_auto_upsert_integ"
	rc := dlqRedisClient(t)
	ctx := context.Background()
	cleanRedisKeys(t, rc, ns)

	sk, err := docdb.New(ctx, docdb.Config{
		URI: mongoURI(), Database: "sluice_integration", Collection: ns,
		MaxPoolSize: 10, MinPoolSize: 1,
	})
	require.NoError(t, err)

	// Track how many times the contract is called for each key. On normal flush,
	// bad_ keys are rejected. On DLQ processing, we accept them (simulating a
	// transient issue that's now resolved).
	callCount := 0
	transientContract := func(key string, payload []byte) (*sluice.WriteModel, error) {
		if len(key) >= 4 && key[:4] == "bad_" {
			callCount++
			// First call = normal flush, reject. Second call = DLQ upsert, accept.
			if callCount <= 3 {
				return nil, fmt.Errorf("transient error for key: %s", key)
			}
		}
		return inventoryContract(key, payload)
	}

	sl, err := sluice.New(ns).
		WithRedis(sluice.RedisConfig{Addrs: []string{redisAddr()}, PoolSize: 20}).
		WithSink(sk).
		WithWriteContract(transientContract).
		WithFlushWindow(100*time.Millisecond).
		WithMaxBatchSize(500).
		WithBandCount(1).
		WithKeyTTL(30*time.Second).
		WithDLQAutoProcess(200*time.Millisecond, sluice.DLQUpsert).
		Build(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = sl.DrainAndClose(shutCtx)
	})

	// Write bad keys that will fail on first attempt.
	for i := 0; i < 3; i++ {
		require.NoError(t, sl.Write(ctx, fmt.Sprintf("bad_%d", i), makePayload("nm_upsert")))
	}

	dlqKey := shield.DLQKey(ns, 0)

	// Wait for bad keys to land in DLQ.
	require.Eventually(t, func() bool {
		n, _ := rc.ZCard(ctx, dlqKey).Result()
		return n >= 3
	}, 5*time.Second, 50*time.Millisecond, "expected 3 keys in DLQ")

	// Auto-processor should process them with Upsert and write to Mongo.
	coll := mongoCollection(t, ns)
	require.Eventually(t, func() bool {
		n := countDocs(t, coll, bson.M{"nudge_master_id": "nm_upsert"})
		return n >= 3
	}, 10*time.Second, 200*time.Millisecond, "expected 3 documents in MongoDB from DLQ upsert")

	// DLQ should be empty after processing.
	require.Eventually(t, func() bool {
		n, _ := rc.ZCard(ctx, dlqKey).Result()
		return n == 0
	}, 5*time.Second, 100*time.Millisecond, "DLQ should be empty after auto-upsert")
}

// TestDLQAutoProcess_StopsCleanlyOnShutdown verifies that DrainAndClose
// properly stops the DLQ auto-processor goroutine without hanging.
func TestDLQAutoProcess_StopsCleanlyOnShutdown(t *testing.T) {
	const ns = "dlq_auto_shutdown_integ"
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
		WithFlushWindow(100*time.Millisecond).
		WithMaxBatchSize(500).
		WithBandCount(1).
		WithKeyTTL(30*time.Second).
		WithDLQAutoProcess(100*time.Millisecond, sluice.DLQUpsert).
		Build(ctx)
	require.NoError(t, err)

	// Let the processor tick a few times.
	time.Sleep(350 * time.Millisecond)

	// DrainAndClose must complete promptly.
	done := make(chan error, 1)
	go func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		done <- sl.DrainAndClose(shutCtx)
	}()

	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("DrainAndClose did not return — DLQ auto-processor may be stuck")
	}
}

// dlqProcessCounter counts records seen by the DLQ auto-processor. It exists
// so tests can assert that processing HAPPENED rather than trying to catch the
// DLQ sorted set at a nonzero depth — the auto-processor drains on its own
// ticker, so any poll for a transient depth is inherently racy.
type dlqProcessCounter struct {
	mu        sync.Mutex
	processed int
}

func (c *dlqProcessCounter) RecordDLQProcess(_, _ string, processed, _, _ int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.processed += processed
}

func (c *dlqProcessCounter) total() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.processed
}

func (c *dlqProcessCounter) RecordWrite(string)                                    {}
func (c *dlqProcessCounter) RecordDegradedWrite(string, error)                     {}
func (c *dlqProcessCounter) RecordRedisOp(string, string, time.Duration, error)    {}
func (c *dlqProcessCounter) RecordFlush(string, string, int, time.Duration, error) {}
func (c *dlqProcessCounter) RecordDirtyQueueDepth(string, string, int)             {}
func (c *dlqProcessCounter) RecordContractError(string, string, error)             {}
func (c *dlqProcessCounter) RecordDeadLetter(string, string, int)                  {}
func (c *dlqProcessCounter) RecordWarmUp(string, time.Duration, error)             {}
func (c *dlqProcessCounter) RecordRead(string, time.Duration, bool, error)         {}
func (c *dlqProcessCounter) RecordHotSetSize(string, int)                          {}

// TestDLQAutoProcess_ContinuesAfterProcessingError verifies that the
// auto-processor doesn't stop if an individual DLQ processing cycle fails.
func TestDLQAutoProcess_ContinuesAfterProcessingError(t *testing.T) {
	const ns = "dlq_auto_resilient_integ"
	rc := dlqRedisClient(t)
	ctx := context.Background()
	cleanRedisKeys(t, rc, ns)

	sk, err := docdb.New(ctx, docdb.Config{
		URI: mongoURI(), Database: "sluice_integration", Collection: ns,
		MaxPoolSize: 10, MinPoolSize: 1,
	})
	require.NoError(t, err)

	// Contract always rejects "bad_" keys — they stay in DLQ even after processing.
	permanentReject := func(key string, payload []byte) (*sluice.WriteModel, error) {
		if len(key) >= 4 && key[:4] == "bad_" {
			return nil, fmt.Errorf("permanent reject: %s", key)
		}
		return inventoryContract(key, payload)
	}

	counter := &dlqProcessCounter{}

	sl, err := sluice.New(ns).
		WithRedis(sluice.RedisConfig{Addrs: []string{redisAddr()}, PoolSize: 20}).
		WithSink(sk).
		WithWriteContract(permanentReject).
		WithMetrics(counter).
		WithFlushWindow(100*time.Millisecond).
		WithMaxBatchSize(500).
		WithBandCount(1).
		WithKeyTTL(30*time.Second).
		WithDLQAutoProcess(150*time.Millisecond, sluice.DLQIgnore).
		Build(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = sl.DrainAndClose(shutCtx)
	})

	// Write bad keys.
	for i := 0; i < 2; i++ {
		require.NoError(t, sl.Write(ctx, fmt.Sprintf("bad_%d", i), makePayload("nm_fail")))
	}

	dlqKey := shield.DLQKey(ns, 0)

	// Assert on the processor's own accounting, not on DLQ depth. The
	// auto-processor drains on a 150ms ticker while the flush window is 100ms,
	// so a poll for "DLQ currently holds 2 records" can legitimately never
	// observe a nonzero depth even when both records went through — the
	// records land and are drained between two polls. Counting processed
	// records is race-free: it only ever increases, so a poll cannot miss it.
	require.Eventually(t, func() bool {
		return counter.total() >= 2
	}, 5*time.Second, 50*time.Millisecond, "first batch should be processed out of the DLQ")

	require.Eventually(t, func() bool {
		n, _ := rc.ZCard(ctx, dlqKey).Result()
		return n == 0
	}, 5*time.Second, 100*time.Millisecond, "DLQ should be drained by Ignore strategy")

	// Write more bad keys — processor should still be running.
	for i := 10; i < 12; i++ {
		require.NoError(t, sl.Write(ctx, fmt.Sprintf("bad_%d", i), makePayload("nm_fail2")))
	}

	// The point of the test: a further cycle runs after the first one, so the
	// cumulative count climbs past the first batch's 2.
	require.Eventually(t, func() bool {
		return counter.total() >= 4
	}, 5*time.Second, 50*time.Millisecond, "second batch should also be processed — processor is still alive")

	require.Eventually(t, func() bool {
		n, _ := rc.ZCard(ctx, dlqKey).Result()
		return n == 0
	}, 5*time.Second, 100*time.Millisecond, "second batch should also be drained")
}
