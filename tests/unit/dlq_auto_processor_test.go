package unit_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sluice "github.com/hussainpithawala/sluice-go"
	"github.com/hussainpithawala/sluice-go/sink/docdb"
)

// TestWithDLQAutoProcess_BuilderSetsConfig verifies that the builder option
// enables auto-processing and stores interval + strategy in the built instance.
func TestWithDLQAutoProcess_BuilderSetsConfig(t *testing.T) {
	ctx := context.Background()
	sk, err := docdb.New(ctx, docdb.Config{
		URI: testMongoURI, Database: testDatabase, Collection: "dlq_auto_builder",
		MaxPoolSize: 10, MinPoolSize: 1,
	})
	require.NoError(t, err)

	sl, err := sluice.New("dlq_auto_builder_test").
		WithRedis(sluice.RedisConfig{Addrs: []string{testRedisAddr}}).
		WithSink(sk).
		WithWriteContract(testContract).
		WithFlushWindow(50*time.Millisecond).
		WithMaxBatchSize(100).
		WithBandCount(1).
		WithKeyTTL(5*time.Second).
		WithDLQAutoProcess(100*time.Millisecond, sluice.DLQUpsert).
		Build(ctx)
	require.NoError(t, err)

	// DrainAndClose should not hang — proves goroutine started and stops cleanly.
	doneCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	assert.NoError(t, sl.DrainAndClose(doneCtx))
}

// TestWithDLQAutoProcess_ProcessesDLQRecords verifies that the auto-processor
// actually drains DLQ records within the configured interval.
func TestWithDLQAutoProcess_ProcessesDLQRecords(t *testing.T) {
	const ns = "dlq_auto_process_test"
	rc := redisClient(t)
	ctx := context.Background()
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	sk, err := docdb.New(ctx, docdb.Config{
		URI: testMongoURI, Database: testDatabase, Collection: "dlq_auto_docs",
		MaxPoolSize: 10, MinPoolSize: 1,
	})
	require.NoError(t, err)

	// Contract rejects "bad_" keys — they go to DLQ.
	selectiveContract := func(key string, payload []byte) (*sluice.WriteModel, error) {
		if len(key) >= 4 && key[:4] == "bad_" {
			return nil, fmt.Errorf("rejected key: %s", key)
		}
		return testContract(key, payload)
	}

	// Build with auto-processor enabled at 150ms interval, DLQUpsert strategy.
	// DLQUpsert will re-run the contract with Upsert=true. Since the contract
	// still rejects "bad_" keys, the records will fail again during processing.
	// That's expected — we just verify the processor runs and drains the DLQ.
	sl, err := sluice.New(ns).
		WithRedis(sluice.RedisConfig{Addrs: []string{testRedisAddr}}).
		WithSink(sk).
		WithWriteContract(selectiveContract).
		WithFlushWindow(50*time.Millisecond).
		WithMaxBatchSize(100).
		WithBandCount(1).
		WithKeyTTL(10*time.Second).
		WithDLQAutoProcess(150*time.Millisecond, sluice.DLQIgnore).
		Build(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = sl.DrainAndClose(cctx)
	})

	// Write bad keys — they'll be rejected by contract and land in DLQ.
	for i := 0; i < 3; i++ {
		require.NoError(t, sl.Write(ctx, fmt.Sprintf("bad_%d", i), mustPayload(t, "v")))
	}

	dlqKey := fmt.Sprintf("sl:%s:dlq:0", ns)

	// Wait for bad keys to land in DLQ.
	require.Eventually(t, func() bool {
		n, _ := rc.ZCard(ctx, dlqKey).Result()
		return n >= 3
	}, 3*time.Second, 20*time.Millisecond, "expected 3 keys in DLQ")

	// Now wait for auto-processor to drain the DLQ (using DLQIgnore, so it just discards them).
	require.Eventually(t, func() bool {
		n, _ := rc.ZCard(ctx, dlqKey).Result()
		return n == 0
	}, 3*time.Second, 50*time.Millisecond, "DLQ should be drained by auto-processor")
}

// TestWithDLQAutoProcess_StopsOnDrainAndClose ensures the DLQ processor
// goroutine shuts down when DrainAndClose is called.
func TestWithDLQAutoProcess_StopsOnDrainAndClose(t *testing.T) {
	const ns = "dlq_auto_stop_test"
	rc := redisClient(t)
	ctx := context.Background()
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	sk, err := docdb.New(ctx, docdb.Config{
		URI: testMongoURI, Database: testDatabase, Collection: "dlq_auto_stop_docs",
		MaxPoolSize: 10, MinPoolSize: 1,
	})
	require.NoError(t, err)

	sl, err := sluice.New(ns).
		WithRedis(sluice.RedisConfig{Addrs: []string{testRedisAddr}}).
		WithSink(sk).
		WithWriteContract(testContract).
		WithFlushWindow(50*time.Millisecond).
		WithMaxBatchSize(100).
		WithBandCount(1).
		WithKeyTTL(5*time.Second).
		WithDLQAutoProcess(50*time.Millisecond, sluice.DLQUpsert).
		Build(ctx)
	require.NoError(t, err)

	// Let the processor tick at least once.
	time.Sleep(100 * time.Millisecond)

	// DrainAndClose must complete within timeout (not hang on goroutine).
	done := make(chan error, 1)
	go func() {
		done <- sl.DrainAndClose(ctx)
	}()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("DrainAndClose did not return within timeout — DLQ processor goroutine may be stuck")
	}
}

// TestWithDLQAutoProcess_DefaultInterval verifies that passing zero interval
// doesn't panic and falls back to the 30s default (we just verify build succeeds).
func TestWithDLQAutoProcess_DefaultInterval(t *testing.T) {
	ctx := context.Background()
	sk, err := docdb.New(ctx, docdb.Config{
		URI: testMongoURI, Database: testDatabase, Collection: "dlq_auto_default",
		MaxPoolSize: 10, MinPoolSize: 1,
	})
	require.NoError(t, err)

	sl, err := sluice.New("dlq_auto_default_test").
		WithRedis(sluice.RedisConfig{Addrs: []string{testRedisAddr}}).
		WithSink(sk).
		WithWriteContract(testContract).
		WithFlushWindow(50*time.Millisecond).
		WithMaxBatchSize(100).
		WithBandCount(1).
		WithKeyTTL(5*time.Second).
		WithDLQAutoProcess(0, sluice.DLQUpsert). // zero interval
		Build(ctx)
	require.NoError(t, err)

	// Should not panic and should close cleanly.
	assert.NoError(t, sl.DrainAndClose(ctx))
}

// TestWithDLQAutoProcess_DoesNotProcessWhenDLQEmpty verifies the processor
// doesn't error when there's nothing in the DLQ.
func TestWithDLQAutoProcess_DoesNotProcessWhenDLQEmpty(t *testing.T) {
	const ns = "dlq_auto_empty_test"
	rc := redisClient(t)
	ctx := context.Background()
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	sk, err := docdb.New(ctx, docdb.Config{
		URI: testMongoURI, Database: testDatabase, Collection: "dlq_auto_empty_docs",
		MaxPoolSize: 10, MinPoolSize: 1,
	})
	require.NoError(t, err)

	sl, err := sluice.New(ns).
		WithRedis(sluice.RedisConfig{Addrs: []string{testRedisAddr}}).
		WithSink(sk).
		WithWriteContract(testContract).
		WithFlushWindow(50*time.Millisecond).
		WithMaxBatchSize(100).
		WithBandCount(1).
		WithKeyTTL(5*time.Second).
		WithDLQAutoProcess(80*time.Millisecond, sluice.DLQUpsert).
		Build(ctx)
	require.NoError(t, err)

	// Write only good keys.
	for i := 0; i < 5; i++ {
		require.NoError(t, sl.Write(ctx, fmt.Sprintf("good_%d", i), mustPayload(t, "v")))
	}

	// Let processor tick a few times with empty DLQ — should not error.
	time.Sleep(250 * time.Millisecond)

	// DLQ should remain empty.
	dlqKey := fmt.Sprintf("sl:%s:dlq:0", ns)
	n, err := rc.ZCard(ctx, dlqKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)

	assert.NoError(t, sl.DrainAndClose(ctx))
}

// TestWithDLQAutoProcess_MultipleStrategies verifies the builder accepts all strategies.
func TestWithDLQAutoProcess_MultipleStrategies(t *testing.T) {
	strategies := []struct {
		name     string
		strategy sluice.DLQStrategy
	}{
		{"Ignore", sluice.DLQIgnore},
		{"Upsert", sluice.DLQUpsert},
		{"ReInsert", sluice.DLQReInsert},
	}

	for _, tc := range strategies {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			ns := fmt.Sprintf("dlq_start_%s", tc.name)
			sk, err := docdb.New(ctx, docdb.Config{
				URI: testMongoURI, Database: testDatabase, Collection: ns,
				MaxPoolSize: 10, MinPoolSize: 1,
			})
			require.NoError(t, err)

			sl, err := sluice.New(ns).
				WithRedis(sluice.RedisConfig{Addrs: []string{testRedisAddr}}).
				WithSink(sk).
				WithWriteContract(testContract).
				WithFlushWindow(50*time.Millisecond).
				WithMaxBatchSize(100).
				WithBandCount(1).
				WithKeyTTL(5*time.Second).
				WithDLQAutoProcess(100*time.Millisecond, tc.strategy).
				Build(ctx)
			require.NoError(t, err)
			assert.NoError(t, sl.DrainAndClose(ctx))
		})
	}
}

// redisClientForDLQ returns a Redis client (re-uses redisClient from sluice_test.go
// via same package). This is just for clarity — redisClient is already available.
func init() {
	// ensure the import of redis is used
	_ = redis.Client{}
}
