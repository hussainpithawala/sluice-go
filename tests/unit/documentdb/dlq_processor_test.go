package documentdb_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hussainpithawala/sluice-go/internal/dlq"
	"github.com/hussainpithawala/sluice-go/internal/shield"
	"github.com/hussainpithawala/sluice-go/sink"
)

const (
	dlqProcBands  = 2
	dlqProcKeyTTL = 10 * time.Second
)

// dlqNoopMetrics satisfies dlq.MetricsRecorder.
type dlqNoopMetrics struct{}

func (dlqNoopMetrics) RecordDLQProcess(string, string, int, int, int) {}

// allFailSink returns per-key permanent errors for every model.
type allFailSink struct {
	calls atomic.Int64
}

func (s *allFailSink) BulkWrite(_ context.Context, models []sink.WriteModel) (*sink.BulkWriteResult, error) {
	s.calls.Add(1)
	res := &sink.BulkWriteResult{}
	for _, m := range models {
		res.Errors = append(res.Errors, sink.SinkError{
			CorrelationKey: m.CorrelationKey,
			Code:           11000,
			Class:          sink.ClassPermanent,
			Err:            fmt.Errorf("dup"),
		})
	}
	return res, nil
}
func (s *allFailSink) Write(context.Context, sink.WriteModel) error { return errors.New("x") }
func (s *allFailSink) Ping(context.Context) error                   { return nil }
func (s *allFailSink) Close(context.Context) error                  { return nil }
func (s *allFailSink) ClassifiesErrors()                            {}

// keyFailSink fails specific keys permanently, succeeds all others.
type keyFailSink struct {
	calls   atomic.Int64
	failSet map[string]bool
}

func (s *keyFailSink) BulkWrite(_ context.Context, models []sink.WriteModel) (*sink.BulkWriteResult, error) {
	s.calls.Add(1)
	res := &sink.BulkWriteResult{}
	for _, m := range models {
		if s.failSet[m.CorrelationKey] {
			res.Errors = append(res.Errors, sink.SinkError{
				CorrelationKey: m.CorrelationKey,
				Code:           11000,
				Class:          sink.ClassPermanent,
				Err:            fmt.Errorf("dup"),
			})
		}
	}
	return res, nil
}
func (s *keyFailSink) Write(context.Context, sink.WriteModel) error { return errors.New("x") }
func (s *keyFailSink) Ping(context.Context) error                   { return nil }
func (s *keyFailSink) Close(context.Context) error                  { return nil }
func (s *keyFailSink) ClassifiesErrors()                            {}

func dlqTestShield(t *testing.T, ns string) *shield.Shield {
	t.Helper()
	sh, err := shield.New(shield.RedisConfig{
		Addrs:       []string{testRedisAddr},
		DialTimeout: 5 * time.Second,
		PoolSize:    10,
	}, ns, dlqProcBands, dlqProcKeyTTL, 4*time.Hour, 0, false)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sh.Close() })
	return sh
}

func seedDLQ(t *testing.T, sh *shield.Shield, ns string, keys []string) {
	t.Helper()
	ctx := context.Background()
	for _, k := range keys {
		_, wErr := sh.Write(ctx, k, []byte(`{"v":1}`), nil)
		require.NoError(t, wErr)
	}
	// Wait for keys to land in dirty set, then flush to move to DLQ.
	time.Sleep(50 * time.Millisecond)
	for band := 0; band < dlqProcBands; band++ {
		require.NoError(t, sh.MoveToDeadLetter(ctx, band, keysForBand(keys, band), "test"))
	}
}

func keysForBand(keys []string, band int) []string {
	var out []string
	for _, k := range keys {
		if shield.BandForKey(k, dlqProcBands) == band {
			out = append(out, k)
		}
	}
	return out
}

func dlqDepth(t *testing.T, ns string) int64 {
	t.Helper()
	rc := redis.NewClient(&redis.Options{Addr: testRedisAddr})
	defer func() { _ = rc.Close() }()
	var total int64
	for band := 0; band < dlqProcBands; band++ {
		n, err := rc.ZCard(context.Background(), shield.DLQKey(ns, band)).Result()
		require.NoError(t, err)
		total += n
	}
	return total
}

func passThroughContract(ck string, payload []byte) (*sink.WriteModel, error) {
	return &sink.WriteModel{
		Filter: map[string]string{"_id": ck},
		Update: string(payload),
		Upsert: true,
	}, nil
}

// TestDLQProcessor_NoProgressExitsLoop proves that when every record in a
// batch fails (per-key sink errors), Process returns after one BulkWrite per
// band — not a tight loop.
func TestDLQProcessor_NoProgressExitsLoop(t *testing.T) {
	const ns = "dlq_noprogress_unit"
	rc := redisClient(t)
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	sh := dlqTestShield(t, ns)
	fs := &allFailSink{}

	keys := make([]string, 10)
	for i := range keys {
		keys[i] = fmt.Sprintf("ck-%04d", i)
	}
	seedDLQ(t, sh, ns, keys)
	require.Positive(t, dlqDepth(t, ns), "DLQ must be seeded")

	proc := dlq.NewProcessor(sh, fs, passThroughContract, dlqNoopMetrics{}, dlq.Config{
		Strategy:     dlq.Upsert,
		MaxBatchSize: 1000,
	})

	result, err := proc.Process(context.Background())
	require.NoError(t, err)

	assert.Equal(t, int64(dlqProcBands), fs.calls.Load(),
		"BulkWrite should be called at most once per band (no tight loop)")
	assert.Positive(t, result.Failed, "all records should be counted as failed")
	assert.Zero(t, result.Succeeded, "no records should succeed")

	// DLQ entries should remain (they failed, not committed).
	assert.Positive(t, dlqDepth(t, ns), "failed records stay in DLQ for next tick")
}

// TestDLQProcessor_ContractErrorCommitsDuringReplay proves that records
// whose contract fails during DLQ replay are committed (removed from DLQ)
// rather than being re-drained indefinitely.
func TestDLQProcessor_ContractErrorCommitsDuringReplay(t *testing.T) {
	const ns = "dlq_contract_commit_unit"
	rc := redisClient(t)
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	sh := dlqTestShield(t, ns)

	keys := []string{"ck-a", "ck-b", "ck-c"}
	seedDLQ(t, sh, ns, keys)
	require.Positive(t, dlqDepth(t, ns))

	alwaysFailContract := func(string, []byte) (*sink.WriteModel, error) {
		return nil, errors.New("bad payload")
	}

	// Sink should never be called — all records fail contract first.
	fs := &allFailSink{}
	proc := dlq.NewProcessor(sh, fs, alwaysFailContract, dlqNoopMetrics{}, dlq.Config{
		Strategy:     dlq.Upsert,
		MaxBatchSize: 1000,
	})

	result, err := proc.Process(context.Background())
	require.NoError(t, err)

	assert.Zero(t, fs.calls.Load(), "sink should never be called when all contracts fail")
	assert.Equal(t, len(keys), result.Failed, "all keys should be counted as failed")
	assert.Zero(t, dlqDepth(t, ns), "contract-failed keys must be removed from DLQ")
}

// TestDLQProcessor_PartialProgressContinues proves that when a batch has
// partial success, the loop re-drains for remaining records, then exits
// on the no-progress iteration.
func TestDLQProcessor_PartialProgressContinues(t *testing.T) {
	const ns = "dlq_partial_unit"
	rc := redisClient(t)
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	sh := dlqTestShield(t, ns)

	keys := make([]string, 6)
	for i := range keys {
		keys[i] = fmt.Sprintf("ck-%04d", i)
	}
	failSet := map[string]bool{keys[1]: true, keys[3]: true, keys[5]: true}
	fs := &keyFailSink{failSet: failSet}

	seedDLQ(t, sh, ns, keys)
	initialDepth := dlqDepth(t, ns)
	require.Positive(t, initialDepth)

	proc := dlq.NewProcessor(sh, fs, passThroughContract, dlqNoopMetrics{}, dlq.Config{
		Strategy:     dlq.Upsert,
		MaxBatchSize: 1000,
	})

	result, err := proc.Process(context.Background())
	require.NoError(t, err)

	assert.Positive(t, result.Succeeded, "some records should succeed")
	assert.Positive(t, result.Failed, "some records should fail")
	finalDepth := dlqDepth(t, ns)
	assert.Less(t, finalDepth, initialDepth, "successful records should be removed from DLQ")
	assert.Positive(t, finalDepth, "failed records should remain in DLQ")
}

// ─── Additional sink stubs ──────────────────────────────────────────────────

// bulkWriteErrorSink returns a top-level (non-per-key) error from BulkWrite.
type bulkWriteErrorSink struct{ calls atomic.Int64 }

func (s *bulkWriteErrorSink) BulkWrite(context.Context, []sink.WriteModel) (*sink.BulkWriteResult, error) {
	s.calls.Add(1)
	return nil, errors.New("network failure")
}
func (s *bulkWriteErrorSink) Write(context.Context, sink.WriteModel) error { return errors.New("x") }
func (s *bulkWriteErrorSink) Ping(context.Context) error                   { return nil }
func (s *bulkWriteErrorSink) Close(context.Context) error                  { return nil }
func (s *bulkWriteErrorSink) ClassifiesErrors()                            {}

// unattributedErrorSink returns per-key errors with an empty CorrelationKey.
type unattributedErrorSink struct{ calls atomic.Int64 }

func (s *unattributedErrorSink) BulkWrite(_ context.Context, models []sink.WriteModel) (*sink.BulkWriteResult, error) {
	s.calls.Add(1)
	res := &sink.BulkWriteResult{}
	for range models {
		res.Errors = append(res.Errors, sink.SinkError{
			CorrelationKey: "",
			Code:           99999,
			Class:          sink.ClassPermanent,
			Err:            fmt.Errorf("unattributed"),
		})
	}
	return res, nil
}
func (s *unattributedErrorSink) Write(context.Context, sink.WriteModel) error {
	return errors.New("x")
}
func (s *unattributedErrorSink) Ping(context.Context) error  { return nil }
func (s *unattributedErrorSink) Close(context.Context) error { return nil }
func (s *unattributedErrorSink) ClassifiesErrors()           {}

// successSink always succeeds.
type successSink struct{ calls atomic.Int64 }

func (s *successSink) BulkWrite(_ context.Context, _ []sink.WriteModel) (*sink.BulkWriteResult, error) {
	s.calls.Add(1)
	return &sink.BulkWriteResult{}, nil
}
func (s *successSink) Write(context.Context, sink.WriteModel) error { return nil }
func (s *successSink) Ping(context.Context) error                   { return nil }
func (s *successSink) Close(context.Context) error                  { return nil }
func (s *successSink) ClassifiesErrors()                            {}

// ─── HIGH severity gap tests ────────────────────────────────────────────────

// TestDLQProcessor_BulkWriteErrorPropagates proves that a top-level BulkWrite
// error (not per-key) propagates from Process as an error.
func TestDLQProcessor_BulkWriteErrorPropagates(t *testing.T) {
	const ns = "dlq_bwerr_unit"
	rc := redisClient(t)
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	sh := dlqTestShield(t, ns)
	fs := &bulkWriteErrorSink{}

	keys := []string{"ck-bwe-a", "ck-bwe-b"}
	seedDLQ(t, sh, ns, keys)
	require.Positive(t, dlqDepth(t, ns))

	proc := dlq.NewProcessor(sh, fs, passThroughContract, dlqNoopMetrics{}, dlq.Config{
		Strategy:     dlq.Upsert,
		MaxBatchSize: 1000,
	})

	_, err := proc.Process(context.Background())
	require.Error(t, err, "top-level BulkWrite error must propagate")
	assert.Contains(t, err.Error(), "bulk write")
	assert.Equal(t, int64(1), fs.calls.Load(), "BulkWrite should have been called once before error")

	// DLQ entries must remain since nothing was committed.
	assert.Positive(t, dlqDepth(t, ns), "records must stay in DLQ after BulkWrite error")
}

// TestDLQProcessor_UnattributedErrorAbortsUpsert proves that sink errors
// without a CorrelationKey abort the entire batch (CheckAttribution).
func TestDLQProcessor_UnattributedErrorAbortsUpsert(t *testing.T) {
	const ns = "dlq_unattr_unit"
	rc := redisClient(t)
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	sh := dlqTestShield(t, ns)
	fs := &unattributedErrorSink{}

	keys := []string{"ck-ua-a", "ck-ua-b"}
	seedDLQ(t, sh, ns, keys)
	require.Positive(t, dlqDepth(t, ns))

	proc := dlq.NewProcessor(sh, fs, passThroughContract, dlqNoopMetrics{}, dlq.Config{
		Strategy:     dlq.Upsert,
		MaxBatchSize: 1000,
	})

	_, err := proc.Process(context.Background())
	require.Error(t, err, "unattributed sink error must abort")
	assert.Contains(t, err.Error(), "attribution")

	// Nothing committed, DLQ intact.
	assert.Positive(t, dlqDepth(t, ns))
}

// TestDLQProcessor_ExpiredPayloadCleanedFromDLQ proves that when a DLQ
// entry's payload hash has expired (deleted from Redis), DrainDLQ removes
// the orphan from the ZSET and does not return it.
func TestDLQProcessor_ExpiredPayloadCleanedFromDLQ(t *testing.T) {
	const ns = "dlq_expired_unit"
	rc := redisClient(t)
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	sh := dlqTestShield(t, ns)

	keys := []string{"ck-exp-a", "ck-exp-b"}
	seedDLQ(t, sh, ns, keys)
	require.Positive(t, dlqDepth(t, ns))

	// Delete all payload hashes to simulate TTL expiry.
	ctx := context.Background()
	for _, k := range keys {
		hk := shield.PayloadKey(ns, shield.BandForKey(k, dlqProcBands), k)
		require.NoError(t, rc.Del(ctx, hk).Err())
	}

	// Process: DrainDLQ should find no valid payloads, clean up orphans.
	fs := &successSink{}
	proc := dlq.NewProcessor(sh, fs, passThroughContract, dlqNoopMetrics{}, dlq.Config{
		Strategy:     dlq.Upsert,
		MaxBatchSize: 1000,
	})

	result, err := proc.Process(ctx)
	require.NoError(t, err)
	assert.Zero(t, fs.calls.Load(), "no BulkWrite needed — all payloads expired")
	assert.Zero(t, result.Processed, "expired records are not counted as processed")

	// Orphan entries should have been ZRem'd by drainDLQAction.
	assert.Zero(t, dlqDepth(t, ns), "orphan DLQ entries must be cleaned up")
}

// TestDLQProcessor_ContextCancellation proves that a cancelled context
// stops processing between bands.
func TestDLQProcessor_ContextCancellation(t *testing.T) {
	const ns = "dlq_ctx_cancel_unit"
	rc := redisClient(t)
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	sh := dlqTestShield(t, ns)
	fs := &allFailSink{}

	keys := make([]string, 6)
	for i := range keys {
		keys[i] = fmt.Sprintf("ck-cc-%04d", i)
	}
	seedDLQ(t, sh, ns, keys)
	require.Positive(t, dlqDepth(t, ns))

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	proc := dlq.NewProcessor(sh, fs, passThroughContract, dlqNoopMetrics{}, dlq.Config{
		Strategy:     dlq.Upsert,
		MaxBatchSize: 1000,
	})

	_, err := proc.Process(ctx)
	require.Error(t, err, "cancelled context must cause error")
	assert.Zero(t, fs.calls.Load(), "no BulkWrite should occur with cancelled context")
}

// TestDLQProcessor_ReInsertStrategyPath proves the ReInsert strategy happy
// path with lock and backoff integration.
func TestDLQProcessor_ReInsertWithLock(t *testing.T) {
	const ns = "dlq_reinsert_lock_unit"
	rc := redisClient(t)
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	sh := dlqTestShield(t, ns)

	keys := []string{"ck-ri-a", "ck-ri-b"}
	seedDLQ(t, sh, ns, keys)
	require.Positive(t, dlqDepth(t, ns))

	fs := &successSink{}
	proc := dlq.NewProcessor(sh, fs, passThroughContract, dlqNoopMetrics{}, dlq.Config{
		Strategy:     dlq.ReInsert,
		MaxBatchSize: 1000,
		LockTTL:      5 * time.Second,
	})

	result, err := proc.Process(context.Background())
	require.NoError(t, err)
	assert.Equal(t, len(keys), result.Succeeded, "all records should be reinserted")
	assert.Zero(t, dlqDepth(t, ns), "DLQ should be empty after successful reinsert")
}

// ─── backoffDelay unit tests ────────────────────────────────────────────────

// TestBackoffDelay exercises the pure backoffDelay function via the
// processor's observable behavior: seed 1 key, run with known BackoffBase,
// check the DLQ ZSET score to verify the computed delay.
func TestDLQProcessor_BackoffDelayComputation(t *testing.T) {
	const ns = "dlq_bodelay_unit"
	rc := redisClient(t)
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	sh := dlqTestShield(t, ns)
	fs := &allFailSink{}

	keys := []string{"ck-bd-a"}
	seedDLQ(t, sh, ns, keys)

	base := 2 * time.Second
	maxDelay := 10 * time.Second
	proc := dlq.NewProcessor(sh, fs, passThroughContract, dlqNoopMetrics{}, dlq.Config{
		Strategy:     dlq.Upsert,
		MaxBatchSize: 1000,
		BackoffBase:  base,
		BackoffMax:   maxDelay,
	})

	beforeRun := time.Now()
	_, err := proc.Process(context.Background())
	require.NoError(t, err)

	// After attempt 1, the score should be ~now + base (2s).
	ctx := context.Background()
	dlqKey := shield.DLQKey(ns, shield.BandForKey("ck-bd-a", dlqProcBands))
	scores, err := rc.ZRangeByScoreWithScores(ctx, dlqKey, &redis.ZRangeBy{
		Min: "-inf", Max: "+inf",
	}).Result()
	require.NoError(t, err)
	require.Len(t, scores, 1)

	scoreMs := scores[0].Score
	expectedMin := float64(beforeRun.Add(base).UnixMilli()) - 500 // 500ms tolerance
	expectedMax := float64(beforeRun.Add(base).UnixMilli()) + 2000
	assert.Greater(t, scoreMs, expectedMin, "score should be at least now+base")
	assert.Less(t, scoreMs, expectedMax, "score should not greatly exceed now+base")

	// Verify dlq_attempts was incremented to 1.
	hk := shield.PayloadKey(ns, shield.BandForKey("ck-bd-a", dlqProcBands), "ck-bd-a")
	attStr, err := rc.HGet(ctx, hk, "dlq_attempts").Result()
	require.NoError(t, err)
	att, err := strconv.Atoi(attStr)
	require.NoError(t, err)
	assert.Equal(t, 1, att, "dlq_attempts should be 1 after first failure")
}

// TestDLQProcessor_BackoffCapsAtMax proves that exponential backoff is
// capped at BackoffMax even after many attempts.
func TestDLQProcessor_BackoffCapsAtMax(t *testing.T) {
	const ns = "dlq_bomax_unit"
	rc := redisClient(t)
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	sh := dlqTestShield(t, ns)
	fs := &allFailSink{}

	keys := []string{"ck-bm-a"}
	seedDLQ(t, sh, ns, keys)

	cfg := dlq.Config{
		Strategy:     dlq.Upsert,
		MaxBatchSize: 1000,
		BackoffBase:  time.Millisecond,
		BackoffMax:   5 * time.Millisecond,
	}

	// Run 10 iterations. After a few, the delay should be capped at BackoffMax.
	for i := 0; i < 10; i++ {
		time.Sleep(6 * time.Millisecond) // wait for backoff to elapse
		proc := dlq.NewProcessor(sh, fs, passThroughContract, dlqNoopMetrics{}, cfg)
		_, err := proc.Process(context.Background())
		require.NoError(t, err)
	}

	// Key should still be in DLQ (no MaxDLQAttempts set).
	assert.Positive(t, dlqDepth(t, ns), "key should remain in DLQ without max attempts")

	// Verify attempts were counted.
	ctx := context.Background()
	hk := shield.PayloadKey(ns, shield.BandForKey("ck-bm-a", dlqProcBands), "ck-bm-a")
	attStr, err := rc.HGet(ctx, hk, "dlq_attempts").Result()
	require.NoError(t, err)
	att, err := strconv.Atoi(attStr)
	require.NoError(t, err)
	assert.Equal(t, 10, att, "dlq_attempts should track all 10 runs")

	// The score should be roughly now + BackoffMax (not growing past it).
	dlqKey := shield.DLQKey(ns, shield.BandForKey("ck-bm-a", dlqProcBands))
	scores, err := rc.ZRangeByScoreWithScores(ctx, dlqKey, &redis.ZRangeBy{
		Min: "-inf", Max: "+inf",
	}).Result()
	require.NoError(t, err)
	require.Len(t, scores, 1)

	maxFuture := float64(time.Now().Add(cfg.BackoffMax).UnixMilli()) + 2000
	assert.Less(t, scores[0].Score, maxFuture, "score should not exceed now + BackoffMax")
}

// TestDLQProcessor_MixedDeferAndPark proves that in a single batch, some
// keys can be deferred while others exceed MaxDLQAttempts and are parked.
func TestDLQProcessor_MixedDeferAndPark(t *testing.T) {
	const ns = "dlq_mixpark_unit"
	rc := redisClient(t)
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	sh := dlqTestShield(t, ns)
	fs := &allFailSink{}

	// Seed two keys: "ck-old" and "ck-new".
	keys := []string{"ck-old", "ck-new"}
	seedDLQ(t, sh, ns, keys)
	require.Positive(t, dlqDepth(t, ns))

	cfg := dlq.Config{
		Strategy:       dlq.Upsert,
		MaxBatchSize:   1000,
		BackoffBase:    time.Millisecond,
		BackoffMax:     time.Millisecond,
		MaxDLQAttempts: 2,
	}

	// Run "ck-old" through 2 attempts so it's at the limit.
	for i := 0; i < 2; i++ {
		time.Sleep(3 * time.Millisecond)
		proc := dlq.NewProcessor(sh, fs, passThroughContract, dlqNoopMetrics{}, cfg)
		_, err := proc.Process(context.Background())
		require.NoError(t, err)
	}

	// Both keys should still be in DLQ (attempt=2 for both, deferred).
	assert.Positive(t, dlqDepth(t, ns))

	// Now manually set "ck-new" back to attempt=0 so it has room,
	// while "ck-old" stays at attempt=2 (will be parked on next run).
	ctx := context.Background()
	hk := shield.PayloadKey(ns, shield.BandForKey("ck-new", dlqProcBands), "ck-new")
	require.NoError(t, rc.HSet(ctx, hk, "dlq_attempts", 0).Err())

	time.Sleep(3 * time.Millisecond)

	// Run again: "ck-old" exceeds max (attempt 2 -> 3 > max=2) → parked.
	// "ck-new" still has attempts left (attempt 0 -> 1 ≤ max=2) → deferred.
	proc := dlq.NewProcessor(sh, fs, passThroughContract, dlqNoopMetrics{}, cfg)
	result, err := proc.Process(ctx)
	require.NoError(t, err)

	// The parked key counts as succeeded, the deferred key counts as failed.
	assert.Positive(t, result.Succeeded, "parked key should count as succeeded")
	assert.Positive(t, result.Failed, "deferred key should count as failed")
}
