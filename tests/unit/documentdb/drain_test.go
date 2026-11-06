package documentdb_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sluice "github.com/hussainpithawala/sluice-go"
	"github.com/hussainpithawala/sluice-go/internal/shield"
	"github.com/hussainpithawala/sluice-go/sink"
)

// ── controllable sink ───────────────────────────────────────────────────────

type controllableSink struct {
	mu            sync.Mutex
	calls         int
	flushedKeys   []string
	blockAfterN   int // block BulkWrite after N successful calls (0 = never block)
	failForN      int // fail the first N calls (0 = never fail)
	permanentKeys map[string]bool
	blocked       chan struct{} // closed when the sink enters its blocked state
	release       chan struct{} // close to unblock
}

func newControllableSink() *controllableSink {
	return &controllableSink{
		blocked: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (c *controllableSink) BulkWrite(ctx context.Context, models []sink.WriteModel) (*sink.BulkWriteResult, error) {
	c.mu.Lock()
	c.calls++
	call := c.calls
	failFor := c.failForN
	blockAfter := c.blockAfterN
	c.mu.Unlock()

	if failFor > 0 && call <= failFor {
		return nil, errors.New("simulated sink failure")
	}

	if blockAfter > 0 && call > blockAfter {
		select {
		case <-c.blocked:
		default:
			close(c.blocked)
		}
		select {
		case <-c.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	var sinkErrors []sink.SinkError
	var flushed []string
	c.mu.Lock()
	for _, m := range models {
		if c.permanentKeys[m.CorrelationKey] {
			sinkErrors = append(sinkErrors, sink.SinkError{
				CorrelationKey: m.CorrelationKey,
				Code:           11000,
				Class:          sink.ClassPermanent,
				Err:            errors.New("permanent error"),
			})
		} else {
			flushed = append(flushed, m.CorrelationKey)
		}
	}
	c.flushedKeys = append(c.flushedKeys, flushed...)
	c.mu.Unlock()

	return &sink.BulkWriteResult{
		UpsertedCount: int64(len(flushed)),
		Errors:        sinkErrors,
	}, nil
}

func (c *controllableSink) Write(_ context.Context, _ sink.WriteModel) error { return nil }
func (c *controllableSink) Ping(_ context.Context) error                     { return nil }
func (c *controllableSink) Close(_ context.Context) error                    { return nil }
func (c *controllableSink) ClassifiesErrors()                                {}

func (c *controllableSink) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// ── helpers ─────────────────────────────────────────────────────────────────

const (
	drainBands    = 4
	drainBatch    = 10
	drainFlushWin = time.Hour // keep the timer out of the way; we drain manually
)

func drainContract(_ string, payload []byte) (*sluice.WriteModel, error) {
	return &sluice.WriteModel{Update: string(payload), Upsert: true}, nil
}

func buildDrainSluice(t *testing.T, ns string, sk sink.FlushSink, opts ...func(*sluice.Builder)) *sluice.Sluice {
	t.Helper()
	ctx := context.Background()
	b := sluice.New(ns).
		WithRedis(sluice.RedisConfig{Addrs: []string{testRedisAddr}}).
		WithSink(sk).
		WithWriteContract(drainContract).
		WithFlushWindow(drainFlushWin).
		WithMaxBatchSize(drainBatch).
		WithBandCount(drainBands).
		WithKeyTTL(30 * time.Second).
		WithActivityWindow(4 * time.Hour)
	for _, opt := range opts {
		opt(b)
	}
	sl, err := b.Build(ctx)
	require.NoError(t, err)
	return sl
}

func seedKeys(t *testing.T, sl *sluice.Sluice, prefix string, n int) int {
	t.Helper()
	ctx := context.Background()
	written := 0
	for i := 0; i < n; i++ {
		ck := fmt.Sprintf("%s_%04d", prefix, i)
		if err := sl.Write(ctx, ck, []byte(fmt.Sprintf(`{"k":"%s"}`, ck))); err == nil {
			written++
		}
	}
	return written
}

func totalDirtyDepth(t *testing.T, rc *redis.Client, ns string, bands int) int64 {
	t.Helper()
	var total int64
	ctx := context.Background()
	for band := 0; band < bands; band++ {
		n, err := rc.ZCard(ctx, shield.DirtyKey(ns, band)).Result()
		if err == nil {
			total += n
		}
	}
	return total
}

// ── tests ───────────────────────────────────────────────────────────────────

func TestDrain_FullBacklogFlushed(t *testing.T) {
	const ns = "drain_full_backlog"
	rc := redisClient(t)
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	cs := newControllableSink()
	sl := buildDrainSluice(t, ns, cs)

	n := 3 * drainBatch
	written := seedKeys(t, sl, "full", n)
	require.Equal(t, n, written)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dr, err := sl.DrainAndCloseWithResult(ctx)
	require.NoError(t, err)

	assert.Equal(t, int64(n), dr.RecordsFlushed, "all records must be flushed")
	assert.Zero(t, dr.RecordsAbandoned, "nothing should be abandoned")
	assert.Zero(t, dr.RecordsDeadLettered)
	assert.Nil(t, dr.FinalError)
	assert.Zero(t, totalDirtyDepth(t, rc, ns, drainBands))
}

func TestDrain_DeadlineRespected(t *testing.T) {
	const ns = "drain_deadline"
	rc := redisClient(t)
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	cs := newControllableSink()
	cs.blockAfterN = 1 // allow one batch, then block
	sl := buildDrainSluice(t, ns, cs)

	n := 3 * drainBatch
	written := seedKeys(t, sl, "deadline", n)
	require.Equal(t, n, written)

	deadline := 500 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()

	start := time.Now()
	dr, err := sl.DrainAndCloseWithResult(ctx)
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.True(t, errors.Is(err, sluice.ErrIncompleteDrain), "must return ErrIncompleteDrain")

	var de *sluice.DrainError
	assert.True(t, errors.As(err, &de), "must be unwrappable to *DrainError")

	assert.Greater(t, dr.RecordsFlushed, int64(0), "at least one batch must flush")
	assert.Greater(t, dr.RecordsAbandoned, int64(0), "some records must be abandoned")
	assert.Less(t, elapsed, deadline+500*time.Millisecond, "must return within deadline + 500ms")
}

func TestDrain_RestartRecovery(t *testing.T) {
	const ns = "drain_restart"
	rc := redisClient(t)
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	cs := newControllableSink()
	cs.blockAfterN = 1
	sl := buildDrainSluice(t, ns, cs)

	n := 3 * drainBatch
	seedKeys(t, sl, "restart", n)

	// First drain: short deadline, leaves records behind
	ctx1, cancel1 := context.WithTimeout(context.Background(), 300*time.Millisecond)
	dr1, _ := sl.DrainAndCloseWithResult(ctx1)
	cancel1()
	close(cs.release) // unblock for cleanup

	abandoned := dr1.RecordsAbandoned
	require.Greater(t, abandoned, int64(0), "first drain must leave records")

	// Second instance: full drain with healthy sink
	cs2 := newControllableSink()
	sl2 := buildDrainSluice(t, ns, cs2)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()
	dr2, err := sl2.DrainAndCloseWithResult(ctx2)
	require.NoError(t, err)

	totalFlushed := dr1.RecordsFlushed + dr2.RecordsFlushed
	assert.Equal(t, int64(n), totalFlushed, "all records must eventually be flushed across restarts")
	assert.Zero(t, dr2.RecordsAbandoned)
}

func TestDrain_TransientThenRecover(t *testing.T) {
	const ns = "drain_transient"
	rc := redisClient(t)
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	cs := newControllableSink()
	cs.failForN = 2 // first 2 BulkWrite calls fail
	sl := buildDrainSluice(t, ns, cs)

	n := drainBatch
	seedKeys(t, sl, "transient", n)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dr, err := sl.DrainAndCloseWithResult(ctx)
	require.NoError(t, err)

	assert.Equal(t, int64(n), dr.RecordsFlushed)
	assert.Zero(t, dr.RecordsAbandoned)
	assert.Nil(t, dr.FinalError)
}

func TestDrain_SinkDownBackoffBounded(t *testing.T) {
	const ns = "drain_backoff"
	rc := redisClient(t)
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	cs := newControllableSink()
	cs.failForN = 10000 // always fail
	sl := buildDrainSluice(t, ns, cs)

	seedKeys(t, sl, "backoff", drainBatch)

	deadline := time.Second
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	dr, err := sl.DrainAndCloseWithResult(ctx)

	require.Error(t, err)
	assert.True(t, errors.Is(err, sluice.ErrIncompleteDrain))
	assert.Greater(t, dr.RecordsAbandoned, int64(0))

	// Backoff schedule: 50ms, 100ms, 200ms, 400ms, 800ms, 1000ms, ...
	// In 1s with ~50ms per flush call, we expect roughly 5-8 calls per band,
	// not hundreds. With 4 bands, total calls should be bounded.
	calls := cs.callCount()
	assert.Less(t, calls, 100, "calls must be bounded by backoff, got %d", calls)
}

func TestDrain_PermanentErrorsDeadLettered(t *testing.T) {
	const ns = "drain_permanent"
	rc := redisClient(t)
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	cs := newControllableSink()
	cs.permanentKeys = map[string]bool{
		"perm_0000": true,
		"perm_0001": true,
		"perm_0002": true,
	}
	sl := buildDrainSluice(t, ns, cs)

	goodN := 7
	seedKeys(t, sl, "perm", goodN+3) // 3 permanent + 7 good = 10

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dr, err := sl.DrainAndCloseWithResult(ctx)
	require.NoError(t, err)

	assert.Equal(t, int64(goodN), dr.RecordsFlushed)
	assert.Equal(t, int64(3), dr.RecordsDeadLettered)
	assert.Zero(t, dr.RecordsAbandoned)
}

func TestDrain_AccountingIdentity(t *testing.T) {
	const ns = "drain_accounting"
	rc := redisClient(t)
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	cs := newControllableSink()
	cs.permanentKeys = map[string]bool{"acct_0000": true, "acct_0001": true}
	cs.blockAfterN = 2 // allow 2 batches then block

	// Use a large MaxBatchSize so volume signals do not fire during seeding;
	// the drain is the only path that flushes records.
	sl := buildDrainSluice(t, ns, cs, func(b *sluice.Builder) {
		b.WithMaxBatchSize(1000)
	})

	n := 50
	written := seedKeys(t, sl, "acct", n)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dr, _ := sl.DrainAndCloseWithResult(ctx)
	close(cs.release) // unblock for cleanup

	total := dr.RecordsFlushed + dr.RecordsAbandoned + dr.RecordsDeadLettered
	assert.Equal(t, int64(written), total,
		"accounting identity: flushed(%d) + abandoned(%d) + dead_lettered(%d) = %d, want %d",
		dr.RecordsFlushed, dr.RecordsAbandoned, dr.RecordsDeadLettered, total, written)
}

func TestDrain_ConcurrentWritesDuringShutdown(t *testing.T) {
	const ns = "drain_concurrent"
	rc := redisClient(t)
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	cs := newControllableSink()
	sl := buildDrainSluice(t, ns, cs, func(b *sluice.Builder) {
		b.WithFlushWindow(50 * time.Millisecond) // fast flushes
	})

	// Pre-seed some keys
	seedKeys(t, sl, "pre", drainBatch)

	// Hammer writes concurrently with drain
	var wg sync.WaitGroup
	var accepted atomic.Int64
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ck := fmt.Sprintf("concurrent_%04d", i)
			if err := sl.Write(context.Background(), ck, []byte(`{"c":1}`)); err == nil {
				accepted.Add(1)
			}
		}(i)
	}

	// Start drain while writes are in flight
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dr, _ := sl.DrainAndCloseWithResult(ctx)
	wg.Wait()

	// Writes that returned nil are either flushed or abandoned.
	// Writes that got ErrLibraryClosed are not counted.
	total := dr.RecordsFlushed + dr.RecordsAbandoned + dr.RecordsDeadLettered
	require.GreaterOrEqual(t, total, int64(drainBatch), "at least the pre-seeded keys must be accounted for")
}

func TestDrain_HotColdTTL(t *testing.T) {
	const ns = "drain_hotcold"
	rc := redisClient(t)
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	cs := newControllableSink()
	sl := buildDrainSluice(t, ns, cs, func(b *sluice.Builder) {
		b.WithHotAwareFlush(true).
			WithKeyTTL(10 * time.Second).
			WithActivityWindow(time.Hour)
	})

	ctx := context.Background()
	// Mark "hot_001" as hot
	require.NoError(t, sl.Write(ctx, "hot_001", []byte(`{"t":"hot"}`)))
	require.NoError(t, sl.Write(ctx, "cold_001", []byte(`{"t":"cold"}`)))

	// Set hot marker via direct shield access is not possible from external test.
	// Instead we rely on HotAwareFlush: the cold key gets KeyTTL, the hot key
	// is not marked hot (no HotLoad), so both get KeyTTL. This test validates
	// the drain path exercises commitFlushed normally.
	drainCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dr, err := sl.DrainAndCloseWithResult(drainCtx)
	require.NoError(t, err)
	assert.Equal(t, int64(2), dr.RecordsFlushed)
	assert.Zero(t, dr.RecordsAbandoned)

	// Both keys should have a TTL after flush
	for _, ck := range []string{"hot_001", "cold_001"} {
		band := shield.BandForKey(ck, drainBands)
		pttl, err := rc.PTTL(ctx, shield.PayloadKey(ns, band, ck)).Result()
		require.NoError(t, err)
		assert.Greater(t, pttl, time.Duration(0), "%s should have a post-flush TTL", ck)
	}
}

func TestDrain_SecondCallReturnsNil(t *testing.T) {
	const ns = "drain_second_call"
	rc := redisClient(t)
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	cs := newControllableSink()
	cs.blockAfterN = 1
	sl := buildDrainSluice(t, ns, cs)

	seedKeys(t, sl, "second", 3*drainBatch)

	// First drain: short deadline → incomplete
	ctx1, cancel1 := context.WithTimeout(context.Background(), 300*time.Millisecond)
	_, err1 := sl.DrainAndCloseWithResult(ctx1)
	cancel1()
	close(cs.release)

	require.Error(t, err1)
	assert.True(t, errors.Is(err1, sluice.ErrIncompleteDrain))

	// Second drain: returns nil (already closed)
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	dr2, err2 := sl.DrainAndCloseWithResult(ctx2)
	assert.NoError(t, err2)
	assert.Zero(t, dr2.RecordsFlushed)
	assert.Zero(t, dr2.RecordsAbandoned)
}

func TestDrain_ExpiredCtxGraceClose(t *testing.T) {
	const ns = "drain_grace_close"
	rc := redisClient(t)
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	closeCalled := make(chan struct{})
	gs := &graceCloseSink{closeCalled: closeCalled}
	sl := buildDrainSluice(t, ns, gs)

	seedKeys(t, sl, "grace", drainBatch)

	// Already-expired context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _ = sl.DrainAndCloseWithResult(ctx)

	// The sink's Close must still be called (with a fresh 5s grace context)
	select {
	case <-closeCalled:
		// ok
	case <-time.After(6 * time.Second):
		t.Fatal("sink.Close was not called within grace period")
	}
}

type graceCloseSink struct {
	closeCalled chan struct{}
}

func (g *graceCloseSink) BulkWrite(_ context.Context, models []sink.WriteModel) (*sink.BulkWriteResult, error) {
	return &sink.BulkWriteResult{UpsertedCount: int64(len(models))}, nil
}
func (g *graceCloseSink) Write(_ context.Context, _ sink.WriteModel) error { return nil }
func (g *graceCloseSink) Ping(_ context.Context) error                     { return nil }
func (g *graceCloseSink) Close(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	close(g.closeCalled)
	return nil
}
func (g *graceCloseSink) ClassifiesErrors() {}
