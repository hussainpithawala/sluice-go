package documentdb_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"

	sluice "github.com/hussainpithawala/sluice-go"
	"github.com/hussainpithawala/sluice-go/internal/shield"
	sluiceprom "github.com/hussainpithawala/sluice-go/metrics/prometheus"
	"github.com/hussainpithawala/sluice-go/sink"
	"github.com/hussainpithawala/sluice-go/sink/docdb"
	sourcedocdb "github.com/hussainpithawala/sluice-go/source/docdb"
)

const (
	durabilityBands     = 2
	durabilityKeyTTL    = 10 * time.Second
	durabilityActWindow = time.Minute
)

// flushErrs collects errors passed to the OnFlush callback.
type flushErrs struct {
	mu   sync.Mutex
	errs []error
}

func (f *flushErrs) record(_ []string, _ *sluice.BulkWriteResult, err error) {
	if err == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errs = append(f.errs, err)
}

func (f *flushErrs) has(target error) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, err := range f.errs {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

func buildDurabilitySluice(t *testing.T, ns string, coll *mongo.Collection) (*sluice.Sluice, *prometheus.Registry, *flushErrs) {
	t.Helper()
	ctx := context.Background()
	sk, err := docdb.New(ctx, docdb.Config{
		URI: testMongoURI, Database: testDatabase, Collection: coll.Name(),
		MaxPoolSize: 10, MinPoolSize: 1,
	})
	require.NoError(t, err)

	reg := prometheus.NewRegistry()
	fe := &flushErrs{}
	sl, err := sluice.New(ns).
		WithRedis(sluice.RedisConfig{Addrs: []string{testRedisAddr}}).
		WithSink(sk).
		WithSource(sourcedocdb.NewSourceWithClient(sk.Client(), testDatabase, coll.Name())).
		WithWriteContract(hotContract).
		WithReadContract(hotReadContract()).
		WithHotAwareFlush(true).
		WithActivityWindow(durabilityActWindow).
		WithKeyTTL(durabilityKeyTTL).
		WithFlushWindow(50 * time.Millisecond).
		WithBandCount(durabilityBands).
		WithMetrics(sluiceprom.NewRecorderWithRegistry(ns, reg)).
		OnFlush(fe.record).
		Build(ctx)
	require.NoError(t, err)

	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = sl.DrainAndClose(closeCtx)
	})
	return sl, reg, fe
}

// outageSink simulates a datastore outage: BulkWrite fails with a nil
// result, as the real sinks do when the whole batch fails.
type outageSink struct{}

var errOutage = errors.New("simulated datastore outage")

func (outageSink) BulkWrite(context.Context, []sink.WriteModel) (*sink.BulkWriteResult, error) {
	return nil, errOutage
}
func (outageSink) Write(context.Context, sink.WriteModel) error { return errOutage }
func (outageSink) Ping(context.Context) error                   { return nil }
func (outageSink) Close(context.Context) error                  { return nil }

func TestFlush_SinkOutageDoesNotPanicOrLoseWrites(t *testing.T) {
	const ns = "flush_outage_unit"
	rc := redisClient(t)
	ctx := context.Background()
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	var (
		mu       sync.Mutex
		gotErr   error
		gotCalls int
	)
	sl, err := sluice.New(ns).
		WithRedis(sluice.RedisConfig{Addrs: []string{testRedisAddr}}).
		WithSink(outageSink{}).
		WithWriteContract(hotContract).
		WithKeyTTL(time.Second). // outage must outlast KeyTTL
		WithFlushWindow(50 * time.Millisecond).
		WithBandCount(durabilityBands).
		OnFlush(func(_ []string, result *sluice.BulkWriteResult, err error) {
			mu.Lock()
			defer mu.Unlock()
			gotCalls++
			gotErr = err
			_ = len(result.Errors) // a callback that reads result before err must not panic
		}).
		Build(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = sl.DrainAndClose(closeCtx)
	})

	const ck = "outage_001"
	require.NoError(t, sl.Write(ctx, ck, mustHotPayload(t, "v", "push", 1)))

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return gotCalls > 0
	}, 3*time.Second, 50*time.Millisecond, "callback should run for the failed flush")
	mu.Lock()
	assert.ErrorIs(t, gotErr, errOutage)
	mu.Unlock()

	// Outlast KeyTTL: the write must stay dirty and persistent throughout.
	time.Sleep(1500 * time.Millisecond)
	band := shield.BandForKey(ck, durabilityBands)
	_, err = rc.ZScore(ctx, shield.DirtyKey(ns, band), ck).Result()
	assert.NoError(t, err, "write must stay dirty during the outage")
	assert.Equal(t, time.Duration(-1), payloadTTL(t, rc, ns, ck), "unflushed payload must not expire during the outage")
}

// gatedSink blocks its first BulkWrite until released, so a test can land a
// write while a flush is in flight. It records every payload it persists.
type gatedSink struct {
	entered chan struct{} // closed when the first BulkWrite starts
	release chan struct{} // close to let the first BulkWrite finish
	once    sync.Once

	mu      sync.Mutex
	written []string // payloads, in write order
}

func newGatedSink() *gatedSink {
	return &gatedSink{entered: make(chan struct{}), release: make(chan struct{})}
}

func (g *gatedSink) BulkWrite(_ context.Context, models []sink.WriteModel) (*sink.BulkWriteResult, error) {
	first := false
	g.once.Do(func() { first = true; close(g.entered) })
	if first {
		<-g.release
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, m := range models {
		g.written = append(g.written, m.Update.(string))
	}
	return &sink.BulkWriteResult{UpsertedCount: int64(len(models))}, nil
}
func (g *gatedSink) Write(context.Context, sink.WriteModel) error { return nil }
func (g *gatedSink) Ping(context.Context) error                   { return nil }
func (g *gatedSink) Close(context.Context) error                  { return nil }

func (g *gatedSink) persisted() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.written...)
}

// TestFlush_WriteDuringInFlightFlushIsNotLost drives the commit race through
// the real engine: v2 lands while v1's BulkWrite is blocked. An unconditional
// commit would drop v2's dirty entry, so the datastore would keep v1 forever.
func TestFlush_WriteDuringInFlightFlushIsNotLost(t *testing.T) {
	const ns = "flush_race_unit"
	rc := redisClient(t)
	ctx := context.Background()
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	gs := newGatedSink()
	sl, err := sluice.New(ns).
		WithRedis(sluice.RedisConfig{Addrs: []string{testRedisAddr}}).
		WithSink(gs).
		WithWriteContract(func(_ string, payload []byte) (*sluice.WriteModel, error) {
			return &sluice.WriteModel{Update: string(payload), Upsert: true}, nil
		}).
		WithFlushWindow(50 * time.Millisecond).
		WithBandCount(durabilityBands).
		Build(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = sl.DrainAndClose(closeCtx)
	})

	const ck = "race_e2e_001"
	require.NoError(t, sl.Write(ctx, ck, []byte("v1")))

	select {
	case <-gs.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("flush of v1 never started")
	}
	require.NoError(t, sl.Write(ctx, ck, []byte("v2"))) // lands mid-flush
	close(gs.release)

	assert.Eventually(t, func() bool {
		p := gs.persisted()
		return len(p) > 0 && p[len(p)-1] == "v2"
	}, 3*time.Second, 50*time.Millisecond, "v2 must reach the datastore; got %v", gs.persisted())

	band := shield.BandForKey(ck, durabilityBands)
	assert.Eventually(t, func() bool {
		_, err := rc.ZScore(ctx, shield.DirtyKey(ns, band), ck).Result()
		return errors.Is(err, redis.Nil)
	}, 3*time.Second, 50*time.Millisecond, "v2 should be committed once flushed")
}

// recordingSink records degraded (direct) writes.
type recordingSink struct {
	mu     sync.Mutex
	direct []string
}

func (r *recordingSink) BulkWrite(_ context.Context, models []sink.WriteModel) (*sink.BulkWriteResult, error) {
	return &sink.BulkWriteResult{UpsertedCount: int64(len(models))}, nil
}
func (r *recordingSink) Write(_ context.Context, m sink.WriteModel) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.direct = append(r.direct, m.Update.(string))
	return nil
}
func (r *recordingSink) Ping(context.Context) error  { return nil }
func (r *recordingSink) Close(context.Context) error { return nil }

// TestBatchedWrite_RedisFailureFallsBackToDegradedMode: previously a failed
// batch was only logged — Write had already returned nil, so the data was
// gone. Now the writer sees the failure and degraded mode persists it.
func TestBatchedWrite_RedisFailureFallsBackToDegradedMode(t *testing.T) {
	const ns = "batch_degraded_unit"
	rc := redisClient(t)
	ctx := context.Background()
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	rs := &recordingSink{}
	sl, err := sluice.New(ns).
		WithRedis(sluice.RedisConfig{Addrs: []string{testRedisAddr}}).
		WithSink(rs).
		WithWriteContract(func(_ string, payload []byte) (*sluice.WriteModel, error) {
			return &sluice.WriteModel{Update: string(payload), Upsert: true}, nil
		}).
		WithBatchedWrites(50, 10*time.Millisecond).
		WithDegradedModeDirect(true).
		WithBandCount(durabilityBands).
		Build(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = sl.DrainAndClose(closeCtx)
	})

	const ck = "batch_degraded_001"
	// Make the journal write fail for ck only (HSET → WRONGTYPE) while the
	// dirty/DLQ sets stay readable, so degraded mode can confirm nothing is
	// pending and write directly.
	require.NoError(t, rc.Set(ctx, shield.PayloadKey(ns, shield.BandForKey(ck, durabilityBands), ck), "x", 0).Err())

	require.NoError(t, sl.Write(ctx, ck, []byte("payload-1")), "degraded mode should absorb the failure")

	rs.mu.Lock()
	defer rs.mu.Unlock()
	assert.Equal(t, []string{"payload-1"}, rs.direct, "failed batched write must reach the sink directly")
}

// buildDegradedSluice builds a degraded-mode sluice whose flushes always
// fail (so pending versions stay pending) and whose direct writes are
// recorded.
func buildDegradedSluice(t *testing.T, ns string) (*sluice.Sluice, *recordingSink) {
	t.Helper()
	rs := &recordingSink{}
	sl, err := sluice.New(ns).
		WithRedis(sluice.RedisConfig{Addrs: []string{testRedisAddr}}).
		WithSink(failingBulkSink{rs}).
		WithWriteContract(func(_ string, payload []byte) (*sluice.WriteModel, error) {
			return &sluice.WriteModel{Update: string(payload), Upsert: true}, nil
		}).
		WithDegradedModeDirect(true).
		WithFlushWindow(time.Hour). // keep the engine out of the way
		WithBandCount(durabilityBands).
		Build(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = sl.DrainAndClose(closeCtx)
	})
	return sl, rs
}

// failingBulkSink fails every flush (pending versions stay pending) but
// records direct degraded writes via the embedded recordingSink.
type failingBulkSink struct{ *recordingSink }

func (failingBulkSink) BulkWrite(context.Context, []sink.WriteModel) (*sink.BulkWriteResult, error) {
	return nil, errOutage
}

// breakJournalWrite makes the next Redis journal write for ck fail with
// WRONGTYPE (payload hash replaced by a string) without touching the
// dirty/DLQ sets the degraded-mode probe reads.
func breakJournalWrite(t *testing.T, rc *redis.Client, ns, ck string) {
	t.Helper()
	require.NoError(t, rc.Set(context.Background(),
		shield.PayloadKey(ns, shield.BandForKey(ck, durabilityBands), ck), "x", 0).Err())
}

func (r *recordingSink) directWrites() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.direct...)
}

// TestDegradedWrite_RefusedWhileOlderVersionPending is the lost update: v1
// awaits flush in Redis, the Redis write of v2 fails, and a direct write of
// v2 would later be overwritten by v1's flush. It must be refused instead.
func TestDegradedWrite_RefusedWhileOlderVersionPending(t *testing.T) {
	const ns = "degraded_pending_unit"
	rc := redisClient(t)
	ctx := context.Background()
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	sl, rs := buildDegradedSluice(t, ns)
	const ck = "degraded_pending_001"
	band := shield.BandForKey(ck, durabilityBands)

	for _, tc := range []struct {
		name   string
		setKey string // set holding the older, still-writable version
	}{
		{"awaiting flush", shield.DirtyKey(ns, band)},
		{"dead-lettered", shield.DLQKey(ns, band)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, rc.Del(ctx, shield.DirtyKey(ns, band), shield.DLQKey(ns, band)).Err())
			require.NoError(t, rc.ZAdd(ctx, tc.setKey, redis.Z{Score: 1, Member: ck}).Err()) // older v1
			breakJournalWrite(t, rc, ns, ck)

			err := sl.Write(ctx, ck, []byte("v2"))
			require.Error(t, err)
			assert.ErrorIs(t, err, sluice.ErrDegradedWriteUnsafe)
			assert.ErrorIs(t, err, sluice.ErrRedisUnavailable, "callers treating Redis errors as retryable keep working")
			assert.Empty(t, rs.directWrites(), "v2 must not be written directly while v1 can still overwrite it")
		})
	}
}

func TestDegradedWrite_DirectWhenNothingPending(t *testing.T) {
	const ns = "degraded_safe_unit"
	rc := redisClient(t)
	ctx := context.Background()
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	sl, rs := buildDegradedSluice(t, ns)
	const ck = "degraded_safe_001"
	breakJournalWrite(t, rc, ns, ck)

	require.NoError(t, sl.Write(ctx, ck, []byte("v1")))
	assert.Equal(t, []string{"v1"}, rs.directWrites(), "no pending version: direct write is safe")
}

func TestDegradedWrite_RefusedWhenPendingCannotBeRuledOut(t *testing.T) {
	const ns = "degraded_unknown_unit"
	rc := redisClient(t)
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	sl, rs := buildDegradedSluice(t, ns)

	// A dead context fails the Redis write and the probe alike — the same
	// shape as Redis being unreachable.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := sl.Write(ctx, "degraded_unknown_001", []byte("v1"))
	require.Error(t, err)
	assert.ErrorIs(t, err, sluice.ErrDegradedWriteUnsafe)
	assert.Empty(t, rs.directWrites())
}

func payloadTTL(t *testing.T, rc *redis.Client, ns, ck string) time.Duration {
	t.Helper()
	d, err := rc.PTTL(context.Background(), shield.PayloadKey(ns, shield.BandForKey(ck, durabilityBands), ck)).Result()
	require.NoError(t, err)
	return d
}

func TestFlush_AppliesPostFlushTTLByTemperature(t *testing.T) {
	const ns = "flush_ttl_unit"
	rc := redisClient(t)
	ctx := context.Background()
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	sl, _, _ := buildDurabilitySluice(t, ns, mongoCollectionForHotTest(t, "flush_ttl_docs"))

	require.NoError(t, sl.HotLoad(ctx, "ttl_hot"))
	require.NoError(t, sl.Write(ctx, "ttl_hot", mustHotPayload(t, "v", "push", 1)))
	require.NoError(t, sl.Write(ctx, "ttl_cold", mustHotPayload(t, "v", "push", 1)))

	// Both start persistent and gain a TTL only once flushed.
	assert.Eventually(t, func() bool {
		return payloadTTL(t, rc, ns, "ttl_cold") > 0 && payloadTTL(t, rc, ns, "ttl_hot") > 0
	}, 3*time.Second, 50*time.Millisecond, "flushed payloads should get a TTL")

	cold := payloadTTL(t, rc, ns, "ttl_cold")
	hot := payloadTTL(t, rc, ns, "ttl_hot")
	assert.LessOrEqual(t, cold, durabilityKeyTTL, "cold key gets KeyTTL, not ActivityWindow")
	assert.Greater(t, hot, durabilityKeyTTL, "hot key gets ActivityWindow")
	assert.LessOrEqual(t, hot, durabilityActWindow)
}

func TestFlush_MissingPayloadIsSurfacedNotDropped(t *testing.T) {
	const ns = "flush_missing_unit"
	rc := redisClient(t)
	ctx := context.Background()
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	_, reg, fe := buildDurabilitySluice(t, ns, mongoCollectionForHotTest(t, "flush_missing_docs"))

	// A dirty entry with no payload: what an eviction or manual DEL leaves.
	const ck = "lost_001"
	band := shield.BandForKey(ck, durabilityBands)
	require.NoError(t, rc.ZAdd(ctx, shield.DirtyKey(ns, band),
		redis.Z{Score: float64(time.Now().UnixMilli()), Member: ck}).Err())

	lossMetric := "sluice_" + ns + "_unflushed_expiry_total"
	assert.Eventually(t, func() bool {
		v, ok := metricValue(t, reg, lossMetric, nil)
		return ok && v == 1
	}, 3*time.Second, 50*time.Millisecond, "loss must be counted")

	assert.True(t, fe.has(sluice.ErrPayloadMissing), "loss must reach the OnFlush callback")

	_, err := rc.ZScore(ctx, shield.DLQKey(ns, band), ck).Result()
	assert.NoError(t, err, "lost key should be dead-lettered for investigation")
	_, err = rc.ZScore(ctx, shield.DirtyKey(ns, band), ck).Result()
	assert.ErrorIs(t, err, redis.Nil, "lost key should leave the dirty set")
}
