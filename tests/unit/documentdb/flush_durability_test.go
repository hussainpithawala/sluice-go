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
