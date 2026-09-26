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
