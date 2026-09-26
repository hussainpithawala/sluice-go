package documentdb_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"

	sluice "github.com/hussainpithawala/sluice-go"
	sluiceprom "github.com/hussainpithawala/sluice-go/metrics/prometheus"
	"github.com/hussainpithawala/sluice-go/sink/docdb"
	sourcedocdb "github.com/hussainpithawala/sluice-go/source/docdb"
)

// buildMeteredSluice is buildHotSluice wired to a Prometheus recorder on a
// private registry, with a fast hot-set sampler.
func buildMeteredSluice(t *testing.T, ns string, coll *mongo.Collection) (*sluice.Sluice, *prometheus.Registry) {
	t.Helper()
	ctx := context.Background()
	sk, err := docdb.New(ctx, docdb.Config{
		URI: testMongoURI, Database: testDatabase, Collection: coll.Name(),
		MaxPoolSize: 10, MinPoolSize: 1,
	})
	require.NoError(t, err)

	reg := prometheus.NewRegistry()
	sl, err := sluice.New(ns).
		WithRedis(sluice.RedisConfig{Addrs: []string{testRedisAddr}}).
		WithSink(sk).
		WithSource(sourcedocdb.NewSourceWithClient(sk.Client(), testDatabase, coll.Name())).
		WithWriteContract(hotContract).
		WithReadContract(hotReadContract()).
		WithIndexContract(hotIndexContract).
		WithActivityWindow(time.Minute).
		WithFlushWindow(50 * time.Millisecond).
		WithBandCount(2).
		WithKeyTTL(10 * time.Second).
		WithHotSetSampleInterval(100 * time.Millisecond).
		WithMetrics(sluiceprom.NewRecorderWithRegistry(ns, reg)).
		Build(ctx)
	require.NoError(t, err)

	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = sl.DrainAndClose(closeCtx)
	})
	return sl, reg
}

// metricValue returns the gauge value or histogram sample count of the
// series named name whose labels include want; ok is false if absent.
func metricValue(t *testing.T, reg *prometheus.Registry, name string, want map[string]string) (float64, bool) {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
	series:
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			for k, v := range want {
				if labels[k] != v {
					continue series
				}
			}
			return sampleValue(m), true
		}
	}
	return 0, false
}

func sampleValue(m *dto.Metric) float64 {
	switch {
	case m.GetGauge() != nil:
		return m.GetGauge().GetValue()
	case m.GetCounter() != nil:
		return m.GetCounter().GetValue()
	case m.GetHistogram() != nil:
		return float64(m.GetHistogram().GetSampleCount())
	}
	return 0
}

func TestGaugeSampler_EmitsHotSetSize(t *testing.T) {
	const ns = "gauge_sampler_unit"
	rc := redisClient(t)
	ctx := context.Background()
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	sl, reg := buildMeteredSluice(t, ns, mongoCollectionForHotTest(t, "gauge_sampler_docs"))

	for i := 0; i < 3; i++ {
		// HotLoad sets the marker synchronously; hydration failing for an
		// unknown key is irrelevant here.
		require.NoError(t, sl.HotLoad(ctx, fmt.Sprintf("gauge_hot_%d", i)))
	}

	name := "sluice_" + ns + "_hot_set_size"
	assert.Eventually(t, func() bool {
		v, ok := metricValue(t, reg, name, nil)
		return ok && v == 3
	}, 2*time.Second, 50*time.Millisecond, "hot_set_size should reach 3")
}

func TestRead_RecordsHotAndColdTiers(t *testing.T) {
	const ns = "read_metrics_unit"
	rc := redisClient(t)
	ctx := context.Background()
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	sl, reg := buildMeteredSluice(t, ns, mongoCollectionForHotTest(t, "read_metrics_docs"))
	name := "sluice_" + ns + "_read_duration_seconds"

	// L2 hit: journal-resident key is a hot read.
	require.NoError(t, sl.Write(ctx, "read_hot_1", mustHotPayload(t, "v", "push", 1)))
	_, err := sl.Read(ctx, "read_hot_1")
	require.NoError(t, err)
	hot, ok := metricValue(t, reg, name, map[string]string{"is_hot": "true", "error": ""})
	require.True(t, ok, "hot read series should exist")
	assert.Equal(t, float64(1), hot)

	// L3 miss: absent everywhere is a cold read carrying the error label.
	_, err = sl.Read(ctx, "read_absent_1")
	require.ErrorIs(t, err, sluice.ErrRecordNotFound)
	cold, ok := metricValue(t, reg, name, map[string]string{"is_hot": "false"})
	require.True(t, ok, "cold read series should exist")
	assert.Equal(t, float64(1), cold)
}
