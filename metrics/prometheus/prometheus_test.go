package prometheus

import (
	"errors"
	"testing"
	"time"

	"github.com/hussainpithawala/sluice-go/internal/localjournal"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRecorder(t *testing.T) {
	// Use a custom registry to avoid polluting the default registry
	reg := prometheus.NewRegistry()
	rec := NewRecorderWithRegistry("test_namespace", reg)

	require.NotNil(t, rec)
	assert.Equal(t, "test_namespace", rec.namespace)
}

func TestRecorder_RecordWrite(t *testing.T) {
	reg := prometheus.NewRegistry()
	rec := NewRecorderWithRegistry("test", reg)

	rec.RecordWrite("ns")
	rec.RecordWrite("ns")

	// Gather metrics
	metrics, err := reg.Gather()
	require.NoError(t, err)

	// Find the write_total metric
	found := false
	for _, mf := range metrics {
		if mf.GetName() == "sluice_test_write_total" {
			found = true
			assert.Equal(t, float64(2), mf.GetMetric()[0].GetCounter().GetValue())
		}
	}
	assert.True(t, found, "write_total metric not found")
}

func TestRecorder_RecordRedisOp(t *testing.T) {
	reg := prometheus.NewRegistry()
	rec := NewRecorderWithRegistry("test", reg)

	rec.RecordRedisOp("ns", "write", 100*time.Millisecond, nil)
	rec.RecordRedisOp("ns", "read", 50*time.Millisecond, errors.New("timeout"))

	metrics, err := reg.Gather()
	require.NoError(t, err)

	found := false
	for _, mf := range metrics {
		if mf.GetName() == "sluice_test_redis_op_duration_seconds" {
			found = true
			assert.Len(t, mf.GetMetric(), 2) // Two different label combinations
		}
	}
	assert.True(t, found, "redis_op_duration_seconds metric not found")
}

func TestRecorder_RecordFlush(t *testing.T) {
	reg := prometheus.NewRegistry()
	rec := NewRecorderWithRegistry("test", reg)

	rec.RecordFlush("ns", "band_0", 100, 250*time.Millisecond, nil)

	metrics, err := reg.Gather()
	require.NoError(t, err)

	foundDuration := false
	foundBatchSize := false
	for _, mf := range metrics {
		if mf.GetName() == "sluice_test_flush_duration_seconds" {
			foundDuration = true
		}
		if mf.GetName() == "sluice_test_flush_batch_size" {
			foundBatchSize = true
			assert.Equal(t, float64(100), mf.GetMetric()[0].GetHistogram().GetSampleSum())
		}
	}
	assert.True(t, foundDuration, "flush_duration_seconds metric not found")
	assert.True(t, foundBatchSize, "flush_batch_size metric not found")
}

func TestRecorder_RecordLocalCache(t *testing.T) {
	reg := prometheus.NewRegistry()
	rec := NewRecorderWithRegistry("test", reg)

	rec.RecordLocalCacheHit("ns")
	rec.RecordLocalCacheHit("ns")
	rec.RecordLocalCacheMiss("ns", localjournal.MissExpired)
	rec.RecordLocalCacheMiss("ns", localjournal.MissAbsent)

	metrics, err := reg.Gather()
	require.NoError(t, err)

	foundHit := false
	foundMiss := false
	for _, mf := range metrics {
		if mf.GetName() == "sluice_test_local_cache_hit_total" {
			foundHit = true
			assert.Equal(t, float64(2), mf.GetMetric()[0].GetCounter().GetValue())
		}
		if mf.GetName() == "sluice_test_local_cache_miss_total" {
			foundMiss = true
			assert.Len(t, mf.GetMetric(), 2) // Two different miss reasons
		}
	}
	assert.True(t, foundHit, "local_cache_hit_total metric not found")
	assert.True(t, foundMiss, "local_cache_miss_total metric not found")
}

func TestRecorder_RecordRead(t *testing.T) {
	reg := prometheus.NewRegistry()
	rec := NewRecorderWithRegistry("test", reg)

	rec.RecordRead("ns", 500*time.Microsecond, true, nil)
	rec.RecordRead("ns", 10*time.Millisecond, false, nil)

	metrics, err := reg.Gather()
	require.NoError(t, err)

	found := false
	for _, mf := range metrics {
		if mf.GetName() == "sluice_test_read_duration_seconds" {
			found = true
			assert.Len(t, mf.GetMetric(), 2) // Hot and cold reads
		}
	}
	assert.True(t, found, "read_duration_seconds metric not found")
}

func TestRecorder_Unregister(t *testing.T) {
	reg := prometheus.NewRegistry()
	rec := NewRecorderWithRegistry("test", reg)

	// Record some metrics
	rec.RecordWrite("ns")

	// Verify metrics exist
	metrics, err := reg.Gather()
	require.NoError(t, err)
	assert.NotEmpty(t, metrics)

	// Unregister
	rec.Unregister(reg)

	// Verify metrics are gone
	metrics, err = reg.Gather()
	require.NoError(t, err)
	assert.Empty(t, metrics)
}
