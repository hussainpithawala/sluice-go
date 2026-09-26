// Package prometheus provides a Prometheus-backed implementation of the
// sluice.MetricsRecorder interface. Import this package only if you want
// to export sluice metrics to Prometheus for Grafana visualization.
//
// Usage:
//
//	rec := prometheus.NewRecorder("nudge_inventory")
//	sl, _ := sluice.New("nudge_inventory").
//	    WithMetrics(rec).
//	    Build(ctx)
//
//	// Expose metrics via HTTP for Prometheus scraping
//	http.Handle("/metrics", promhttp.Handler())
//	go http.ListenAndServe(":2112", nil)
package prometheus

import (
	"strconv"
	"time"

	sluice "github.com/hussainpithawala/sluice-go"
	"github.com/hussainpithawala/sluice-go/internal/localjournal"
	"github.com/prometheus/client_golang/prometheus"
)

// Recorder implements sluice.MetricsRecorder using Prometheus metrics.
// All metrics are registered with the default Prometheus registry on creation.
type Recorder struct {
	namespace string

	// Write path
	writeTotal         *prometheus.CounterVec
	degradedWriteTotal *prometheus.CounterVec
	redisOpDuration    *prometheus.HistogramVec
	flushDuration      *prometheus.HistogramVec
	flushBatchSize     *prometheus.HistogramVec
	dirtyQueueDepth    *prometheus.GaugeVec
	contractErrorTotal *prometheus.CounterVec
	deadLetterTotal    *prometheus.CounterVec
	dlqProcessTotal    *prometheus.CounterVec

	// Hot/cold regime
	warmupDuration *prometheus.HistogramVec
	readDuration   *prometheus.HistogramVec
	hotSetSize     *prometheus.GaugeVec

	// L1 Local Journal
	localCacheHitTotal  *prometheus.CounterVec
	localCacheMissTotal *prometheus.CounterVec
	localSetSize        *prometheus.GaugeVec
	broadcastLag        *prometheus.HistogramVec
}

// NewRecorder creates a new Prometheus-backed MetricsRecorder.
// The namespace parameter is used as the Prometheus metric namespace prefix
// (e.g., "nudge_inventory" → "sluice_nudge_inventory_write_total").
//
// All metrics are registered with the default Prometheus registry. If you
// need a custom registry, use NewRecorderWithRegistry.
func NewRecorder(namespace string) *Recorder {
	return NewRecorderWithRegistry(namespace, prometheus.DefaultRegisterer)
}

// NewRecorderWithRegistry creates a new Prometheus-backed MetricsRecorder
// with a custom Prometheus registerer. Use this if you need to isolate
// sluice metrics from other Prometheus metrics in your application.
func NewRecorderWithRegistry(namespace string, reg prometheus.Registerer) *Recorder {
	r := &Recorder{
		namespace: namespace,

		// Write path
		writeTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "sluice",
			Subsystem: namespace,
			Name:      "write_total",
			Help:      "Total number of successful writes to the Redis journal.",
		}, []string{}),

		degradedWriteTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "sluice",
			Subsystem: namespace,
			Name:      "degraded_write_total",
			Help:      "Total number of degraded writes (direct to sink when Redis is unavailable).",
		}, []string{"error"}),

		redisOpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "sluice",
			Subsystem: namespace,
			Name:      "redis_op_duration_seconds",
			Help:      "Duration of Redis operations in seconds.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"op", "error"}),

		flushDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "sluice",
			Subsystem: namespace,
			Name:      "flush_duration_seconds",
			Help:      "Duration of flush operations to the backing store in seconds.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}, []string{"band", "error"}),

		flushBatchSize: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "sluice",
			Subsystem: namespace,
			Name:      "flush_batch_size",
			Help:      "Number of keys flushed per batch operation.",
			Buckets:   []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000},
		}, []string{"band"}),

		dirtyQueueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "sluice",
			Subsystem: namespace,
			Name:      "dirty_queue_depth",
			Help:      "Current depth of the dirty queue (pending flush) per band.",
		}, []string{"band"}),

		contractErrorTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "sluice",
			Subsystem: namespace,
			Name:      "contract_error_total",
			Help:      "Total number of contract validation errors.",
		}, []string{"correlation_key"}),

		deadLetterTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "sluice",
			Subsystem: namespace,
			Name:      "dead_letter_total",
			Help:      "Total number of records routed to the dead-letter queue.",
		}, []string{"band"}),

		dlqProcessTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "sluice",
			Subsystem: namespace,
			Name:      "dlq_process_total",
			Help:      "Total number of DLQ records processed.",
		}, []string{"strategy", "outcome"}),

		// Hot/cold regime
		warmupDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "sluice",
			Subsystem: namespace,
			Name:      "warmup_duration_seconds",
			Help:      "Duration of HotLoad (warm-up) operations in seconds.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		}, []string{"error"}),

		readDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "sluice",
			Subsystem: namespace,
			Name:      "read_duration_seconds",
			Help:      "Duration of Read operations in seconds.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"is_hot", "error"}),

		hotSetSize: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "sluice",
			Subsystem: namespace,
			Name:      "hot_set_size",
			Help:      "Current number of hot correlation keys in the journal.",
		}, []string{}),

		// L1 Local Journal
		localCacheHitTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "sluice",
			Subsystem: namespace,
			Name:      "local_cache_hit_total",
			Help:      "Total number of L1 local cache hits.",
		}, []string{}),

		localCacheMissTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "sluice",
			Subsystem: namespace,
			Name:      "local_cache_miss_total",
			Help:      "Total number of L1 local cache misses.",
		}, []string{"reason"}),

		localSetSize: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "sluice",
			Subsystem: namespace,
			Name:      "local_set_size",
			Help:      "Current number of entries in the L1 local cache.",
		}, []string{}),

		broadcastLag: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "sluice",
			Subsystem: namespace,
			Name:      "broadcast_lag_seconds",
			Help:      "Lag between a write and its broadcast arrival at peer pods.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		}, []string{}),
	}

	// Register all metrics
	reg.MustRegister(
		r.writeTotal,
		r.degradedWriteTotal,
		r.redisOpDuration,
		r.flushDuration,
		r.flushBatchSize,
		r.dirtyQueueDepth,
		r.contractErrorTotal,
		r.deadLetterTotal,
		r.dlqProcessTotal,
		r.warmupDuration,
		r.readDuration,
		r.hotSetSize,
		r.localCacheHitTotal,
		r.localCacheMissTotal,
		r.localSetSize,
		r.broadcastLag,
	)

	return r
}

// ── Write path ──────────────────────────────────────────────────────────────

func (r *Recorder) RecordWrite(_ string) {
	r.writeTotal.WithLabelValues().Inc()
}

func (r *Recorder) RecordDegradedWrite(_ string, err error) {
	errStr := "unknown"
	if err != nil {
		errStr = err.Error()
	}
	r.degradedWriteTotal.WithLabelValues(errStr).Inc()
}

func (r *Recorder) RecordRedisOp(_ string, op string, duration time.Duration, err error) {
	errStr := ""
	if err != nil {
		errStr = err.Error()
	}
	r.redisOpDuration.WithLabelValues(op, errStr).Observe(duration.Seconds())
}

func (r *Recorder) RecordFlush(_ string, bandStr string, batchSize int, duration time.Duration, err error) {
	errStr := ""
	if err != nil {
		errStr = err.Error()
	}
	r.flushDuration.WithLabelValues(bandStr, errStr).Observe(duration.Seconds())
	r.flushBatchSize.WithLabelValues(bandStr).Observe(float64(batchSize))
}

func (r *Recorder) RecordDirtyQueueDepth(_ string, bandStr string, depth int) {
	r.dirtyQueueDepth.WithLabelValues(bandStr).Set(float64(depth))
}

func (r *Recorder) RecordContractError(_ string, correlationKey string, err error) {
	r.contractErrorTotal.WithLabelValues(correlationKey).Inc()
}

func (r *Recorder) RecordDeadLetter(_ string, bandStr string, count int) {
	r.deadLetterTotal.WithLabelValues(bandStr).Add(float64(count))
}

func (r *Recorder) RecordDLQProcess(_ string, strategy string, processed, succeeded, failed int) {
	r.dlqProcessTotal.WithLabelValues(strategy, "processed").Add(float64(processed))
	r.dlqProcessTotal.WithLabelValues(strategy, "succeeded").Add(float64(succeeded))
	r.dlqProcessTotal.WithLabelValues(strategy, "failed").Add(float64(failed))
}

// ── Hot/cold regime ─────────────────────────────────────────────────────────

func (r *Recorder) RecordWarmUp(_ string, duration time.Duration, err error) {
	errStr := ""
	if err != nil {
		errStr = err.Error()
	}
	r.warmupDuration.WithLabelValues(errStr).Observe(duration.Seconds())
}

func (r *Recorder) RecordRead(_ string, duration time.Duration, isHot bool, err error) {
	errStr := ""
	if err != nil {
		errStr = err.Error()
	}
	r.readDuration.WithLabelValues(strconv.FormatBool(isHot), errStr).Observe(duration.Seconds())
}

func (r *Recorder) RecordHotSetSize(_ string, size int) {
	r.hotSetSize.WithLabelValues().Set(float64(size))
}

// ── L1 Local Journal ────────────────────────────────────────────────────────

func (r *Recorder) RecordLocalCacheHit(_ string) {
	r.localCacheHitTotal.WithLabelValues().Inc()
}

func (r *Recorder) RecordLocalCacheMiss(_ string, reason localjournal.MissReason) {
	r.localCacheMissTotal.WithLabelValues(string(reason)).Inc()
}

func (r *Recorder) RecordLocalSetSize(_ string, size int) {
	r.localSetSize.WithLabelValues().Set(float64(size))
}

func (r *Recorder) RecordBroadcastLag(_ string, lag time.Duration) {
	r.broadcastLag.WithLabelValues().Observe(lag.Seconds())
}

// ── Helpers ─────────────────────────────────────────────────────────────────

// Describe implements prometheus.Collector (optional, for advanced use).
func (r *Recorder) Describe(ch chan<- *prometheus.Desc) {
	// No-op: we use MustRegister, which handles description.
}

// Collect implements prometheus.Collector (optional, for advanced use).
func (r *Recorder) Collect(ch chan<- prometheus.Metric) {
	// No-op: we use MustRegister, which handles collection.
}

// Unregister removes all sluice metrics from the Prometheus registry.
// Useful for testing or when shutting down a sluice instance.
func (r *Recorder) Unregister(reg prometheus.Registerer) {
	reg.Unregister(r.writeTotal)
	reg.Unregister(r.degradedWriteTotal)
	reg.Unregister(r.redisOpDuration)
	reg.Unregister(r.flushDuration)
	reg.Unregister(r.flushBatchSize)
	reg.Unregister(r.dirtyQueueDepth)
	reg.Unregister(r.contractErrorTotal)
	reg.Unregister(r.deadLetterTotal)
	reg.Unregister(r.dlqProcessTotal)
	reg.Unregister(r.warmupDuration)
	reg.Unregister(r.readDuration)
	reg.Unregister(r.hotSetSize)
	reg.Unregister(r.localCacheHitTotal)
	reg.Unregister(r.localCacheMissTotal)
	reg.Unregister(r.localSetSize)
	reg.Unregister(r.broadcastLag)
}

// Compile-time check that Recorder implements the MetricsRecorder interface.
// This ensures we don't miss any methods if the interface changes.
var _ sluice.MetricsRecorder = (*Recorder)(nil)
