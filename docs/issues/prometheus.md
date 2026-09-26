# RFP #16: Prometheus Metrics Export for `sluice-go`

**Status:** Proposed  
**Target Release:** v1.0.8 (Consolidated Release)  
**Dependencies:** Core `MetricsRecorder` interface (v1.0.7), L1 Local Journal metrics (v1.0.8)

---

## 1. Context & Problem Statement

`sluice` exposes a comprehensive `MetricsRecorder` interface that captures every meaningful event in the system — write throughput, flush latency, dirty queue depth, hot/cold read ratios, L1 cache hit/miss rates, DLQ processing outcomes, and broadcast convergence lag. However, the current implementation only ships with a `noopMetrics` default and a `logMetrics` example that writes structured logs via `slog`.

This creates a significant operational gap for production adopters:

1. **No Native Observability Path**: Teams must write their own `MetricsRecorder` implementation to bridge `sluice` internals into their monitoring stack (Prometheus, Datadog, CloudWatch, etc.). This is a non-trivial integration effort that every adopter must repeat independently.
2. **Logs Are Insufficient for Dashboards**: Structured logs are excellent for debugging, but they cannot power real-time dashboards, alerting rules, or SLO tracking. Operators need time-series metrics with proper labels to build Grafana panels like "p99 flush latency per band" or "L1 cache hit ratio over time."
3. **Missing Production-Grade Reference**: Without a first-class Prometheus exporter, adopters lack a reference implementation that demonstrates the correct metric types (counter vs. gauge vs. histogram), label cardinality discipline, and bucket selection for each signal.
4. **Fragmented Ecosystem**: Different adopters building ad-hoc Prometheus exporters leads to inconsistent metric naming, divergent label schemas, and duplicated effort across the community.

The `sluice` library has matured into a production-grade data platform with multiple deployment topologies (Full Pod, Reader-Only Pod, Bulk Pre-Warmer). It deserves a first-class, zero-config observability story that matches its architectural sophistication.

---

## 2. The Goal

Introduce a **`metrics/prometheus` sub-package** that provides a production-ready, opt-in Prometheus exporter for `sluice`. This exporter must:

1. **Implement the full `MetricsRecorder` interface** — every method, including the v1.0.8 L1 Local Journal metrics (`RecordLocalCacheHit`, `RecordLocalCacheMiss`, `RecordLocalSetSize`, `RecordBroadcastLag`).
2. **Use correct Prometheus metric types** — counters for totals, gauges for current state, histograms for latencies and sizes with carefully chosen buckets.
3. **Expose rich, queryable labels** — namespace, band, operation, error, hot/cold, miss reason, DLQ strategy/outcome — enabling powerful Grafana queries like `rate(sluice_nudge_inventory_flush_duration_seconds_bucket{band="3"}[5m])`.
4. **Be opt-in and dependency-isolated** — the Prometheus client library is only pulled in when the sub-package is imported, keeping the core `sluice` dependency tree lean for users who don't need Prometheus.
5. **Support custom registries** — for users who want to isolate `sluice` metrics from other application metrics or run multiple `sluice` instances in the same process.
6. **Ship with a reference Grafana dashboard** — a pre-built JSON dashboard covering all critical operational views.

---

## 3. Proposed Design & Architecture

### 3.1 Package Structure
    sluice-go/
    ├── docker-compose.yml
    ├── metrics/
    │ └── prometheus/
    │ ├── prometheus.go # Recorder implementation
    │ └── prometheus_test.go # Unit tests
    ├── dashboards/
    │ └── sluice-overview.json # Reference Grafana dashboard
    └── examples/
    └── nudge_prometheus/
    └── main.go # End-to-end example
    ├── monitoring/
    │   ├── prometheus/
    │   │   └── prometheus.yml
    │   └── grafana/
    │       └── provisioning/
    │           ├── datasources/
    │           │   └── prometheus.yml
    │           └── dashboards/
    │               └── dashboard.yml
    

The sub-package lives under `metrics/prometheus` to leave room for future exporters (e.g., `metrics/datadog`, `metrics/cloudwatch`) without namespace collisions.

### 3.2 The `Recorder` Struct

```go
package prometheus

import (
	"time"
	"github.com/hussainpithawala/sluice-go/internal/localjournal"
	"github.com/prometheus/client_golang/prometheus"
)

// Recorder implements sluice.MetricsRecorder using Prometheus metrics.
// All metrics are registered with the default Prometheus registry on creation.
type Recorder struct {
	namespace string

	// Write path
	writeTotal          *prometheus.CounterVec
	degradedWriteTotal  *prometheus.CounterVec
	redisOpDuration     *prometheus.HistogramVec
	flushDuration       *prometheus.HistogramVec
	flushBatchSize      *prometheus.HistogramVec
	dirtyQueueDepth     *prometheus.GaugeVec
	contractErrorTotal  *prometheus.CounterVec
	deadLetterTotal     *prometheus.CounterVec
	dlqProcessTotal     *prometheus.CounterVec

	// Hot/cold regime
	warmupDuration *prometheus.HistogramVec
	readDuration   *prometheus.HistogramVec
	hotSetSize     *prometheus.GaugeVec

	// L1 Local Journal (v1.0.8)
	localCacheHitTotal  *prometheus.CounterVec
	localCacheMissTotal *prometheus.CounterVec
	localSetSize        *prometheus.GaugeVec
	broadcastLag        *prometheus.HistogramVec
}
```

## 3.4 Metric Naming Convention

All metrics follow the pattern: sluice_{namespace}_{metric_name}_{unit}

|        Component       |          Example         |
|:----------------------:|:------------------------:|
| Fixed prefix           | sluice                   |
| Namespace (sub-system) | nudge_inventory          |
| Metric name            | flush_duration           |
| Unit suffix            | _seconds, _total, _bytes |
| Local Testing          | Requires mock services   |
| Custom Logic           | Lambda functions         |

Example full metric names:

1. sluice_nudge_inventory_write_total
2. sluice_nudge_inventory_flush_duration_seconds
3. sluice_nudge_inventory_local_cache_hit_total
4. sluice_nudge_inventory_broadcast_lag_seconds

## 3.5 Label Design
Labels are carefully chosen to maximize query power while avoiding cardinality explosion:

| Label           	| Applied To                               	| Cardinality        	| Rationale                                                 	|              	|                	|              	|        	|
|-----------------	|------------------------------------------	|--------------------	|-----------------------------------------------------------	|--------------	|----------------	|--------------	|--------	|
| band            	| flush, dirty queue, dead letter          	| Low (16 default)   	| Per-band visibility is essential for diagnosing hot bands 	|              	|                	|              	|        	|
| op              	| Redis operations                         	| Low (~10 ops)      	| Distinguishes write"                                      	|  "read"      	|  "readjournal" 	|  "readfresh" 	|  etc." 	|
| error           	| Redis ops, degraded writes, warmup, read 	| Medium             	| Empty string for success, error message for failures      	|              	|                	|              	|        	|
| is_hot          	| Read duration                            	| 2 (true/false)     	| Critical for monitoring hot/cold regime health            	|              	|                	|              	|        	|
| reason          	| L1 cache miss                            	| Low (~3 reasons)   	| expired"                                                  	|  "not_found" 	|  "evicted      	|              	|        	|
| strategy        	| DLQ process                              	| Low (3 strategies) 	| upsert"                                                   	|  "ignore"    	|  "reinsert     	|              	|        	|
| outcome         	| DLQ process                              	| 3                  	| processed"                                                	|  "succeeded" 	|  "failed       	|              	|        	|
| correlation_key 	| Contract errors                          	| High               	| ⚠️ Use with caution — only for debugging, not alerting     	|              	|                	|              	|        	|

Cardinality Warning: The correlation_key label on contract_error_total can explode in cardinality if contract errors are frequent. This metric is intended for debugging, not alerting. Operators should monitor the aggregate rate and investigate specific keys via logs.

## 3.6 Histogram Bucket Selection

Buckets are chosen based on the expected distribution of each signal:

| Metric                    	| Buckets                                                        	| Rationale                                              	|
|---------------------------	|----------------------------------------------------------------	|--------------------------------------------------------	|
| redis_op_duration_seconds 	| Prometheus defaults (0.0005 → 10s)                             	| Sub-ms to multi-second range covers all Redis ops      	|
| flush_duration_seconds    	| 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10 	| Tuned for DocumentDB/DynamoDB/Postgres flush latencies 	|
| flush_batch_size          	| 1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000              	| Matches typical MaxBatchSize configurations            	|
| warmup_duration_seconds   	| 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5     	| HotLoad latency from Source (typically 1-50ms)         	|
| read_duration_seconds     	| Prometheus defaults                                            	| Covers sub-ms hot reads to multi-ms cold reads         	|
| broadcast_lag_seconds     	| 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5     	| Cross-pod convergence lag (typically 1-100ms)          	|

## 4 Full Metrics Catalog

### 4.1 Write Path Metrics

| Metric                                	| Type      	| Labels          	| Description                                               	|                                                      	|
|---------------------------------------	|-----------	|-----------------	|-----------------------------------------------------------	|------------------------------------------------------	|
| sluice_{ns}_write_total               	| Counter   	| —               	| Total successful writes to the Redis journal              	|                                                      	|
| sluice_{ns}_degraded_write_total      	| Counter   	| error           	| Degraded writes (direct to sink when Redis unavailable)   	|                                                      	|
| sluice_{ns}_redis_op_duration_seconds 	| Histogram 	| op"             	|  "error                                                   	| Duration of every Redis operation                    	|
| sluice_{ns}_flush_duration_seconds    	| Histogram 	| band"           	|  "error                                                   	| Duration of flush operations to the backing store    	|
| sluice_{ns}_flush_batch_size          	| Histogram 	| band            	| Number of keys flushed per batch operation                	|                                                      	|
| sluice_{ns}_dirty_queue_depth         	| Gauge     	| band            	| Current depth of the dirty queue (pending flush) per band 	|                                                      	|
| sluice_{ns}_contract_error_total      	| Counter   	| correlation_key 	| Total contract validation errors                          	|                                                      	|
| sluice_{ns}_dead_letter_total         	| Counter   	| band            	| Total records routed to the dead-letter queue             	|                                                      	|
| sluice_{ns}_dlq_process_total         	| Counter   	| strategy"       	|  "outcome                                                 	| Total DLQ records processed, by strategy and outcome 	|

### 4.2 Hot/Cold Regime Metrics

| Metric                              	| Type      	| Labels  	| Description                                           	|                              	|                     	|
|-------------------------------------	|-----------	|---------	|-------------------------------------------------------	|------------------------------	|---------------------	|
| sluice_{ns}_warmup_duration_seconds 	| Histogram 	| error   	| Duration of HotLoad (warm-up) operations              	|                              	|                     	|
| sluice_{ns}_read_duration_seconds   	| Histogram 	| is_hot" 	|  "error                                               	| Duration of Read" operations 	|  split by hot/cold" 	|
| sluice_{ns}_hot_set_size            	| Gauge     	| —       	| Current number of hot correlation keys in the journal 	|                              	|                     	|

### 4.3 L1 Local Journal Metrics (v1.0.8)

| Metric                             	| Type      	| Labels 	| Description                                                	|
|------------------------------------	|-----------	|--------	|------------------------------------------------------------	|
| sluice_{ns}_local_cache_hit_total  	| Counter   	| —      	| Total L1 local cache hits                                  	|
| sluice_{ns}_local_cache_miss_total 	| Counter   	| reason 	| Total L1 local cache misses, by reason                     	|
| sluice_{ns}_local_set_size         	| Gauge     	| —      	| Current number of entries in the L1 local cache            	|
| sluice_{ns}_broadcast_lag_seconds  	| Histogram 	| —      	| Lag between a write and its broadcast arrival at peer pods 	|

## 5. Usage Example
### 5.1 Basic Setup

```go
import (
"net/http"
"github.com/prometheus/client_golang/prometheus/promhttp"
sluiceprom "github.com/hussainpithawala/sluice-go/metrics/prometheus"
)

// Create the Prometheus-backed metrics recorder
metricsRec := sluiceprom.NewRecorder("nudge_inventory")

// Wire it into sluice
sl, _ := sluice.New("nudge_inventory").
WithRedis(sluice.RedisConfig{Addrs: []string{"redis:6379"}}).
WithSink(sk).
WithWriteContract(contract).
WithMetrics(metricsRec).  // ← Prometheus metrics!
Build(ctx)

// Expose /metrics endpoint for Prometheus scraping
http.Handle("/metrics", promhttp.Handler())
go http.ListenAndServe(":9090", nil)
```
### 5.2 Custom Registry (Multi-Instance)

```go
reg := prometheus.NewRegistry()
metricsRec := sluiceprom.NewRecorderWithRegistry("nudge_inventory", reg)

// Expose only sluice metrics on a dedicated endpoint
http.Handle("/sluice-metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
```

### 5.3 Prometheus Scrape Configuration

```yaml
scrape_configs:
  - job_name: 'sluice'
    scrape_interval: 15s
    static_configs:
      - targets: ['sluice-pod-1:9090', 'sluice-pod-2:9090']
    metric_relabel_configs:
      - source_labels: [__name__]
        regex: 'sluice_.*'
        action: keep
```

## 6. Reference Grafana Dashboard

A reference dashboard JSON is provided at dashboards/sluice-overview.json with the following panels:

### 6.1 Write Throughput & Latency
Write Rate: rate(sluice_{ns}_write_total[5m]) — writes/sec over time
Redis Op Latency (p50/p95/p99): histogram_quantile(0.99, rate(sluice_{ns}_redis_op_duration_seconds_bucket[5m]))
Flush Latency by Band: Heatmap of sluice_{ns}_flush_duration_seconds_bucket

### 6.2 Flush Engine Health
Dirty Queue Depth per Band: sluice_{ns}_dirty_queue_depth — 16 time series overlaid
Flush Batch Size Distribution: histogram_quantile(0.50, rate(sluice_{ns}_flush_batch_size_bucket[5m]))
Flush Error Rate: rate(sluice_{ns}_flush_duration_seconds_count{error!=""}[5m])

### 6.3 Hot/Cold Regime
Hot vs Cold Read Ratio: rate(sluice_{ns}_read_duration_seconds_count{is_hot="true"}[5m]) vs is_hot="false"
Hot Set Size Over Time: sluice_{ns}_hot_set_size
HotLoad Latency (p99): histogram_quantile(0.99, rate(sluice_{ns}_warmup_duration_seconds_bucket[5m]))

### 6.4 L1 Local Journal (v1.0.8)
* L1 Hit Ratio: rate(sluice_{ns}_local_cache_hit_total[5m]) / (rate(sluice_{ns}_local_cache_hit_total[5m]) + rate(sluice_{ns}_local_cache_miss_total[5m]))
* L1 Miss Reasons Breakdown: rate(sluice_{ns}_local_cache_miss_total[5m]) by (reason)
* L1 Cache Size: sluice_{ns}_local_set_size
* Broadcast Lag Distribution: histogram_quantile(0.99, rate(sluice_{ns}_broadcast_lag_seconds_bucket[5m]))

### 6.5 DLQ Health
* DLQ Inflow Rate: rate(sluice_{ns}_dead_letter_total[5m]) by (band)
* DLQ Processing Rate: rate(sluice_{ns}_dlq_process_total[5m]) by (strategy, outcome)
* Contract Error Rate: rate(sluice_{ns}_contract_error_total[5m])

## 6.6 Alerting Rules (Reference)

```yaml
groups:
  - name: sluice
    rules:
      - alert: SluiceHighFlushLatency
        expr: histogram_quantile(0.99, rate(sluice_nudge_inventory_flush_duration_seconds_bucket[5m])) > 1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "sluice flush p99 latency exceeds 1s"

      - alert: SluiceDirtyQueueBacklog
        expr: sluice_nudge_inventory_dirty_queue_depth > 5000
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "sluice dirty queue depth exceeds 5000 — flush engine falling behind"

      - alert: SluiceL1HitRatioLow
        expr: |
          rate(sluice_nudge_inventory_local_cache_hit_total[5m])
          / (rate(sluice_nudge_inventory_local_cache_hit_total[5m])
             + rate(sluice_nudge_inventory_local_cache_miss_total[5m])) < 0.5
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "sluice L1 cache hit ratio below 50% — consider increasing MaxEntries or LocalTTL"

      - alert: SluiceDLQInflowSpike
        expr: rate(sluice_nudge_inventory_dead_letter_total[5m]) > 10
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "sluice DLQ inflow exceeds 10 records/sec — investigate contract errors"

      - alert: SluiceBroadcastLagHigh
        expr: histogram_quantile(0.99, rate(sluice_nudge_inventory_broadcast_lag_seconds_bucket[5m])) > 1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "sluice cross-pod broadcast p99 lag exceeds 1s — check Redis Streams health"
```

## 7. Resolved Design Decisions (Strict Boundaries)

1. Opt-In Sub-Package: The Prometheus client dependency is only pulled in when metrics/prometheus is imported. Core sluice users who don't need Prometheus pay zero dependency cost.
2. Default Registry: NewRecorder uses prometheus.DefaultRegisterer for zero-config setup. NewRecorderWithRegistry is provided for advanced isolation.
3. No Metric Auto-Discovery: All metrics are explicitly registered at construction time. No dynamic metric creation based on runtime labels — this prevents cardinality surprises.
4. Error Label Strategy: Error labels contain the full error message for debugging. Operators should use error="" filters to isolate successful operations in dashboards.
5. Compile-Time Interface Check: The Recorder struct includes a compile-time assertion (var _ MetricsRecorder = (*Recorder)(nil)) to ensure it never drifts from the interface as new methods are added.
6. Thread Safety: All Prometheus metric types are inherently thread-safe. No additional locking is needed in the Recorder.
7. Unregister Support: An Unregister(reg) method is provided for testing or multi-instance cleanup, removing all metrics from the registry.
8. No Push Gateway Support: The exporter is pull-based (HTTP /metrics endpoint). Push Gateway integration is out of scope — operators can use promtool or the Prometheus Push Gateway client separately if needed.
9. Namespace Validation: The namespace is used directly as the Prometheus sub-system. No sanitization is performed — operators must ensure their namespace is a valid Prometheus identifier (alphanumeric + underscores).

## 8. Implementation Plan

1. Phase 1: Core Implementation
    2. Create metrics/prometheus/prometheus.go with the Recorder struct and all metric definitions.
    3. Implement all 16 MetricsRecorder methods.
    4. Add compile-time interface check.
    5. Add Unregister method for cleanup.
2. Phase 2: Unit Tests
    1. Create metrics/prometheus/prometheus_test.go.
    2. Test each metric type (counter increment, gauge set, histogram observe).
    3. Test label cardinality (verify correct label combinations).
    4. Test custom registry isolation.
    5. Test Unregister cleanup.
3. Phase 3: Reference Example
    1. Create examples/nudge_prometheus/main.go demonstrating end-to-end setup.
    2. Include HTTP server exposing /metrics and /health endpoints.
    3. Run simulated write + hot-read workload to generate metric traffic.
4. Phase 4: Grafana Dashboard
    1. Create dashboards/sluice-overview.json with all panels described in §6.
    2. Include variables for namespace selection.
    3. Include alerting rule examples.
5. Phase 5: Documentation
    1. Update README.md with a "Prometheus metrics export" section.
    2. Document all exposed metrics in a table.
    3. Provide Prometheus scrape config example.
    4. Link to the reference Grafana dashboard.

11. Success Criteria
    ✅ metrics/prometheus package compiles and passes all unit tests.
    ✅ All 16 MetricsRecorder methods are implemented and produce correct Prometheus metrics.
    ✅ examples/nudge_prometheus/main.go runs end-to-end and exposes /metrics with real data.
    ✅ Grafana dashboard renders correctly with all panels populated.
    ✅ Alerting rules fire correctly when thresholds are breached.
    ✅ No regression in core sluice tests — the Prometheus sub-package is fully isolated.