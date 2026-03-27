package sluice

import "time"

// MetricsRecorder emits library-internal telemetry into the caller's
// monitoring system (Prometheus, Datadog, CloudWatch, etc.).
type MetricsRecorder interface {
	RecordWrite(namespace string)
	RecordDegradedWrite(namespace string, reason error)
	RecordRedisOp(namespace, op string, duration time.Duration, err error)
	RecordFlush(namespace, band string, batchSize int, duration time.Duration, err error)
	RecordDirtyQueueDepth(namespace, band string, depth int)
	RecordContractError(namespace, correlationKey string, err error)
}

type noopMetrics struct{}

func (n *noopMetrics) RecordWrite(_ string)                                             {}
func (n *noopMetrics) RecordDegradedWrite(_ string, _ error)                           {}
func (n *noopMetrics) RecordRedisOp(_ string, _ string, _ time.Duration, _ error)     {}
func (n *noopMetrics) RecordFlush(_ string, _ string, _ int, _ time.Duration, _ error) {}
func (n *noopMetrics) RecordDirtyQueueDepth(_ string, _ string, _ int)                {}
func (n *noopMetrics) RecordContractError(_ string, _ string, _ error)                {}
