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

	// RecordDeadLetter is called when one or more records are moved to the
	// dead-letter set after a permanent sink failure (e.g. duplicate key).
	// count is the number of correlation keys moved in this operation.
	RecordDeadLetter(namespace, band string, count int)
}

type noopMetrics struct{}

func (n *noopMetrics) RecordWrite(_ string)                                            {}
func (n *noopMetrics) RecordDegradedWrite(_ string, _ error)                           {}
func (n *noopMetrics) RecordRedisOp(_ string, _ string, _ time.Duration, _ error)      {}
func (n *noopMetrics) RecordFlush(_ string, _ string, _ int, _ time.Duration, _ error) {}
func (n *noopMetrics) RecordDirtyQueueDepth(_ string, _ string, _ int)                 {}
func (n *noopMetrics) RecordContractError(_ string, _ string, _ error)                 {}
func (n *noopMetrics) RecordDeadLetter(_ string, _ string, _ int)                      {}
