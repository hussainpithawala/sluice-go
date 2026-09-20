package sluice

import (
	"time"

	"github.com/hussainpithawala/sluice-go/internal/dlq"
	"github.com/hussainpithawala/sluice-go/internal/engine"
	"github.com/hussainpithawala/sluice-go/internal/localjournal"
)

// MetricsRecorder emits library-internal telemetry into the caller's
// monitoring system (Prometheus, Datadog, CloudWatch, etc.).
type MetricsRecorder interface {
	RecordRead(namespace string, duration time.Duration, isHot bool, err error)
	RecordWrite(namespace string)
	RecordDegradedWrite(namespace string, reason error)

	// ── Hot/Cold Regime Metrics ────────────────────────────────────────────
	RecordWarmUp(namespace string, duration time.Duration, err error)
	RecordHotSetSize(namespace string, size int)

	dlq.MetricsRecorder
	engine.MetricsRecorder
	localjournal.MetricsRecorder
}

type noopMetrics struct{}

func (n *noopMetrics) RecordWrite(_ string)                                            {}
func (n *noopMetrics) RecordDegradedWrite(_ string, _ error)                           {}
func (n *noopMetrics) RecordRedisOp(_ string, _ string, _ time.Duration, _ error)      {}
func (n *noopMetrics) RecordFlush(_ string, _ string, _ int, _ time.Duration, _ error) {}
func (n *noopMetrics) RecordDirtyQueueDepth(_ string, _ string, _ int)                 {}
func (n *noopMetrics) RecordContractError(_ string, _ string, _ error)                 {}
func (n *noopMetrics) RecordDeadLetter(_ string, _ string, _ int)                      {}
func (n *noopMetrics) RecordDLQProcess(_ string, _ string, _ int, _ int, _ int)        {}
func (n *noopMetrics) RecordWarmUp(_ string, _ time.Duration, _ error)                 {}
func (n *noopMetrics) RecordRead(_ string, _ time.Duration, _ bool, _ error)           {}
func (n *noopMetrics) RecordHotSize(_ string, _ int)                                   {}
func (n *noopMetrics) RecordHotSetSize(_ string, _ int)                                {}
func (n *noopMetrics) RecordLocalCacheHit(_ string)                                    {}
func (n *noopMetrics) RecordLocalCacheMiss(_ string, _ localjournal.MissReason)        {}
func (n *noopMetrics) RecordLocalSetSize(_ string, _ int)                              {}
