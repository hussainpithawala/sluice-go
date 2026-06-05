// Package engine runs the flush loop that drains dirty Redis keys
// into the sink via BulkWrite. One goroutine per band, dual-trigger.
package engine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hussainpithawala/sluice-go/internal/shield"
	"github.com/hussainpithawala/sluice-go/sink"
)

// Config holds the engine-level tunables passed from the top-level sluice.Config.
type Config struct {
	Namespace    string
	BandCount    int
	FlushWindow  time.Duration
	MaxBatchSize int
}

// MetricsRecorder is the subset of telemetry methods the engine needs.
// Any type that satisfies the public sluice.MetricsRecorder also satisfies
// this interface.
type MetricsRecorder interface {
	RecordFlush(namespace, band string, batchSize int, duration time.Duration, err error)
	RecordDirtyQueueDepth(namespace, band string, depth int)
	RecordContractError(namespace, correlationKey string, err error)
	RecordDeadLetter(namespace, band string, count int)
	RecordRedisOp(namespace, op string, duration time.Duration, err error)
}

// WriteContract is the domain function the caller contributes.
// Returns a resolved WriteModel ready for BulkWrite assembly.
type WriteContract func(correlationKey string, payload []byte) (*sink.WriteModel, error)

// OnFlushCallback is invoked after every BulkWrite attempt — success or failure.
type OnFlushCallback func(correlationKeys []string, result *sink.BulkWriteResult, err error)

// ErrCodeDuplicateKey is the MongoDB / AWS DocumentDB error code for a
// unique-index violation. The engine uses this to route permanent failures
// to the dead-letter set instead of retrying them indefinitely.
const ErrCodeDuplicateKey = 11000

// Engine runs one goroutine per band.
// Each goroutine wakes on a time-window trigger or a volume trigger,
// drains dirty keys from Redis, applies the WriteContract,
// and executes a BulkWrite on the sink using a two-phase commit protocol.
type Engine struct {
	cfg           Config
	shield        *shield.Shield
	sink          sink.FlushSink
	contract      WriteContract
	metrics       MetricsRecorder
	callback      OnFlushCallback
	volumeSignals []chan struct{}
	wg            sync.WaitGroup
	stopCh        chan struct{}
	once          sync.Once
}

// New constructs an Engine. Call Start() to begin band goroutines.
func New(
	cfg Config,
	sh *shield.Shield,
	sk sink.FlushSink,
	contract WriteContract,
	metrics MetricsRecorder,
	cb OnFlushCallback,
) *Engine {
	signals := make([]chan struct{}, cfg.BandCount)
	for i := range signals {
		signals[i] = make(chan struct{}, 1)
	}
	return &Engine{
		cfg:           cfg,
		shield:        sh,
		sink:          sk,
		contract:      contract,
		metrics:       metrics,
		callback:      cb,
		volumeSignals: signals,
		stopCh:        make(chan struct{}),
	}
}

// Start launches one goroutine per band. Non-blocking.
func (e *Engine) Start() {
	for band := 0; band < e.cfg.BandCount; band++ {
		e.wg.Add(1)
		go e.runBand(band)
	}
}

// SignalVolume sends a non-blocking volume trigger to a band's goroutine.
func (e *Engine) SignalVolume(band int) {
	select {
	case e.volumeSignals[band] <- struct{}{}:
	default:
	}
}

// DrainAndStop signals all band goroutines to perform a final flush and exit.
// Blocks until all goroutines have returned. Safe to call exactly once.
func (e *Engine) DrainAndStop() {
	e.once.Do(func() {
		close(e.stopCh)
		e.wg.Wait()
	})
}

func (e *Engine) runBand(band int) {
	defer e.wg.Done()
	ticker := time.NewTicker(e.cfg.FlushWindow)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			_ = e.flushBand(ctx, band)
			cancel()
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), e.cfg.FlushWindow*4)
			_ = e.flushBand(ctx, band)
			cancel()
		case <-e.volumeSignals[band]:
			ctx, cancel := context.WithTimeout(context.Background(), e.cfg.FlushWindow*4)
			_ = e.flushBand(ctx, band)
			cancel()
		}
	}
}

// flushBand implements a two-phase commit for a single band:
//
//  1. DrainBand — read dirty keys + payloads from Redis (no ZREM yet).
//  2. Apply WriteContract to each record.
//  3. BulkWrite to the sink.
//  4. Partition results:
//     - Success            → CommitKeys (ZREM from dirty set).
//     - Permanent failure  → MoveToDeadLetter (duplicate key, code 11000).
//     - Transient failure  → no action; keys remain in dirty set for retry.
//     - Total failure      → no action; all keys remain in dirty set for retry.
//
// This ensures a BulkWrite failure — including a unique-ID collision during
// DrainAndClose — never causes silent record loss.
func (e *Engine) flushBand(ctx context.Context, band int) error {
	start := time.Now()
	bandStr := fmt.Sprintf("%d", band)

	// Emit pre-flush dirty queue depth for observability.
	if depth, err := e.shield.DirtyQueueDepth(ctx, band); err == nil {
		e.metrics.RecordDirtyQueueDepth(e.cfg.Namespace, bandStr, int(depth))
	}

	// Phase 1: read without removing. Expired-TTL keys are cleaned by DrainBand
	// internally; all other keys remain in the dirty set until CommitKeys.
	records, err := e.shield.DrainBand(ctx, band, e.cfg.MaxBatchSize)
	if err != nil {
		e.metrics.RecordFlush(e.cfg.Namespace, bandStr, 0, time.Since(start), err)
		return err
	}
	if len(records) == 0 {
		return nil
	}

	// Apply WriteContract — build sink models.
	models := make([]sink.WriteModel, 0, len(records))
	corrKeys := make([]string, 0, len(records))

	for _, rec := range records {
		wm, contractErr := e.contract(rec.CorrelationKey, rec.Payload)
		if contractErr != nil {
			// Contract violation is a permanent failure for this key.
			// Move to dead-letter to break an infinite retry loop.
			e.metrics.RecordContractError(e.cfg.Namespace, rec.CorrelationKey, contractErr)
			_ = e.shield.MoveToDeadLetter(ctx, band,
				[]string{rec.CorrelationKey}, "contract_violation")
			e.metrics.RecordDeadLetter(e.cfg.Namespace, bandStr, 1)
			if e.callback != nil {
				e.callback(
					[]string{rec.CorrelationKey},
					&sink.BulkWriteResult{
						Errors: []sink.SinkError{
							{CorrelationKey: rec.CorrelationKey,
								Err: fmt.Errorf("%w: %v", sink.ErrContractViolation, contractErr)},
						},
					},
					contractErr,
				)
			}
			continue
		}
		models = append(models, sink.WriteModel{
			CorrelationKey: rec.CorrelationKey,
			Filter:         wm.Filter,
			Update:         wm.Update,
			Upsert:         wm.Upsert,
		})
		corrKeys = append(corrKeys, rec.CorrelationKey)
	}

	if len(models) == 0 {
		return nil
	}

	// Phase 2: BulkWrite.
	result, flushErr := e.sink.BulkWrite(ctx, models)
	duration := time.Since(start)
	e.metrics.RecordFlush(e.cfg.Namespace, bandStr, len(models), duration, flushErr)

	if flushErr != nil {
		// Total failure — network, timeout, or unrecognised error.
		// Do not commit any keys. All keys remain in the dirty set and will
		// be retried on the next flush cycle.
		if e.callback != nil {
			e.callback(corrKeys, result, flushErr)
		}
		return flushErr
	}

	// Partial or full success — partition results.
	permanentKeys := make([]string, 0)
	transientKeys := make(map[string]bool)

	if result != nil {
		for _, se := range result.Errors {
			if se.Code == ErrCodeDuplicateKey {
				// Non-retryable: unique-index violation.
				// Moving to dead-letter prevents an infinite retry loop
				// while preserving the payload for investigation and replay.
				permanentKeys = append(permanentKeys, se.CorrelationKey)
			} else {
				// Retryable: transient sink error (write concern, timeout, etc.).
				// No action needed — key stays in dirty set.
				transientKeys[se.CorrelationKey] = true
			}
		}
	}

	// Build the set of successfully written keys:
	// all corrKeys minus permanent failures minus transient failures.
	failedKeys := make(map[string]bool, len(permanentKeys)+len(transientKeys))
	for _, k := range permanentKeys {
		failedKeys[k] = true
	}
	for k := range transientKeys {
		failedKeys[k] = true
	}

	successKeys := make([]string, 0, len(corrKeys))
	for _, k := range corrKeys {
		if !failedKeys[k] {
			successKeys = append(successKeys, k)
		}
	}

	// Commit successful keys — ZREM from dirty set.
	if len(successKeys) > 0 {
		if commitErr := e.shield.CommitKeys(ctx, band, successKeys); commitErr != nil {
			// CommitKeys failure is non-fatal: worst case these keys are
			// re-flushed on the next cycle. Upsert semantics make that safe.
			e.metrics.RecordRedisOp(e.cfg.Namespace, "commit_keys", 0, commitErr)
		}
	}

	// Route permanently failed keys to dead-letter.
	if len(permanentKeys) > 0 {
		if dlqErr := e.shield.MoveToDeadLetter(ctx, band, permanentKeys, "duplicate_key"); dlqErr != nil {
			// If MoveToDeadLetter fails, keys stay in dirty set — they will
			// fail again on retry, but they will not be silently lost.
			e.metrics.RecordRedisOp(e.cfg.Namespace, "move_to_dlq", 0, dlqErr)
		} else {
			e.metrics.RecordDeadLetter(e.cfg.Namespace, bandStr, len(permanentKeys))
		}
	}

	if e.callback != nil {
		e.callback(corrKeys, result, nil)
	}

	return nil
}
