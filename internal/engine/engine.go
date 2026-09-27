// Package engine runs the flush loop that drains dirty Redis keys
// into the sink via BulkWrite. One goroutine per band, dual-trigger.
package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/hussainpithawala/sluice-go/internal/shield"
	"github.com/hussainpithawala/sluice-go/sink"
)

// Config holds the engine-level tunables passed from the top-level sluice.Config.
type Config struct {
	Namespace      string
	BandCount      int
	FlushWindow    time.Duration
	MaxBatchSize   int
	KeyTTL         time.Duration // Post-flush TTL for cold payloads; also the backlog flush threshold
	ActivityWindow time.Duration // Hot correlation_key session TTL (post-flush TTL for hot payloads)
	HotAwareFlush  bool          // Give flushed hot keys the ActivityWindow TTL and refresh their marker
}

// MetricsRecorder is the subset of telemetry methods the engine needs.
// Any type that satisfies the public sluice.MetricsRecorder also satisfies
// this interface.
type MetricsRecorder interface {
	// RecordUnflushedExpiry counts dirty keys whose payload was gone at flush
	// time. Unflushed payloads are persistent, so any non-zero value is data
	// loss (eviction, FLUSHDB, manual deletion) and should page someone.
	RecordUnflushedExpiry(namespace, band string, count int)
	RecordFlush(namespace, band string, batchSize int, duration time.Duration, err error)
	RecordDirtyQueueDepth(namespace, band string, depth int)
	RecordContractError(namespace, correlationKey string, err error)
	RecordDeadLetter(namespace, band string, count int)
	RecordRedisOp(namespace, op string, duration time.Duration, err error)
}

// WriteContract is the domain function the caller contributes.
// Returns a resolved WriteModel ready for BulkWrite assembly.
type WriteContract func(correlationKey string, payload []byte) (*sink.WriteModel, error)

// ReadContract loads the current payload for a correlation key from the
// backing sink/document store. It is used by the hot/cold regime when a correlation_key
// is cold and must be warmed into Redis.
type ReadContract func(correlationKey string) ([]byte, error)

// IndexContract extracts secondary-index fields from a payload.
//
// Suggested value handling:
//   - string values       -> equality SET index
//   - int/int64/float64   -> range ZSET index
//   - time.Time           -> range ZSET index using Unix milliseconds
type IndexContract func(correlationKey string, payload []byte) (map[string]interface{}, error)

// OnFlushCallback is invoked after every BulkWrite attempt — success or failure.
type OnFlushCallback func(correlationKeys []string, result *sink.BulkWriteResult, err error)

// ErrPayloadMissing is reported to OnFlushCallback for dirty keys whose
// payload was gone from Redis before it could be flushed (data loss).
var ErrPayloadMissing = errors.New("sluice: unflushed payload missing from Redis")

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
	writeContract WriteContract
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
		writeContract: contract,
		metrics:       metrics,
		callback:      cb,
		volumeSignals: signals,
		stopCh:        make(chan struct{}),
	}
}

// Start launches one goroutine per band and the pre-eviction flusher. Non-blocking.
func (e *Engine) Start() {
	for band := 0; band < e.cfg.BandCount; band++ {
		e.wg.Add(1)
		go e.runBand(band)
	}
	e.startPreEvictionFlusher()
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

// startPreEvictionFlusher monitors the oldest dirty keys and forces a flush
// of any band whose oldest entry has waited close to KeyTTL. Unflushed
// payloads are persistent and cannot expire, so this bounds flush latency
// for backlogged bands rather than preventing expiry.
func (e *Engine) startPreEvictionFlusher() {
	if e.cfg.KeyTTL <= 0 {
		return
	}

	// Check at half the KeyTTL interval
	interval := e.cfg.KeyTTL / 2
	if interval < time.Second {
		interval = time.Second
	}

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-e.stopCh:
				return
			case <-ticker.C:
				now := float64(time.Now().UnixMilli())
				// Threshold: current time - KeyTTL + 5s safety buffer
				threshold := now - float64(e.cfg.KeyTTL.Milliseconds()) + 5000

				for band := 0; band < e.cfg.BandCount; band++ {
					// OldestDirtyScore returns the score of the oldest item in the dirty ZSET
					// without leaking redis.Z types to the engine package.
					oldestScore, err := e.shield.OldestDirtyScore(context.Background(), band)
					if err == nil && oldestScore > 0 && oldestScore < threshold {
						e.SignalVolume(band)
					}
				}
			}
		}
	}()
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
//     - Success            → CommitFlushed: ZREM from dirty set + post-flush TTL
//     (ActivityWindow if hot, else KeyTTL; unflushed payloads have none).
//     - Permanent failure  → DeadLetterIfUnchanged (duplicate key, code 11000).
//     Both are conditional on the drained write sequence: a key rewritten
//     mid-flush stays dirty so its newer payload is flushed next cycle.
//     - Transient failure  → no action; keys remain in dirty set for retry.
//     - Total failure      → no action; all keys remain in dirty set for retry.
// This ensures a BulkWrite failure — including a unique-ID collision during
// DrainAndClose — never causes silent record loss.

// UPDATED: Now enforces CheckAttribution to prevent success-by-elimination silent loss,
// and uses SinkError.IsPermanent() for accurate dead-letter routing.

// ... [Keep imports, Config, MetricsRecorder, Contracts, Engine struct, New, Start, SignalVolume, DrainAndStop, startPreEvictionFlusher, runBand exactly as they are] ...
func (e *Engine) flushBand(ctx context.Context, band int) error {
	start := time.Now()
	bandStr := fmt.Sprintf("%d", band)

	if depth, err := e.shield.DirtyQueueDepth(ctx, band); err == nil {
		e.metrics.RecordDirtyQueueDepth(e.cfg.Namespace, bandStr, int(depth))
	}

	// Phase 1: read without removing.
	records, err := e.shield.DrainBand(ctx, band, e.cfg.MaxBatchSize)
	if err != nil {
		e.metrics.RecordFlush(e.cfg.Namespace, bandStr, 0, time.Since(start), err)
		return err
	}
	if len(records) == 0 {
		return nil
	}

	seqs := make(map[string]string, len(records))
	var missing []shield.VersionedKey
	models := make([]sink.WriteModel, 0, len(records))
	corrKeys := make([]string, 0, len(records))

	for _, rec := range records {
		if rec.Payload == nil {
			missing = append(missing, shield.VersionedKey{CorrelationKey: rec.CorrelationKey, Seq: rec.Seq})
			continue
		}
		seqs[rec.CorrelationKey] = rec.Seq
		wm, contractErr := e.writeContract(rec.CorrelationKey, rec.Payload)
		if contractErr != nil {
			e.metrics.RecordContractError(e.cfg.Namespace, rec.CorrelationKey, contractErr)
			e.deadLetter(ctx, band, bandStr,
				[]shield.VersionedKey{{CorrelationKey: rec.CorrelationKey, Seq: rec.Seq}}, "contract_violation")
			if e.callback != nil {
				e.callback(
					[]string{rec.CorrelationKey},
					&sink.BulkWriteResult{
						Errors: []sink.SinkError{
							{CorrelationKey: rec.CorrelationKey, Err: fmt.Errorf("%w: %v", sink.ErrContractViolation, contractErr)},
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

	e.handleMissingPayloads(ctx, band, bandStr, missing)
	if len(models) == 0 {
		return nil
	}

	// Phase 2: BulkWrite.
	result, flushErr := e.sink.BulkWrite(ctx, models)
	duration := time.Since(start)
	e.metrics.RecordFlush(e.cfg.Namespace, bandStr, len(models), duration, flushErr)

	if flushErr != nil {
		// Total failure — network, timeout, write concern, or unrecognised error.
		// Do not commit any keys. All keys remain in the dirty set and will be retried.
		if e.callback != nil {
			e.callback(corrKeys, result, flushErr)
		}
		return flushErr
	}

	// NEW: Attribution Guard. Prevents success-by-elimination silent loss.
	// If the sink returns partial errors but fails to attribute them to a key, we abort the commit.
	if err := sink.CheckAttribution(models, result); err != nil {
		slog.Error("sluice: flush aborted due to unattributed sink error", "err", err, "namespace", e.cfg.Namespace, "band", band)
		if e.callback != nil {
			e.callback(corrKeys, result, err)
		}
		return err // Return error to keep all keys in the dirty set for retry
	}

	// Partition results using the new IsPermanent() method.
	permanentKeys := make([]string, 0)
	transientKeys := make(map[string]bool)
	if result != nil {
		for _, se := range result.Errors {
			if se.IsPermanent() {
				permanentKeys = append(permanentKeys, se.CorrelationKey)
			} else {
				transientKeys[se.CorrelationKey] = true
			}
		}
	}

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

	e.commitFlushed(ctx, band, successKeys, seqs)

	if len(permanentKeys) > 0 {
		items := make([]shield.VersionedKey, len(permanentKeys))
		for i, k := range permanentKeys {
			items[i] = shield.VersionedKey{CorrelationKey: k, Seq: seqs[k]}
		}
		e.deadLetter(ctx, band, bandStr, items, "permanent_sink_error")
	}

	if e.callback != nil {
		e.callback(corrKeys, result, nil)
	}
	return nil
}

// commitFlushed commits successfully flushed keys with their post-flush TTL:
// ActivityWindow for hot keys (whose marker is also refreshed) when
// HotAwareFlush is on, KeyTTL otherwise. Three pipelines per batch rather
// than several round-trips per key.
//
// Each commit is conditional on the key's drained write sequence: a key
// rewritten while the batch was in flight — even within the same
// millisecond — keeps its dirty entry and stays persistent, so the newer
// payload is flushed next cycle instead of being dropped.
func (e *Engine) commitFlushed(ctx context.Context, band int, successKeys []string, seqs map[string]string) {
	if len(successKeys) == 0 {
		return
	}
	var hot []bool
	if e.cfg.HotAwareFlush {
		var err error
		if hot, err = e.shield.HotKeys(ctx, successKeys); err != nil {
			// Fall back to the cold TTL; the next Read or write refreshes it.
			e.metrics.RecordRedisOp(e.cfg.Namespace, "hot_keys", 0, err)
			hot = nil
		}
	}

	items := make([]shield.FlushedKey, len(successKeys))
	var hotKeys []string
	for i, ck := range successKeys {
		ttl := e.cfg.KeyTTL
		if hot != nil && hot[i] {
			ttl = e.cfg.ActivityWindow
			hotKeys = append(hotKeys, ck)
		}
		items[i] = shield.FlushedKey{CorrelationKey: ck, Seq: seqs[ck], TTL: ttl}
	}

	if err := e.shield.RefreshHotMarkers(ctx, hotKeys, e.cfg.ActivityWindow); err != nil {
		e.metrics.RecordRedisOp(e.cfg.Namespace, "refresh_hot_markers", 0, err)
	}
	t := time.Now()
	changed, err := e.shield.CommitFlushed(ctx, band, items)
	// On error nothing is lost: uncommitted keys stay dirty and are
	// re-flushed (upserts are idempotent).
	e.metrics.RecordRedisOp(e.cfg.Namespace, "commit_flushed", time.Since(t), err)
	if len(changed) > 0 {
		slog.Debug("sluice: keys rewritten during flush left dirty",
			"namespace", e.cfg.Namespace, "band", band, "count", len(changed))
	}
}

// deadLetter moves keys to the dead-letter set, skipping any rewritten since
// they were drained (their new payload is retried normally). On failure the
// keys stay dirty — they will fail again on retry, but they will not be
// silently lost.
func (e *Engine) deadLetter(ctx context.Context, band int, bandStr string, items []shield.VersionedKey, reason string) {
	moved, err := e.shield.DeadLetterIfUnchanged(ctx, band, items, reason)
	if err != nil {
		e.metrics.RecordRedisOp(e.cfg.Namespace, "move_to_dlq", 0, err)
		return
	}
	if len(moved) > 0 {
		e.metrics.RecordDeadLetter(e.cfg.Namespace, bandStr, len(moved))
	}
}

// handleMissingPayloads surfaces dirty keys whose payload was gone at flush
// time. Unflushed payloads are persistent, so this is data loss that already
// happened (eviction, FLUSHDB, manual deletion). It is recorded as a metric,
// logged per key, dead-lettered, and reported to the flush callback — never
// dropped silently.
//
// The dead-letter entry carries no payload, so a DLQ run will discard it;
// the log and metric are the durable record. A key rewritten since it was
// drained is not dead-lettered: its new payload is flushed normally (only
// the earlier version was lost).
func (e *Engine) handleMissingPayloads(ctx context.Context, band int, bandStr string, items []shield.VersionedKey) {
	if len(items) == 0 {
		return
	}
	keys := make([]string, len(items))
	for i, it := range items {
		keys[i] = it.CorrelationKey
	}
	e.metrics.RecordUnflushedExpiry(e.cfg.Namespace, bandStr, len(keys))
	for _, ck := range keys {
		slog.Error("sluice: unflushed payload missing at flush time — data lost",
			"namespace", e.cfg.Namespace, "band", band, "correlation_key", ck)
	}

	// On failure keys stay in the dirty set and are reported again next cycle.
	e.deadLetter(ctx, band, bandStr, items, "payload_missing_before_flush")

	if e.callback != nil {
		errs := make([]sink.SinkError, len(keys))
		for i, ck := range keys {
			errs[i] = sink.SinkError{CorrelationKey: ck, Err: ErrPayloadMissing}
		}
		e.callback(keys, &sink.BulkWriteResult{Errors: errs}, ErrPayloadMissing)
	}
}
