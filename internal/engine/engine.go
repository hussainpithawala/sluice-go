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
	Namespace        string
	BandCount        int
	FlushWindow      time.Duration
	FlushTimeout     time.Duration // Explicit flush context timeout; zero falls back to FlushWindow*4
	MaxBatchSize     int
	KeyTTL           time.Duration // Post-flush TTL for cold payloads; also the backlog flush threshold
	ActivityWindow   time.Duration // Hot correlation_key session TTL (post-flush TTL for hot payloads)
	HotAwareFlush    bool          // Give flushed hot keys the ActivityWindow TTL and refresh their marker
	FlushLock        bool          // Acquire a per-band Redis advisory lock before flushing
	AdaptiveBatch    bool          // Enable adaptive batch sizing based on flush duration
	MaxRetryAttempts int           // 0 = unlimited retries (default); >0 = dead-letter after N transient failures
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
	RecordFlushTimeout(namespace, band string, batchSize int, deadline time.Duration)
	RecordFlushLockSkip(namespace, band string)
	RecordDirtyQueueDepth(namespace, band string, depth int)
	RecordContractError(namespace, correlationKey string, err error)
	RecordDeadLetter(namespace, band string, count int)
	RecordRedisOp(namespace, op string, duration time.Duration, err error)
	RecordTransientRetry(namespace, band string, count int)
	RecordOldestDirtyAge(namespace, band string, age time.Duration)
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
	adaptive      *AdaptiveBatch
	volumeSignals []chan struct{}
	wg            sync.WaitGroup
	stopCh        chan struct{}
	once          sync.Once

	// The following fields are indexed by band and accessed only from that
	// band's goroutine (runBand), so no mutex is needed.
	lastBatchWarn     []time.Time      // rate-limit: one WARN per band per FlushWindow
	lastTransientWarn []time.Time      // rate-limit: one WARN per band per FlushWindow
	attempts          []map[string]int // per-band, per-key transient retry counter
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
	var ab *AdaptiveBatch
	if cfg.AdaptiveBatch {
		target := cfg.FlushTimeout
		if target <= 0 {
			target = cfg.FlushWindow * 4
		}
		ab = NewAdaptiveBatch(cfg.MaxBatchSize, 100, target*3/5)
	}
	attempts := make([]map[string]int, cfg.BandCount)
	for i := range attempts {
		attempts[i] = make(map[string]int)
	}
	return &Engine{
		cfg:               cfg,
		shield:            sh,
		sink:              sk,
		writeContract:     contract,
		metrics:           metrics,
		callback:          cb,
		adaptive:          ab,
		volumeSignals:     signals,
		stopCh:            make(chan struct{}),
		lastBatchWarn:     make([]time.Time, cfg.BandCount),
		lastTransientWarn: make([]time.Time, cfg.BandCount),
		attempts:          attempts,
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

func (e *Engine) effectiveBatchSize() int {
	if e.adaptive != nil {
		return e.adaptive.Size()
	}
	return e.cfg.MaxBatchSize
}

func (e *Engine) flushTimeout() time.Duration {
	if e.cfg.FlushTimeout > 0 {
		return e.cfg.FlushTimeout
	}
	return e.cfg.FlushWindow * 4
}

// lockedFlush acquires the per-band advisory lock before flushing (when
// FlushLock is enabled) and skips the cycle if another pod holds it.
// On shutdown (drain=true) the lock is bypassed so the final flush always runs.
func (e *Engine) lockedFlush(ctx context.Context, band int, drain bool) error {
	if !e.cfg.FlushLock || drain {
		return e.flushBand(ctx, band)
	}
	bandStr := fmt.Sprintf("%d", band)
	acquired, err := e.shield.TryFlushLock(ctx, band, e.flushTimeout()+5*time.Second)
	if err != nil {
		e.metrics.RecordFlushLockSkip(e.cfg.Namespace, bandStr)
		return err
	}
	if !acquired {
		e.metrics.RecordFlushLockSkip(e.cfg.Namespace, bandStr)
		return nil
	}
	defer func(shield *shield.Shield, ctx context.Context, band int) {
		err := shield.ReleaseFlushLock(ctx, band)
		if err != nil {
			slog.Warn("sluice: release flush lock error",
				"namespace", e.cfg.Namespace, "band", band)
		}
	}(e.shield, ctx, band)
	return e.flushBand(ctx, band)
}

func (e *Engine) runBand(band int) {
	defer e.wg.Done()
	ticker := time.NewTicker(e.cfg.FlushWindow)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			if err := e.lockedFlush(ctx, band, true); err != nil {
				slog.Debug("sluice: flush error on shutdown",
					"namespace", e.cfg.Namespace, "band", band, "err", err)
			}
			cancel()
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), e.flushTimeout())
			if err := e.lockedFlush(ctx, band, false); err != nil {
				slog.Debug("sluice: flush error on ticker",
					"namespace", e.cfg.Namespace, "band", band, "err", err)
			}
			cancel()
		case <-e.volumeSignals[band]:
			ctx, cancel := context.WithTimeout(context.Background(), e.flushTimeout())
			if err := e.lockedFlush(ctx, band, false); err != nil {
				slog.Debug("sluice: flush error on volume signal",
					"namespace", e.cfg.Namespace, "band", band, "err", err)
			}
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
//     - Success            → CommitFlushed: ZREM from dirty set + post-flush TTL.
//     - Permanent failure  → DeadLetterIfUnchanged (IsPermanent() == true).
//     - Transient failure  → no action; keys remain in dirty set for retry.
//     - Total failure      → no action; all keys remain in dirty set for retry.
func (e *Engine) flushBand(ctx context.Context, band int) error {
	start := time.Now()
	bandStr := fmt.Sprintf("%d", band)
	var batchSize int
	defer func() {
		timedOut := errors.Is(ctx.Err(), context.DeadlineExceeded)
		if timedOut {
			e.metrics.RecordFlushTimeout(e.cfg.Namespace, bandStr, batchSize, e.flushTimeout())
			slog.Warn("sluice: flush timed out",
				"namespace", e.cfg.Namespace, "band", band,
				"batch_size", batchSize, "deadline", e.flushTimeout())
		}
		if e.adaptive != nil && batchSize > 0 {
			e.adaptive.Adjust(time.Since(start), timedOut)
		}
	}()

	e.emitBandGauges(ctx, band, bandStr)

	records, err := e.shield.DrainBand(ctx, band, e.effectiveBatchSize())
	if err != nil {
		e.metrics.RecordFlush(e.cfg.Namespace, bandStr, 0, time.Since(start), err)
		return err
	}
	if len(records) == 0 {
		return nil
	}

	models, corrKeys, seqs := e.resolveRecords(ctx, band, bandStr, records)
	if len(models) == 0 {
		return nil
	}
	batchSize = len(models)

	result, flushErr := e.sink.BulkWrite(ctx, models)
	e.metrics.RecordFlush(e.cfg.Namespace, bandStr, batchSize, time.Since(start), flushErr)

	if flushErr != nil {
		e.warnBatchFailure(band, batchSize, flushErr)
		if e.callback != nil {
			e.callback(corrKeys, result, flushErr)
		}
		return flushErr
	}
	if err := sink.CheckAttribution(models, result); err != nil {
		slog.Error("sluice: flush aborted due to unattributed sink error",
			"err", err, "namespace", e.cfg.Namespace, "band", band)
		if e.callback != nil {
			e.callback(corrKeys, result, err)
		}
		return err
	}

	permanentKeys, transientKeys := e.partitionErrors(result)
	e.trackTransientRetries(band, bandStr, transientKeys)
	e.enforceRetryCap(ctx, band, bandStr, transientKeys, seqs)
	successKeys := subtractKeys(corrKeys, permanentKeys, transientKeys)

	e.commitFlushed(ctx, band, successKeys, seqs)
	e.clearAttempts(band, successKeys, permanentKeys)
	e.deadLetterPermanent(ctx, band, bandStr, permanentKeys, seqs)

	if e.callback != nil {
		e.callback(corrKeys, result, nil)
	}
	return nil
}

// emitBandGauges records the dirty queue depth and oldest entry age for a band.
func (e *Engine) emitBandGauges(ctx context.Context, band int, bandStr string) {
	if depth, err := e.shield.DirtyQueueDepth(ctx, band); err == nil {
		e.metrics.RecordDirtyQueueDepth(e.cfg.Namespace, bandStr, int(depth))
	}
	if score, err := e.shield.OldestDirtyScore(ctx, band); err == nil && score > 0 {
		ageMs := float64(time.Now().UnixMilli()) - score
		e.metrics.RecordOldestDirtyAge(e.cfg.Namespace, bandStr, time.Duration(ageMs)*time.Millisecond)
	}
}

// resolveRecords applies the WriteContract to each drained record, routing
// missing payloads and contract errors to their respective handlers. Returns
// the models ready for BulkWrite, their correlation keys, and the per-key
// write sequences.
func (e *Engine) resolveRecords(
	ctx context.Context, band int, bandStr string, records []shield.FlushRecord,
) ([]sink.WriteModel, []string, map[string]string) {
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
			e.handleContractError(ctx, band, bandStr, rec, contractErr)
			continue
		}
		models = append(models, sink.WriteModel{
			CorrelationKey: rec.CorrelationKey,
			Filter:         wm.Filter,
			Update:         wm.Update,
			Upsert:         wm.Upsert,
			Many:           wm.Many,
		})
		corrKeys = append(corrKeys, rec.CorrelationKey)
	}

	e.handleMissingPayloads(ctx, band, bandStr, missing)
	return models, corrKeys, seqs
}

// handleContractError dead-letters a single record whose WriteContract failed
// and notifies the flush callback.
func (e *Engine) handleContractError(ctx context.Context, band int, bandStr string, rec shield.FlushRecord, err error) {
	e.metrics.RecordContractError(e.cfg.Namespace, rec.CorrelationKey, err)
	e.deadLetter(ctx, band, bandStr,
		[]shield.VersionedKey{{CorrelationKey: rec.CorrelationKey, Seq: rec.Seq}}, "contract_violation")
	if e.callback != nil {
		e.callback(
			[]string{rec.CorrelationKey},
			&sink.BulkWriteResult{
				Errors: []sink.SinkError{
					{CorrelationKey: rec.CorrelationKey,
						Err: fmt.Errorf("%w: %v", sink.ErrContractViolation, err)},
				},
			},
			err,
		)
	}
}

// warnBatchFailure emits a rate-limited WARN when an entire BulkWrite fails.
func (e *Engine) warnBatchFailure(band, batchSize int, err error) {
	if time.Since(e.lastBatchWarn[band]) > e.cfg.FlushWindow {
		slog.Warn("sluice: bulk write failed (whole batch)",
			"namespace", e.cfg.Namespace, "band", band,
			"batch_size", batchSize, "err", err)
		e.lastBatchWarn[band] = time.Now()
	}
}

// partitionErrors classifies per-key sink errors into permanent (DLQ) and
// transient (retry) buckets.
func (e *Engine) partitionErrors(result *sink.BulkWriteResult) ([]string, map[string]bool) {
	var permanentKeys []string
	transientKeys := make(map[string]bool)
	if result == nil {
		return permanentKeys, transientKeys
	}
	for _, se := range result.Errors {
		if se.IsPermanent() {
			permanentKeys = append(permanentKeys, se.CorrelationKey)
		} else {
			transientKeys[se.CorrelationKey] = true
		}
	}
	return permanentKeys, transientKeys
}

// trackTransientRetries increments per-key attempt counters and emits a
// metric plus a rate-limited WARN for transient failures.
func (e *Engine) trackTransientRetries(band int, bandStr string, transientKeys map[string]bool) {
	if len(transientKeys) == 0 {
		return
	}
	for k := range transientKeys {
		e.attempts[band][k]++
	}
	e.metrics.RecordTransientRetry(e.cfg.Namespace, bandStr, len(transientKeys))
	if time.Since(e.lastTransientWarn[band]) > e.cfg.FlushWindow {
		sample := make([]string, 0, 5)
		for k := range transientKeys {
			sample = append(sample, k)
			if len(sample) >= 5 {
				break
			}
		}
		slog.Warn("sluice: transient sink errors — keys left for retry",
			"namespace", e.cfg.Namespace, "band", band,
			"count", len(transientKeys), "sample_keys", sample)
		e.lastTransientWarn[band] = time.Now()
	}
}

// enforceRetryCap routes transient keys that exceeded MaxRetryAttempts to the
// DLQ. Capped keys are removed from transientKeys so they are not counted as
// still-retrying in the success/fail partition.
func (e *Engine) enforceRetryCap(ctx context.Context, band int, bandStr string, transientKeys map[string]bool, seqs map[string]string) {
	if e.cfg.MaxRetryAttempts <= 0 {
		return
	}
	var cappedKeys []string
	for k := range transientKeys {
		if e.attempts[band][k] > e.cfg.MaxRetryAttempts {
			cappedKeys = append(cappedKeys, k)
		}
	}
	if len(cappedKeys) == 0 {
		return
	}
	items := make([]shield.VersionedKey, len(cappedKeys))
	for i, k := range cappedKeys {
		items[i] = shield.VersionedKey{CorrelationKey: k, Seq: seqs[k]}
		delete(e.attempts[band], k)
		delete(transientKeys, k)
	}
	slog.Warn("sluice: transient errors exceeded retry cap — routing to DLQ",
		"namespace", e.cfg.Namespace, "band", band,
		"count", len(cappedKeys), "max_attempts", e.cfg.MaxRetryAttempts)
	e.deadLetter(ctx, band, bandStr, items, "transient_retry_exhausted")
}

// subtractKeys returns the elements of all that are in neither permanent nor
// transient.
func subtractKeys(all []string, permanent []string, transient map[string]bool) []string {
	failed := make(map[string]bool, len(permanent)+len(transient))
	for _, k := range permanent {
		failed[k] = true
	}
	for k := range transient {
		failed[k] = true
	}
	out := make([]string, 0, len(all))
	for _, k := range all {
		if !failed[k] {
			out = append(out, k)
		}
	}
	return out
}

// clearAttempts removes attempt counters for keys that no longer need tracking.
func (e *Engine) clearAttempts(band int, successKeys, permanentKeys []string) {
	for _, k := range successKeys {
		delete(e.attempts[band], k)
	}
	for _, k := range permanentKeys {
		delete(e.attempts[band], k)
	}
}

// deadLetterPermanent moves permanently-failed keys to the DLQ.
func (e *Engine) deadLetterPermanent(ctx context.Context, band int, bandStr string, permanentKeys []string, seqs map[string]string) {
	if len(permanentKeys) == 0 {
		return
	}
	items := make([]shield.VersionedKey, len(permanentKeys))
	for i, k := range permanentKeys {
		items[i] = shield.VersionedKey{CorrelationKey: k, Seq: seqs[k]}
	}
	e.deadLetter(ctx, band, bandStr, items, "permanent_sink_error")
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
