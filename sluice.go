// Package sluice provides a general-purpose wide-breadth write buffer that
// absorbs high-velocity event streams using Redis as a velocity shield and
// drains them asynchronously into any document store via BulkWrite batches.
//
// Import alias convention (module path contains a hyphen):
//
//	import sluice "github.com/hussainpithawala/sluice-go"
//
// Quickstart:
//
//	s, _ := sluice.New("nudge_inventory").
//	    WithRedis(sluice.RedisConfig{Addrs: []string{"redis:6379"}}).
//	    WithSink(sk).
//	    WithWriteContract(myContract).
//	    Build(ctx)
//
//	defer s.DrainAndClose(ctx)
//	s.Write(ctx, correlationKey, payload)
package sluice

import (
	"context"
	"errors"
	"fmt"
	"time"

	"log/slog"

	"github.com/cespare/xxhash/v2"
	"github.com/hussainpithawala/sluice-go/internal/dlq"
	"github.com/hussainpithawala/sluice-go/internal/engine"
	"github.com/hussainpithawala/sluice-go/internal/shield"
	"github.com/hussainpithawala/sluice-go/sink"
	"github.com/hussainpithawala/sluice-go/source"
	"github.com/redis/go-redis/v9"
)

// New returns a Builder initialised with production-safe defaults.
// namespace isolates all Redis keys for this instance.
func New(namespace string) *Builder {
	return &Builder{cfg: defaultConfig(namespace)}
}

func (b *Builder) WithRedis(rc RedisConfig) *Builder  { b.cfg.Redis = rc; return b }
func (b *Builder) WithSink(s sink.FlushSink) *Builder { b.sk = s; return b }

// WithSource sets the read-side persistence backend (e.g., source/docdb.Source).
// Required if WithReadContract is used.
func (b *Builder) WithSource(s source.Source) *Builder { b.src = s; return b }

// WithReadContract sets the domain function that translates a correlation key
// into a datastore-agnostic ReadModel for the Source to execute.
func (b *Builder) WithReadContract(rc ReadContract) *Builder { b.cfg.ReadContract = rc; return b }

// WithIndexContract sets the domain function that extracts secondary index fields.
func (b *Builder) WithIndexContract(ic IndexContract) *Builder { b.cfg.IndexContract = ic; return b }
func (b *Builder) WithWriteContract(wc WriteContract) *Builder { b.contract = wc; return b }
func (b *Builder) WithFlushWindow(d time.Duration) *Builder    { b.cfg.FlushWindow = d; return b }
func (b *Builder) WithMaxBatchSize(n int) *Builder             { b.cfg.MaxBatchSize = n; return b }
func (b *Builder) WithBandCount(n int) *Builder                { b.cfg.BandCount = n; return b }
func (b *Builder) WithKeyTTL(d time.Duration) *Builder         { b.cfg.KeyTTL = d; return b }
func (b *Builder) WithDegradedModeDirect(v bool) *Builder      { b.cfg.DegradedModeDirect = v; return b }
func (b *Builder) WithMetrics(m MetricsRecorder) *Builder      { b.cfg.Metrics = m; return b }

// WithActivityWindow sets the TTL for hot correlation_key sessions.
// Active users remain in the Redis journal for this duration. Default is 4 hours.
func (b *Builder) WithActivityWindow(d time.Duration) *Builder {
	b.cfg.ActivityWindow = d
	return b
}

// WithHotAwareFlush enables post-commit TTL extension. When true, successfully
// flushed hot correlation_keys have their ActivityWindow TTL refreshed, keeping them in the journal.
func (b *Builder) WithHotAwareFlush(v bool) *Builder {
	b.cfg.HotAwareFlush = v
	return b
}

// WithContentDedup enables xxHash64 payload deduplication. Identical payloads
// will only refresh the Redis TTL and skip redundant dirty-queue insertions.
func (b *Builder) WithContentDedup(v bool) *Builder {
	b.cfg.ContentDedup = v
	return b
}
func (b *Builder) OnFlush(cb OnFlushCallback) *Builder { b.callback = cb; return b }

// WithBatchedWrites enables pipelined Redis writes. Instead of one Redis
// round-trip per Write() call, writes are buffered and flushed in a single
// pipeline when the buffer reaches size entries or window elapses.
// Recommended for high-velocity streams (>10K writes/sec).
func (b *Builder) WithBatchedWrites(size int, window time.Duration) *Builder {
	b.cfg.BatchedWrites = true
	b.cfg.WriteBatchSize = size
	b.cfg.WriteBatchWindow = window
	return b
}

// WithDLQAutoProcess enables a background goroutine that periodically processes
// dead-letter records using the given strategy. The ticker fires every interval
// and calls ProcessDLQ internally. Stopped automatically by DrainAndClose.
func (b *Builder) WithDLQAutoProcess(interval time.Duration, strategy DLQStrategy) *Builder {
	b.cfg.DLQAutoProcess = true
	b.cfg.DLQProcessInterval = interval
	b.cfg.DLQProcessStrategy = strategy
	return b
}

func (b *Builder) WithIdempotencyTTL(d time.Duration) *Builder {
	b.cfg.IdempotencyTTL = d
	return b
}

// Build validates configuration, connects to Redis, pings the sink,
// starts band goroutines, and returns a ready Sluice instance.
func (b *Builder) Build(ctx context.Context) (*Sluice, error) {
	if err := b.validate(); err != nil {
		return nil, err
	}
	metrics := b.cfg.Metrics
	if metrics == nil {
		metrics = &noopMetrics{}
	}
	if err := b.sk.Ping(ctx); err != nil {
		return nil, fmt.Errorf("sluice: sink ping failed: %w", err)
	}

	if b.cfg.ReadContract != nil && b.src == nil {
		return nil, errors.New("sluice: WithSource() is required when WithReadContract() is used")
	}

	sh, err := shield.New(b.cfg.Redis.toInternal(), b.cfg.Namespace, b.cfg.BandCount, b.cfg.KeyTTL, b.cfg.ActivityWindow)
	if err != nil {
		return nil, err
	}
	// Wrap callback to convert internal types to public types
	var wrappedCb engine.OnFlushCallback
	if b.callback != nil {
		wrappedCb = func(keys []string, result *sink.BulkWriteResult, err error) {
			pubResult := &BulkWriteResult{
				InsertedCount: result.InsertedCount,
				MatchedCount:  result.MatchedCount,
				ModifiedCount: result.ModifiedCount,
				UpsertedCount: result.UpsertedCount,
				Errors:        make([]SinkError, len(result.Errors)),
			}
			for i, se := range result.Errors {
				pubResult.Errors[i] = SinkError{CorrelationKey: se.CorrelationKey, Code: se.Code, Err: se.Err}
			}
			b.callback(keys, pubResult, err)
		}
	}
	// Wrap contract to convert public WriteModel to sink.WriteModel
	wrappedContract := func(correlationKey string, payload []byte) (*sink.WriteModel, error) {
		wm, err := b.contract(correlationKey, payload)
		if err != nil {
			return nil, err
		}
		return &sink.WriteModel{
			CorrelationKey: "", // not used in flush path
			Filter:         wm.Filter,
			Update:         wm.Update,
			Upsert:         wm.Upsert,
		}, nil
	}
	eng := engine.New(b.cfg.toInternal(), sh, b.sk, wrappedContract, metrics, wrappedCb)
	eng.Start()

	// Enable batched writes if configured.
	if b.cfg.BatchedWrites {
		sh.EnableBatching(b.cfg.WriteBatchSize, b.cfg.WriteBatchWindow)
		sh.SetVolumeSignaler(func(band int) { eng.SignalVolume(band) })
		sh.StartBatcher(ctx)
	}

	s := &Sluice{cfg: b.cfg, shield: sh, engine: eng, sk: b.sk, src: b.src, contract: b.contract, metrics: metrics}

	if b.cfg.DLQAutoProcess {
		s.startDLQProcessor()
	}

	return s, nil
}

func (b *Builder) validate() error {
	if b.cfg.Namespace == "" {
		return ErrMissingNamespace
	}
	if b.sk == nil {
		return ErrMissingSink
	}
	if b.contract == nil {
		return ErrMissingContract
	}
	if len(b.cfg.Redis.Addrs) == 0 {
		return ErrMissingRedis
	}
	if b.cfg.BandCount <= 0 {
		b.cfg.BandCount = 16
	}
	if b.cfg.MaxBatchSize <= 0 {
		b.cfg.MaxBatchSize = 1000
	}
	if b.cfg.FlushWindow <= 0 {
		b.cfg.FlushWindow = 250 * time.Millisecond
	}
	if b.cfg.KeyTTL <= 0 {
		b.cfg.KeyTTL = 30 * time.Second
	}
	return nil
}

// IsHot checks if a correlation key is currently in the hot Redis set.
func (s *Sluice) IsHot(ctx context.Context, correlationKey string) (bool, error) {
	if s.closed.Load() {
		return false, ErrLibraryClosed
	}
	t := time.Now()
	// We can just try to Read it, or add an Exists method to shield.
	// For simplicity, shield.Read returns isHot flag.
	_, isHot, err := s.shield.Read(ctx, correlationKey)
	s.metrics.RecordRedisOp(s.cfg.Namespace, "is_hot", time.Since(t), err)
	return isHot, err
}

// Read returns current state from the journal.
// Hot correlation_key: sub-millisecond Redis read with lazy TTL refresh.
// Cold correlation_key: falls back to the configured Source via ReadContract.
func (s *Sluice) Read(ctx context.Context, correlationKey string) ([]byte, error) {
	if s.closed.Load() {
		return nil, ErrLibraryClosed
	}

	t := time.Now()
	payload, pttl, err := s.shield.ReadWithTTL(ctx, correlationKey)

	// Hot path: found in Redis journal
	if err == nil && payload != nil {
		s.metrics.RecordRead(s.cfg.Namespace, time.Since(t), true, nil)

		// Lazy refresh: if remaining TTL is < 20% of ActivityWindow, refresh it synchronously.
		threshold := s.cfg.ActivityWindow / 5
		if pttl > 0 && pttl < threshold {
			_ = s.shield.SetHotMarker(ctx, correlationKey, s.cfg.ActivityWindow)
			_ = s.shield.RefreshHotTTL(ctx, s.shield.BandFor(correlationKey), []string{correlationKey})
		}
		return payload, nil
	}

	// Cold path: fallback to the backing datastore via Source
	if s.cfg.ReadContract != nil && s.src != nil {
		// 1. Caller defines the datastore-agnostic filter
		model, modelErr := s.cfg.ReadContract(correlationKey)
		if modelErr != nil {
			s.metrics.RecordRead(s.cfg.Namespace, time.Since(t), false, modelErr)
			return nil, modelErr
		}

		// 2. Source executes the query and returns raw bytes
		srcPayload, srcErr := s.src.Read(ctx, *model)
		s.metrics.RecordRead(s.cfg.Namespace, time.Since(t), false, srcErr)

		if srcErr != nil {
			// Map internal source error to public sluice sentinel error
			if errors.Is(srcErr, source.ErrRecordNotFound) {
				return nil, ErrRecordNotFound
			}
			return nil, srcErr
		}
		return srcPayload, nil
	}

	s.metrics.RecordRead(s.cfg.Namespace, time.Since(t), false, ErrRecordNotFound)
	return nil, ErrRecordNotFound
}

// HotLoad activates a correlation_key on user login.
// Loads the document from the Source into the Redis journal and sets the hot marker.
func (s *Sluice) HotLoad(ctx context.Context, correlationKey string) ([]byte, error) {
	if s.closed.Load() {
		return nil, ErrLibraryClosed
	}
	if s.src == nil || s.cfg.ReadContract == nil {
		return nil, ErrMissingReadContract
	}

	t := time.Now()

	// 1. Caller defines the filter
	model, err := s.cfg.ReadContract(correlationKey)
	if err != nil {
		s.metrics.RecordWarmUp(s.cfg.Namespace, time.Since(t), err)
		return nil, err
	}

	// 2. Source executes the query
	payload, err := s.src.Read(ctx, *model)
	s.metrics.RecordWarmUp(s.cfg.Namespace, time.Since(t), err)
	if err != nil {
		// Map internal source error to public sluice sentinel error
		if errors.Is(err, source.ErrRecordNotFound) {
			return nil, ErrRecordNotFound
		}
		return nil, err
	}

	// 3. Write to Redis journal and set the hot marker
	if err := s.shield.Write(ctx, correlationKey, payload); err != nil {
		return nil, err
	}
	if err := s.shield.SetHotMarker(ctx, correlationKey, s.cfg.ActivityWindow); err != nil {
		return nil, err
	}

	return payload, nil
}

// Write buffers payload under correlationKey in Redis and returns immediately.
// The document store is never touched during Write().
// Safe for concurrent use from any number of goroutines.
//
// Features orchestrated here:
//  1. Content Deduplication: Skips dirty-queue insertion if payload hash matches.
//  2. Index Maintenance: Updates secondary SET/ZSET indexes via pipeline.
//  3. Hot Regime Signaling: Triggers immediate flush if the correlation_key is currently hot.
//  4. Standard Volume Trigger: Falls back to depth-based flush if not hot/batched.
func (s *Sluice) Write(ctx context.Context, correlationKey string, payload []byte) error {
	if s.closed.Load() {
		return ErrLibraryClosed
	}
	if correlationKey == "" {
		return ErrEmptyCorrelationKey
	}

	t := time.Now()
	var written bool
	var err error

	// 1. Execute the Redis write (with optional xxHash64 deduplication)
	if s.cfg.ContentDedup {
		hash := fmt.Sprintf("%d", xxhash.Sum64(payload))
		written, err = s.shield.WriteDedup(ctx, correlationKey, payload, hash)
	} else {
		written = true
		err = s.shield.Write(ctx, correlationKey, payload)
	}

	s.metrics.RecordRedisOp(s.cfg.Namespace, "write", time.Since(t), err)

	// Handle Redis failures via degraded mode or hard error
	if err != nil {
		if s.cfg.DegradedModeDirect {
			return s.degradedWrite(ctx, correlationKey, payload)
		}
		return fmt.Errorf("%w: %v", ErrRedisUnavailable, err)
	}

	// Only proceed with indexing and signaling if the record was actually
	// added to the dirty set (i.e., not short-circuited by deduplication).
	if written {
		s.metrics.RecordWrite(s.cfg.Namespace)

		// 2. Index maintenance via pipeline (best-effort, Valkey-safe)
		if s.cfg.IndexContract != nil {
			indexes, idxErr := s.cfg.IndexContract(correlationKey, payload)
			if idxErr == nil && len(indexes) > 0 {
				// We ignore the error here because index maintenance is eventually
				// consistent and should not block or fail the primary write path.
				_ = s.shield.UpdateIndexes(ctx, correlationKey, indexes, s.cfg.ActivityWindow)
			}
		}

		// 3. Volume signaling (Hot regime vs Standard depth check)
		band := s.shield.BandFor(correlationKey)
		signaled := false

		if s.cfg.HotAwareFlush {
			isHot, hotErr := s.shield.IsHot(ctx, correlationKey)
			if hotErr == nil && isHot {
				// Hot correlation_key: signal immediate flush for sub-ms Read() consistency
				s.engine.SignalVolume(band)
				signaled = true
			}
		}

		// Standard depth-based volume trigger
		// (skipped if batched writes are enabled, or if hot regime already signaled)
		if !signaled && !s.cfg.BatchedWrites {
			if depth, depthErr := s.shield.DirtyQueueDepth(ctx, band); depthErr == nil {
				if int(depth) >= s.cfg.MaxBatchSize {
					s.engine.SignalVolume(band)
				}
			}
		}
	}

	return nil
}

// WriteIdempotent ensures exactly-once delivery using SETNX EX.
func (s *Sluice) WriteIdempotent(ctx context.Context, correlationKey string, payload []byte, idempotencyKey string) error {
	if s.closed.Load() {
		return ErrLibraryClosed
	}
	if correlationKey == "" || idempotencyKey == "" {
		return ErrEmptyCorrelationKey
	}

	// Idempotency key shares the {band} hash tag for Cluster Mode safety
	idemKey := fmt.Sprintf("sl:%s:idem:{%d}:%s", s.cfg.Namespace, s.shield.BandFor(correlationKey), idempotencyKey)

	ok, err := s.shield.SetNX(ctx, idemKey, "1", s.cfg.IdempotencyTTL)
	if err != nil {
		return err
	}
	if !ok {
		return ErrDuplicateIdempotencyKey
	}

	return s.Write(ctx, correlationKey, payload)
}

// Query performs compound queries safely across bands using in-process SET intersection.
func (s *Sluice) Query(ctx context.Context, q Query) ([]QueryResult, error) {
	if s.closed.Load() {
		return nil, ErrLibraryClosed
	}
	if len(q.Equality) == 0 {
		return nil, fmt.Errorf("sluice: at least one equality filter is required")
	}

	var results []QueryResult

	// Iterate per band to ensure CROSSSLOT safety (all keys in SINTER share {band} tag)
	for band := 0; band < s.cfg.BandCount; band++ {
		var eqKeys []string
		for field, val := range q.Equality {
			eqKeys = append(eqKeys, shield.IndexKey(s.cfg.Namespace, band, field, val))
		}

		candidates, err := s.shield.SInter(ctx, eqKeys...)
		if err != nil && err != redis.Nil {
			return nil, err
		}

		for _, ck := range candidates {
			valid := true
			// Check RangeMin
			for field, min := range q.RangeMin {
				score, err := s.shield.ZScore(ctx, shield.RangeIndexKey(s.cfg.Namespace, band, field), ck)
				if err != nil || score < min {
					valid = false
					break
				}
			}
			// Check RangeMax
			if valid {
				for field, max := range q.RangeMax {
					score, err := s.shield.ZScore(ctx, shield.RangeIndexKey(s.cfg.Namespace, band, field), ck)
					if err != nil || score > max {
						valid = false
						break
					}
				}
			}

			if valid {
				payload, _, _ := s.shield.Read(ctx, ck)
				if payload != nil {
					results = append(results, QueryResult{CorrelationKey: ck, Payload: payload})
				}
			}
		}
	}
	return results, nil
}

// DrainAndClose flushes all remaining dirty keys, stops band goroutines,
// and releases Redis and sink connections. Call exactly once during shutdown.
func (s *Sluice) DrainAndClose(ctx context.Context) error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	if s.dlqCancel != nil {
		s.dlqCancel()
		<-s.dlqDone
	}
	// Stop the batcher first so all in-flight writes land in Redis before drain.
	s.shield.StopBatcher()
	s.engine.DrainAndStop()
	if err := s.shield.Close(); err != nil {
		return fmt.Errorf("sluice: redis close: %w", err)
	}
	return s.sk.Close(ctx)
}

// ── DLQ Processing ──────────────────────────────────────────────────────────

// WithDLQBatchSize sets the per-band batch size for DLQ processing.
func WithDLQBatchSize(n int) DLQOption {
	return func(o *dlqOptions) { o.maxBatchSize = n }
}

// WithKeyMutator sets the key mutation function for DLQReInsert strategy.
// If not set, DefaultKeyMutator is used.
func WithKeyMutator(fn func(string) string) DLQOption {
	return func(o *dlqOptions) { o.keyMutator = fn }
}

// WithDLQLogger sets a structured logger for DLQ processing.
// Defaults to slog.Default().
func WithDLQLogger(l *slog.Logger) DLQOption {
	return func(o *dlqOptions) { o.logger = l }
}

// DefaultKeyMutator appends a timestamp suffix to avoid key collision.
// Used by DLQReInsert when no custom KeyMutator is provided.
func DefaultKeyMutator(oldKey string) string {
	return dlq.DefaultKeyMutator(oldKey)
}

// ProcessDLQ drains and handles dead-letter records across all bands using the
// given strategy. This is a one-shot operation — call it from a cron job, admin
// endpoint, or CLI tool when you want to process accumulated DLQ records.
func (s *Sluice) ProcessDLQ(ctx context.Context, strategy DLQStrategy, opts ...DLQOption) (*DLQResult, error) {
	if s.closed.Load() {
		return nil, ErrLibraryClosed
	}

	o := &dlqOptions{maxBatchSize: s.cfg.MaxBatchSize}
	for _, opt := range opts {
		opt(o)
	}

	// Wrap the public WriteContract to produce sink.WriteModel.
	wrappedContract := func(correlationKey string, payload []byte) (*sink.WriteModel, error) {
		wm, err := s.contract(correlationKey, payload)
		if err != nil {
			return nil, err
		}
		return &sink.WriteModel{
			Filter: wm.Filter,
			Update: wm.Update,
			Upsert: wm.Upsert,
		}, nil
	}

	proc := dlq.NewProcessor(s.shield, s.sk, wrappedContract, s.metrics, dlq.Config{
		Strategy:     dlq.Strategy(strategy),
		MaxBatchSize: o.maxBatchSize,
		KeyMutator:   o.keyMutator,
		Logger:       o.logger,
	})

	result, err := proc.Process(ctx)
	if err != nil {
		return nil, err
	}
	return &DLQResult{
		Processed: result.Processed,
		Succeeded: result.Succeeded,
		Failed:    result.Failed,
	}, nil
}

func (s *Sluice) startDLQProcessor() {
	interval := s.cfg.DLQProcessInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.dlqCancel = cancel
	s.dlqDone = make(chan struct{})

	go func() {
		defer close(s.dlqDone)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if s.closed.Load() {
					return
				}
				result, err := s.ProcessDLQ(ctx, s.cfg.DLQProcessStrategy)
				if err != nil {
					slog.Warn("sluice: DLQ auto-process failed",
						"namespace", s.cfg.Namespace,
						"error", err,
					)
					continue
				}
				if result.Processed > 0 {
					slog.Info("sluice: DLQ auto-processed",
						"namespace", s.cfg.Namespace,
						"processed", result.Processed,
						"succeeded", result.Succeeded,
						"failed", result.Failed,
					)
				}
			}
		}
	}()
}

func (s *Sluice) degradedWrite(ctx context.Context, correlationKey string, payload []byte) error {
	wm, err := s.contract(correlationKey, payload)
	if err != nil {
		s.metrics.RecordContractError(s.cfg.Namespace, correlationKey, err)
		return fmt.Errorf("%w: %v", ErrContractViolation, err)
	}
	t := time.Now()
	writeErr := s.sk.Write(ctx, sink.WriteModel{
		CorrelationKey: correlationKey,
		Filter:         wm.Filter,
		Update:         wm.Update,
		Upsert:         wm.Upsert,
	})
	s.metrics.RecordDegradedWrite(s.cfg.Namespace, writeErr)
	s.metrics.RecordRedisOp(s.cfg.Namespace, "degraded_write", time.Since(t), writeErr)
	return writeErr
}
