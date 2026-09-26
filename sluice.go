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
	"github.com/hussainpithawala/sluice-go/internal/broadcast"
	"github.com/hussainpithawala/sluice-go/internal/dlq"
	"github.com/hussainpithawala/sluice-go/internal/engine"
	"github.com/hussainpithawala/sluice-go/internal/localjournal"
	"github.com/hussainpithawala/sluice-go/internal/shield"
	"github.com/hussainpithawala/sluice-go/sink"
	"github.com/hussainpithawala/sluice-go/source"
)

// New returns a Builder initialised with production-safe defaults.
// namespace isolates all Redis keys for this instance.
func New(namespace string) *Builder {
	return &Builder{cfg: DefaultConfig(namespace)}
}

func (b *Builder) WithRedis(rc RedisConfig) *Builder  { b.cfg.Redis = rc; return b }
func (b *Builder) WithSink(s sink.FlushSink) *Builder { b.sk = s; return b }

// WithSource sets the read-side persistence backend (e.g., source/docdb.Source).
// Required if WithReadContract is used.
func (b *Builder) WithSource(s source.Source) *Builder { b.src = s; return b }

// Builder Methods for single Key Contracts

// WithReadContract sets the domain function that translates a correlation key
// into a datastore-agnostic ReadModel for the Source to execute.
func (b *Builder) WithReadContract(rc ReadContract) *Builder { b.cfg.ReadContract = rc; return b }

// WithIndexContract sets the domain function that extracts secondary index fields.
func (b *Builder) WithIndexContract(ic IndexContract) *Builder {
	b.indexContract = ic
	b.cfg.IndexContract = ic
	return b
}

// Builder methods for BulkContracts

// WithReadBulkContract configures the bulk read execution plan translator.
func (b *Builder) WithReadBulkContract(fn ReadBulkContract) *Builder {
	b.readBulkContract = fn
	b.cfg.ReadBulkContract = fn
	return b
}

// WithIndexBulkContract configures the bulk secondary index extractor.
func (b *Builder) WithIndexBulkContract(fn IndexBulkContract) *Builder {
	b.indexBulkContract = fn
	b.cfg.IndexBulkContract = fn
	return b
}

func (b *Builder) WithWriteContract(wc WriteContract) *Builder { b.writeContract = wc; return b }
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

// WithLocalCache enables the L1 in-process read tier.
func (b *Builder) WithLocalCache(cfg localjournal.LocalCacheConfig) *Builder {
	b.localCacheCfg = cfg
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

// WithHotSetSampleInterval sets how often the hot set size gauge is sampled.
// Sampling SCANs the Redis keyspace, so keep this coarse on large deployments.
// Zero uses the 30s default; a negative value disables sampling.
func (b *Builder) WithHotSetSampleInterval(d time.Duration) *Builder {
	b.cfg.HotSetSampleInterval = d
	return b
}

// WithIndexSweepInterval sets how often one band's secondary indexes are swept
// for members whose payload has expired. Zero uses the 15s default; a
// negative value disables the sweeper (Query still prunes what it touches).
func (b *Builder) WithIndexSweepInterval(d time.Duration) *Builder {
	b.cfg.IndexSweepInterval = d
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

	sh, err := shield.New(b.cfg.Redis.toInternal(), b.cfg.Namespace, b.cfg.BandCount, b.cfg.KeyTTL, b.cfg.ActivityWindow)
	if err != nil {
		return nil, err
	}

	var eng *engine.Engine
	var wrappedCb engine.OnFlushCallback

	// ── WRITE PATH: Only initialize if both Sink and WriteContract are present ──
	// No mutual exclusion enforcement. If only one is provided, the engine
	// simply won't start, and Write() will return ErrWriteNotConfigured at call time.
	if b.sk != nil && b.writeContract != nil {
		if err := b.sk.Ping(ctx); err != nil {
			return nil, fmt.Errorf("sluice: sink ping failed: %w", err)
		}

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

		wrappedContract := func(correlationKey string, payload []byte) (*sink.WriteModel, error) {
			wm, err := b.writeContract(correlationKey, payload)
			if err != nil {
				return nil, err
			}
			return &sink.WriteModel{
				CorrelationKey: "",
				Filter:         wm.Filter,
				Update:         wm.Update,
				Upsert:         wm.Upsert,
			}, nil
		}

		eng = engine.New(b.cfg.toInternal(), sh, b.sk, wrappedContract, metrics, wrappedCb)
		eng.Start()

		if b.cfg.BatchedWrites {
			sh.EnableBatching(b.cfg.WriteBatchSize, b.cfg.WriteBatchWindow)
			sh.SetVolumeSignaler(func(band int) { eng.SignalVolume(band) })
			sh.StartBatcher(ctx)
		}
	}

	s := &Sluice{
		cfg:               b.cfg,
		shield:            sh,
		engine:            eng, // nil if write path not configured
		sk:                b.sk,
		src:               b.src,
		writeContract:     b.writeContract,
		readContract:      b.cfg.ReadContract,
		indexContract:     b.indexContract,
		readBulkContract:  b.readBulkContract,
		indexBulkContract: b.indexBulkContract,
		metrics:           metrics,
	}

	// ── L1 Local Journal (opt-in, default-off) ────────────────────────────
	// ── L1 Local Journal (opt-in, default-off) ────────────────────────────
	if b.localCacheCfg.Mode != localjournal.LocalCacheOff {
		if b.localCacheCfg.MaxEntries <= 0 {
			b.localCacheCfg.MaxEntries = 200_000
		}
		if b.localCacheCfg.LocalTTL <= 0 {
			b.localCacheCfg.LocalTTL = 60 * time.Second
		}

		namespace := s.shield.Namespace()

		// SINGLE instantiation of s.local
		s.local = localjournal.New(localjournal.Config{
			Namespace:  namespace,
			MaxEntries: b.localCacheCfg.MaxEntries,
			LocalTTL:   b.localCacheCfg.LocalTTL,
			Metrics:    s.metrics,
		})

		if b.localCacheCfg.Mode == localjournal.LocalCachePushPull {
			if b.localCacheCfg.Retention <= 0 {
				b.localCacheCfg.Retention = 200_000
			}

			s.shield.EnableBroadcast(shield.BroadcastConfig{
				Mode:   b.localCacheCfg.Broadcast,
				MaxLen: b.localCacheCfg.Retention,
			})

			// Subscriber correctly receives the one and only s.local
			s.broadcastSub = broadcast.NewSubscriber(
				s.shield.Client(),
				shield.BroadcastKey(namespace),
				s.local,
				s.metrics,
				broadcast.Config{
					Namespace: namespace,
					Mode:      broadcast.Mode(b.localCacheCfg.Broadcast),
					Stream:    shield.BroadcastKey(namespace),
					MaxLen:    b.localCacheCfg.Retention,
				},
			)
			s.broadcastSub.Start()
		}
	}
	// Inside Build(ctx), after shield.New and EnableBatching:
	// ── L1 Local Journal (opt-in, default-off) ────────────────────────────
	if b.cfg.DLQAutoProcess {
		s.startDLQProcessor()
	}
	// Gauges are sampled rather than event-driven; skip the SCAN cost
	// entirely when nobody is recording metrics.
	if _, noop := metrics.(*noopMetrics); !noop {
		s.startGaugeSampler()
	}
	if s.cfg.IndexContract != nil || s.indexContract != nil || s.indexBulkContract != nil {
		s.startIndexSweeper()
	}

	return s, nil
}

func (b *Builder) validate() error {
	if b.cfg.Namespace == "" {
		return ErrMissingNamespace
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
	_, _, isHot, err := s.shield.ReadWithTTL(ctx, correlationKey)
	s.metrics.RecordRedisOp(s.cfg.Namespace, "is_hot", time.Since(t), err)
	return isHot, err
}

func (s *Sluice) Read(ctx context.Context, correlationKey string) ([]byte, error) {
	if s.closed.Load() {
		return nil, ErrLibraryClosed
	}

	// Hot = served from L1 or L2 (no Source round-trip); cold = L3 fallback.
	t := time.Now()

	// ── Tier 1: L1 Local Journal ─────────────────────────────────────────
	if s.local != nil {
		p, _, ok := s.local.Get(correlationKey)
		if ok {
			s.metrics.RecordRead(s.cfg.Namespace, time.Since(t), true, nil)
			return p, nil
		}
	}

	// ── Tier 2: L2 Redis Journal ─────────────────────────────────────────
	tRead := time.Now()
	jr, err := s.shield.ReadJournal(ctx, correlationKey)
	s.metrics.RecordRedisOp(s.cfg.Namespace, "readjournal", time.Since(tRead), err)

	if err != nil {
		s.metrics.RecordRead(s.cfg.Namespace, time.Since(t), false, err)
		return nil, err
	}

	if jr.Found {
		if s.local != nil {
			s.local.Put(correlationKey, jr.Payload, jr.Version)
		}

		if s.cfg.HotAwareFlush && jr.PTTL >= 0 && jr.PTTL < s.cfg.ActivityWindow/5 {
			go func() {
				bgCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				defer cancel()
				_ = s.shield.RefreshHotTTL(bgCtx, s.shield.BandFor(correlationKey), []string{correlationKey})
			}()
		}

		s.metrics.RecordRead(s.cfg.Namespace, time.Since(t), true, nil)
		return jr.Payload, nil
	}

	// ── Tier 3: L3 Source Fallback (Cold Read) ─────────────────────────
	// ── Tier 3: L3 Source Fallback (Cold Read) ─────────────────────────
	if s.src != nil && s.readContract != nil {
		readModel, err := s.readContract(correlationKey)
		if err != nil {
			s.metrics.RecordRead(s.cfg.Namespace, time.Since(t), false, err)
			return nil, fmt.Errorf("read contract: %w", err)
		}
		hydrateTs := time.Now().UnixMilli() // captured before the Source read
		payload, err := s.src.Read(ctx, *readModel)
		s.metrics.RecordRead(s.cfg.Namespace, time.Since(t), false, err)
		if err != nil {
			if errors.Is(err, source.ErrRecordNotFound) {
				return nil, ErrRecordNotFound
			}
			return nil, err
		}

		// ── Asynchronous Read-Through Cache Hydration ─────────────────
		// Return payload immediately to prioritize fast cold reads.
		// Hydration of L2, L1, and Indexes happens in the background.
		go s.hydrateReadThrough(correlationKey, payload, hydrateTs, true)

		return payload, nil
	}
	s.metrics.RecordRead(s.cfg.Namespace, time.Since(t), false, ErrRecordNotFound)
	return nil, ErrRecordNotFound
}

// hydrateReadThrough seeds L2, L1 (when withL1), and secondary indexes with a
// payload read from the Source. Best-effort; runs off the caller's path.
//
// L2 hydration only fills a journal miss and never marks the key dirty — see
// shield.HydrateJournal. Indexes are only updated when the Source payload was
// actually written; a winning journal entry was already indexed by the write
// path.
func (s *Sluice) hydrateReadThrough(correlationKey string, payload []byte, hydrateTs int64, withL1 bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	hr, err := s.shield.HydrateJournal(ctx, correlationKey, payload, hydrateTs)
	if err != nil {
		s.metrics.RecordRedisOp(s.cfg.Namespace, "write_hydration", 0, err)
		return
	}
	s.applyHydration(ctx, correlationKey, hr, withL1, s.indexContract)
}

// applyHydration mirrors a hydration outcome into L1 and, when the Source
// payload won, into the secondary indexes.
func (s *Sluice) applyHydration(ctx context.Context, correlationKey string, hr shield.HydrateResult, withL1 bool, ic IndexContract) {
	if withL1 && s.local != nil {
		s.local.Put(correlationKey, hr.Payload, hr.Version)
	}
	if !hr.Written || ic == nil {
		return
	}
	indexFields, idxErr := ic(correlationKey, hr.Payload)
	if idxErr == nil && len(indexFields) > 0 {
		if err := s.shield.UpdateIndexes(ctx, correlationKey, indexFields, s.cfg.ActivityWindow); err != nil {
			s.metrics.RecordRedisOp(s.cfg.Namespace, "updateindexes_hydration", 0, err)
		}
	}
}

// ReadFresh is the strong-consistency escape hatch (RFC §5.8).
// It bypasses L1 entirely and reads from the Redis journal (L2), falling
// through to the Source (L3) on a journal miss — identical semantics to
// v1.0.7 Read(). Use for money-critical reads that cannot tolerate
// bounded staleness; use Read() for everything else.
// ReadFresh is the strong-consistency escape hatch (RFC §5.8).
// It bypasses L1 entirely and reads from the Redis journal (L2), falling
// through to the Source (L3) on a journal miss.
func (s *Sluice) ReadFresh(ctx context.Context, correlationKey string) ([]byte, error) {
	if s.closed.Load() {
		return nil, ErrLibraryClosed
	}

	tRead := time.Now()
	jr, err := s.shield.ReadJournal(ctx, correlationKey)
	s.metrics.RecordRedisOp(s.cfg.Namespace, "readfresh", time.Since(tRead), err)
	if err != nil {
		return nil, err
	}

	if jr.Found {
		// Deliberately do NOT heal L1 here. ReadFresh callers want the
		// journal's authoritative view; seeding L1 would mask freshness.
		return jr.Payload, nil
	}

	// ── L3: Source fallback ─────────────────────────────────────────────
	// ── L3: Source fallback ─────────────────────────────────────────────
	if s.src != nil && s.readContract != nil {
		readModel, err := s.readContract(correlationKey)
		if err != nil {
			return nil, fmt.Errorf("read contract: %w", err)
		}
		hydrateTs := time.Now().UnixMilli() // captured before the Source read
		payload, err := s.src.Read(ctx, *readModel)
		if err != nil {
			if errors.Is(err, source.ErrRecordNotFound) {
				return nil, ErrRecordNotFound
			}
			return nil, err
		}

		// ── Asynchronous Read-Through Cache Hydration (L2 + Index only) ─────────────────
		go s.hydrateReadThrough(correlationKey, payload, hydrateTs, false)

		return payload, nil
	}
	return nil, ErrRecordNotFound
}

// ReadOld returns current state from the journal.
// Hot correlation_key: sub-millisecond Redis read with lazy TTL refresh.
// Cold correlation_key: falls back to the configured Source via ReadContract.
// ReadOld returns current state from the journal.
// Deprecated: Superseded by the new tiered Read() method.
func (s *Sluice) ReadOld(ctx context.Context, correlationKey string) ([]byte, error) {
	if s.closed.Load() {
		return nil, ErrLibraryClosed
	}

	t := time.Now()
	payload, pttl, _, err := s.shield.ReadWithTTL(ctx, correlationKey)

	// Hot path: found in Redis journal
	if err == nil && payload != nil {
		s.metrics.RecordRead(s.cfg.Namespace, time.Since(t), true, nil)
		threshold := s.cfg.ActivityWindow / 5
		if pttl > 0 && pttl < threshold {
			_ = s.shield.SetHotMarker(ctx, correlationKey, s.cfg.ActivityWindow)
			_ = s.shield.RefreshHotTTL(ctx, s.shield.BandFor(correlationKey), []string{correlationKey})
		}
		return payload, nil
	}

	// Cold path: fallback to the backing datastore via Source
	// Cold path: fallback to the backing datastore via Source
	if s.cfg.ReadContract != nil && s.src != nil {
		model, modelErr := s.cfg.ReadContract(correlationKey)
		if modelErr != nil {
			s.metrics.RecordRead(s.cfg.Namespace, time.Since(t), false, modelErr)
			return nil, modelErr
		}

		hydrateTs := time.Now().UnixMilli() // captured before the Source read
		srcPayload, srcErr := s.src.Read(ctx, *model)
		s.metrics.RecordRead(s.cfg.Namespace, time.Since(t), false, srcErr)
		if srcErr != nil {
			if errors.Is(srcErr, source.ErrRecordNotFound) {
				return nil, ErrRecordNotFound
			}
			return nil, srcErr
		}

		// ── Asynchronous Read-Through Cache Hydration ─────────────────
		go s.hydrateReadThrough(correlationKey, srcPayload, hydrateTs, true)

		return srcPayload, nil
	}

	s.metrics.RecordRead(s.cfg.Namespace, time.Since(t), false, ErrRecordNotFound)
	return nil, ErrRecordNotFound
}

// HotLoad activates a correlation_key on user login.
// It synchronously sets the hot marker (the "command") and asynchronously
// hydrates the payload from the Source into the L2/L1 journals (the "side effect").
// The caller should NOT block on payload hydration — this is a fire-and-forget operation.
func (s *Sluice) HotLoad(ctx context.Context, correlationKey string) error {
	if s.closed.Load() {
		return ErrLibraryClosed
	}
	if s.src == nil {
		return ErrMissingSource
	}
	if s.cfg.ReadContract == nil {
		return ErrMissingReadContract
	}

	// 1. SYNCHRONOUS: Set the hot marker immediately.
	//    This is the actual "command" — it tells the flush engine that this
	//    key is now hot, so subsequent Write() calls will trigger immediate flushes.
	//    This is a single Redis SET NX EX — sub-millisecond.
	if err := s.shield.SetHotMarker(ctx, correlationKey, s.cfg.ActivityWindow); err != nil {
		return fmt.Errorf("hotload: failed to set hot marker: %w", err)
	}

	// 2. ASYNCHRONOUS: Hydrate the payload from Source → L2 → L1 → Indexes.
	//    This is a side effect, not the primary objective. The caller should
	//    not wait for this. If it fails, the next Read() will simply do a cold
	//    read from the Source, which is graceful degradation.
	go func() {
		bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		t := time.Now()

		// 2a. Read from Source (cold read)
		model, err := s.cfg.ReadContract(correlationKey)
		if err != nil {
			s.metrics.RecordWarmUp(s.cfg.Namespace, time.Since(t), err)
			return
		}

		hydrateTs := time.Now().UnixMilli() // captured before the Source read
		payload, err := s.src.Read(bgCtx, *model)
		s.metrics.RecordWarmUp(s.cfg.Namespace, time.Since(t), err)
		if err != nil {
			return // Source miss or error — graceful degradation
		}

		// 2b. Hydrate L2 (fills a journal miss only, never dirty) + seed broadcast
		hr, err := s.shield.HydrateHotLoad(bgCtx, correlationKey, payload, hydrateTs)
		if err != nil {
			s.metrics.RecordRedisOp(s.cfg.Namespace, "hotload_hydration", 0, err)
			if !hr.Written {
				return // journal write failed; a broadcast-only failure still hydrates L1/indexes
			}
		}

		// 2c/2d. Hydrate L1 and secondary indexes (best-effort)
		s.applyHydration(bgCtx, correlationKey, hr, true, s.cfg.IndexContract)
	}()

	return nil
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
	if s.writeContract == nil {
		return ErrMissingWriteContract
	}
	if s.sk == nil {
		return ErrMissingSink
	}
	if correlationKey == "" {
		return ErrEmptyCorrelationKey
	}

	t := time.Now()

	// Capture L1 version BEFORE the Redis write.
	// This guarantees l1Version <= journal_ts. If a subsequent ReadJournal heal
	// or Phase 2 broadcast arrives with the journal's exact ts, it will safely
	// update L1 without violating the strict (>) version gating.
	l1Version := t.UnixMilli()

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

		// ── L1 Write-Through (Sub-microsecond same-pod consistency) ─────
		if s.local != nil {
			applied := s.local.Put(correlationKey, payload, l1Version)
			slog.Debug("WRITE-THROUGH", "crn", correlationKey, "version", l1Version, "applied", applied)
		}
		// 2. Index maintenance via pipeline (best-effort, Valkey-safe)
		if s.cfg.IndexContract != nil {
			indexes, idxErr := s.cfg.IndexContract(correlationKey, payload)
			if idxErr == nil && len(indexes) > 0 {
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
	if s.writeContract == nil {
		return ErrMissingWriteContract
	}
	if s.sk == nil {
		return ErrMissingSink
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

		t := time.Now()
		matches, err := s.shield.QueryBand(ctx, band, eqKeys, q.RangeMin, q.RangeMax)
		s.metrics.RecordRedisOp(s.cfg.Namespace, "query_band", time.Since(t), err)
		if err != nil {
			return nil, err
		}
		for _, m := range matches {
			results = append(results, QueryResult{CorrelationKey: m.CorrelationKey, Payload: m.Payload})
		}
	}
	return results, nil
}

// DrainAndClose flushes all remaining dirty keys, stops band goroutines,
// and releases Redis and sink connections. Call exactly once during shutdown.
func (s *Sluice) DrainAndClose(ctx context.Context) error {
	if s.broadcastSub != nil {
		s.broadcastSub.Stop()
	}
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	if s.dlqCancel != nil {
		s.dlqCancel()
		<-s.dlqDone
	}
	if s.gaugeCancel != nil {
		s.gaugeCancel()
		<-s.gaugeDone
	}
	if s.sweepCancel != nil {
		s.sweepCancel()
		<-s.sweepDone
	}

	if s.engine != nil {
		if s.cfg.BatchedWrites {
			s.shield.StopBatcher()
		}
		s.engine.DrainAndStop()
	}

	if err := s.shield.Close(); err != nil {
		return fmt.Errorf("sluice: redis close: %w", err)
	}

	if s.sk != nil {
		return s.sk.Close(ctx)
	}
	return nil
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

	if s.writeContract == nil {
		return nil, ErrMissingWriteContract
	}
	if s.sk == nil {
		return nil, ErrMissingSink
	}

	o := &dlqOptions{maxBatchSize: s.cfg.MaxBatchSize}
	for _, opt := range opts {
		opt(o)
	}

	// Wrap the public WriteContract to produce sink.WriteModel.
	wrappedContract := func(correlationKey string, payload []byte) (*sink.WriteModel, error) {
		wm, err := s.writeContract(correlationKey, payload)
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

// localSetSampleInterval is how often the L1 size gauge is emitted. Cache.Len
// is an in-process shard walk, so it can run far more often than the SCAN.
const localSetSampleInterval = 5 * time.Second

// startGaugeSampler periodically emits the gauges that have no natural event
// to hang off: hot set size (Redis SCAN) and L1 local set size.
func (s *Sluice) startGaugeSampler() {
	hotInterval := s.cfg.HotSetSampleInterval
	if hotInterval == 0 {
		hotInterval = 30 * time.Second
	}
	if hotInterval < 0 && s.local == nil {
		return // nothing to sample
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.gaugeCancel = cancel
	s.gaugeDone = make(chan struct{})

	sampleHot := func() {
		scanCtx, cancel := context.WithTimeout(ctx, hotInterval)
		defer cancel()
		t := time.Now()
		n, err := s.shield.CountHotMarkers(scanCtx)
		s.metrics.RecordRedisOp(s.cfg.Namespace, "count_hot", time.Since(t), err)
		if err == nil {
			s.metrics.RecordHotSetSize(s.cfg.Namespace, n)
		}
	}
	sampleLocal := func() {
		s.metrics.RecordLocalSetSize(s.cfg.Namespace, s.local.Len())
	}

	go func() {
		defer close(s.gaugeDone)

		// Nil channels never fire, disabling the corresponding case.
		var hotC, localC <-chan time.Time
		if hotInterval > 0 {
			ht := time.NewTicker(hotInterval)
			defer ht.Stop()
			hotC = ht.C
			sampleHot()
		}
		if s.local != nil {
			lt := time.NewTicker(localSetSampleInterval)
			defer lt.Stop()
			localC = lt.C
			sampleLocal()
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-hotC:
				sampleHot()
			case <-localC:
				sampleLocal()
			}
		}
	}()
}

// indexSweepBatch is the SSCAN/ZSCAN page size (and EXISTS pipeline size)
// used by the index sweeper.
const indexSweepBatch = 500

// startIndexSweeper prunes index members whose payload has expired, one band
// per tick, so index memory tracks the live journal even when nothing queries.
func (s *Sluice) startIndexSweeper() {
	interval := s.cfg.IndexSweepInterval
	if interval < 0 {
		return
	}
	if interval == 0 {
		interval = 15 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.sweepCancel = cancel
	s.sweepDone = make(chan struct{})

	go func() {
		defer close(s.sweepDone)

		// One-time: bring index keys written before the registry existed
		// under the sweeper's reach.
		if _, err := s.shield.RegisterExistingIndexes(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("sluice: index registry bootstrap failed", "namespace", s.cfg.Namespace, "error", err)
		}

		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		band := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				sweepCtx, cancel := context.WithTimeout(ctx, interval)
				t := time.Now()
				scanned, pruned, err := s.shield.SweepIndexBand(sweepCtx, band, indexSweepBatch)
				cancel()
				s.metrics.RecordRedisOp(s.cfg.Namespace, "index_sweep", time.Since(t), err)
				if pruned > 0 {
					slog.Debug("sluice: index sweep", "namespace", s.cfg.Namespace, "band", band, "scanned", scanned, "pruned", pruned)
				}
				band = (band + 1) % s.cfg.BandCount
			}
		}
	}()
}

func (s *Sluice) degradedWrite(ctx context.Context, correlationKey string, payload []byte) error {
	wm, err := s.writeContract(correlationKey, payload)
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

// Test methods

// TestConfig is a minimal wiring struct for integration tests that need to
// inject a pre-built shield and L1 cache without going through the full
// Build() pipeline (which requires sink, engine, source, etc.).
//
// NOT part of the public API contract — guarded by build tag or test-only
// file if you prefer.
type TestConfig struct {
	Namespace string
	Shield    *shield.Shield
	Local     *localjournal.Cache
	Metrics   MetricsRecorder
}

// NewForTest assembles a Sluice from pre-built dependencies. Test-only.
func NewForTest(cfg TestConfig) *Sluice {
	return &Sluice{
		cfg:     DefaultConfig(cfg.Namespace),
		shield:  cfg.Shield,
		local:   cfg.Local,
		metrics: cfg.Metrics,
	}
}
