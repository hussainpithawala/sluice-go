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
	"fmt"
	"sync/atomic"
	"time"

	"log/slog"

	"github.com/hussainpithawala/sluice-go/internal/dlq"
	"github.com/hussainpithawala/sluice-go/internal/engine"
	"github.com/hussainpithawala/sluice-go/internal/shield"
	"github.com/hussainpithawala/sluice-go/sink"
)

// Sluice is the main entry point. Safe for concurrent use.
// Construct via New().Build() — never instantiate directly.
type Sluice struct {
	cfg      Config
	shield   *shield.Shield
	engine   *engine.Engine
	sk       sink.FlushSink
	contract WriteContract
	metrics  MetricsRecorder
	closed   atomic.Bool
}

// Builder assembles a Sluice instance with a fluent API.
type Builder struct {
	cfg      Config
	sk       sink.FlushSink
	contract WriteContract
	callback OnFlushCallback
}

// New returns a Builder initialised with production-safe defaults.
// namespace isolates all Redis keys for this instance.
func New(namespace string) *Builder {
	return &Builder{cfg: defaultConfig(namespace)}
}

func (b *Builder) WithRedis(rc RedisConfig) *Builder           { b.cfg.Redis = rc; return b }
func (b *Builder) WithSink(s sink.FlushSink) *Builder          { b.sk = s; return b }
func (b *Builder) WithWriteContract(wc WriteContract) *Builder { b.contract = wc; return b }
func (b *Builder) WithFlushWindow(d time.Duration) *Builder    { b.cfg.FlushWindow = d; return b }
func (b *Builder) WithMaxBatchSize(n int) *Builder             { b.cfg.MaxBatchSize = n; return b }
func (b *Builder) WithBandCount(n int) *Builder                { b.cfg.BandCount = n; return b }
func (b *Builder) WithKeyTTL(d time.Duration) *Builder         { b.cfg.KeyTTL = d; return b }
func (b *Builder) WithDegradedModeDirect(v bool) *Builder      { b.cfg.DegradedModeDirect = v; return b }
func (b *Builder) WithMetrics(m MetricsRecorder) *Builder      { b.cfg.Metrics = m; return b }
func (b *Builder) OnFlush(cb OnFlushCallback) *Builder         { b.callback = cb; return b }

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
	sh, err := shield.New(b.cfg.Redis.toInternal(), b.cfg.Namespace, b.cfg.BandCount, b.cfg.KeyTTL)
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
	wrappedContract := func(crn string, payload []byte) (*sink.WriteModel, error) {
		wm, err := b.contract(crn, payload)
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
	return &Sluice{cfg: b.cfg, shield: sh, engine: eng, sk: b.sk, contract: b.contract, metrics: metrics}, nil
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

// Write buffers payload under correlationKey in Redis and returns immediately.
// The document store is never touched during Write().
// Safe for concurrent use from any number of goroutines.
func (s *Sluice) Write(ctx context.Context, correlationKey string, payload []byte) error {
	if s.closed.Load() {
		return ErrLibraryClosed
	}
	if correlationKey == "" {
		return ErrEmptyCorrelationKey
	}
	t := time.Now()
	err := s.shield.Write(ctx, correlationKey, payload)
	s.metrics.RecordRedisOp(s.cfg.Namespace, "write", time.Since(t), err)
	if err != nil {
		if s.cfg.DegradedModeDirect {
			return s.degradedWrite(ctx, correlationKey, payload)
		}
		return fmt.Errorf("%w: %v", ErrRedisUnavailable, err)
	}
	s.metrics.RecordWrite(s.cfg.Namespace)
	band := s.shield.BandFor(correlationKey)
	if depth, depthErr := s.shield.DirtyQueueDepth(ctx, band); depthErr == nil {
		if int(depth) >= s.cfg.MaxBatchSize {
			s.engine.SignalVolume(band)
		}
	}
	return nil
}

// DrainAndClose flushes all remaining dirty keys, stops band goroutines,
// and releases Redis and sink connections. Call exactly once during shutdown.
func (s *Sluice) DrainAndClose(ctx context.Context) error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	s.engine.DrainAndStop()
	if err := s.shield.Close(); err != nil {
		return fmt.Errorf("sluice: redis close: %w", err)
	}
	return s.sk.Close(ctx)
}

// ── DLQ Processing ──────────────────────────────────────────────────────────

// DLQOption configures the behaviour of ProcessDLQ.
type DLQOption func(*dlqOptions)

type dlqOptions struct {
	maxBatchSize int
	keyMutator   func(string) string
	logger       *slog.Logger
}

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
	wrappedContract := func(crn string, payload []byte) (*sink.WriteModel, error) {
		wm, err := s.contract(crn, payload)
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
