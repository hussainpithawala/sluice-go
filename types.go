package sluice

import (
	"context"
	"crypto/tls"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/hussainpithawala/sluice-go/internal/engine"
	"github.com/hussainpithawala/sluice-go/internal/shield"
	"github.com/hussainpithawala/sluice-go/sink"
	"github.com/hussainpithawala/sluice-go/source"
)

// ─── Core Structures ─────────────────────────────────────────────────────────

// Sluice is the main entry point. Safe for concurrent use.
// Construct via New().Build() — never instantiate directly.
type Sluice struct {
	cfg       Config
	shield    *shield.Shield
	engine    *engine.Engine
	sk        sink.FlushSink
	src       source.Source
	contract  WriteContract
	metrics   MetricsRecorder
	closed    atomic.Bool
	dlqCancel context.CancelFunc
	dlqDone   chan struct{}
}

// Builder assembles a Sluice instance with a fluent API.
type Builder struct {
	cfg      Config
	sk       sink.FlushSink
	src      source.Source
	contract WriteContract
	callback OnFlushCallback
}

// ─── Configuration ───────────────────────────────────────────────────────────

// Config holds every tunable for the library.
// Construct via the Builder — do not instantiate directly.
type Config struct {
	Namespace          string
	BandCount          int
	FlushWindow        time.Duration
	MaxBatchSize       int
	KeyTTL             time.Duration // In-flight dirty key TTL
	ActivityWindow     time.Duration // Hot correlation_key session TTL (default 4h)
	DegradedModeDirect bool
	HotAwareFlush      bool          // Extend TTL on successful commit
	ContentDedup       bool          // Enable xxHash64 payload deduplication
	IdempotencyTTL     time.Duration // TTL for WriteIdempotent keys

	Redis   RedisConfig
	Metrics MetricsRecorder

	BatchedWrites    bool
	WriteBatchSize   int
	WriteBatchWindow time.Duration

	DLQAutoProcess     bool
	DLQProcessInterval time.Duration
	DLQProcessStrategy DLQStrategy

	ReadContract  ReadContract
	IndexContract IndexContract
}

// RedisConfig holds Redis connectivity parameters.
type RedisConfig struct {
	Network      string
	Addrs        []string
	ClusterMode  bool
	Username     string
	Password     string
	DB           int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	PoolSize     int
	TLSConfig    *tls.Config
}

// ─── Domain Contracts ────────────────────────────────────────────────────────

// WriteContract is the domain function the caller contributes for the write path.
type WriteContract func(correlationKey string, payload []byte) (*WriteModel, error)

// ReadContract translates a correlation key into a datastore-agnostic ReadModel.
type ReadContract func(correlationKey string) (*source.ReadModel, error)

// IndexContract extracts secondary index fields from a payload.
type IndexContract func(correlationKey string, payload []byte) (map[string]interface{}, error)

// ─── Write Path Models ───────────────────────────────────────────────────────

// WriteModel is the fully resolved upsert instruction returned by a WriteContract.
type WriteModel struct {
	Filter interface{}
	Update interface{}
	Upsert bool
}

// FlushRecord is the internal unit that travels through the pipeline.
type FlushRecord struct {
	CorrelationKey string
	Payload        []byte
	ReceivedAt     time.Time
}

// OnFlushCallback is invoked after every BulkWrite attempt.
type OnFlushCallback func(correlationKeys []string, result *BulkWriteResult, err error)

// BulkWriteResult carries the outcome of a sink BulkWrite call.
type BulkWriteResult struct {
	InsertedCount int64
	MatchedCount  int64
	ModifiedCount int64
	UpsertedCount int64
	Errors        []SinkError
}

// SinkError ties a write failure back to its originating correlation key.
type SinkError struct {
	CorrelationKey string
	Code           int
	Err            error
}

// ErrCodeDuplicateKey is the MongoDB / AWS DocumentDB error code for a unique-index violation.
const ErrCodeDuplicateKey = 11000

// ─── DLQ Models ──────────────────────────────────────────────────────────────

// DLQStrategy selects how dead-letter records are processed.
type DLQStrategy int

const (
	DLQIgnore DLQStrategy = iota
	DLQUpsert
	DLQReInsert
)

// DLQResult carries the outcome of a ProcessDLQ invocation.
type DLQResult struct {
	Processed int
	Succeeded int
	Failed    int
}

// DLQOption configures the behaviour of ProcessDLQ.
type DLQOption func(*dlqOptions)

type dlqOptions struct {
	maxBatchSize int
	keyMutator   func(string) string
	logger       *slog.Logger
}

// ─── Query Models ────────────────────────────────────────────────────────────

// Query defines the parameters for a compound lookup against the Redis journal.
type Query struct {
	Equality map[string]string
	RangeMin map[string]float64
	RangeMax map[string]float64
}

// QueryResult represents a single record returned by a Query operation.
type QueryResult struct {
	CorrelationKey string
	Payload        []byte
}
