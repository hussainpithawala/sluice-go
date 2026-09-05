package sluice

import (
	"crypto/tls"
	"time"

	"github.com/hussainpithawala/sluice-go/internal/engine"
	"github.com/hussainpithawala/sluice-go/internal/shield"
)

// Config holds every tunable for the library.
// Construct via the Builder — do not instantiate directly.
type Config struct {
	Namespace          string
	BandCount          int
	FlushWindow        time.Duration
	MaxBatchSize       int
	KeyTTL             time.Duration // In-flight dirty key TTL
	ActivityWindow     time.Duration // Hot CRN session TTL (default 4h)
	DegradedModeDirect bool
	HotAwareFlush      bool // Extend TTL on successful commit

	ContentDedup   bool          // Enable xxHash64 payload deduplication
	IdempotencyTTL time.Duration // TTL for WriteIdempotent keys

	Redis   RedisConfig
	Metrics MetricsRecorder

	// BatchedWrites enables pipelined Redis writes. When true, Write() calls
	// are buffered in memory and flushed to Redis in a single pipeline.
	BatchedWrites    bool
	WriteBatchSize   int           // buffer capacity before pipeline flush; default 200
	WriteBatchWindow time.Duration // max time before a partial buffer is flushed; default 5ms

	// DLQAutoProcess enables a background ticker that periodically calls
	// ProcessDLQ with the configured strategy.
	DLQAutoProcess     bool
	DLQProcessInterval time.Duration // ticker interval; default 30s
	DLQProcessStrategy DLQStrategy   // strategy for auto-processing; default DLQUpsert

	// Domain Contracts
	ReadContract  ReadContract  // Translates CRN to datastore-agnostic ReadModel
	IndexContract IndexContract // Extracts secondary index fields from payload
}

// RedisConfig holds Redis connectivity parameters.
//
// ClusterMode selects a cluster-aware client and MUST be set explicitly —
// it is NOT inferred from len(Addrs).
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

// toInternal converts to internal shield.RedisConfig.
func (c RedisConfig) toInternal() shield.RedisConfig {
	return shield.RedisConfig{
		Network:      c.Network,
		Addrs:        c.Addrs,
		ClusterMode:  c.ClusterMode,
		Username:     c.Username,
		Password:     c.Password,
		DB:           c.DB,
		DialTimeout:  c.DialTimeout,
		ReadTimeout:  c.ReadTimeout,
		WriteTimeout: c.WriteTimeout,
		PoolSize:     c.PoolSize,
		TLSConfig:    c.TLSConfig,
	}
}

// toInternal converts to internal engine.Config.
// Note: ReadContract and IndexContract are NOT passed to the engine.
// The engine only handles the write path (flushing dirty keys to the sink).
// The read path (HotLoad/Read) is orchestrated directly by Sluice using the Source.
func (c Config) toInternal() engine.Config {
	return engine.Config{
		Namespace:      c.Namespace,
		BandCount:      c.BandCount,
		FlushWindow:    c.FlushWindow,
		MaxBatchSize:   c.MaxBatchSize,
		KeyTTL:         c.KeyTTL,
		ActivityWindow: c.ActivityWindow,
		HotAwareFlush:  c.HotAwareFlush,
	}
}

func defaultConfig(namespace string) Config {
	return Config{
		Namespace:          namespace,
		BandCount:          16,
		FlushWindow:        250 * time.Millisecond,
		MaxBatchSize:       1000,
		KeyTTL:             30 * time.Second,
		ActivityWindow:     4 * time.Hour,
		IdempotencyTTL:     4 * time.Hour,
		DegradedModeDirect: true,
		HotAwareFlush:      true,
		ContentDedup:       false,
	}
}
