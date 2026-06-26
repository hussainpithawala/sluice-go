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
	KeyTTL             time.Duration
	DegradedModeDirect bool
	Redis              RedisConfig
	Metrics            MetricsRecorder

	// BatchedWrites enables pipelined Redis writes. When true, Write() calls
	// are buffered in memory and flushed to Redis in a single pipeline,
	// reducing per-call round-trips from N to 1. Opt-in; default false.
	BatchedWrites    bool
	WriteBatchSize   int           // buffer capacity before pipeline flush; default 200
	WriteBatchWindow time.Duration // max time before a partial buffer is flushed; default 5ms

	// DLQAutoProcess enables a background ticker that periodically calls
	// ProcessDLQ with the configured strategy. Opt-in; default false.
	DLQAutoProcess     bool
	DLQProcessInterval time.Duration // ticker interval; default 30s
	DLQProcessStrategy DLQStrategy   // strategy for auto-processing; default DLQUpsert
}

// RedisConfig holds Redis connectivity parameters.
// Single Addrs entry = standalone mode. Multiple entries = Cluster mode.
type RedisConfig struct {
	// Network type to use, either tcp or unix.
	// Default is tcp.
	Network string

	// Redis server address in "host:port" format.
	Addrs []string

	// Username to authenticate the current connection when Redis ACLs are used.
	// See: https://redis.io/commands/auth.
	Username string

	// Password to authenticate the current connection.
	// See: https://redis.io/commands/auth.
	Password string

	// Redis DB to select after connecting to a server.
	// See: https://redis.io/commands/select.
	DB int

	// Dial timeout for establishing new connections.
	// Default is 5 seconds.
	DialTimeout time.Duration

	// Timeout for socket reads.
	// If timeout is reached, read commands will fail with a timeout error
	// instead of blocking.
	//
	// Use value -1 for no timeout and 0 for default.
	// Default is 3 seconds.
	ReadTimeout time.Duration

	// Timeout for socket writes.
	// If timeout is reached, write commands will fail with a timeout error
	// instead of blocking.
	//
	// Use value -1 for no timeout and 0 for default.
	// Default is ReadTimout.
	WriteTimeout time.Duration

	// Maximum number of socket connections.
	// Default is 10 connections per every CPU as reported by runtime.NumCPU.
	PoolSize int

	// TLS Config used to connect to a server.
	// TLS will be negotiated only if this field is set.
	TLSConfig *tls.Config
}

// toInternal converts to internal shield.RedisConfig.
func (c RedisConfig) toInternal() shield.RedisConfig {
	return shield.RedisConfig{
		Addrs: c.Addrs, Password: c.Password, DB: c.DB,
		DialTimeout: c.DialTimeout, ReadTimeout: c.ReadTimeout,
		WriteTimeout: c.WriteTimeout, PoolSize: c.PoolSize,
		TLSConfig: c.TLSConfig,
	}
}

// toInternal converts to internal engine.Config.
func (c Config) toInternal() engine.Config {
	return engine.Config{
		Namespace:    c.Namespace,
		BandCount:    c.BandCount,
		FlushWindow:  c.FlushWindow,
		MaxBatchSize: c.MaxBatchSize,
	}
}

func defaultConfig(namespace string) Config {
	return Config{
		Namespace:          namespace,
		BandCount:          16,
		FlushWindow:        250 * time.Millisecond,
		MaxBatchSize:       1000,
		KeyTTL:             30 * time.Second,
		DegradedModeDirect: true,
	}
}
