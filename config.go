package sluice

import (
	"crypto/tls"
	"time"

	"github.com/hussainpithawala/sluice-go/internal/engine"
	"github.com/hussainpithawala/sluice-go/internal/shield"
)

// ReadContract loads the current payload for a correlation key from the
// backing sink/document store. Used by HotLoad() and cold Read() fallback.
type ReadContract func(correlationKey string) ([]byte, error)

// IndexContract extracts secondary-index fields from a payload.
//
// string values      -> equality SET index
// int/int64/float64  -> range ZSET index
// time.Time          -> range ZSET index using Unix milliseconds
type IndexContract func(correlationKey string, payload []byte) (map[string]interface{}, error)

// Config holds every tunable for the library.
// Construct via the Builder — do not instantiate directly.
type Config struct {
	Namespace          string
	BandCount          int
	FlushWindow        time.Duration
	KeyTTL             time.Duration
	ActivityWindow     time.Duration // TTL for hot CRN sessions (default 4h)
	MaxBatchSize       int
	Redis              RedisConfig
	DegradedModeDirect bool
	HotAwareFlush      bool // Extend TTL on successful commit
	Metrics            MetricsRecorder

	ContentDedup   bool          // Enable xxHash64 payload deduplication
	IdempotencyTTL time.Duration // TTL for WriteIdempotent keys

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

	ReadContract  ReadContract
	IndexContract IndexContract
}

// RedisConfig holds Redis connectivity parameters.
//
// ClusterMode selects a cluster-aware client and MUST be set explicitly —
// it is NOT inferred from len(Addrs). A cluster-mode-enabled deployment
// (e.g. AWS ElastiCache CME) is commonly reached via a SINGLE configuration
// endpoint address, which is indistinguishable from a single standalone
// node by address count alone. Get this wrong and commands routed to a
// slot outside whichever node you happen to hit will fail once the cluster
// issues a MOVED redirect that a standalone client doesn't follow.
type RedisConfig struct {
	// Network type to use, either tcp or unix.
	// Default is tcp.
	Network string

	// Redis server address(es) in "host:port" format. For cluster mode,
	// typically a single cluster configuration endpoint is sufficient —
	// go-redis discovers the full shard topology from it. See ClusterMode.
	Addrs []string

	// ClusterMode selects a cluster-aware client (go-redis ClusterClient)
	// when true, or a standalone client when false. Required — does not
	// default based on Addrs. See type-level doc above for why.
	ClusterMode bool

	// Username to authenticate the current connection when Redis ACLs are used.
	// See: https://redis.io/commands/auth.
	Username string

	// Password to authenticate the current connection.
	// See: https://redis.io/commands/auth.
	Password string

	// Redis DB to select after connecting to a server.
	// See: https://redis.io/commands/select.
	// NOTE: cluster-mode-enabled clusters only support DB 0.
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
//
// Previously this dropped Username and Network entirely — ACL-authenticated
// connections (Username set, no error surfaced) would silently connect
// without the username, working only by accident on deployments that don't
// enforce ACL user matching. Fixed alongside the ClusterMode plumbing since
// this file was already being touched for that.
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
func (c Config) toInternal() engine.Config {
	return engine.Config{
		Namespace:      c.Namespace,
		BandCount:      c.BandCount,
		FlushWindow:    c.FlushWindow,
		MaxBatchSize:   c.MaxBatchSize,
		KeyTTL:         c.KeyTTL,
		ActivityWindow: c.ActivityWindow,
		HotAwareFlush:  c.HotAwareFlush,
		ReadContract:   engine.ReadContract(c.ReadContract),
		IndexContract:  engine.IndexContract(c.IndexContract),
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
		ReadContract:       nil,
		IndexContract:      nil,
	}
}

// Builder methods for new fields
func (b *Builder) WithActivityWindow(d time.Duration) *Builder { b.cfg.ActivityWindow = d; return b }
func (b *Builder) WithHotAwareFlush(v bool) *Builder           { b.cfg.HotAwareFlush = v; return b }
func (b *Builder) WithContentDedup(v bool) *Builder            { b.cfg.ContentDedup = v; return b }
func (b *Builder) WithIdempotencyTTL(d time.Duration) *Builder { b.cfg.IdempotencyTTL = d; return b }
func (b *Builder) WithReadContract(rc ReadContract) *Builder   { b.cfg.ReadContract = rc; return b }
func (b *Builder) WithIndexContract(ic IndexContract) *Builder { b.cfg.IndexContract = ic; return b }
