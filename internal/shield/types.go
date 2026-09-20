package shield

import (
	"context"
	"crypto/tls"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// FlushRecord is the internal unit that travels through the pipeline.
type FlushRecord struct {
	CorrelationKey string
	Payload        []byte
	ReceivedAt     time.Time
	JournalTS      int64 // UnixMilli write ts; 0 if absent
}

// WriteOptions configures optional behaviors for a single write operation.
type WriteOptions struct {
	ForceHot     bool
	ContentHash  string
	DedupEnabled bool
	IndexesJSON  string
}

// RedisConfig holds Redis connectivity parameters.
type RedisConfig struct {
	// Network type to use, either tcp or unix.
	// Default is tcp.
	//
	// Ignored when ClusterMode is true: go-redis ClusterOptions has no
	// Network field, since a unix socket cannot reach a multi-node cluster.
	Network string

	// Redis server address(es) in "host:port" format.
	//
	// For cluster-mode-enabled deployments (including AWS ElastiCache CME),
	// this is typically a single cluster CONFIGURATION ENDPOINT — NOT
	// multiple node addresses. Address count alone cannot distinguish
	// "one node, standalone" from "one config endpoint, cluster" — see
	// ClusterMode below, which is the actual switch.
	Addrs []string

	// ClusterMode explicitly selects a cluster-aware client (go-redis
	// ClusterClient) when true, or a standalone client when false.
	//
	// This MUST be set explicitly — it is intentionally not inferred from
	// len(Addrs), because a cluster-mode-enabled deployment (e.g. AWS
	// ElastiCache CME) is commonly configured with a SINGLE address (the
	// cluster configuration endpoint), which previously caused this package
	// to silently build a standalone client against what is actually a
	// multi-shard cluster. Get this wrong and every command that lands on
	// a slot outside the one node you happen to hit will fail once the
	// cluster redirects it (MOVED) — a standalone client doesn't follow
	// those redirects.
	ClusterMode bool

	// Username to authenticate the current connection when Redis ACLs are used.
	// See: https://redis.io/commands/auth.
	Username string

	// Password to authenticate the current connection.
	// See: https://redis.io/commands/auth.
	Password string

	// Redis DB to select after connecting to a server.
	// See: https://redis.io/commands/select.
	// NOTE: cluster-mode-enabled clusters only support DB 0. Setting this
	// nonzero with ClusterMode=true will fail at connection/command time.
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

// VolumeSignaler is called by the batcher after flushing a batch to signal
// the engine that a band may have reached its volume threshold.
type VolumeSignaler func(band int)

// writeEntry is a single buffered write waiting to be pipelined.
type writeEntry struct {
	correlationKey string
	payload        []byte
	ts             float64
}

// Shield manages all Redis interactions for the library.
type Shield struct {
	client         redis.UniversalClient
	namespace      string
	bandCount      int
	keyTTL         time.Duration
	activityWindow time.Duration
	dlqTTL         time.Duration // how long dead-letter payload hashes are kept
	writeScript    *redis.Script
	writeScriptSHA string // pre-loaded SHA; used by flushBatch to avoid sending script text each time

	// Batching fields — all nil/zero when batching is disabled.
	batchCh        chan writeEntry
	batchSize      int
	batchWin       time.Duration
	stopBatch      chan struct{}
	batchWg        sync.WaitGroup
	volumeSignaler VolumeSignaler
	batchCtx       context.Context // parent context; cancellation stops the batcher

	// Broadcast fields — all zero when broadcasting is disabled.
	broadcastEnabled bool
	broadcastMode    BroadcastMode
	broadcastMaxLen  int64
}

type JournalRead struct {
	Payload []byte
	PTTL    time.Duration
	Version int64 // journal write ts (UnixMilli); 0 when not found
	Found   bool
}

// atomicWriteLua handles content deduplication atomically without cjson.
// Returns 0 if deduplicated (TTL refreshed), 1 if written.
const atomicWriteLua = `redis.call('HSET', KEYS[1], 'p', ARGV[1], 'ts', ARGV[2]) redis.call('EXPIRE', KEYS[1], tonumber(ARGV[4])) redis.call('ZADD', KEYS[2], ARGV[2], ARGV[3]) return 1`

const atomicDedupWriteLua = `
local payloadKey = KEYS[1]
local dirtyKey = KEYS[2]
local payload = ARGV[1]
local score = ARGV[2]
local corrKey = ARGV[3]
local ttl = tonumber(ARGV[4])
local contentHash = ARGV[5]

local existingHash = redis.call('HGET', payloadKey, 'h')
if existingHash == contentHash then
    redis.call('EXPIRE', payloadKey, ttl)
    return 0
end

redis.call('HSET', payloadKey, 'p', payload, 'ts', score, 'h', contentHash)
redis.call('EXPIRE', payloadKey, ttl)
redis.call('ZADD', dirtyKey, score, corrKey)
return 1
`

// Broadcasting related changes

// BroadcastMode selects what the broadcaster emits per message.
type BroadcastMode int

const (
	// BroadcastPayload includes the full payload (~1KB messages).
	// Maximum read offload: peers can Put directly without refetching.
	// Use when read:write ratio is high (current ad-tech profile).
	BroadcastPayload BroadcastMode = iota

	// BroadcastInvalidation sends only crn/band/ts/kind (~40B messages).
	// Peers refetch from Redis on next Read() miss.
	// Use when write velocity approaches read velocity.
	BroadcastInvalidation
)

// BroadcastConfig configures the L1 broadcast stream.
type BroadcastConfig struct {
	// Mode selects payload vs. invalidation broadcasting.
	Mode BroadcastMode

	// MaxLen is the XADD MAXLEN ~ N retention bound.
	// Default: 200_000 (≈30s of hot-write traffic at reference scale).
	// Longer gaps heal lazily via ReadJournal fallback.
	MaxLen int64
}

// broadcastKind distinguishes message intent in the stream.
// The subscriber treats them uniformly (version-checked Put),
// but the kind is preserved for telemetry and future differentiation.
type broadcastKind string

const (
	broadcastKindUpsert broadcastKind = "upsert" // normal write-through
	broadcastKindSeed   broadcastKind = "seed"   // HotLoad promotion (step 2.5)
	broadcastKindTouch  broadcastKind = "touch"  // optional TTL extension (deferred)
)
