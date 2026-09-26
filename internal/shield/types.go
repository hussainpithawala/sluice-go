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
	// Seq is the payload's per-key write sequence ('v', bumped by every
	// write) at drain time; "" for entries written before sequencing. Commit
	// and dead-letter are conditional on it, so a write that lands while the
	// batch is in flight — even within the same millisecond — is never
	// removed from the dirty set.
	Seq string
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
	kind           broadcastKind
	// done receives this entry's own write result once its batch has been
	// executed (buffered, so the batcher never blocks on it). nil for
	// direct writes.
	done chan error
}

// Shield manages all Redis interactions for the library.
type Shield struct {
	client         redis.UniversalClient
	namespace      string
	bandCount      int
	keyTTL         time.Duration
	activityWindow time.Duration
	dlqTTL         time.Duration // how long dead-letter payload hashes are kept
	// Scripts are pre-loaded in New and run via EVALSHA; every call site
	// reloads and retries on NOSCRIPT (script cache flushed by a restart or
	// failover), so a cold cache never fails a write.
	writeScript   *redis.Script
	hydrateScript *redis.Script

	commitScript     *redis.Script // conditional commit + post-flush TTL
	deadLetterScript *redis.Script // conditional move to dead-letter
	extendTTLScript  *redis.Script // extend-only TTL refresh for flushed payloads

	// Batching fields — all nil/zero when batching is disabled.
	batchCh        chan writeEntry
	batchSize      int
	batchWin       time.Duration
	stopBatch      chan struct{}
	stopOnce       sync.Once
	batchWg        sync.WaitGroup
	volumeSignaler VolumeSignaler
	batchCtx       context.Context // values only; the batcher runs until StopBatcher

	// batchMu guards batchRunning. Senders hold the read lock while handing
	// an entry to batchCh, so once StopBatcher holds the write lock no entry
	// can enter the channel behind the batcher's final drain.
	batchMu      sync.RWMutex
	batchRunning bool

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

// atomicWriteLua stores the payload and marks the key dirty in one step.
//
// KEYS[1]=payload hash, KEYS[2]=dirty set; ARGV[1]=payload, ARGV[2]=ts,
// ARGV[3]=correlation key.
//
// An unflushed payload exists only in Redis, so it must never carry a TTL.
// PERSIST is required, not just omitting EXPIRE: HSET keeps whatever TTL the
// hash already has, so re-writing a previously flushed key would otherwise
// inherit its post-flush TTL and could expire before the new value is
// flushed. The post-flush TTL is applied by commitFlushedLua after the sink
// confirms the write.
//
// HINCRBY 'v' gives every write a per-key sequence, which commit and
// dead-letter compare against: the ms timestamp alone cannot tell apart two
// writes in the same millisecond.
const atomicWriteLua = `
redis.call('HSET', KEYS[1], 'p', ARGV[1], 'ts', ARGV[2])
redis.call('HINCRBY', KEYS[1], 'v', 1)
redis.call('PERSIST', KEYS[1])
redis.call('ZADD', KEYS[2], ARGV[2], ARGV[3])
return 1`

// commitFlushedLua commits one flushed key atomically, but only if the
// payload is still the version that was drained (same 'v'). Otherwise a
// newer write landed while the batch was in flight: the key is left dirty
// and persistent so that write is flushed on the next cycle.
//
// On commit it removes the key from the dirty set and applies the post-flush
// TTL; when the key is indexed (its idxv hash exists), the index expiry
// queue is scored with that expiry so its index entries are pruned when it
// dies. A non-positive TTL commits without setting one (PEXPIRE 0 deletes).
//
// KEYS[1]=dirty set, KEYS[2]=payload hash, KEYS[3]=index expiry ZSET,
// KEYS[4]=idxv hash; ARGV[1]=correlation key, ARGV[2]=drained seq ("" for
// pre-sequencing entries), ARGV[3]=ttl in ms, ARGV[4]=expiry (Unix ms).
// Returns 1 if committed, 0 if skipped because the payload changed.
const commitFlushedLua = `
local cur = redis.call('HGET', KEYS[2], 'v') or ''
if cur ~= ARGV[2] then
    return 0
end
redis.call('ZREM', KEYS[1], ARGV[1])
local ttl = tonumber(ARGV[3])
if ttl > 0 then
    redis.call('PEXPIRE', KEYS[2], ttl)
    if redis.call('EXISTS', KEYS[4]) == 1 then
        redis.call('ZADD', KEYS[3], ARGV[4], ARGV[1])
    end
end
return 1`

// deadLetterIfUnchangedLua moves one key from the dirty set to the
// dead-letter set, but only if its payload is still the drained version.
// A newer write means the failure applied to a superseded payload; the key
// stays dirty and the new payload is retried normally.
//
// KEYS[1]=dirty set, KEYS[2]=dead-letter set, KEYS[3]=payload hash;
// ARGV[1]=correlation key, ARGV[2]=drained seq, ARGV[3]=failure ts (ms),
// ARGV[4]=reason, ARGV[5]=dead-letter TTL in ms.
// Returns 1 if dead-lettered, 0 if skipped.
const deadLetterIfUnchangedLua = `
local cur = redis.call('HGET', KEYS[3], 'v') or ''
if cur ~= ARGV[2] then
    return 0
end
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1])
redis.call('HSET', KEYS[3], 'dlq_reason', ARGV[4], 'dlq_at', ARGV[3])
redis.call('PEXPIRE', KEYS[3], ARGV[5])
redis.call('ZREM', KEYS[1], ARGV[1])
return 1`

// extendTTLLua extends a flushed payload's TTL to at least ARGV[1] ms.
// A persistent key (PTTL -1) is unflushed and is left untouched, as is a
// missing key (-2); an existing longer TTL is never shortened. An indexed
// key's expiry-queue entry is re-scored to match.
//
// KEYS[1]=payload hash, KEYS[2]=index expiry ZSET, KEYS[3]=idxv hash;
// ARGV[1]=ttl in ms, ARGV[2]=expiry (Unix ms), ARGV[3]=correlation key.
// Returns 1 if extended, else 0.
const extendTTLLua = `
local pttl = redis.call('PTTL', KEYS[1])
if pttl >= 0 and pttl < tonumber(ARGV[1]) then
    redis.call('PEXPIRE', KEYS[1], ARGV[1])
    if redis.call('EXISTS', KEYS[3]) == 1 then
        redis.call('ZADD', KEYS[2], ARGV[2], ARGV[3])
    end
    return 1
end
return 0`

// hydrateLua seeds the journal with a payload read from the Source, without
// marking the key dirty (store data must never be flushed back to the sink).
//
// KEYS[1]=payload hash
// ARGV[1]=payload, ARGV[2]=hydration ts (captured BEFORE the Source read),
// ARGV[3]=ttl in ms
//
// Hydration only fills a journal miss. Every write lands in the journal
// before the store, so an existing entry — pending flush, flushed, or
// dead-lettered — is always at least as new as anything read from the
// store, including a Write() that landed while the Source read was in flight.
// A timestamp comparison alone is not enough: a flush can commit (clearing
// the dirty marker) between the Source read and this script. The existing
// entry is left untouched apart from an extend-only TTL bump.
// Returns {1} if written, or {0, current payload, current ts} if skipped.
const hydrateLua = `
local ttl = tonumber(ARGV[3])
local cur = redis.call('HMGET', KEYS[1], 'p', 'ts')
if cur[1] then
    local pttl = redis.call('PTTL', KEYS[1])
    if pttl >= 0 and pttl < ttl then
        redis.call('PEXPIRE', KEYS[1], ttl)
    end
    return {0, cur[1], cur[2] or '0'}
end
redis.call('HSET', KEYS[1], 'p', ARGV[1], 'ts', ARGV[2])
redis.call('HDEL', KEYS[1], 'h')
redis.call('PEXPIRE', KEYS[1], ttl)
return {1}
`

// HydrateResult reports the outcome of a journal hydration.
type HydrateResult struct {
	// Written is true when the Source payload was stored in the journal.
	Written bool
	// Payload and Version are the journal's effective state after the call:
	// the hydrated payload when Written, otherwise the newer entry that won.
	Payload []byte
	Version int64
}

// atomicDedupWriteLua is atomicWriteLua with xxHash64 content deduplication.
//
// KEYS[1]=payload hash, KEYS[2]=dirty set; ARGV[1]=payload, ARGV[2]=ts,
// ARGV[3]=correlation key, ARGV[4]=ttl in ms, ARGV[5]=content hash.
//
// A duplicate refreshes the TTL of a flushed entry (extend-only, so a hot
// key's longer TTL is kept) and leaves an unflushed, persistent entry alone.
// A new payload is persisted exactly as in atomicWriteLua.
// Returns 0 if deduplicated, 1 if written.
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
    local pttl = redis.call('PTTL', payloadKey)
    if pttl >= 0 and pttl < ttl then
        redis.call('PEXPIRE', payloadKey, ttl)
    end
    return 0
end

redis.call('HSET', payloadKey, 'p', payload, 'ts', score, 'h', contentHash)
redis.call('HINCRBY', payloadKey, 'v', 1)
redis.call('PERSIST', payloadKey)
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
