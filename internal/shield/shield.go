// Package shield manages all Redis interactions on behalf of sluice.
// No Redis type leaks beyond this package boundary.
package shield

import (
	"context"
	"crypto/tls"
	"fmt"
	"hash/fnv"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// FlushRecord is the internal unit that travels through the pipeline.
type FlushRecord struct {
	CorrelationKey string
	Payload        []byte
	ReceivedAt     time.Time
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
}

const atomicWriteLua = `
local payloadKey = KEYS[1]
local dirtyKey = KEYS[2]
local payload = ARGV[1]
local score = ARGV[2]
local corrKey = ARGV[3]
local ttl = tonumber(ARGV[4])
local activityWindow = tonumber(ARGV[5])
local indexJson = ARGV[6]
local namespace = ARGV[7]
local band = tonumber(ARGV[8])
local contentHash = ARGV[9]
local dedupEnabled = tonumber(ARGV[10])
local forceHot = tonumber(ARGV[11])

local currentTtl = ttl
local isHot = redis.call('EXISTS', payloadKey)
if isHot == 1 or forceHot == 1 then
    currentTtl = activityWindow
end

if dedupEnabled == 1 and contentHash and contentHash ~= '' then
    local existingHash = redis.call('HGET', payloadKey, 'h')
    if existingHash == contentHash then
        redis.call('EXPIRE', payloadKey, currentTtl)
        return 2 -- 2 indicates deduplicated, no dirty queue update needed
    end
end

redis.call('HSET', payloadKey, 'p', payload, 'ts', score)
if contentHash and contentHash ~= '' then
    redis.call('HSET', payloadKey, 'h', contentHash)
end
redis.call('EXPIRE', payloadKey, currentTtl)
redis.call('ZADD', dirtyKey, score, corrKey)

if indexJson and indexJson ~= '' and indexJson ~= 'null' then
    local ok, indexes = pcall(cjson.decode, indexJson)
    if ok and type(indexes) == "table" then
        for field, val in pairs(indexes) do
            if type(val) == "string" then
                local eqKey = string.format('sl:%s:idx:{%d}:%s:%s', namespace, band, field, val)
                redis.call('SADD', eqKey, corrKey)
                redis.call('EXPIRE', eqKey, currentTtl)
            elseif type(val) == "number" then
                local ridxKey = string.format('sl:%s:ridx:{%d}:%s', namespace, band, field)
                redis.call('ZADD', ridxKey, val, corrKey)
                redis.call('EXPIRE', ridxKey, currentTtl)
            end
        end
    end
end
return 1
`

// New initialises the Redis client and validates connectivity.
func New(cfg RedisConfig, namespace string, bandCount int, keyTTL time.Duration, activityWindow time.Duration) (*Shield, error) {
	if strings.ContainsAny(namespace, "{}") {
		// A '{' anywhere in namespace would hijack Redis Cluster's hash-tag
		// parsing (only the substring between the FIRST '{' and the next '}'
		// in a key is hashed), silently breaking the {band} co-location
		// atomicWriteLua depends on. Reject up front rather than let this
		// surface as an intermittent CROSSSLOT error later.
		return nil, fmt.Errorf("sluice/shield: namespace %q must not contain '{' or '}' — "+
			"this would interfere with cluster hash-tag routing", namespace)
	}

	applyDefaults(&cfg)
	if len(cfg.Addrs) == 0 {
		return nil, fmt.Errorf("sluice/shield: at least one address is required")
	}

	var client redis.UniversalClient
	if cfg.ClusterMode {
		if cfg.DB != 0 {
			// Cluster-mode-enabled deployments only expose DB 0. Silently
			// ignoring a nonzero DB here would look like it took effect.
			return nil, fmt.Errorf("sluice/shield: DB must be 0 in cluster mode, got %d", cfg.DB)
		}
		// Username must be passed explicitly: go-redis only sends the two-arg
		// AUTH <user> <pass> form when Username is set. Omitting it sends
		// AUTH <pass>, which authenticates as the "default" ACL user and
		// silently ignores the configured user — this works by accident on
		// deployments that don't enforce per-user ACLs, and fails confusingly
		// on ElastiCache RBAC, which does.
		client = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: cfg.Addrs, Username: cfg.Username, Password: cfg.Password,
			DialTimeout: cfg.DialTimeout, ReadTimeout: cfg.ReadTimeout,
			WriteTimeout: cfg.WriteTimeout, PoolSize: cfg.PoolSize, TLSConfig: cfg.TLSConfig,
		})
	} else {
		client = redis.NewClient(&redis.Options{
			Network: cfg.Network, Addr: cfg.Addrs[0],
			Username: cfg.Username, Password: cfg.Password, DB: cfg.DB,
			DialTimeout: cfg.DialTimeout, ReadTimeout: cfg.ReadTimeout,
			WriteTimeout: cfg.WriteTimeout, PoolSize: cfg.PoolSize, TLSConfig: cfg.TLSConfig,
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.DialTimeout)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("sluice/shield: ping: %w", err)
	}
	// Pre-load the write script so the batcher can use EVALSHA in pipelines,
	// avoiding sending the full script text on every pipelined batch flush.
	// For cluster clients, go-redis broadcasts SCRIPT LOAD to all nodes —
	// this is what makes EVALSHA safe to call against any shard immediately
	// after New() returns, without a NOSCRIPT race on shards added later
	// (e.g. mid-resharding). If you reshard this cluster in the future,
	// re-verify that newly added shards receive the script — go-redis
	// broadcasts to the topology known at ScriptLoad time, not to nodes
	// that join afterward.
	sha, err := client.ScriptLoad(ctx, atomicWriteLua).Result()
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("sluice/shield: script load: %w", err)
	}
	return &Shield{
		client:         client,
		namespace:      namespace,
		bandCount:      bandCount,
		keyTTL:         keyTTL,
		activityWindow: activityWindow,
		dlqTTL:         7 * 24 * time.Hour, // dead-letter payloads kept 7 days
		writeScript:    redis.NewScript(atomicWriteLua),
		writeScriptSHA: sha,
	}, nil
}

// Write atomically stores payload and marks the correlation key dirty.
// When batching is enabled, the write is buffered and pipelined to Redis
// by the background batcher goroutine. Returns nil immediately in that case.
func (s *Shield) Write(ctx context.Context, correlationKey string, payload []byte, opts WriteOptions) (int, error) {
	band := s.BandFor(correlationKey)
	forceHotInt := 0
	if opts.ForceHot {
		forceHotInt = 1
	}
	dedupInt := 0
	if opts.DedupEnabled {
		dedupInt = 1
	}

	res, err := s.writeScript.Run(ctx, s.client,
		[]string{s.payloadKey(correlationKey), s.dirtyKeyForBand(band)},
		payload, float64(time.Now().UnixMilli()), correlationKey,
		int64(s.keyTTL.Seconds()), int64(s.activityWindow.Seconds()),
		opts.IndexesJSON, s.namespace, band,
		opts.ContentHash, dedupInt, forceHotInt,
	).Int()

	return res, err
}

// SetNX implements exactly-once delivery via SETNX EX.
func (s *Shield) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) (bool, error) {
	return s.client.SetNX(ctx, key, value, expiration).Result()
}

// HotLoad forces a payload into the journal with ActivityWindow TTL.
func (s *Shield) HotLoad(ctx context.Context, correlationKey string, payload []byte) error {
	_, err := s.Write(ctx, correlationKey, payload, WriteOptions{ForceHot: true})
	return err
}

// Read fetches the payload from Redis. Returns (payload, isHot, error).
func (s *Shield) Read(ctx context.Context, correlationKey string) ([]byte, bool, error) {
	key := s.payloadKey(correlationKey)
	val, err := s.client.HGet(ctx, key, "p").Result()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	// Fire-and-forget TTL refresh to keep it hot
	go func() {
		bgCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		_ = s.client.Expire(bgCtx, key, s.activityWindow).Err()
	}()

	return []byte(val), true, nil
}

// ZRangeWithScores fetches the oldest scores in a dirty set for pre-eviction checks.
func (s *Shield) ZRangeWithScores(ctx context.Context, band int, start, stop int64) ([]redis.Z, error) {
	return s.client.ZRangeWithScores(ctx, s.dirtyKeyForBand(band), start, stop).Result()
}

// SInter performs a safe SET intersection within a single band (Cluster-safe).
func (s *Shield) SInter(ctx context.Context, keys ...string) ([]string, error) {
	return s.client.SInter(ctx, keys...).Result()
}

// ZScore fetches the score of a member in a range index.
func (s *Shield) ZScore(ctx context.Context, key, member string) (float64, error) {
	return s.client.ZScore(ctx, key, member).Result()
}

// IndexKey returns the equality SET key for a field/value pair in a specific band.
func IndexKey(namespace string, band int, field, value string) string {
	return fmt.Sprintf("sl:%s:idx:{%d}:%s:%s", namespace, band, field, value)
}

// RangeIndexKey returns the range ZSET key for a field in a specific band.
func RangeIndexKey(namespace string, band int, field string) string {
	return fmt.Sprintf("sl:%s:ridx:{%d}:%s", namespace, band, field)
}

// DrainBand reads up to maxBatch dirty keys and returns their payloads as
// FlushRecords ready for BulkWrite assembly.
//
// Two-phase commit contract:
//   - Keys whose payload hash has expired (TTL elapsed) are removed from the
//     dirty set immediately — there is nothing to flush for them.
//   - Keys with a valid payload are returned WITHOUT being removed from the
//     dirty set. The caller MUST call CommitKeys after a confirmed successful
//     BulkWrite, and MoveToDeadLetter for permanently failed keys.
//     Keys that fail with a transient error require no action here — they
//     remain in the dirty set and are retried on the next flush cycle.
//
// This design ensures that a BulkWrite failure (including unique-ID collision
// during DrainAndClose) never causes silent record loss.
func (s *Shield) DrainBand(ctx context.Context, band, maxBatch int) ([]FlushRecord, error) {
	dirtyKey := s.dirtyKeyForBand(band)

	members, err := s.client.ZRangeByScoreWithScores(ctx, dirtyKey, &redis.ZRangeBy{
		Min: "-inf", Max: "+inf", Offset: 0, Count: int64(maxBatch),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("sluice/shield: zrangebyscore band %d: %w", band, err)
	}
	if len(members) == 0 {
		return nil, nil
	}

	corrKeys := make([]string, len(members))
	hashKeys := make([]string, len(members))
	for i, m := range members {
		corrKeys[i] = m.Member.(string)
		hashKeys[i] = s.payloadKey(corrKeys[i])
	}

	// Pipeline all HMGET calls. NOTE: this is "one network round-trip" only
	// under cluster-mode-disabled. Under CME, if hashKeys for this band span
	// multiple shards — they won't, since payloadKey now shares this band's
	// hash tag, so every key here is guaranteed on ONE shard — a cluster-aware
	// client would otherwise silently split this into N per-shard pipelines.
	// Keeping payloadKey band-tagged is what preserves the single-round-trip
	// property this comment originally assumed.
	pipe := s.client.Pipeline()
	cmds := make([]*redis.SliceCmd, len(hashKeys))
	for i, hk := range hashKeys {
		cmds[i] = pipe.HMGet(ctx, hk, "p", "ts")
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, fmt.Errorf("sluice/shield: pipeline hmget band %d: %w", band, err)
	}

	records := make([]FlushRecord, 0, len(members))
	// expired collects keys whose payload TTL elapsed before we could flush them.
	// These are cleaned from the dirty set immediately — there is nothing to write.
	expired := make([]interface{}, 0)

	for i, cmd := range cmds {
		vals, cmdErr := cmd.Result()
		if cmdErr != nil || vals[0] == nil {
			// Payload hash evicted by Redis TTL — safe to remove from dirty set.
			expired = append(expired, corrKeys[i])
			continue
		}
		payload, ok := vals[0].(string)
		if !ok || payload == "" {
			expired = append(expired, corrKeys[i])
			continue
		}
		// Valid payload — return WITHOUT ZREMing. CommitKeys is called
		// by the engine only after a confirmed successful BulkWrite.
		records = append(records, FlushRecord{
			CorrelationKey: corrKeys[i],
			Payload:        []byte(payload),
			ReceivedAt:     time.Now(),
		})
	}

	// Clean up expired keys immediately — there is nothing to flush for them.
	if len(expired) > 0 {
		_ = s.client.ZRem(ctx, dirtyKey, expired...).Err()
	}

	return records, nil
}

// CommitKeys removes successfully persisted correlation keys from the dirty
// sorted set. Must be called by the engine after a BulkWrite that confirmed
// every key in the list was written (or upserted) successfully.
//
// Calling this for keys that were never in the dirty set is a no-op.
func (s *Shield) CommitKeys(ctx context.Context, band int, corrKeys []string) error {
	if len(corrKeys) == 0 {
		return nil
	}
	members := make([]interface{}, len(corrKeys))
	for i, k := range corrKeys {
		members[i] = k
	}
	if err := s.client.ZRem(ctx, s.dirtyKeyForBand(band), members...).Err(); err != nil {
		return fmt.Errorf("sluice/shield: commit keys band %d: %w", band, err)
	}
	return nil
}

// MoveToDeadLetter moves permanently failed keys out of the dirty sorted set
// and into the dead-letter sorted set for this band. The payload hash is
// preserved with an extended TTL (dlqTTL) and annotated with the failure
// reason, so records can be inspected and replayed without data loss.
//
// Use this for non-retryable sink errors — primarily unique-index violations
// (ErrCodeDuplicateKey / MongoDB code 11000). Calling this for transient errors
// would incorrectly suppress legitimate retries.
//
// Dead-letter key: sl:{namespace}:dlq:{band}   (band is also the hash tag)
// Payload key:     sl:{namespace}:payload:{band}:corrKey   (TTL extended to dlqTTL)
func (s *Shield) MoveToDeadLetter(ctx context.Context, band int, corrKeys []string, reason string) error {
	if len(corrKeys) == 0 {
		return nil
	}

	now := float64(time.Now().UnixMilli())
	dirtyKey := s.dirtyKeyForBand(band)
	dlqKey := s.dlqKey(band)

	// NOTE: this pipeline mixes keys tagged {band} (dirtyKey, dlqKey, and
	// every payloadKey(ck) below, since payloadKey now embeds BandFor(ck))
	// with plain pipelined commands rather than a single atomic script.
	// That's fine for a pipeline (each command executes independently;
	// pipelining is not the same atomicity guarantee as the Lua script),
	// but it does mean every key referenced here — dirty set, DLQ set, and
	// every payload hash — must belong to the SAME band as the caller
	// passed in. Since payloadKey() derives its tag from BandFor(ck)
	// internally, this only holds if every ck in corrKeys actually belongs
	// to `band` — i.e. the caller (engine.go) must never mix correlation
	// keys from different bands into one MoveToDeadLetter call. Looking at
	// engine.go's flushBand, this holds today (each call is scoped to one
	// band's flush cycle) — flagging the invariant so it isn't broken by a
	// future refactor.
	pipe := s.client.Pipeline()

	for _, ck := range corrKeys {
		// Enqueue in dead-letter sorted set (score = failure timestamp).
		pipe.ZAdd(ctx, dlqKey, redis.Z{Score: now, Member: ck})
		// Annotate the payload hash with failure metadata.
		pipe.HSet(ctx, s.payloadKey(ck),
			"dlq_reason", reason,
			"dlq_at", fmt.Sprintf("%.0f", now),
		)
		// Extend payload hash TTL so it survives for the full DLQ inspection window.
		pipe.Expire(ctx, s.payloadKey(ck), s.dlqTTL)
	}

	// Remove from dirty sorted set — these will not be retried via normal flush.
	members := make([]interface{}, len(corrKeys))
	for i, k := range corrKeys {
		members[i] = k
	}
	pipe.ZRem(ctx, dirtyKey, members...)

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("sluice/shield: move to dead-letter band %d: %w", band, err)
	}
	return nil
}

// DirtyQueueDepth returns the number of keys currently queued for a band.
func (s *Shield) DirtyQueueDepth(ctx context.Context, band int) (int64, error) {
	return s.client.ZCard(ctx, s.dirtyKeyForBand(band)).Result()
}

// DeadLetterDepth returns the number of keys currently in the dead-letter
// set for a band. A non-zero value indicates records that require investigation.
func (s *Shield) DeadLetterDepth(ctx context.Context, band int) (int64, error) {
	return s.client.ZCard(ctx, s.dlqKey(band)).Result()
}

// DrainDLQ reads up to maxBatch dead-letter keys for a band and returns their
// payloads as FlushRecords. Mirrors DrainBand but operates on the DLQ sorted
// set instead of the dirty set.
//
// Keys whose payload hash has expired are removed from the DLQ immediately.
// Valid keys are returned WITHOUT being removed — the caller must call
// CommitDLQKeys after successful processing.
func (s *Shield) DrainDLQ(ctx context.Context, band, maxBatch int) ([]FlushRecord, error) {
	dlqKey := s.dlqKey(band)

	members, err := s.client.ZRangeByScoreWithScores(ctx, dlqKey, &redis.ZRangeBy{
		Min: "-inf", Max: "+inf", Offset: 0, Count: int64(maxBatch),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("sluice/shield: zrangebyscore dlq band %d: %w", band, err)
	}
	if len(members) == 0 {
		return nil, nil
	}

	corrKeys := make([]string, len(members))
	hashKeys := make([]string, len(members))
	for i, m := range members {
		corrKeys[i] = m.Member.(string)
		hashKeys[i] = s.payloadKey(corrKeys[i])
	}

	// Pipeline all HMGET calls — single round-trip per shard; all keys here
	// share this band's hash tag (see DrainBand note above).
	pipe := s.client.Pipeline()
	cmds := make([]*redis.SliceCmd, len(hashKeys))
	for i, hk := range hashKeys {
		cmds[i] = pipe.HMGet(ctx, hk, "p", "ts")
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, fmt.Errorf("sluice/shield: pipeline hmget dlq band %d: %w", band, err)
	}

	records := make([]FlushRecord, 0, len(members))
	expired := make([]interface{}, 0)

	for i, cmd := range cmds {
		vals, cmdErr := cmd.Result()
		if cmdErr != nil || vals[0] == nil {
			expired = append(expired, corrKeys[i])
			continue
		}
		payload, ok := vals[0].(string)
		if !ok || payload == "" {
			expired = append(expired, corrKeys[i])
			continue
		}
		records = append(records, FlushRecord{
			CorrelationKey: corrKeys[i],
			Payload:        []byte(payload),
			ReceivedAt:     time.Now(),
		})
	}

	if len(expired) > 0 {
		_ = s.client.ZRem(ctx, dlqKey, expired...).Err()
	}

	return records, nil
}

// CommitDLQKeys removes processed correlation keys from the dead-letter sorted
// set and deletes their payload hashes. Call after successfully handling DLQ
// records (ignore, upsert, or reinsert).
func (s *Shield) CommitDLQKeys(ctx context.Context, band int, corrKeys []string) error {
	if len(corrKeys) == 0 {
		return nil
	}

	dlqKey := s.dlqKey(band)
	pipe := s.client.Pipeline()

	members := make([]interface{}, len(corrKeys))
	for i, k := range corrKeys {
		members[i] = k
		pipe.Del(ctx, s.payloadKey(k))
	}
	pipe.ZRem(ctx, dlqKey, members...)

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("sluice/shield: commit dlq keys band %d: %w", band, err)
	}
	return nil
}

// BandCount returns the number of bands configured for this Shield instance.
func (s *Shield) BandCount() int { return s.bandCount }

// Namespace returns the namespace configured for this Shield instance.
func (s *Shield) Namespace() string { return s.namespace }

// BandForKey returns the band index for correlationKey using FNV-32a.
// Exported at package level so callers that need to predict where a key lands
// — chiefly tests building expected key names — hash it exactly the way the
// Shield does instead of reimplementing it.
func BandForKey(correlationKey string, bandCount int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(correlationKey))
	return int(h.Sum32()) % bandCount
}

// BandFor returns the band index for the given correlation key using FNV-32a.
func (s *Shield) BandFor(correlationKey string) int {
	return BandForKey(correlationKey, s.bandCount)
}

func (s *Shield) Close() error { return s.client.Close() }

// ── Write batcher ─────────────────────────────────────────────────────────────

// EnableBatching configures the shield for batched pipeline writes.
// Must be called before StartBatcher. batchSize is the max entries per pipeline
// flush; batchWindow is the max time before a partial buffer is flushed.
func (s *Shield) EnableBatching(batchSize int, batchWindow time.Duration) {
	if batchSize <= 0 {
		batchSize = 200
	}
	if batchWindow <= 0 {
		batchWindow = 5 * time.Millisecond
	}
	s.batchSize = batchSize
	s.batchWin = batchWindow
	// Channel capacity = 4x batch size to absorb bursts without blocking callers.
	s.batchCh = make(chan writeEntry, batchSize*4)
	s.stopBatch = make(chan struct{})
}

// SetVolumeSignaler sets the callback invoked after each batch flush to trigger
// volume-based drain. Must be called before StartBatcher.
func (s *Shield) SetVolumeSignaler(fn VolumeSignaler) {
	s.volumeSignaler = fn
}

// StartBatcher launches the background goroutine that consumes from the write
// channel and flushes entries to Redis via pipeline. No-op if batching is not enabled.
// ctx is the application-level parent context; cancellation unblocks the batcher immediately.
func (s *Shield) StartBatcher(ctx context.Context) {
	if s.batchCh == nil {
		return
	}
	s.batchCtx = ctx
	s.batchWg.Add(1)
	go s.runBatcher()
}

// StopBatcher signals the batcher to drain remaining entries and stop.
// Blocks until the goroutine has exited. No-op if batching is not enabled.
func (s *Shield) StopBatcher() {
	if s.stopBatch == nil {
		return
	}
	close(s.stopBatch)
	s.batchWg.Wait()
}

func (s *Shield) runBatcher() {
	defer s.batchWg.Done()
	buf := make([]writeEntry, 0, s.batchSize)
	ticker := time.NewTicker(s.batchWin)
	defer ticker.Stop()

	flush := func() {
		if len(buf) == 0 {
			return
		}
		// Copy before reset: buf[:0] reuses the underlying array, so any
		// future async use of flushBatch would race against the next append.
		// A copy makes the hand-off safe regardless of how flushBatch evolves.
		ready := make([]writeEntry, len(buf))
		copy(ready, buf)
		buf = buf[:0]
		s.flushBatch(ready)
	}

	for {
		select {
		case <-s.batchCtx.Done():
			// Application context cancelled (e.g. SIGTERM). Flush what we have and stop.
			flush()
			return
		case entry := <-s.batchCh:
			buf = append(buf, entry)
			if len(buf) >= s.batchSize {
				flush()
				ticker.Reset(s.batchWin)
			}
		case <-ticker.C:
			flush()
		case <-s.stopBatch:
			// Drain any remaining entries in the channel.
			for {
				select {
				case entry := <-s.batchCh:
					buf = append(buf, entry)
					if len(buf) >= s.batchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}
		}
	}
}

// RefreshHotTTL extends the ActivityWindow TTL on successfully flushed hot CRNs.
// This ensures active users remain in the fast-path journal for subsequent Read() calls.
func (s *Shield) RefreshHotTTL(ctx context.Context, band int, corrKeys []string) error {
	if len(corrKeys) == 0 {
		return nil
	}
	pipe := s.client.Pipeline()
	for _, ck := range corrKeys {
		pipe.Expire(ctx, s.payloadKey(ck), s.activityWindow)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// OldestDirtyScore returns the score (timestamp) of the oldest item in a band's
// dirty sorted set. Returns 0 if the set is empty. Used by the engine's
// pre-eviction flusher to force a flush before Redis TTL expires the payload.
func (s *Shield) OldestDirtyScore(ctx context.Context, band int) (float64, error) {
	res, err := s.client.ZRangeWithScores(ctx, s.dirtyKeyForBand(band), 0, 0).Result()
	if err != nil || len(res) == 0 {
		return 0, err
	}
	return res[0].Score, nil
}

func (s *Shield) flushBatch(entries []writeEntry) {
	ctx, cancel := context.WithTimeout(s.batchCtx, 10*time.Second)
	defer cancel()

	// Use EvalSha (pre-loaded at startup) so the pipeline carries only the
	// 40-byte SHA rather than the full Lua script text on every flush.
	// Each key's HSET+EXPIRE+ZADD remains atomic inside the Lua execution —
	// this holds under cluster mode because payloadKey(e.correlationKey) and
	// dirtyKey(e.correlationKey) now always share e's band hash tag.
	//
	// Cluster-mode note: if entries in this batch span multiple bands (which
	// they typically will), a cluster-aware client transparently splits this
	// pipeline into per-shard sub-pipelines and executes them in parallel —
	// this IS the parallelism the migration to CME was for. The "one
	// round-trip" framing below only holds per-shard, not for the whole
	// pipeline; the 10s context timeout bounds the slowest shard's
	// round-trip, not a single network call. Re-validate this timeout
	// against real multi-shard latency under load before relying on it.
	pipe := s.client.Pipeline()
	ttlSec := int64(s.keyTTL.Seconds())

	for _, e := range entries {
		pipe.EvalSha(ctx, s.writeScriptSHA,
			[]string{s.payloadKey(e.correlationKey), s.dirtyKey(e.correlationKey)},
			e.payload, fmt.Sprintf("%.0f", e.ts), e.correlationKey, ttlSec,
		)
	}

	// Collect the unique bands touched by this batch and append ZCARD for each
	// into the same pipeline — one round-trip (per shard) for writes + depth
	// checks combined.
	var touchedBands []int
	if s.volumeSignaler != nil {
		seen := make(map[int]bool, s.bandCount)
		for _, e := range entries {
			if b := s.BandFor(e.correlationKey); !seen[b] {
				seen[b] = true
				touchedBands = append(touchedBands, b)
			}
		}
		zcardCmds := make([]*redis.IntCmd, len(touchedBands))
		for i, b := range touchedBands {
			zcardCmds[i] = pipe.ZCard(ctx, s.dirtyKeyForBand(b))
		}
		if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
			slog.Error("sluice/shield: batch pipeline exec failed",
				"namespace", s.namespace, "batch_size", len(entries), "err", err)
		}
		for i, b := range touchedBands {
			if depth, err := zcardCmds[i].Result(); err == nil && int(depth) >= s.batchSize {
				s.volumeSignaler(b)
			}
		}
		return
	}

	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		slog.Error("sluice/shield: batch pipeline exec failed",
			"namespace", s.namespace, "batch_size", len(entries), "err", err)
	}
}

// ── Key naming ────────────────────────────────────────────────────────────────
//
// CLUSTER HASH-TAG CONTRACT: payloadKey and dirtyKeyForBand MUST share a
// hash tag for a given band, since atomicWriteLua operates on both keys in
// one Lua script execution, which Redis Cluster requires to be single-slot.
// Redis Cluster hashes only the substring between the FIRST '{' and the
// following '}' in a key — see New()'s namespace validation, which rejects
// any namespace containing '{' or '}' to prevent it from hijacking this.

// The exported PayloadKey/DirtyKey/DLQKey functions below are the single
// source of truth for the on-wire key layout. Tests assert against real keys
// via these rather than hardcoding the format — hardcoded copies silently
// rotted when the {band} hash tags were introduced, so the assertions kept
// looking for keys that no longer existed and only failed on a timeout.

// PayloadKey returns the payload hash key for a correlation key in a band.
// Pattern: sl:<namespace>:payload:{<band>}:<correlationKey>
func PayloadKey(namespace string, band int, ck string) string {
	return fmt.Sprintf("sl:%s:payload:{%d}:%s", namespace, band, ck)
}

// DirtyKey returns the dirty sorted-set key for a band.
// Pattern: sl:<namespace>:dirty:{<band>}
func DirtyKey(namespace string, band int) string {
	return fmt.Sprintf("sl:%s:dirty:{%d}", namespace, band)
}

// DLQKey returns the dead-letter sorted-set key for a band.
// Pattern: sl:<namespace>:dlq:{<band>}
func DLQKey(namespace string, band int) string {
	return fmt.Sprintf("sl:%s:dlq:{%d}", namespace, band)
}

func (s *Shield) payloadKey(ck string) string {
	return PayloadKey(s.namespace, s.BandFor(ck), ck)
}

func (s *Shield) dirtyKey(ck string) string {
	return s.dirtyKeyForBand(s.BandFor(ck))
}

func (s *Shield) dirtyKeyForBand(band int) string {
	return DirtyKey(s.namespace, band)
}

// dlqKey returns the dead-letter sorted set key for a band.
func (s *Shield) dlqKey(band int) string {
	return DLQKey(s.namespace, band)
}

func applyDefaults(c *RedisConfig) {
	if c.Network == "" {
		c.Network = "tcp"
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = 5 * time.Second
	}
	if c.ReadTimeout == 0 {
		c.ReadTimeout = 3 * time.Second
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = 3 * time.Second
	}
	if c.PoolSize == 0 {
		c.PoolSize = 20
	}
}
