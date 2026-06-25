// Package shield manages all Redis interactions on behalf of sluice.
// No Redis type leaks beyond this package boundary.
package shield

import (
	"context"
	"crypto/tls"
	"fmt"
	"hash/fnv"
	"log/slog"
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

// RedisConfig holds Redis connectivity parameters.
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

// atomicWriteLua atomically stores payload + marks the key dirty in one round-trip.
// KEYS[1]=payload hash  KEYS[2]=dirty sorted set
// ARGV[1]=payload  ARGV[2]=score(ms)  ARGV[3]=corrKey  ARGV[4]=ttl(s)
const atomicWriteLua = `
redis.call('HSET', KEYS[1], 'p', ARGV[1], 'ts', ARGV[2])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[4]))
redis.call('ZADD', KEYS[2], ARGV[2], ARGV[3])
return 1
`

// New initialises the Redis client and validates connectivity.
func New(cfg RedisConfig, namespace string, bandCount int, keyTTL time.Duration) (*Shield, error) {
	applyDefaults(&cfg)
	var client redis.UniversalClient
	if len(cfg.Addrs) == 1 {
		client = redis.NewClient(&redis.Options{
			Addr: cfg.Addrs[0], Password: cfg.Password, DB: cfg.DB,
			DialTimeout: cfg.DialTimeout, ReadTimeout: cfg.ReadTimeout,
			WriteTimeout: cfg.WriteTimeout, PoolSize: cfg.PoolSize, TLSConfig: cfg.TLSConfig,
		})
	} else {
		client = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: cfg.Addrs, Password: cfg.Password,
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
	// For cluster clients, go-redis broadcasts SCRIPT LOAD to all nodes.
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
		dlqTTL:         7 * 24 * time.Hour, // dead-letter payloads kept 7 days
		writeScript:    redis.NewScript(atomicWriteLua),
		writeScriptSHA: sha,
	}, nil
}

// Write atomically stores payload and marks the correlation key dirty.
// When batching is enabled, the write is buffered and pipelined to Redis
// by the background batcher goroutine. Returns nil immediately in that case.
func (s *Shield) Write(ctx context.Context, correlationKey string, payload []byte) error {
	if s.batchCh != nil {
		s.batchCh <- writeEntry{
			correlationKey: correlationKey,
			payload:        payload,
			ts:             float64(time.Now().UnixMilli()),
		}
		return nil
	}
	return s.writeScript.Run(ctx, s.client,
		[]string{s.payloadKey(correlationKey), s.dirtyKey(correlationKey)},
		payload, float64(time.Now().UnixMilli()), correlationKey, int64(s.keyTTL.Seconds()),
	).Err()
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

	// Pipeline all HMGET calls — one network round-trip for the entire batch.
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
// Dead-letter key: sl:{namespace}:dlq:{band}
// Payload key:     sl:{namespace}:payload:{corrKey}   (TTL extended to dlqTTL)
func (s *Shield) MoveToDeadLetter(ctx context.Context, band int, corrKeys []string, reason string) error {
	if len(corrKeys) == 0 {
		return nil
	}

	now := float64(time.Now().UnixMilli())
	dirtyKey := s.dirtyKeyForBand(band)
	dlqKey := s.dlqKey(band)

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

	// Pipeline all HMGET calls — one network round-trip for the entire batch.
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

// BandFor returns the band index for the given correlation key using FNV-32a.
func (s *Shield) BandFor(correlationKey string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(correlationKey))
	return int(h.Sum32()) % s.bandCount
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

func (s *Shield) flushBatch(entries []writeEntry) {
	ctx, cancel := context.WithTimeout(s.batchCtx, 10*time.Second)
	defer cancel()

	// Use EvalSha (pre-loaded at startup) so the pipeline carries only the
	// 40-byte SHA rather than the full Lua script text on every flush.
	// Each key's HSET+EXPIRE+ZADD remains atomic inside the Lua execution.
	pipe := s.client.Pipeline()
	ttlSec := int64(s.keyTTL.Seconds())

	for _, e := range entries {
		pipe.EvalSha(ctx, s.writeScriptSHA,
			[]string{s.payloadKey(e.correlationKey), s.dirtyKey(e.correlationKey)},
			e.payload, fmt.Sprintf("%.0f", e.ts), e.correlationKey, ttlSec,
		)
	}

	// Collect the unique bands touched by this batch and append ZCARD for each
	// into the same pipeline — one round-trip for writes + depth checks combined.
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

func (s *Shield) payloadKey(ck string) string {
	return fmt.Sprintf("sl:%s:payload:%s", s.namespace, ck)
}

func (s *Shield) dirtyKey(ck string) string {
	return s.dirtyKeyForBand(s.BandFor(ck))
}

func (s *Shield) dirtyKeyForBand(band int) string {
	return fmt.Sprintf("sl:%s:dirty:%d", s.namespace, band)
}

// dlqKey returns the dead-letter sorted set key for a band.
// Pattern: sl:{namespace}:dlq:{band}
func (s *Shield) dlqKey(band int) string {
	return fmt.Sprintf("sl:%s:dlq:%d", s.namespace, band)
}

func applyDefaults(c *RedisConfig) {
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
