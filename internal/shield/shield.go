// Package shield manages all Redis interactions on behalf of sluice.
// No Redis type leaks beyond this package boundary.
package shield

import (
	"context"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/hussainpithawala/sluice-go"
	"github.com/redis/go-redis/v9"
)

// Shield manages all Redis interactions for the library.
type Shield struct {
	client      redis.UniversalClient
	namespace   string
	bandCount   int
	keyTTL      time.Duration
	writeScript *redis.Script
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
func New(cfg sluice.RedisConfig, namespace string, bandCount int, keyTTL time.Duration) (*Shield, error) {
	applyDefaults(&cfg)
	var client redis.UniversalClient
	if len(cfg.Addrs) == 1 {
		client = redis.NewClient(&redis.Options{
			Addr: cfg.Addrs[0], Password: cfg.Password, DB: cfg.DB,
			DialTimeout: cfg.DialTimeout, ReadTimeout: cfg.ReadTimeout,
			WriteTimeout: cfg.WriteTimeout, PoolSize: cfg.PoolSize,
		})
	} else {
		client = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: cfg.Addrs, Password: cfg.Password,
			DialTimeout: cfg.DialTimeout, ReadTimeout: cfg.ReadTimeout,
			WriteTimeout: cfg.WriteTimeout, PoolSize: cfg.PoolSize,
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.DialTimeout)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("sluice/shield: ping: %w", err)
	}
	return &Shield{
		client: client, namespace: namespace,
		bandCount: bandCount, keyTTL: keyTTL,
		writeScript: redis.NewScript(atomicWriteLua),
	}, nil
}

// Write atomically stores payload and marks the correlation key dirty.
func (s *Shield) Write(ctx context.Context, correlationKey string, payload []byte) error {
	return s.writeScript.Run(ctx, s.client,
		[]string{s.payloadKey(correlationKey), s.dirtyKey(correlationKey)},
		payload, float64(time.Now().UnixMilli()), correlationKey, int64(s.keyTTL.Seconds()),
	).Err()
}

// DrainBand reads up to maxBatch dirty keys, fetches payloads via pipeline,
// removes them from the dirty set, and returns FlushRecords for BulkWrite.
func (s *Shield) DrainBand(ctx context.Context, band, maxBatch int) ([]sluice.FlushRecord, error) {
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
	pipe := s.client.Pipeline()
	cmds := make([]*redis.SliceCmd, len(hashKeys))
	for i, hk := range hashKeys {
		cmds[i] = pipe.HMGet(ctx, hk, "p", "ts")
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, fmt.Errorf("sluice/shield: pipeline hmget band %d: %w", band, err)
	}
	records  := make([]sluice.FlushRecord, 0, len(members))
	toRemove := make([]interface{}, 0, len(members))
	for i, cmd := range cmds {
		toRemove = append(toRemove, corrKeys[i])
		vals, cmdErr := cmd.Result()
		if cmdErr != nil || vals[0] == nil {
			continue
		}
		payload, ok := vals[0].(string)
		if !ok || payload == "" {
			continue
		}
		records = append(records, sluice.FlushRecord{
			CorrelationKey: corrKeys[i],
			Payload:        []byte(payload),
			ReceivedAt:     time.Now(),
		})
	}
	if len(toRemove) > 0 {
		_ = s.client.ZRem(ctx, dirtyKey, toRemove...).Err()
	}
	return records, nil
}

// DirtyQueueDepth returns queued dirty key count for a band.
func (s *Shield) DirtyQueueDepth(ctx context.Context, band int) (int64, error) {
	return s.client.ZCard(ctx, s.dirtyKeyForBand(band)).Result()
}

// BandFor returns the band index for the given correlation key using FNV-32a.
func (s *Shield) BandFor(correlationKey string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(correlationKey))
	return int(h.Sum32()) % s.bandCount
}

func (s *Shield) Close() error { return s.client.Close() }

func (s *Shield) payloadKey(ck string) string    { return fmt.Sprintf("sl:%s:payload:%s", s.namespace, ck) }
func (s *Shield) dirtyKey(ck string) string      { return s.dirtyKeyForBand(s.BandFor(ck)) }
func (s *Shield) dirtyKeyForBand(band int) string { return fmt.Sprintf("sl:%s:dirty:%d", s.namespace, band) }

func applyDefaults(c *sluice.RedisConfig) {
	if c.DialTimeout == 0  { c.DialTimeout = 5 * time.Second }
	if c.ReadTimeout == 0  { c.ReadTimeout = 3 * time.Second }
	if c.WriteTimeout == 0 { c.WriteTimeout = 3 * time.Second }
	if c.PoolSize == 0     { c.PoolSize = 20 }
}
