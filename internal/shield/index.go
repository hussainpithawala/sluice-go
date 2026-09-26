package shield

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// ── Secondary index lifecycle ───────────────────────────────────────────────
//
// Equality (SET) and range (ZSET) index keys are shared by many correlation
// keys, so Redis TTLs cannot expire individual members. Three structures keep
// the indexes consistent with the journal:
//
//   - idxv:{band}:{ck}  HASH of the key's currently indexed values. Lets a
//     re-index SREM the key from the set of a value it no longer has, and
//     lets pruning find every index a dead key belongs to.
//   - idxreg:{band}     SET of index key names in the band. Lets the sweeper
//     enumerate indexes without a keyspace SCAN (cluster-safe: same slot).
//   - Pruning           members whose payload is gone are removed lazily by
//     QueryBand and proactively by SweepIndexBand.
//
// All three share the {band} hash tag, so every pipeline stays single-slot.

const (
	idxvEqPrefix = "s:" // idxv hash value for an equality field: "s:" + value
	idxvRange    = "n"  // idxv hash value for a range field (score lives in the ZSET)

	// queryChunk bounds how many candidates are resolved per pipeline.
	queryChunk = 500
)

// IndexValuesKey returns the per-key hash that records its indexed values.
func IndexValuesKey(namespace string, band int, ck string) string {
	return fmt.Sprintf("sl:%s:idxv:{%d}:%s", namespace, band, ck)
}

// IndexRegistryKey returns the per-band set of index key names.
func IndexRegistryKey(namespace string, band int) string {
	return fmt.Sprintf("sl:%s:idxreg:{%d}", namespace, band)
}

// indexScore normalises a numeric index value; ok is false for non-numerics.
func indexScore(val interface{}) (float64, bool) {
	switch n := val.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	}
	return 0, false
}

// queueIndexUpdate appends the commands that move ck from its previously
// indexed values (old, read from idxv) to fields.
func (s *Shield) queueIndexUpdate(ctx context.Context, pipe redis.Pipeliner, ck string, fields map[string]interface{}, old map[string]string, ttl time.Duration) {
	band := s.BandFor(ck)
	regKey := IndexRegistryKey(s.namespace, band)
	idxvKey := IndexValuesKey(s.namespace, band, ck)

	next := make(map[string]interface{}, len(fields))
	for field, val := range fields {
		if v, ok := val.(string); ok {
			eqKey := IndexKey(s.namespace, band, field, v)
			pipe.SAdd(ctx, eqKey, ck)
			pipe.Expire(ctx, eqKey, ttl)
			pipe.SAdd(ctx, regKey, eqKey)
			next[field] = idxvEqPrefix + v
		} else if score, ok := indexScore(val); ok {
			ridxKey := RangeIndexKey(s.namespace, band, field)
			pipe.ZAdd(ctx, ridxKey, redis.Z{Score: score, Member: ck})
			pipe.Expire(ctx, ridxKey, ttl)
			pipe.SAdd(ctx, regKey, ridxKey)
			next[field] = idxvRange
		}
	}

	// Drop ck from indexes of values it no longer has. ZADD already moved
	// range scores in place, so only removed range fields need a ZREM.
	for field, prev := range old {
		if cur, ok := next[field]; ok && cur == prev {
			continue
		}
		if v, ok := strings.CutPrefix(prev, idxvEqPrefix); ok {
			pipe.SRem(ctx, IndexKey(s.namespace, band, field, v), ck)
		} else if _, still := next[field]; !still {
			pipe.ZRem(ctx, RangeIndexKey(s.namespace, band, field), ck)
		}
	}

	pipe.Del(ctx, idxvKey)
	if len(next) > 0 {
		pipe.HSet(ctx, idxvKey, next)
		pipe.Expire(ctx, idxvKey, ttl)
		pipe.Expire(ctx, regKey, ttl)
	}
}

// indexValues fetches the idxv hashes for cks in one pipeline.
func (s *Shield) indexValues(ctx context.Context, cks []string) ([]map[string]string, error) {
	pipe := s.client.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(cks))
	for i, ck := range cks {
		cmds[i] = pipe.HGetAll(ctx, IndexValuesKey(s.namespace, s.BandFor(ck), ck))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	out := make([]map[string]string, len(cks))
	for i, cmd := range cmds {
		out[i] = cmd.Val()
	}
	return out, nil
}

// pruneIndexMembers removes dead correlation keys from every index they
// belong to, as recorded in idxv. Keys indexed before idxv existed have no
// hash, so they are removed from the caller-supplied fallback keys instead.
//
// Best-effort: a Write that lands between the caller's liveness check and
// this prune loses its index entries until the key is written again.
func (s *Shield) pruneIndexMembers(ctx context.Context, band int, cks, fallbackSets, fallbackZSets []string) error {
	if len(cks) == 0 {
		return nil
	}
	olds, err := s.indexValues(ctx, cks)
	if err != nil {
		return err
	}

	pipe := s.client.Pipeline()
	for i, ck := range cks {
		for _, key := range fallbackSets {
			pipe.SRem(ctx, key, ck)
		}
		for _, key := range fallbackZSets {
			pipe.ZRem(ctx, key, ck)
		}
		for field, prev := range olds[i] {
			if v, ok := strings.CutPrefix(prev, idxvEqPrefix); ok {
				pipe.SRem(ctx, IndexKey(s.namespace, band, field, v), ck)
			} else {
				pipe.ZRem(ctx, RangeIndexKey(s.namespace, band, field), ck)
			}
		}
		pipe.Del(ctx, IndexValuesKey(s.namespace, band, ck))
	}
	_, err = pipe.Exec(ctx)
	return err
}

// IndexMatch is a live correlation key that satisfied a band query.
type IndexMatch struct {
	CorrelationKey string
	Payload        []byte
}

// QueryBand resolves an equality intersection plus range bounds within one
// band. Candidates are checked in pipelined chunks (one round-trip per chunk
// rather than several per candidate), and candidates whose payload has
// expired are pruned from the indexes as a side effect.
func (s *Shield) QueryBand(ctx context.Context, band int, eqKeys []string, rangeMin, rangeMax map[string]float64) ([]IndexMatch, error) {
	candidates, err := s.client.SInter(ctx, eqKeys...).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	rangeFields := make([]string, 0, len(rangeMin)+len(rangeMax))
	for f := range rangeMin {
		rangeFields = append(rangeFields, f)
	}
	for f := range rangeMax {
		if _, dup := rangeMin[f]; !dup {
			rangeFields = append(rangeFields, f)
		}
	}
	rangeKeys := make([]string, len(rangeFields))
	for i, f := range rangeFields {
		rangeKeys[i] = RangeIndexKey(s.namespace, band, f)
	}

	var matches []IndexMatch
	var dead []string
	for start := 0; start < len(candidates); start += queryChunk {
		chunk := candidates[start:min(start+queryChunk, len(candidates))]

		pipe := s.client.Pipeline()
		payloads := make([]*redis.StringCmd, len(chunk))
		scores := make([][]*redis.FloatCmd, len(chunk))
		for i, ck := range chunk {
			payloads[i] = pipe.HGet(ctx, PayloadKey(s.namespace, band, ck), "p")
			scores[i] = make([]*redis.FloatCmd, len(rangeKeys))
			for j, key := range rangeKeys {
				scores[i][j] = pipe.ZScore(ctx, key, ck)
			}
		}
		if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
			return nil, err
		}

	candidate:
		for i, ck := range chunk {
			payload, perr := payloads[i].Result()
			if perr != nil || payload == "" {
				dead = append(dead, ck)
				continue
			}
			for j, field := range rangeFields {
				score, serr := scores[i][j].Result()
				if serr != nil {
					continue candidate
				}
				if lo, ok := rangeMin[field]; ok && score < lo {
					continue candidate
				}
				if hi, ok := rangeMax[field]; ok && score > hi {
					continue candidate
				}
			}
			matches = append(matches, IndexMatch{CorrelationKey: ck, Payload: []byte(payload)})
		}
	}

	// Pruning is housekeeping; a failure must not fail the query.
	_ = s.pruneIndexMembers(ctx, band, dead, eqKeys, rangeKeys)
	return matches, nil
}

// SweepIndexBand walks every index registered for band and prunes members
// whose payload no longer exists. Index keys that have expired are dropped
// from the registry. Returns the number of members scanned and pruned.
func (s *Shield) SweepIndexBand(ctx context.Context, band int, batch int) (scanned, pruned int, err error) {
	regKey := IndexRegistryKey(s.namespace, band)
	keys, err := s.client.SMembers(ctx, regKey).Result()
	if err != nil {
		return 0, 0, err
	}

	for _, key := range keys {
		isRange := strings.Contains(key, ":ridx:")
		var cursor uint64
		seen := false
		for {
			var members []string
			if isRange {
				var pairs []string
				pairs, cursor, err = s.client.ZScan(ctx, key, cursor, "", int64(batch)).Result()
				for i := 0; i < len(pairs); i += 2 { // ZSCAN returns member, score pairs
					members = append(members, pairs[i])
				}
			} else {
				members, cursor, err = s.client.SScan(ctx, key, cursor, "", int64(batch)).Result()
			}
			if err != nil {
				return scanned, pruned, err
			}
			seen = seen || len(members) > 0
			scanned += len(members)

			var n int
			n, err = s.pruneDead(ctx, band, key, isRange, members)
			pruned += n
			if err != nil {
				return scanned, pruned, err
			}
			if cursor == 0 {
				break
			}
		}
		if !seen {
			// Empty index key means it expired or was fully pruned.
			s.client.SRem(ctx, regKey, key)
		}
	}
	return scanned, pruned, nil
}

// pruneDead checks payload liveness for members of one index key and prunes
// the dead ones. key is used as the fallback for members without idxv.
func (s *Shield) pruneDead(ctx context.Context, band int, key string, isRange bool, members []string) (int, error) {
	if len(members) == 0 {
		return 0, nil
	}
	pipe := s.client.Pipeline()
	exists := make([]*redis.IntCmd, len(members))
	for i, ck := range members {
		exists[i] = pipe.Exists(ctx, PayloadKey(s.namespace, band, ck))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}

	var dead []string
	for i, cmd := range exists {
		if cmd.Val() == 0 {
			dead = append(dead, members[i])
		}
	}
	var sets, zsets []string
	if isRange {
		zsets = []string{key}
	} else {
		sets = []string{key}
	}
	return len(dead), s.pruneIndexMembers(ctx, band, dead, sets, zsets)
}

// RegisterExistingIndexes adds index keys written before the registry
// existed, so the sweeper can reach them. It SCANs the keyspace (every
// master in cluster mode) and is meant to run once at sweeper start.
func (s *Shield) RegisterExistingIndexes(ctx context.Context) (int, error) {
	var total int
	for _, kind := range []string{"idx", "ridx"} {
		n, err := s.scanKeys(ctx, fmt.Sprintf("sl:%s:%s:*", s.namespace, kind), func(ctx context.Context, keys []string) error {
			pipe := s.client.Pipeline()
			for _, key := range keys {
				band, ok := bandFromKey(key)
				if !ok {
					continue
				}
				pipe.SAdd(ctx, IndexRegistryKey(s.namespace, band), key)
			}
			_, err := pipe.Exec(ctx)
			return err
		})
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// bandFromKey extracts the band from a key's {band} hash tag.
func bandFromKey(key string) (int, bool) {
	open := strings.IndexByte(key, '{')
	end := strings.IndexByte(key, '}')
	if open < 0 || end <= open+1 {
		return 0, false
	}
	var band int
	if _, err := fmt.Sscanf(key[open+1:end], "%d", &band); err != nil {
		return 0, false
	}
	return band, true
}
