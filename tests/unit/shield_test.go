package unit

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/hussainpithawala/sluice-go/internal/shield"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testRedisAddr returns the Redis address for testing.
// Defaults to localhost:6379 if not set.
func testRedisAddr() string {
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		return addr
	}
	return "localhost:6379"
}

// newTestShield creates a Shield instance for testing with a unique namespace.
func newTestShield(t *testing.T, namespace string) *shield.Shield {
	cfg := shield.RedisConfig{
		Addrs:        []string{testRedisAddr()},
		ClusterMode:  false,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     10,
	}

	s, err := shield.New(cfg, namespace, 16, 30*time.Second, 4*time.Hour)
	require.NoError(t, err, "failed to create shield")

	// Clean up any existing keys for this namespace
	cleanRedisKeys(t, s.Client(), namespace)

	return s
}

// cleanRedisKeys removes all keys matching the namespace pattern.
func cleanRedisKeys(t *testing.T, client redis.UniversalClient, namespace string) {
	ctx := context.Background()
	pattern := fmt.Sprintf("sl:%s:*", namespace)

	var cursor uint64
	for {
		keys, nextCursor, err := client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			t.Logf("warning: scan failed: %v", err)
			break
		}

		if len(keys) > 0 {
			if err := client.Del(ctx, keys...).Err(); err != nil {
				t.Logf("warning: del failed: %v", err)
			}
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
}

func TestNew_Validation(t *testing.T) {
	t.Run("namespace with braces rejected", func(t *testing.T) {
		cfg := shield.RedisConfig{
			Addrs:       []string{testRedisAddr()},
			ClusterMode: false,
		}
		_, err := shield.New(cfg, "test{bad}", 16, 30*time.Second, 4*time.Hour)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must not contain")
	})

	t.Run("empty addresses rejected", func(t *testing.T) {
		cfg := shield.RedisConfig{
			Addrs:       []string{},
			ClusterMode: false,
		}
		_, err := shield.New(cfg, "test", 16, 30*time.Second, 4*time.Hour)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "at least one address")
	})

	t.Run("cluster mode with non-zero DB rejected", func(t *testing.T) {
		cfg := shield.RedisConfig{
			Addrs:       []string{testRedisAddr()},
			ClusterMode: true,
			DB:          1,
		}
		_, err := shield.New(cfg, "test", 16, 30*time.Second, 4*time.Hour)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "DB must be 0 in cluster mode")
	})
}

func TestWrite_Basic(t *testing.T) {
	s := newTestShield(t, "test_write_basic")
	defer s.Close()

	ctx := context.Background()
	corrKey := "user_123"
	payload := []byte(`{"name":"Alice","age":30}`)

	err := s.Write(ctx, corrKey, payload)
	require.NoError(t, err)

	// Verify payload exists
	readPayload, ttl, found, err := s.ReadWithTTL(ctx, corrKey)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, payload, readPayload)
	assert.Greater(t, ttl, time.Duration(0))

	// Verify dirty queue has the key
	depth, err := s.DirtyQueueDepth(ctx, s.BandFor(corrKey))
	require.NoError(t, err)
	assert.Equal(t, int64(1), depth)
}

func TestWrite_Coalescing(t *testing.T) {
	s := newTestShield(t, "test_write_coalescing")
	defer s.Close()

	ctx := context.Background()
	corrKey := "user_456"

	// Write same key multiple times
	for i := 0; i < 5; i++ {
		payload := []byte(fmt.Sprintf(`{"version":%d}`, i))
		err := s.Write(ctx, corrKey, payload)
		require.NoError(t, err)
	}

	// Dirty queue should have only 1 entry (coalesced)
	depth, err := s.DirtyQueueDepth(ctx, s.BandFor(corrKey))
	require.NoError(t, err)
	assert.Equal(t, int64(1), depth)

	// Payload should be the latest
	readPayload, _, found, err := s.ReadWithTTL(ctx, corrKey)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, []byte(`{"version":4}`), readPayload)
}

func TestWriteDedup_ContentHash(t *testing.T) {
	s := newTestShield(t, "test_write_dedup")
	defer s.Close()

	ctx := context.Background()
	corrKey := "user_789"
	payload := []byte(`{"data":"same"}`)
	hash := "abc123hash"

	// First write should succeed
	written, err := s.WriteDedup(ctx, corrKey, payload, hash)
	require.NoError(t, err)
	assert.True(t, written, "first write should be written")

	// Second write with same hash should be deduplicated
	written, err = s.WriteDedup(ctx, corrKey, payload, hash)
	require.NoError(t, err)
	assert.False(t, written, "second write should be deduplicated")

	// Write with different hash should succeed
	written, err = s.WriteDedup(ctx, corrKey, []byte(`{"data":"different"}`), "def456hash")
	require.NoError(t, err)
	assert.True(t, written, "write with different hash should be written")
}

func TestRead_NotFound(t *testing.T) {
	s := newTestShield(t, "test_read_not_found")
	defer s.Close()

	ctx := context.Background()
	payload, ttl, found, err := s.ReadWithTTL(ctx, "nonexistent_key")
	require.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, payload)
	assert.Equal(t, time.Duration(0), ttl)
}

func TestHotMarker(t *testing.T) {
	s := newTestShield(t, "test_hot_marker")
	defer s.Close()

	ctx := context.Background()
	corrKey := "hot_user_001"

	// Initially not hot
	isHot, err := s.IsHot(ctx, corrKey)
	require.NoError(t, err)
	assert.False(t, isHot)

	// Set hot marker
	err = s.SetHotMarker(ctx, corrKey, 1*time.Hour)
	require.NoError(t, err)

	// Now should be hot
	isHot, err = s.IsHot(ctx, corrKey)
	require.NoError(t, err)
	assert.True(t, isHot)
}

func TestHotLoad(t *testing.T) {
	s := newTestShield(t, "test_hot_load")
	defer s.Close()

	ctx := context.Background()
	corrKey := "hot_user_002"
	payload := []byte(`{"status":"active"}`)

	err := s.HotLoad(ctx, corrKey, payload)
	require.NoError(t, err)

	// Should be marked as hot
	isHot, err := s.IsHot(ctx, corrKey)
	require.NoError(t, err)
	assert.True(t, isHot)

	// Payload should be readable
	readPayload, _, found, err := s.ReadWithTTL(ctx, corrKey)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, payload, readPayload)
}

func TestRead_HotPath(t *testing.T) {
	s := newTestShield(t, "test_read_hot_path")
	defer s.Close()

	ctx := context.Background()
	corrKey := "hot_user_003"
	payload := []byte(`{"premium":true}`)

	// Write and mark as hot
	err := s.HotLoad(ctx, corrKey, payload)
	require.NoError(t, err)

	// Read should return payload and isHot=true
	readPayload, isHot, err := s.Read(ctx, corrKey)
	require.NoError(t, err)
	assert.True(t, isHot)
	assert.Equal(t, payload, readPayload)
}

func TestDrainBand_Basic(t *testing.T) {
	s := newTestShield(t, "test_drain_band_basic")
	defer s.Close()

	ctx := context.Background()
	band := 0

	// Write 3 keys to the same band
	keys := []string{"key_001", "key_002", "key_003"}
	for _, key := range keys {
		// Ensure all keys hash to band 0
		for s.BandFor(key) != band {
			key = key + "_retry"
		}
		err := s.Write(ctx, key, []byte(fmt.Sprintf(`{"key":"%s"}`, key)))
		require.NoError(t, err)
	}

	// Drain the band
	records, err := s.DrainBand(ctx, band, 10)
	require.NoError(t, err)
	assert.Len(t, records, 3)

	// Verify records have payloads
	for _, rec := range records {
		assert.NotEmpty(t, rec.Payload)
		assert.NotEmpty(t, rec.CorrelationKey)
	}

	// Keys should still be in dirty set (not committed yet)
	depth, err := s.DirtyQueueDepth(ctx, band)
	require.NoError(t, err)
	assert.Equal(t, int64(3), depth)
}

func TestCommitKeys(t *testing.T) {
	s := newTestShield(t, "test_commit_keys")
	defer s.Close()

	ctx := context.Background()
	band := 0

	// Write and drain
	key := "commit_test_key"
	for s.BandFor(key) != band {
		key = key + "_x"
	}

	err := s.Write(ctx, key, []byte(`{"test":"data"}`))
	require.NoError(t, err)

	records, err := s.DrainBand(ctx, band, 10)
	require.NoError(t, err)
	assert.Len(t, records, 1)

	// Commit the key
	corrKeys := []string{records[0].CorrelationKey}
	err = s.CommitKeys(ctx, band, corrKeys)
	require.NoError(t, err)

	// Dirty queue should be empty now
	depth, err := s.DirtyQueueDepth(ctx, band)
	require.NoError(t, err)
	assert.Equal(t, int64(0), depth)
}

func TestMoveToDeadLetter(t *testing.T) {
	s := newTestShield(t, "test_move_to_dlq")
	defer s.Close()

	ctx := context.Background()
	band := 0

	// Write a key
	key := "dlq_test_key"
	for s.BandFor(key) != band {
		key = key + "_y"
	}

	err := s.Write(ctx, key, []byte(`{"bad":"data"}`))
	require.NoError(t, err)

	// Drain it
	records, err := s.DrainBand(ctx, band, 10)
	require.NoError(t, err)
	require.Len(t, records, 1)

	// Move to DLQ
	corrKeys := []string{records[0].CorrelationKey}
	err = s.MoveToDeadLetter(ctx, band, corrKeys, "test_reason")
	require.NoError(t, err)

	// Dirty queue should be empty
	depth, err := s.DirtyQueueDepth(ctx, band)
	require.NoError(t, err)
	assert.Equal(t, int64(0), depth)

	// DLQ should have 1 entry
	dlqDepth, err := s.DeadLetterDepth(ctx, band)
	require.NoError(t, err)
	assert.Equal(t, int64(1), dlqDepth)
}

func TestDrainDLQ(t *testing.T) {
	s := newTestShield(t, "test_drain_dlq")
	defer s.Close()

	ctx := context.Background()
	band := 0

	// Write, drain, and move to DLQ
	key := "dlq_drain_key"
	for s.BandFor(key) != band {
		key = key + "_z"
	}

	err := s.Write(ctx, key, []byte(`{"dlq":"test"}`))
	require.NoError(t, err)

	records, err := s.DrainBand(ctx, band, 10)
	require.NoError(t, err)
	require.Len(t, records, 1)

	err = s.MoveToDeadLetter(ctx, band, []string{records[0].CorrelationKey}, "reason")
	require.NoError(t, err)

	// Drain DLQ
	dlqRecords, err := s.DrainDLQ(ctx, band, 10)
	require.NoError(t, err)
	assert.Len(t, dlqRecords, 1)
	assert.Equal(t, records[0].CorrelationKey, dlqRecords[0].CorrelationKey)
}

func TestCommitDLQKeys(t *testing.T) {
	s := newTestShield(t, "test_commit_dlq_keys")
	defer s.Close()

	ctx := context.Background()
	band := 0

	// Setup: write → drain → DLQ → drain DLQ
	key := "dlq_commit_key"
	for s.BandFor(key) != band {
		key = key + "_w"
	}

	err := s.Write(ctx, key, []byte(`{"commit":"dlq"}`))
	require.NoError(t, err)

	records, err := s.DrainBand(ctx, band, 10)
	require.NoError(t, err)
	require.Len(t, records, 1)

	err = s.MoveToDeadLetter(ctx, band, []string{records[0].CorrelationKey}, "test")
	require.NoError(t, err)

	dlqRecords, err := s.DrainDLQ(ctx, band, 10)
	require.NoError(t, err)
	require.Len(t, dlqRecords, 1)

	// Commit DLQ keys
	err = s.CommitDLQKeys(ctx, band, []string{dlqRecords[0].CorrelationKey})
	require.NoError(t, err)

	// DLQ should be empty
	dlqDepth, err := s.DeadLetterDepth(ctx, band)
	require.NoError(t, err)
	assert.Equal(t, int64(0), dlqDepth)
}

func TestSetNX_Idempotency(t *testing.T) {
	s := newTestShield(t, "test_setnx")
	defer s.Close()

	ctx := context.Background()
	key := "idempotency_key_001"

	// First SETNX should succeed
	ok, err := s.SetNX(ctx, key, "value1", 1*time.Hour)
	require.NoError(t, err)
	assert.True(t, ok)

	// Second SETNX with same key should fail
	ok, err = s.SetNX(ctx, key, "value2", 1*time.Hour)
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestUpdateIndexes(t *testing.T) {
	s := newTestShield(t, "test_update_indexes")
	defer s.Close()

	ctx := context.Background()
	corrKey := "indexed_key_001"
	indexes := map[string]interface{}{
		"channel":  "push",
		"priority": float64(5),
		"count":    int64(10),
	}

	err := s.UpdateIndexes(ctx, corrKey, indexes, 1*time.Hour)
	require.NoError(t, err)

	// Verify equality index
	band := s.BandFor(corrKey)
	eqKey := shield.IndexKey(s.Namespace(), band, "channel", "push")
	members, err := s.Client().SMembers(ctx, eqKey).Result()
	require.NoError(t, err)
	assert.Contains(t, members, corrKey)

	// Verify range index
	ridxKey := shield.RangeIndexKey(s.Namespace(), band, "priority")
	score, err := s.ZScore(ctx, ridxKey, corrKey)
	require.NoError(t, err)
	assert.Equal(t, float64(5), score)
}

func TestSInter_ClusterSafe(t *testing.T) {
	s := newTestShield(t, "test_sinter")
	defer s.Close()

	ctx := context.Background()
	band := 0

	// Create two equality indexes in the same band
	key1 := "sinter_key_001"
	key2 := "sinter_key_002"
	key3 := "sinter_key_003"

	for s.BandFor(key1) != band {
		key1 = key1 + "_a"
	}
	for s.BandFor(key2) != band {
		key2 = key2 + "_b"
	}
	for s.BandFor(key3) != band {
		key3 = key3 + "_c"
	}

	// key1 and key2 have channel=push
	// key2 and key3 have campaign=camp_001
	indexes1 := map[string]interface{}{"channel": "push"}
	indexes2 := map[string]interface{}{"channel": "push", "campaign": "camp_001"}
	indexes3 := map[string]interface{}{"campaign": "camp_001"}

	err := s.UpdateIndexes(ctx, key1, indexes1, 1*time.Hour)
	require.NoError(t, err)
	err = s.UpdateIndexes(ctx, key2, indexes2, 1*time.Hour)
	require.NoError(t, err)
	err = s.UpdateIndexes(ctx, key3, indexes3, 1*time.Hour)
	require.NoError(t, err)

	// Intersect: channel=push AND campaign=camp_001
	eqKey1 := shield.IndexKey(s.Namespace(), band, "channel", "push")
	eqKey2 := shield.IndexKey(s.Namespace(), band, "campaign", "camp_001")

	result, err := s.SInter(ctx, eqKey1, eqKey2)
	require.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, key2, result[0])
}

func TestBatching_EnableAndWrite(t *testing.T) {
	s := newTestShield(t, "test_batching")
	defer s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Enable batching
	s.EnableBatching(100, 10*time.Millisecond)
	s.StartBatcher(ctx)
	defer s.StopBatcher()

	// Write multiple keys
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("batch_key_%03d", i)
		payload := []byte(fmt.Sprintf(`{"batch":%d}`, i))
		err := s.Write(ctx, key, payload)
		require.NoError(t, err)
	}

	// Wait for batch to flush
	time.Sleep(50 * time.Millisecond)

	// Verify some keys are in Redis
	found := 0
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("batch_key_%03d", i)
		_, _, f, err := s.ReadWithTTL(ctx, key)
		require.NoError(t, err)
		if f {
			found++
		}
	}

	assert.Greater(t, found, 0, "at least some batched writes should be visible")
}

func TestKeyNaming_ClusterHashTags(t *testing.T) {
	namespace := "test_keys"
	band := 5
	corrKey := "user_999"

	payloadKey := shield.PayloadKey(namespace, band, corrKey)
	dirtyKey := shield.DirtyKey(namespace, band)
	dlqKey := shield.DLQKey(namespace, band)

	// All keys should contain the band hash tag
	assert.Contains(t, payloadKey, "{5}")
	assert.Contains(t, dirtyKey, "{5}")
	assert.Contains(t, dlqKey, "{5}")

	// Verify format
	assert.Equal(t, "sl:test_keys:payload:{5}:user_999", payloadKey)
	assert.Equal(t, "sl:test_keys:dirty:{5}", dirtyKey)
	assert.Equal(t, "sl:test_keys:dlq:{5}", dlqKey)
}

func TestBandForKey_Consistency(t *testing.T) {
	corrKey := "consistent_key"
	bandCount := 16

	// Same key should always hash to same band
	band1 := shield.BandForKey(corrKey, bandCount)
	band2 := shield.BandForKey(corrKey, bandCount)
	assert.Equal(t, band1, band2)

	// Different keys may hash to different bands (probabilistic)
	otherKey := "different_key"
	band3 := shield.BandForKey(otherKey, bandCount)
	// We don't assert they're different, just that the function works
	assert.GreaterOrEqual(t, band3, 0)
	assert.Less(t, band3, bandCount)
}

func TestOldestDirtyScore(t *testing.T) {
	s := newTestShield(t, "test_oldest_dirty")
	defer s.Close()

	ctx := context.Background()
	band := 0

	// Empty band should return 0
	score, err := s.OldestDirtyScore(ctx, band)
	require.NoError(t, err)
	assert.Equal(t, float64(0), score)

	// Write a key
	key := "oldest_key"
	for s.BandFor(key) != band {
		key = key + "_old"
	}

	before := time.Now().UnixMilli()
	err = s.Write(ctx, key, []byte(`{"oldest":true}`))
	require.NoError(t, err)
	after := time.Now().UnixMilli()

	// Score should be within the write window
	score, err = s.OldestDirtyScore(ctx, band)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, int64(score), before)
	assert.LessOrEqual(t, int64(score), after)
}

func TestRefreshHotTTL(t *testing.T) {
	s := newTestShield(t, "test_refresh_hot_ttl")
	defer s.Close()

	ctx := context.Background()
	band := 0

	// Write a key
	key := "refresh_key"
	for s.BandFor(key) != band {
		key = key + "_ref"
	}

	err := s.Write(ctx, key, []byte(`{"refresh":true}`))
	require.NoError(t, err)

	// Get initial TTL
	_, initialTTL, _, err := s.ReadWithTTL(ctx, key)
	require.NoError(t, err)

	// Refresh hot TTL
	err = s.RefreshHotTTL(ctx, band, []string{key})
	require.NoError(t, err)

	// TTL should be extended (approximately to activityWindow)
	_, newTTL, _, err := s.ReadWithTTL(ctx, key)
	require.NoError(t, err)
	assert.Greater(t, newTTL, initialTTL)
}

func TestZRangeWithScores(t *testing.T) {
	s := newTestShield(t, "test_zrange_scores")
	defer s.Close()

	ctx := context.Background()
	band := 0

	// Write multiple keys with small delays to get different scores
	keys := make([]string, 3)
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("score_key_%d", i)
		for s.BandFor(key) != band {
			key = key + "_s"
		}
		keys[i] = key
		err := s.Write(ctx, key, []byte(fmt.Sprintf(`{"i":%d}`, i)))
		require.NoError(t, err)
		time.Sleep(2 * time.Millisecond) // Ensure different timestamps
	}

	// Get scores
	scores, err := s.ZRangeWithScores(ctx, band, 0, 2)
	require.NoError(t, err)
	assert.Len(t, scores, 3)

	// Scores should be in ascending order (oldest first)
	for i := 1; i < len(scores); i++ {
		assert.LessOrEqual(t, scores[i-1].Score, scores[i].Score)
	}
}

func TestConcurrentWrites(t *testing.T) {
	s := newTestShield(t, "test_concurrent")
	defer s.Close()

	ctx := context.Background()
	const numGoroutines = 10
	const writesPerGoroutine = 10

	done := make(chan error, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(gID int) {
			for i := 0; i < writesPerGoroutine; i++ {
				key := fmt.Sprintf("concurrent_%d_%d", gID, i)
				payload := []byte(fmt.Sprintf(`{"g":%d,"i":%d}`, gID, i))
				if err := s.Write(ctx, key, payload); err != nil {
					done <- err
					return
				}
			}
			done <- nil
		}(g)
	}

	// Collect results
	for i := 0; i < numGoroutines; i++ {
		err := <-done
		assert.NoError(t, err)
	}
}

func TestNamespaceIsolation(t *testing.T) {
	// Create two shields with different namespaces
	s1 := newTestShield(t, "namespace_a")
	defer s1.Close()

	s2 := newTestShield(t, "namespace_b")
	defer s2.Close()

	ctx := context.Background()
	key := "shared_key_name"

	// Write to namespace A
	err := s1.Write(ctx, key, []byte(`{"ns":"a"}`))
	require.NoError(t, err)

	// Should not be visible in namespace B
	_, _, found, err := s2.ReadWithTTL(ctx, key)
	require.NoError(t, err)
	assert.False(t, found, "key from namespace_a should not be visible in namespace_b")

	// Write to namespace B
	err = s2.Write(ctx, key, []byte(`{"ns":"b"}`))
	require.NoError(t, err)

	// Read from both
	payloadA, _, _, err := s1.ReadWithTTL(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, []byte(`{"ns":"a"}`), payloadA)

	payloadB, _, _, err := s2.ReadWithTTL(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, []byte(`{"ns":"b"}`), payloadB)
}

func TestExpiredPayloadCleanup(t *testing.T) {
	// Create shield with very short TTL
	cfg := shield.RedisConfig{
		Addrs:       []string{testRedisAddr()},
		ClusterMode: false,
	}
	s, err := shield.New(cfg, "test_expiry", 16, 1*time.Second, 4*time.Hour)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()
	cleanRedisKeys(t, s.Client(), "test_expiry")

	key := "expiry_key"
	err = s.Write(ctx, key, []byte(`{"expires":"soon"}`))
	require.NoError(t, err)

	// Wait for TTL to expire
	time.Sleep(2 * time.Second)

	// DrainBand should clean up expired keys
	band := s.BandFor(key)
	records, err := s.DrainBand(ctx, band, 10)
	require.NoError(t, err)
	assert.Len(t, records, 0, "expired keys should not be returned")

	// Dirty queue should be cleaned
	depth, err := s.DirtyQueueDepth(ctx, band)
	require.NoError(t, err)
	assert.Equal(t, int64(0), depth)
}

func TestEmptyCorrelationKey(t *testing.T) {
	s := newTestShield(t, "test_empty_key")
	defer s.Close()

	ctx := context.Background()

	// Write with empty key should still work (Redis allows it)
	// but it's generally a bad practice
	err := s.Write(ctx, "", []byte(`{"empty":"key"}`))
	// This test documents current behavior - adjust if you want to reject empty keys
	if err != nil {
		t.Logf("Write with empty key returned error (expected): %v", err)
	}
}

func TestLargePayload(t *testing.T) {
	s := newTestShield(t, "test_large_payload")
	defer s.Close()

	ctx := context.Background()
	key := "large_payload_key"

	// Create a 100KB payload
	largePayload := make([]byte, 100*1024)
	for i := range largePayload {
		largePayload[i] = 'x'
	}

	err := s.Write(ctx, key, largePayload)
	require.NoError(t, err)

	// Read it back
	readPayload, _, found, err := s.ReadWithTTL(ctx, key)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, largePayload, readPayload)
}

func TestSpecialCharactersInKey(t *testing.T) {
	s := newTestShield(t, "test_special_chars")
	defer s.Close()

	ctx := context.Background()

	testCases := []string{
		"user@example.com",
		"user:with:colons",
		"user/with/slashes",
		"user-with-dashes",
		"user_with_underscores",
		"user.with.dots",
	}

	for _, key := range testCases {
		t.Run(key, func(t *testing.T) {
			payload := []byte(fmt.Sprintf(`{"key":"%s"}`, key))
			err := s.Write(ctx, key, payload)
			require.NoError(t, err)

			readPayload, _, found, err := s.ReadWithTTL(ctx, key)
			require.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, payload, readPayload)
		})
	}
}

func TestShieldMethods_BandCount_Namespace(t *testing.T) {
	s := newTestShield(t, "test_methods")
	defer s.Close()

	assert.Equal(t, 16, s.BandCount())
	assert.Equal(t, "test_methods", s.Namespace())
}

func TestClose(t *testing.T) {
	s := newTestShield(t, "test_close")

	err := s.Close()
	require.NoError(t, err)

	// Operations after close should fail
	ctx := context.Background()
	_, err = s.Client().Ping(ctx).Result()
	assert.Error(t, err, "operations should fail after Close()")
}
