package documentdb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hussainpithawala/sluice-go/internal/shield"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestShieldIndexed creates a shield with indexEnabled=true, mirroring
// newTestShield but enabling the ixp marker on every write script.
func newTestShieldIndexed(t *testing.T, namespace string) *shield.Shield {
	t.Helper()
	cfg := shield.RedisConfig{
		Addrs:        []string{testRedisAddr()},
		ClusterMode:  false,
		DB:           testRedisDB,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     10,
	}
	s, err := shield.New(cfg, namespace, 16, 30*time.Second, 4*time.Hour, 0, true)
	require.NoError(t, err, "failed to create indexed shield")
	cleanRedisKeys(t, s.Client(), namespace)
	return s
}

// TestIndexReconciliation_FreshWriteSetsMarker verifies that a write to an
// index-enabled shield sets the ixp marker equal to the write sequence,
// and that ClearIndexPending removes it.
func TestIndexReconciliation_FreshWriteSetsMarker(t *testing.T) {
	s := newTestShieldIndexed(t, "test_ixp_fresh_write")
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()

	const ck = "ixp_fresh_001"
	wr, err := s.Write(ctx, ck, []byte(`{"v":1}`), nil)
	require.NoError(t, err)
	require.True(t, wr.Written, "first write must succeed")

	// HGET the payload hash for "ixp" — should exist and equal the WriteResult.Seq
	band := s.BandFor(ck)
	payloadKey := shield.PayloadKey(s.Namespace(), band, ck)
	ixp, err := s.Client().HGet(ctx, payloadKey, "ixp").Result()
	require.NoError(t, err, "ixp field must exist after indexed write")
	assert.Equal(t, wr.Seq, ixp, "ixp must equal the write sequence")

	// ClearIndexPending with that seq — should succeed
	err = s.ClearIndexPending(ctx, ck, wr.Seq)
	require.NoError(t, err)

	// HGET "ixp" again — should be gone (redis.Nil)
	_, err = s.Client().HGet(ctx, payloadKey, "ixp").Result()
	assert.ErrorIs(t, err, redis.Nil, "ixp must be removed after ClearIndexPending")
}

// TestIndexReconciliation_DedupHitWithMarkerReturnsCode3 verifies that a dedup
// hit while the ixp marker is still present returns WriteCodeIndexPend (3),
// signalling the caller to re-index.
func TestIndexReconciliation_DedupHitWithMarkerReturnsCode3(t *testing.T) {
	s := newTestShieldIndexed(t, "test_ixp_dedup_code3")
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()

	const ck = "ixp_dedup_001"
	payload := []byte(`{"dedup":"test"}`)

	// First WriteDedup — succeeds (code 1, ixp set)
	wr1, err := s.WriteDedup(ctx, ck, payload, "h1", nil)
	require.NoError(t, err)
	assert.Equal(t, shield.WriteCodeWritten, wr1.Status, "first write must be code 1")
	assert.True(t, wr1.Written)

	// Do NOT clear the marker (simulating index failure)

	// Second WriteDedup with same hash "h1" — returns code 3 (WriteCodeIndexPend)
	wr2, err := s.WriteDedup(ctx, ck, payload, "h1", nil)
	require.NoError(t, err)
	assert.Equal(t, shield.WriteCodeIndexPend, wr2.Status, "dedup hit with ixp must return code 3")
	assert.False(t, wr2.Written, "dedup hit must not re-write")

	// Seq should match the current v
	seq, err := s.GetSeq(ctx, ck)
	require.NoError(t, err)
	assert.Equal(t, seq, wr2.Seq, "result Seq must match current v")
}

// TestIndexReconciliation_DedupHitWithoutMarkerReturnsCode0 verifies that once
// the ixp marker is cleared (successful index update), a dedup hit returns the
// normal code 0 with no repair signal.
func TestIndexReconciliation_DedupHitWithoutMarkerReturnsCode0(t *testing.T) {
	s := newTestShieldIndexed(t, "test_ixp_dedup_code0")
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()

	const ck = "ixp_nodedup_001"
	payload := []byte(`{"clean":"dedup"}`)

	// WriteDedup, then ClearIndexPending (simulating successful index update)
	wr1, err := s.WriteDedup(ctx, ck, payload, "h1", nil)
	require.NoError(t, err)
	require.Equal(t, shield.WriteCodeWritten, wr1.Status)

	err = s.ClearIndexPending(ctx, ck, wr1.Seq)
	require.NoError(t, err)

	// WriteDedup same hash — returns code 0 (normal dedup, no repair needed)
	wr2, err := s.WriteDedup(ctx, ck, payload, "h1", nil)
	require.NoError(t, err)
	assert.Equal(t, shield.WriteCodeDedup, wr2.Status, "dedup hit without ixp must return code 0")
	assert.False(t, wr2.Written)
}

// TestIndexReconciliation_ClearConditionalSkipsOnNewerVersion verifies that
// ClearIndexPending(ctx, ck, "N") only clears the marker when v==N, protecting
// against stale clears after a newer write has landed.
func TestIndexReconciliation_ClearConditionalSkipsOnNewerVersion(t *testing.T) {
	s := newTestShieldIndexed(t, "test_ixp_conditional")
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()

	const ck = "ixp_cond_001"
	band := s.BandFor(ck)
	payloadKey := shield.PayloadKey(s.Namespace(), band, ck)

	// Write key (v=1, ixp="1")
	wr1, err := s.Write(ctx, ck, []byte(`{"v":1}`), nil)
	require.NoError(t, err)
	require.True(t, wr1.Written)
	seq1 := wr1.Seq

	// Write key again (v=2, ixp="2") — overwrites
	wr2, err := s.Write(ctx, ck, []byte(`{"v":2}`), nil)
	require.NoError(t, err)
	require.True(t, wr2.Written)
	seq2 := wr2.Seq

	// ClearIndexPending(ctx, ck, seq1) — should NOT clear because v is now seq2
	err = s.ClearIndexPending(ctx, ck, seq1)
	require.NoError(t, err)

	// HGET "ixp" — should still be seq2
	ixp, err := s.Client().HGet(ctx, payloadKey, "ixp").Result()
	require.NoError(t, err)
	assert.Equal(t, seq2, ixp, "conditional clear with stale seq must not remove ixp")

	// ClearIndexPending(ctx, ck, seq2) — should clear
	err = s.ClearIndexPending(ctx, ck, seq2)
	require.NoError(t, err)

	// HGET "ixp" — should be gone
	_, err = s.Client().HGet(ctx, payloadKey, "ixp").Result()
	assert.ErrorIs(t, err, redis.Nil, "conditional clear with current seq must remove ixp")
}

// TestIndexReconciliation_NoMarkerWhenDisabled verifies that writes to a shield
// with indexEnabled=false never set the ixp field.
func TestIndexReconciliation_NoMarkerWhenDisabled(t *testing.T) {
	s := newTestShield(t, "test_ixp_disabled")
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()

	const ck = "ixp_disabled_001"
	_, err := s.Write(ctx, ck, []byte(`{"v":1}`), nil)
	require.NoError(t, err)

	band := s.BandFor(ck)
	payloadKey := shield.PayloadKey(s.Namespace(), band, ck)
	_, err = s.Client().HGet(ctx, payloadKey, "ixp").Result()
	assert.ErrorIs(t, err, redis.Nil, "ixp must not be set when indexEnabled is false")
}

// TestIndexReconciliation_NoLeakedMarkersAfterClear writes 100 keys with
// indexEnabled=true, clears the ixp marker on each unconditionally, and
// verifies that none of the payload hashes retain the marker.
func TestIndexReconciliation_NoLeakedMarkersAfterClear(t *testing.T) {
	s := newTestShieldIndexed(t, "test_ixp_no_leak")
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()

	const count = 100
	keys := make([]string, count)
	for i := 0; i < count; i++ {
		ck := fmt.Sprintf("leak_%04d", i)
		keys[i] = ck
		_, err := s.Write(ctx, ck, []byte(fmt.Sprintf(`{"i":%d}`, i)), nil)
		require.NoError(t, err)
		err = s.ClearIndexPending(ctx, ck, "")
		require.NoError(t, err)
	}

	// Verify none have "ixp" field
	for _, ck := range keys {
		band := s.BandFor(ck)
		payloadKey := shield.PayloadKey(s.Namespace(), band, ck)
		_, err := s.Client().HGet(ctx, payloadKey, "ixp").Result()
		assert.ErrorIs(t, err, redis.Nil, "ixp must not remain after unconditional clear for %s", ck)
	}
}

// TestIndexReconciliation_HydrationSetsMarker verifies that HydrateJournal
// sets ixp="0" when indexEnabled is true, since hydration does not assign a
// write sequence (v is not incremented).
func TestIndexReconciliation_HydrationSetsMarker(t *testing.T) {
	s := newTestShieldIndexed(t, "test_ixp_hydrate")
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()

	const ck = "ixp_hydrate_001"
	ts := time.Now().UnixMilli()
	hr, err := s.HydrateJournal(ctx, ck, []byte(`{"src":"store"}`), ts)
	require.NoError(t, err)
	require.True(t, hr.Written)

	band := s.BandFor(ck)
	payloadKey := shield.PayloadKey(s.Namespace(), band, ck)
	ixp, err := s.Client().HGet(ctx, payloadKey, "ixp").Result()
	require.NoError(t, err, "ixp must exist after indexed hydration")
	assert.Equal(t, "0", ixp, "hydration ixp must be '0' (no v assigned)")
}

// TestIndexReconciliation_StaleReindexGuard covers the scenario where a dedup
// hit detects the ixp marker, but a new write lands before the caller can
// re-index. ClearIndexPending with the stale v must not clear the marker set
// by the newer write.
func TestIndexReconciliation_StaleReindexGuard(t *testing.T) {
	s := newTestShieldIndexed(t, "test_ixp_stale_guard")
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()

	const ck = "ixp_stale_001"
	band := s.BandFor(ck)
	payloadKey := shield.PayloadKey(s.Namespace(), band, ck)

	// WriteDedup with hash "h1" -> code 1, ixp=v1
	wr1, err := s.WriteDedup(ctx, ck, []byte(`{"h":"h1"}`), "h1", nil)
	require.NoError(t, err)
	require.Equal(t, shield.WriteCodeWritten, wr1.Status)
	v1 := wr1.Seq

	// Write a new payload (different hash) -> code 1, ixp=v2
	wr2, err := s.WriteDedup(ctx, ck, []byte(`{"h":"h2"}`), "h2", nil)
	require.NoError(t, err)
	require.Equal(t, shield.WriteCodeWritten, wr2.Status)
	v2 := wr2.Seq

	// GetSeq should return v2
	seq, err := s.GetSeq(ctx, ck)
	require.NoError(t, err)
	assert.Equal(t, v2, seq, "current v must be the latest write's sequence")

	// ClearIndexPending(ctx, ck, v1) should NOT clear (v != v1)
	err = s.ClearIndexPending(ctx, ck, v1)
	require.NoError(t, err)

	ixp, err := s.Client().HGet(ctx, payloadKey, "ixp").Result()
	require.NoError(t, err)
	assert.Equal(t, v2, ixp, "stale clear must not remove the newer marker")

	// ClearIndexPending(ctx, ck, v2) should clear
	err = s.ClearIndexPending(ctx, ck, v2)
	require.NoError(t, err)

	_, err = s.Client().HGet(ctx, payloadKey, "ixp").Result()
	assert.ErrorIs(t, err, redis.Nil, "clear with current seq must remove ixp")
}
