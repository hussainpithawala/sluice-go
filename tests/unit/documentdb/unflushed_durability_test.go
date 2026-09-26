package documentdb

import (
	"context"
	"testing"
	"time"

	"github.com/hussainpithawala/sluice-go/internal/shield"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Unflushed payloads exist only in Redis, so they must never carry a TTL.
// These tests pin that invariant at the shield level; PTTL -1 means
// "exists, no expiry".

func payloadPTTL(t *testing.T, s *shield.Shield, ck string) time.Duration {
	t.Helper()
	d, err := s.Client().PTTL(context.Background(), shield.PayloadKey(s.Namespace(), s.BandFor(ck), ck)).Result()
	require.NoError(t, err)
	return d
}

// drainedTS returns the JournalTS DrainBand reports for ck.
func drainedTS(t *testing.T, s *shield.Shield, ck string) int64 {
	t.Helper()
	recs, err := s.DrainBand(context.Background(), s.BandFor(ck), 1000)
	require.NoError(t, err)
	for _, r := range recs {
		if r.CorrelationKey == ck {
			return r.JournalTS
		}
	}
	t.Fatalf("%s not in dirty set", ck)
	return 0
}

// simulateFlush commits ck and applies a post-flush TTL, as the engine does.
func simulateFlush(t *testing.T, s *shield.Shield, ck string, ttl time.Duration) {
	t.Helper()
	ctx := context.Background()
	ts := drainedTS(t, s, ck)
	require.NoError(t, s.CommitKeys(ctx, s.BandFor(ck), []string{ck}))
	require.NoError(t, s.ApplyFlushedTTLs(ctx, []shield.FlushedKey{{CorrelationKey: ck, JournalTS: ts, TTL: ttl}}))
}

func TestWrite_UnflushedPayloadIsPersistent(t *testing.T) {
	s := newTestShield(t, "test_unflushed_persistent")
	t.Cleanup(func() { _ = s.Close() })

	require.NoError(t, s.Write(context.Background(), "persist_001", []byte(`{"v":1}`)))
	assert.Equal(t, time.Duration(-1), payloadPTTL(t, s, "persist_001"))
}

func TestWrite_RewriteOfFlushedKeyClearsTTL(t *testing.T) {
	s := newTestShield(t, "test_rewrite_flushed")
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()
	const ck = "rewrite_001"

	require.NoError(t, s.Write(ctx, ck, []byte(`{"v":1}`)))
	simulateFlush(t, s, ck, 30*time.Second)
	require.Greater(t, payloadPTTL(t, s, ck), time.Duration(0), "flushed payload should have a TTL")

	// HSET alone would keep the 30s TTL on the new, unflushed v2.
	time.Sleep(2 * time.Millisecond)
	require.NoError(t, s.Write(ctx, ck, []byte(`{"v":2}`)))
	assert.Equal(t, time.Duration(-1), payloadPTTL(t, s, ck), "re-written payload must be persistent until flushed")
}

func TestWriteDedup_Persistence(t *testing.T) {
	s := newTestShield(t, "test_dedup_persist") // keyTTL 30s
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()
	const ck = "dedup_001"

	written, err := s.WriteDedup(ctx, ck, []byte(`{"v":1}`), "h1")
	require.NoError(t, err)
	require.True(t, written)
	assert.Equal(t, time.Duration(-1), payloadPTTL(t, s, ck), "new payload must be persistent")

	// Duplicate while unflushed: must not gain a TTL.
	written, err = s.WriteDedup(ctx, ck, []byte(`{"v":1}`), "h1")
	require.NoError(t, err)
	require.False(t, written)
	assert.Equal(t, time.Duration(-1), payloadPTTL(t, s, ck), "duplicate must not add a TTL to an unflushed payload")

	// Duplicate after flush with a short TTL: extended to keyTTL.
	simulateFlush(t, s, ck, 5*time.Second)
	_, err = s.WriteDedup(ctx, ck, []byte(`{"v":1}`), "h1")
	require.NoError(t, err)
	assert.Greater(t, payloadPTTL(t, s, ck), 25*time.Second, "duplicate should extend a flushed TTL")

	// Duplicate after flush with a longer (hot) TTL: never shortened.
	require.NoError(t, s.Client().PExpire(ctx, shield.PayloadKey(s.Namespace(), s.BandFor(ck), ck), time.Hour).Err())
	_, err = s.WriteDedup(ctx, ck, []byte(`{"v":1}`), "h1")
	require.NoError(t, err)
	assert.Greater(t, payloadPTTL(t, s, ck), 50*time.Minute, "duplicate must not shorten a hot TTL")

	// A changed payload after flush is persistent again.
	written, err = s.WriteDedup(ctx, ck, []byte(`{"v":2}`), "h2")
	require.NoError(t, err)
	require.True(t, written)
	assert.Equal(t, time.Duration(-1), payloadPTTL(t, s, ck))
}

func TestApplyFlushedTTLs_GuardsNewerVersion(t *testing.T) {
	s := newTestShield(t, "test_flushed_ttl_guard")
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()
	const ck = "guard_001"

	require.NoError(t, s.Write(ctx, ck, []byte(`{"v":1}`)))
	v1 := drainedTS(t, s, ck)

	// v2 lands while v1's flush is "in flight".
	time.Sleep(2 * time.Millisecond)
	require.NoError(t, s.Write(ctx, ck, []byte(`{"v":2}`)))

	require.NoError(t, s.ApplyFlushedTTLs(ctx, []shield.FlushedKey{{CorrelationKey: ck, JournalTS: v1, TTL: 30 * time.Second}}))
	assert.Equal(t, time.Duration(-1), payloadPTTL(t, s, ck), "v1's TTL must not apply to unflushed v2")

	v2 := drainedTS(t, s, ck)
	require.NoError(t, s.ApplyFlushedTTLs(ctx, []shield.FlushedKey{{CorrelationKey: ck, JournalTS: v2, TTL: 30 * time.Second}}))
	assert.Greater(t, payloadPTTL(t, s, ck), time.Duration(0))
}

func TestApplyFlushedTTLs_SkipsNonPositiveTTL(t *testing.T) {
	s := newTestShield(t, "test_flushed_ttl_zero")
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()
	const ck = "zero_ttl_001"

	require.NoError(t, s.Write(ctx, ck, []byte(`{"v":1}`)))
	ts := drainedTS(t, s, ck)
	require.NoError(t, s.ApplyFlushedTTLs(ctx, []shield.FlushedKey{{CorrelationKey: ck, JournalTS: ts, TTL: 0}}))

	assert.Equal(t, time.Duration(-1), payloadPTTL(t, s, ck), "PEXPIRE 0 would have deleted the key")
}

func TestRefreshHotTTL_OnlyExtendsFlushedPayloads(t *testing.T) {
	s := newTestShield(t, "test_refresh_hot_ttl") // activityWindow 4h
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()

	require.NoError(t, s.Write(ctx, "refresh_unflushed", []byte(`{"v":1}`)))
	require.NoError(t, s.Write(ctx, "refresh_flushed", []byte(`{"v":1}`)))
	simulateFlush(t, s, "refresh_flushed", 5*time.Second)

	for _, ck := range []string{"refresh_unflushed", "refresh_flushed"} {
		require.NoError(t, s.RefreshHotTTL(ctx, s.BandFor(ck), []string{ck}))
	}

	assert.Equal(t, time.Duration(-1), payloadPTTL(t, s, "refresh_unflushed"), "unflushed payload must stay persistent")
	assert.Greater(t, payloadPTTL(t, s, "refresh_flushed"), 3*time.Hour, "flushed payload extended to ActivityWindow")
}

func TestDrainBand_MissingPayloadReturnedNotDropped(t *testing.T) {
	s := newTestShield(t, "test_drain_missing")
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()
	const ck = "missing_001"
	band := s.BandFor(ck)

	// A dirty entry whose payload is gone (e.g. evicted or deleted).
	require.NoError(t, s.Client().ZAdd(ctx, shield.DirtyKey(s.Namespace(), band),
		redis.Z{Score: float64(time.Now().UnixMilli()), Member: ck}).Err())

	recs, err := s.DrainBand(ctx, band, 100)
	require.NoError(t, err)
	require.Len(t, recs, 1)
	assert.Equal(t, ck, recs[0].CorrelationKey)
	assert.Nil(t, recs[0].Payload)

	n, err := s.Client().ZCard(ctx, shield.DirtyKey(s.Namespace(), band)).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), n, "DrainBand must not remove the entry itself")
}
