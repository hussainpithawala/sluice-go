package documentdb

import (
	"context"
	"errors"
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

// drained returns the FlushRecord DrainBand reports for ck.
func drained(t *testing.T, s *shield.Shield, ck string) shield.FlushRecord {
	t.Helper()
	recs, err := s.DrainBand(context.Background(), s.BandFor(ck), 1000)
	require.NoError(t, err)
	for _, r := range recs {
		if r.CorrelationKey == ck {
			return r
		}
	}
	t.Fatalf("%s not in dirty set", ck)
	return shield.FlushRecord{}
}

// commitAs commits ck as if the drained version rec had just been flushed.
// Returns true if committed, false if skipped because ck changed.
func commitAs(t *testing.T, s *shield.Shield, rec shield.FlushRecord, ttl time.Duration) bool {
	t.Helper()
	changed, err := s.CommitFlushed(context.Background(), s.BandFor(rec.CorrelationKey),
		[]shield.FlushedKey{{CorrelationKey: rec.CorrelationKey, Seq: rec.Seq, TTL: ttl}})
	require.NoError(t, err)
	return len(changed) == 0
}

// simulateFlush drains, commits ck and applies a post-flush TTL, as the engine does.
func simulateFlush(t *testing.T, s *shield.Shield, ck string, ttl time.Duration) {
	t.Helper()
	require.True(t, commitAs(t, s, drained(t, s, ck), ttl), "commit of unchanged key must succeed")
}

func isDirtyKey(t *testing.T, s *shield.Shield, ck string) bool {
	t.Helper()
	_, err := s.Client().ZScore(context.Background(), shield.DirtyKey(s.Namespace(), s.BandFor(ck)), ck).Result()
	if errors.Is(err, redis.Nil) {
		return false
	}
	require.NoError(t, err)
	return true
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

// TestCommitFlushed_KeepsWriteThatLandedMidFlush is the commit race: an
// unconditional ZREM would drop v2 from the dirty set, so v2 would never
// reach the datastore.
func TestCommitFlushed_KeepsWriteThatLandedMidFlush(t *testing.T) {
	s := newTestShield(t, "test_commit_race")
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()
	const ck = "race_001"

	require.NoError(t, s.Write(ctx, ck, []byte(`{"v":1}`)))
	v1 := drained(t, s, ck)

	// v2 lands while v1's BulkWrite is "in flight".
	require.NoError(t, s.Write(ctx, ck, []byte(`{"v":2}`)))

	assert.False(t, commitAs(t, s, v1, 30*time.Second), "commit of v1 must be skipped")
	assert.True(t, isDirtyKey(t, s, ck), "v2 must stay dirty so it is flushed next cycle")
	assert.Equal(t, time.Duration(-1), payloadPTTL(t, s, ck), "unflushed v2 must stay persistent")

	v2 := drained(t, s, ck)
	assert.Equal(t, `{"v":2}`, string(v2.Payload))
	assert.True(t, commitAs(t, s, v2, 30*time.Second))
	assert.False(t, isDirtyKey(t, s, ck))
	assert.Greater(t, payloadPTTL(t, s, ck), time.Duration(0))
}

// TestCommitFlushed_SameMillisecondRewrite is why commit compares the write
// sequence rather than the ms timestamp: both writes share one ts, so a ts
// guard would commit (and expire) the unflushed v2.
func TestCommitFlushed_SameMillisecondRewrite(t *testing.T) {
	s := newTestShield(t, "test_commit_same_ms")
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()
	const ck = "same_ms_001"
	payloadKey := shield.PayloadKey(s.Namespace(), s.BandFor(ck), ck)

	require.NoError(t, s.Write(ctx, ck, []byte(`{"v":1}`)))
	v1 := drained(t, s, ck)
	require.NoError(t, s.Write(ctx, ck, []byte(`{"v":2}`)))
	// Force the collision regardless of timing: give v2 v1's timestamp.
	require.NoError(t, s.Client().HSet(ctx, payloadKey, "ts", v1.JournalTS).Err())

	assert.False(t, commitAs(t, s, v1, 30*time.Second), "same-ts rewrite must still be detected")
	assert.True(t, isDirtyKey(t, s, ck))
	assert.Equal(t, time.Duration(-1), payloadPTTL(t, s, ck))
}

func TestCommitFlushed_PreSequencingEntryCommits(t *testing.T) {
	s := newTestShield(t, "test_commit_legacy")
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()
	const ck = "legacy_001"
	band := s.BandFor(ck)

	// An entry written by a version without the 'v' field.
	require.NoError(t, s.Client().HSet(ctx, shield.PayloadKey(s.Namespace(), band, ck), "p", `{"v":1}`, "ts", 1).Err())
	require.NoError(t, s.Client().ZAdd(ctx, shield.DirtyKey(s.Namespace(), band), redis.Z{Score: 1, Member: ck}).Err())

	rec := drained(t, s, ck)
	assert.Empty(t, rec.Seq)
	assert.True(t, commitAs(t, s, rec, 30*time.Second), "entries from before the upgrade must still commit")
	assert.False(t, isDirtyKey(t, s, ck))
}

func TestCommitFlushed_NonPositiveTTLCommitsWithoutExpiry(t *testing.T) {
	s := newTestShield(t, "test_commit_zero_ttl")
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()
	const ck = "zero_ttl_001"

	require.NoError(t, s.Write(ctx, ck, []byte(`{"v":1}`)))
	assert.True(t, commitAs(t, s, drained(t, s, ck), 0))

	assert.False(t, isDirtyKey(t, s, ck))
	assert.Equal(t, time.Duration(-1), payloadPTTL(t, s, ck), "PEXPIRE 0 would have deleted the key")
}

func TestDeadLetterIfUnchanged_SparesRewrittenKey(t *testing.T) {
	s := newTestShield(t, "test_dlq_guard")
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()

	band, keys := findKeysForTest(t, s, "dlq_guard", 2)
	failed, rewritten := keys[0], keys[1]
	for _, ck := range keys {
		require.NoError(t, s.Write(ctx, ck, []byte(`{"v":1}`)))
	}
	recFailed, recRewritten := drained(t, s, failed), drained(t, s, rewritten)
	require.NoError(t, s.Write(ctx, rewritten, []byte(`{"v":2}`))) // fixed payload arrives

	moved, err := s.DeadLetterIfUnchanged(ctx, band, []shield.VersionedKey{
		{CorrelationKey: failed, Seq: recFailed.Seq},
		{CorrelationKey: rewritten, Seq: recRewritten.Seq},
	}, "duplicate_key")
	require.NoError(t, err)
	assert.Equal(t, []string{failed}, moved)

	_, err = s.Client().ZScore(ctx, shield.DLQKey(s.Namespace(), band), failed).Result()
	assert.NoError(t, err, "unchanged failure is dead-lettered")
	assert.False(t, isDirtyKey(t, s, failed))

	assert.True(t, isDirtyKey(t, s, rewritten), "rewritten key must be retried, not dead-lettered")
	_, err = s.Client().ZScore(ctx, shield.DLQKey(s.Namespace(), band), rewritten).Result()
	assert.ErrorIs(t, err, redis.Nil)
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
