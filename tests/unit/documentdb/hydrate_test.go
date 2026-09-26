package documentdb

import (
	"context"
	"testing"
	"time"

	"github.com/hussainpithawala/sluice-go/internal/shield"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHydrateJournal_FillsMissWithoutDirtying proves read-through hydration
// seeds the journal with an ActivityWindow TTL but never marks the key dirty,
// so store data is not flushed back to the sink.
func TestHydrateJournal_FillsMissWithoutDirtying(t *testing.T) {
	s := newTestShield(t, "test_hydrate_miss")
	defer func() { _ = s.Close() }()
	ctx := context.Background()

	key := "hydrate_miss_key"
	payload := []byte(`{"src":"store"}`)
	ts := time.Now().UnixMilli()

	hr, err := s.HydrateJournal(ctx, key, payload, ts)
	require.NoError(t, err)
	assert.True(t, hr.Written)
	assert.Equal(t, payload, hr.Payload)
	assert.Equal(t, ts, hr.Version)

	jr, err := s.ReadJournal(ctx, key)
	require.NoError(t, err)
	require.True(t, jr.Found)
	assert.Equal(t, payload, jr.Payload)
	assert.Equal(t, ts, jr.Version)
	assert.Greater(t, jr.PTTL, 30*time.Second, "hydrated entries live for ActivityWindow, not KeyTTL")

	depth, err := s.DirtyQueueDepth(ctx, s.BandFor(key))
	require.NoError(t, err)
	assert.Zero(t, depth, "hydration must not mark the key dirty")
}

// TestHydrateJournal_PendingWriteWins proves hydration never overwrites a
// journal entry that is still pending flush, even with a newer hydration ts.
func TestHydrateJournal_PendingWriteWins(t *testing.T) {
	s := newTestShield(t, "test_hydrate_pending")
	defer func() { _ = s.Close() }()
	ctx := context.Background()

	key := "hydrate_pending_key"
	written := []byte(`{"src":"write"}`)
	require.NoError(t, s.Write(ctx, key, written))

	hr, err := s.HydrateJournal(ctx, key, []byte(`{"src":"store"}`), time.Now().Add(time.Hour).UnixMilli())
	require.NoError(t, err)
	assert.False(t, hr.Written)
	assert.Equal(t, written, hr.Payload, "the pending write must be returned")
	assert.Positive(t, hr.Version)

	jr, err := s.ReadJournal(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, written, jr.Payload)

	depth, err := s.DirtyQueueDepth(ctx, s.BandFor(key))
	require.NoError(t, err)
	assert.Equal(t, int64(1), depth, "the pending write must stay dirty")
}

// TestHydrateJournal_FlushedEntryWins covers the flush race: a Source read
// returns pre-flush data, the flush commits (clearing the dirty marker), then
// hydration runs with a hydration ts newer than the write. The journal entry
// must still win — it is at least as new as anything read from the store.
func TestHydrateJournal_FlushedEntryWins(t *testing.T) {
	s := newTestShield(t, "test_hydrate_flushed")
	defer func() { _ = s.Close() }()
	ctx := context.Background()

	key := "hydrate_flushed_key"
	written := []byte(`{"v":2}`)
	require.NoError(t, s.Write(ctx, key, written))
	require.NoError(t, s.CommitKeys(ctx, s.BandFor(key), []string{key}))

	hr, err := s.HydrateJournal(ctx, key, []byte(`{"v":1}`), time.Now().Add(time.Hour).UnixMilli())
	require.NoError(t, err)
	assert.False(t, hr.Written)
	assert.Equal(t, written, hr.Payload)

	jr, err := s.ReadJournal(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, written, jr.Payload, "stale store data must not overwrite the flushed journal entry")
}

// TestBulkHydrateJournal_MixedResults proves bulk hydration fills misses,
// preserves existing entries, and returns results in item order.
func TestBulkHydrateJournal_MixedResults(t *testing.T) {
	s := newTestShield(t, "test_bulk_hydrate")
	defer func() { _ = s.Close() }()
	ctx := context.Background()

	existing := []byte(`{"src":"write"}`)
	require.NoError(t, s.Write(ctx, "bulk_existing", existing))

	ts := time.Now().UnixMilli()
	items := []shield.BulkJournalItem{
		{CorrelationKey: "bulk_existing", Payload: []byte(`{"src":"store"}`)},
		{CorrelationKey: "bulk_new", Payload: []byte(`{"src":"store-new"}`)},
	}
	results, err := s.BulkHydrateJournal(ctx, items, ts)
	require.NoError(t, err)
	require.Len(t, results, 2)

	assert.False(t, results[0].Written)
	assert.Equal(t, existing, results[0].Payload)

	assert.True(t, results[1].Written)
	assert.Equal(t, items[1].Payload, results[1].Payload)
	assert.Equal(t, ts, results[1].Version)

	jr, err := s.ReadJournal(ctx, "bulk_new")
	require.NoError(t, err)
	assert.Equal(t, items[1].Payload, jr.Payload)

	var dirty int64
	for band := 0; band < s.BandCount(); band++ {
		n, err := s.DirtyQueueDepth(ctx, band)
		require.NoError(t, err)
		dirty += n
	}
	assert.Equal(t, int64(1), dirty, "only the original Write() may be dirty")
}
