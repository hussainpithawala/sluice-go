package documentdb

import (
	"context"
	"testing"
	"time"

	"github.com/hussainpithawala/sluice-go/internal/shield"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestShieldNoClean is newTestShield without the FLUSHDB, for tests that
// need a second namespace alongside keys already written by the first.
func newTestShieldNoClean(t *testing.T, namespace string) *shield.Shield {
	t.Helper()
	s, err := shield.New(shield.RedisConfig{
		Addrs:       []string{testRedisAddr()},
		DB:          testRedisDB,
		DialTimeout: 5 * time.Second,
		PoolSize:    10,
	}, namespace, 16, 30*time.Second, 4*time.Hour)
	require.NoError(t, err)
	return s
}

// hydrate writes a live payload for ck without marking it dirty.
func hydrate(t *testing.T, s *shield.Shield, ck string) {
	t.Helper()
	_, err := s.HydrateJournal(context.Background(), ck, []byte(`{"v":"`+ck+`"}`), time.Now().UnixMilli())
	require.NoError(t, err)
}

func TestUpdateIndexes_ValueChangeRemovesOldMembership(t *testing.T) {
	s := newTestShield(t, "test_idx_value_change")
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()

	const ck = "idx_change_001"
	band := s.BandFor(ck)
	pushKey := shield.IndexKey(s.Namespace(), band, "channel", "push")
	emailKey := shield.IndexKey(s.Namespace(), band, "channel", "email")

	require.NoError(t, s.UpdateIndexes(ctx, ck, map[string]interface{}{"channel": "push"}, time.Hour))
	require.NoError(t, s.UpdateIndexes(ctx, ck, map[string]interface{}{"channel": "email"}, time.Hour))

	inPush, err := s.Client().SIsMember(ctx, pushKey, ck).Result()
	require.NoError(t, err)
	assert.False(t, inPush, "key must leave the set of its previous value")

	inEmail, err := s.Client().SIsMember(ctx, emailKey, ck).Result()
	require.NoError(t, err)
	assert.True(t, inEmail)

	vals, err := s.Client().HGetAll(ctx, shield.IndexValuesKey(s.Namespace(), band, ck)).Result()
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"channel": "s:email"}, vals)
}

func TestUpdateIndexes_RemovedFieldsAreDropped(t *testing.T) {
	s := newTestShield(t, "test_idx_field_removed")
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()

	const ck = "idx_removed_001"
	band := s.BandFor(ck)

	require.NoError(t, s.UpdateIndexes(ctx, ck, map[string]interface{}{
		"channel": "push", "priority": float64(3),
	}, time.Hour))
	require.NoError(t, s.UpdateIndexes(ctx, ck, map[string]interface{}{}, time.Hour))

	inPush, err := s.Client().SIsMember(ctx, shield.IndexKey(s.Namespace(), band, "channel", "push"), ck).Result()
	require.NoError(t, err)
	assert.False(t, inPush)

	_, err = s.ZScore(ctx, shield.RangeIndexKey(s.Namespace(), band, "priority"), ck)
	assert.Error(t, err, "range membership must be removed with the field")

	n, err := s.Client().Exists(ctx, shield.IndexValuesKey(s.Namespace(), band, ck)).Result()
	require.NoError(t, err)
	assert.Zero(t, n, "idxv hash must be deleted when nothing is indexed")
}

func TestQueryBand_FiltersRangesAndPrunesDeadMembers(t *testing.T) {
	s := newTestShield(t, "test_query_band")
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()

	band, keys := findKeysForTest(t, s, "qb", 3)
	live, lowPriority, dead := keys[0], keys[1], keys[2]

	idx := func(ck string, priority float64) {
		require.NoError(t, s.UpdateIndexes(ctx, ck, map[string]interface{}{
			"channel": "push", "priority": priority,
		}, time.Hour))
	}
	idx(live, 5)
	idx(lowPriority, 1)
	idx(dead, 5)
	hydrate(t, s, live)
	hydrate(t, s, lowPriority)
	// dead has index entries but no payload, as after its journal TTL expired.

	eqKey := shield.IndexKey(s.Namespace(), band, "channel", "push")
	ridxKey := shield.RangeIndexKey(s.Namespace(), band, "priority")

	matches, err := s.QueryBand(ctx, band, []string{eqKey}, map[string]float64{"priority": 3}, nil)
	require.NoError(t, err)
	require.Len(t, matches, 1)
	assert.Equal(t, live, matches[0].CorrelationKey)
	assert.Equal(t, `{"v":"`+live+`"}`, string(matches[0].Payload))

	inSet, err := s.Client().SIsMember(ctx, eqKey, dead).Result()
	require.NoError(t, err)
	assert.False(t, inSet, "dead member must be pruned from the equality set")
	_, err = s.ZScore(ctx, ridxKey, dead)
	assert.Error(t, err, "dead member must be pruned from the range index")

	inSet, err = s.Client().SIsMember(ctx, eqKey, lowPriority).Result()
	require.NoError(t, err)
	assert.True(t, inSet, "live members filtered by range must not be pruned")
}

func TestSweepIndexBand_PrunesDeadMembers(t *testing.T) {
	s := newTestShield(t, "test_index_sweep")
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()

	band, keys := findKeysForTest(t, s, "sweep", 4)
	for _, ck := range keys {
		require.NoError(t, s.UpdateIndexes(ctx, ck, map[string]interface{}{
			"channel": "push", "priority": float64(1),
		}, time.Hour))
	}
	hydrate(t, s, keys[0]) // only keys[0] is live

	scanned, pruned, err := s.SweepIndexBand(ctx, band, 2) // small batch forces multiple pages
	require.NoError(t, err)
	// The first index visited sees all 4 members and prunes the 3 dead ones
	// from every index via idxv, so the second index only has the live one.
	assert.Equal(t, 5, scanned)
	assert.Equal(t, 3, pruned)

	eqKey := shield.IndexKey(s.Namespace(), band, "channel", "push")
	members, err := s.Client().SMembers(ctx, eqKey).Result()
	require.NoError(t, err)
	assert.Equal(t, []string{keys[0]}, members)

	card, err := s.Client().ZCard(ctx, shield.RangeIndexKey(s.Namespace(), band, "priority")).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), card)
}

func TestSweepIndexBand_DropsEmptyIndexesFromRegistry(t *testing.T) {
	s := newTestShield(t, "test_index_sweep_registry")
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()

	const ck = "sweep_reg_001"
	band := s.BandFor(ck)
	require.NoError(t, s.UpdateIndexes(ctx, ck, map[string]interface{}{"channel": "push"}, time.Hour))

	regKey := shield.IndexRegistryKey(s.Namespace(), band)
	eqKey := shield.IndexKey(s.Namespace(), band, "channel", "push")

	// First sweep prunes the only (dead) member, emptying the set.
	_, pruned, err := s.SweepIndexBand(ctx, band, 100)
	require.NoError(t, err)
	assert.Equal(t, 1, pruned)

	// Second sweep finds the index key gone and unregisters it.
	_, _, err = s.SweepIndexBand(ctx, band, 100)
	require.NoError(t, err)
	registered, err := s.Client().SIsMember(ctx, regKey, eqKey).Result()
	require.NoError(t, err)
	assert.False(t, registered)
}

func TestRegisterExistingIndexes_LegacyKeysAreSwept(t *testing.T) {
	s := newTestShield(t, "test_index_legacy")
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()

	band, keys := findKeysForTest(t, s, "legacy", 2)
	eqKey := shield.IndexKey(s.Namespace(), band, "channel", "push")

	// Simulate pre-registry data: raw SADD with no idxv hash or registry entry.
	require.NoError(t, s.Client().SAdd(ctx, eqKey, keys[0], keys[1]).Err())
	hydrate(t, s, keys[0])

	_, pruned, err := s.SweepIndexBand(ctx, band, 100)
	require.NoError(t, err)
	assert.Zero(t, pruned, "unregistered keys are invisible to the sweeper")

	n, err := s.RegisterExistingIndexes(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	_, pruned, err = s.SweepIndexBand(ctx, band, 100)
	require.NoError(t, err)
	assert.Equal(t, 1, pruned)

	members, err := s.Client().SMembers(ctx, eqKey).Result()
	require.NoError(t, err)
	assert.Equal(t, []string{keys[0]}, members)
}

func TestCountHotMarkers(t *testing.T) {
	s := newTestShield(t, "test_count_hot")
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()

	n, err := s.CountHotMarkers(ctx)
	require.NoError(t, err)
	assert.Zero(t, n)

	for _, ck := range []string{"hot_a", "hot_b", "hot_c"} {
		require.NoError(t, s.SetHotMarker(ctx, ck, time.Hour))
	}
	// A different namespace must not be counted.
	other := newTestShieldNoClean(t, "test_count_hot_other")
	t.Cleanup(func() { _ = other.Close() })
	require.NoError(t, other.SetHotMarker(ctx, "hot_x", time.Hour))

	n, err = s.CountHotMarkers(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
}
