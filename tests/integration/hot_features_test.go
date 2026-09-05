//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	sluice "github.com/hussainpithawala/sluice-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
)

func TestHotLoadAndRead_Regime(t *testing.T) {
	const ns = "hot_regime_test"
	coll := mongoCollection(t, ns)
	_, _ = coll.DeleteMany(context.Background(), bson.M{})

	sl, _ := buildHotIntegrationSluice(t, ns)
	ctx := context.Background()
	crn := "crn_hot_1"
	payload := makePayload("nm_hot_1")

	// 1. Cold Read before anything exists
	_, err := sl.Read(ctx, crn)
	assert.ErrorIs(t, err, sluice.ErrRecordNotFound)

	// 2. IsHot should be false initially
	isHot, err := sl.IsHot(ctx, crn)
	require.NoError(t, err)
	assert.False(t, isHot)

	// 3. Write and wait for flush to Mongo
	require.NoError(t, sl.Write(ctx, crn, payload))
	time.Sleep(500 * time.Millisecond)

	// 4. HotLoad warms up the CRN from Mongo into Redis
	hotPayload, err := sl.HotLoad(ctx, crn)
	require.NoError(t, err)

	var decoded inventoryPayload
	require.NoError(t, json.Unmarshal(hotPayload, &decoded))
	assert.Equal(t, "nm_hot_1", decoded.NudgeMasterID)

	// 5. IsHot should now be true
	isHot, err = sl.IsHot(ctx, crn)
	require.NoError(t, err)
	assert.True(t, isHot)

	// 6. Read should hit Redis directly (Hot path)
	readPayload, err := sl.Read(ctx, crn)
	require.NoError(t, err)
	assert.Equal(t, hotPayload, readPayload)
}

func TestWriteIdempotent_ExactlyOnce(t *testing.T) {
	const ns = "idempotency_test"
	coll := mongoCollection(t, ns)
	_, _ = coll.DeleteMany(context.Background(), bson.M{})

	sl, _ := buildHotIntegrationSluice(t, ns)
	ctx := context.Background()
	crn := "crn_idem_1"
	payload, _ := json.Marshal(inventoryPayload{NudgeMasterID: "nm1", Channel: "push", Priority: 1, UpdatedAt: time.Now()})

	// ✅ Make the key unique to this specific test execution
	idemKey := fmt.Sprintf("kafka_offset_199_%s", t.Name())

	// First write succeeds
	err := sl.WriteIdempotent(ctx, crn, payload, idemKey)
	require.NoError(t, err)

	// Second write with same idempotency key is rejected
	err = sl.WriteIdempotent(ctx, crn, payload, idemKey)
	assert.ErrorIs(t, err, sluice.ErrDuplicateIdempotencyKey)

	// Wait for flush
	time.Sleep(1000 * time.Millisecond)

	// Different idempotency key succeeds
	err = sl.WriteIdempotent(ctx, crn, payload, fmt.Sprintf("kafka_offset_100_%s", t.Name()))

	// Only 1 document should exist in Mongo despite 2 calls
	n := countDocs(t, coll, bson.M{"_id": crn})
	assert.Equal(t, int64(1), n)
}

func TestQuery_CompoundIndexes(t *testing.T) {
	const ns = "query_index_test"
	coll := mongoCollection(t, ns)
	_, _ = coll.DeleteMany(context.Background(), bson.M{})

	sl, _ := buildHotIntegrationSluice(t, ns)
	ctx := context.Background()

	// Write 3 records with varying channels and priorities
	p1, _ := json.Marshal(inventoryPayload{NudgeMasterID: "nm1", Channel: "push", Priority: 1, UpdatedAt: time.Now()})
	p2, _ := json.Marshal(inventoryPayload{NudgeMasterID: "nm2", Channel: "sms", Priority: 5, UpdatedAt: time.Now()})
	p3, _ := json.Marshal(inventoryPayload{NudgeMasterID: "nm3", Channel: "push", Priority: 3, UpdatedAt: time.Now()})

	// Write triggers index maintenance via the Lua script directly.
	// No need to call HotLoad before Write (HotLoad requires the record to already exist in MongoDB).
	require.NoError(t, sl.Write(ctx, "crn_q1", p1))
	require.NoError(t, sl.Write(ctx, "crn_q2", p2))
	require.NoError(t, sl.Write(ctx, "crn_q3", p3))

	// Wait for indexing and flush
	time.Sleep(500 * time.Millisecond)

	// Query for channel=push AND priority >= 2
	results, err := sl.Query(ctx, sluice.Query{
		Equality: map[string]string{"channel": "push"},
		RangeMin: map[string]float64{"priority": 2},
	})
	require.NoError(t, err)

	// Should find crn_q3 (priority 3) but NOT crn_q1 (priority 1)
	assert.Len(t, results, 1)
	assert.Equal(t, "crn_q3", results[0].CorrelationKey)
}

func TestContentDedup_SkipsRedundantWrites(t *testing.T) {
	const ns = "content_dedup_test"
	coll := mongoCollection(t, ns)
	_, _ = coll.DeleteMany(context.Background(), bson.M{})

	sl, _ := buildHotIntegrationSluice(t, ns)
	ctx := context.Background()
	crn := "crn_dedup_1"
	payload := makePayload("nm_dedup_1")

	// First write
	require.NoError(t, sl.Write(ctx, crn, payload))
	time.Sleep(500 * time.Millisecond) // Wait for flush to Mongo

	// Second write with EXACT same payload.
	// Because ContentDedup is enabled, xxHash64 matches, Lua script returns 2,
	// and the key is NOT added to the dirty queue again.
	require.NoError(t, sl.Write(ctx, crn, payload))
	time.Sleep(500 * time.Millisecond)

	// Mongo should still only have 1 document (no duplicate upsert triggered)
	n := countDocs(t, coll, bson.M{"_id": crn})
	assert.Equal(t, int64(1), n)
}
