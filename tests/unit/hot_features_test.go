package unit_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	sluice "github.com/hussainpithawala/sluice-go"
	"github.com/hussainpithawala/sluice-go/internal/shield"
	"github.com/hussainpithawala/sluice-go/sink/docdb"
)

// ─── Test Payloads & Contracts ───────────────────────────────────────────────

type hotPayload struct {
	Value    string    `json:"value" bson:"value"`
	Channel  string    `json:"channel" bson:"channel"`
	Priority int       `json:"priority" bson:"priority"`
	Updated  time.Time `json:"updated" bson:"updated"`
}

func hotContract(key string, payload []byte) (*sluice.WriteModel, error) {
	var p hotPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, err
	}
	return &sluice.WriteModel{
		Filter: bson.D{{Key: "_id", Value: key}},
		Update: bson.D{{Key: "$set", Value: bson.D{
			{Key: "value", Value: p.Value},
			{Key: "channel", Value: p.Channel},
			{Key: "priority", Value: p.Priority},
			{Key: "updated", Value: p.Updated},
		}}},
		Upsert: true,
	}, nil
}

func hotReadContract(coll *mongo.Collection) sluice.ReadContract {
	return func(key string) ([]byte, error) {
		ctx := context.Background()
		var p hotPayload
		err := coll.FindOne(ctx, bson.M{"_id": key}).Decode(&p)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				return nil, sluice.ErrRecordNotFound
			}
			return nil, err
		}
		return json.Marshal(p)
	}
}

func hotIndexContract(_ string, payload []byte) (map[string]interface{}, error) {
	var p hotPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, err
	}
	return map[string]interface{}{
		"channel":  p.Channel,                      // Equality SET index.
		"priority": float64(p.Priority),            // Range ZSET index.
		"updated":  float64(p.Updated.UnixMilli()), // Range ZSET index.
	}, nil
}

func mustHotPayload(t *testing.T, value, channel string, priority int) []byte {
	t.Helper()
	b, err := json.Marshal(hotPayload{
		Value: value, Channel: channel, Priority: priority, Updated: time.Now(),
	})
	require.NoError(t, err)
	return b
}

// mongoCollectionForHotTest connects to MongoDB and returns a collection
// scoped to the hot-features unit tests.
func mongoCollectionForHotTest(t *testing.T, name string) *mongo.Collection {
	t.Helper()
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(testMongoURI))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	coll := client.Database(testDatabase).Collection(name)
	_, _ = coll.DeleteMany(context.Background(), bson.M{})
	return coll
}

// buildHotSluice constructs a sluice instance with hot/cold regime,
// indexing, content deduplication, and a short activity window.
func buildHotSluice(t *testing.T, ns string, coll *mongo.Collection) *sluice.Sluice {
	t.Helper()
	ctx := context.Background()
	sk, err := docdb.New(ctx, docdb.Config{
		URI: testMongoURI, Database: testDatabase, Collection: coll.Name(),
		MaxPoolSize: 10, MinPoolSize: 1,
	})
	require.NoError(t, err)

	sl, err := sluice.New(ns).
		WithRedis(sluice.RedisConfig{Addrs: []string{testRedisAddr}}).
		WithSink(sk).
		WithWriteContract(hotContract).
		WithReadContract(hotReadContract(coll)).
		WithIndexContract(hotIndexContract).
		WithContentDedup(true).
		WithActivityWindow(1 * time.Minute).
		WithHotAwareFlush(true).
		WithFlushWindow(50 * time.Millisecond).
		WithMaxBatchSize(100).
		WithBandCount(2).
		WithKeyTTL(10 * time.Second).
		Build(ctx)
	require.NoError(t, err)

	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = sl.DrainAndClose(closeCtx)
	})
	return sl
}

// ─── Tests ───────────────────────────────────────────────────────────────────

func TestHotLoadAndRead_Regime(t *testing.T) {
	const ns = "hot_regime_unit"
	rc := redisClient(t)
	ctx := context.Background()
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	coll := mongoCollectionForHotTest(t, "hot_regime_docs")
	sl := buildHotSluice(t, ns, coll)
	crn := "crn_hot_1"
	payload := mustHotPayload(t, "val1", "push", 5)

	// 1. IsHot should be false initially.
	isHot, err := sl.IsHot(ctx, crn)
	require.NoError(t, err)
	assert.False(t, isHot)

	// 2. Read before HotLoad should fall back to ReadContract (returns ErrRecordNotFound).
	_, err = sl.Read(ctx, crn)
	assert.ErrorIs(t, err, sluice.ErrRecordNotFound)

	// 3. Write and wait for the flush to Mongo.
	require.NoError(t, sl.Write(ctx, crn, payload))
	time.Sleep(200 * time.Millisecond)

	// 4. HotLoad warms up the CRN from Mongo into Redis.
	hotData, err := sl.HotLoad(ctx, crn)
	require.NoError(t, err)
	assert.NotEmpty(t, hotData)

	// 5. IsHot should now be true.
	isHot, err = sl.IsHot(ctx, crn)
	require.NoError(t, err)
	assert.True(t, isHot)

	// 6. Read should hit Redis directly.
	readPayload, err := sl.Read(ctx, crn)
	require.NoError(t, err)
	assert.Equal(t, hotData, readPayload)
}

func TestWriteIdempotent_ExactlyOnce(t *testing.T) {
	const ns = "idem_unit"
	rc := redisClient(t)
	ctx := context.Background()
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	coll := mongoCollectionForHotTest(t, "idem_docs")
	sl := buildHotSluice(t, ns, coll)
	crn := "crn_idem_1"
	payload := mustHotPayload(t, "idem_val", "sms", 1)
	idemKey := "kafka_offset_99"

	// First write succeeds.
	err := sl.WriteIdempotent(ctx, crn, payload, idemKey)
	require.NoError(t, err)

	// Second write with the same idempotency key is rejected.
	err = sl.WriteIdempotent(ctx, crn, payload, idemKey)
	assert.ErrorIs(t, err, sluice.ErrDuplicateIdempotencyKey)

	// A different idempotency key succeeds.
	err = sl.WriteIdempotent(ctx, crn, payload, "kafka_offset_100")
	require.NoError(t, err)
}

func TestContentDedup_SkipsRedundantWrites(t *testing.T) {
	const ns = "dedup_unit"
	rc := redisClient(t)
	ctx := context.Background()
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	coll := mongoCollectionForHotTest(t, "dedup_docs")
	sl := buildHotSluice(t, ns, coll)
	crn := "crn_dedup_1"
	payload := mustHotPayload(t, "dedup_val", "email", 2)

	// First write.
	require.NoError(t, sl.Write(ctx, crn, payload))

	// Wait briefly, then write the exact same payload again.
	// Because ContentDedup is enabled, the xxHash64 fingerprint matches,
	// the Lua script returns 2, and the key is not added to the dirty queue again.
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, sl.Write(ctx, crn, payload))

	// Check the dirty queue depth across all bands. It should be at most 1
	// (or 0 if already flushed). It must not be 2.
	totalDirty := int64(0)
	for band := 0; band < 2; band++ {
		n, _ := rc.ZCard(ctx, shield.DirtyKey(ns, band)).Result()
		totalDirty += n
	}
	assert.LessOrEqual(t, totalDirty, int64(1), "duplicate payload should not increase dirty queue depth")
}
func TestQuery_CompoundIndexes(t *testing.T) {
	const ns = "query_unit"
	rc := redisClient(t)
	ctx := context.Background()
	cleanRedisKeys(t, rc, ns)
	t.Cleanup(func() { cleanRedisKeys(t, rc, ns) })

	coll := mongoCollectionForHotTest(t, "query_docs")
	sl := buildHotSluice(t, ns, coll)

	// Write 3 records with varying channels and priorities.
	// Write() triggers index maintenance via the Lua script directly.
	p1 := mustHotPayload(t, "v1", "push", 1)
	p2 := mustHotPayload(t, "v2", "sms", 5)
	p3 := mustHotPayload(t, "v3", "push", 3)

	require.NoError(t, sl.Write(ctx, "crn_q1", p1))
	require.NoError(t, sl.Write(ctx, "crn_q2", p2))
	require.NoError(t, sl.Write(ctx, "crn_q3", p3))

	// Wait for the flush cycle to process indexes and persist to MongoDB.
	time.Sleep(300 * time.Millisecond)

	// Query for channel=push AND priority >= 2.
	results, err := sl.Query(ctx, sluice.Query{
		Equality: map[string]string{"channel": "push"},
		RangeMin: map[string]float64{"priority": 2},
	})
	require.NoError(t, err)

	// Should find crn_q3 (priority 3) but not crn_q1 (priority 1).
	assert.Len(t, results, 1)
	assert.Equal(t, "crn_q3", results[0].CorrelationKey)
}
