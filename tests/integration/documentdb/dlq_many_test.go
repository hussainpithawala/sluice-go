//go:build integration

package documentdb_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	sluice "github.com/hussainpithawala/sluice-go"
	"github.com/hussainpithawala/sluice-go/sink/docdb"
)

// TestProcessDLQ_Upsert_PreservesMany drives a Many:true contract through the
// real ProcessDLQ path and asserts every matching document is updated, not
// just the first one (UpdateMany, not UpdateOne).
func TestProcessDLQ_Upsert_PreservesMany(t *testing.T) {
	const ns = "dlq_upsert_many_test"
	rc := dlqRedisClient(t)
	ctx := context.Background()
	cleanRedisKeys(t, rc, ns)

	coll := mongoCollection(t, ns)
	_, _ = coll.DeleteMany(ctx, bson.M{})
	t.Cleanup(func() { _, _ = coll.DeleteMany(context.Background(), bson.M{}) })

	// Seed the DLQ: the first sluice's contract rejects every bad_ key.
	sl := buildDLQSluice(t, ns)
	badKeys := seedDLQ(t, sl, rc, ns, 2)
	require.NoError(t, sl.DrainAndClose(ctx))

	// Three campaign documents per dead-lettered user.
	const perUser = 3
	docs := make([]any, 0, len(badKeys)*perUser)
	for _, k := range badKeys {
		for i := 0; i < perUser; i++ {
			docs = append(docs, bson.M{"_id": fmt.Sprintf("%s-camp-%d", k, i), "user_id": k, "fulfilled": false})
		}
	}
	_, err := coll.InsertMany(ctx, docs)
	require.NoError(t, err)

	sk, err := docdb.New(ctx, docdb.Config{
		URI: mongoURI(), Database: "sluice_integration", Collection: ns,
		MaxPoolSize: 10, MinPoolSize: 1,
	})
	require.NoError(t, err)

	manyContract := func(key string, _ []byte) (*sluice.WriteModel, error) {
		return &sluice.WriteModel{
			Filter: bson.M{"user_id": key},
			Update: bson.M{"$set": bson.M{"fulfilled": true}},
			Many:   true,
		}, nil
	}

	sl2, err := sluice.New(ns).
		WithRedis(sluice.RedisConfig{Addrs: []string{redisAddr()}, PoolSize: 20}).
		WithSink(sk).
		WithWriteContract(manyContract).
		WithFlushWindow(100 * time.Millisecond).
		WithBandCount(1).
		WithKeyTTL(30 * time.Second).
		Build(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = sl2.DrainAndClose(shutCtx)
	})

	result, err := sl2.ProcessDLQ(ctx, sluice.DLQUpsert)
	require.NoError(t, err)
	assert.Equal(t, len(badKeys), result.Succeeded)

	for _, k := range badKeys {
		n := countDocs(t, coll, bson.M{"user_id": k, "fulfilled": true})
		assert.Equal(t, int64(perUser), n, "all documents for %s should be updated by DLQ replay", k)
	}
}
