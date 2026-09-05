//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	sluice "github.com/hussainpithawala/sluice-go"
	"github.com/hussainpithawala/sluice-go/sink/docdb"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func redisAddr() string   { return getEnv("REDIS_ADDR", "localhost:6379") }
func mongoURI() string    { return getEnv("MONGO_URI", "mongodb://localhost:27017") }
func sqsEndpoint() string { return getEnv("SQS_ENDPOINT", "http://localhost:4566") }
func kafkaBroker() string { return getEnv("KAFKA_BROKER", "localhost:9092") }

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

type inventoryPayload struct {
	NudgeMasterID string    `json:"nudge_master_id" bson:"nudge_master_id"`
	Channel       string    `json:"channel" bson:"channel"`
	Priority      int       `json:"priority" bson:"priority"`
	CampaignID    string    `json:"campaign_id" bson:"campaign_id"`
	UpdatedAt     time.Time `json:"updated_at" bson:"updated_at"`
}

func inventoryContract(key string, payload []byte) (*sluice.WriteModel, error) {
	var p inventoryPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("invalid payload for key %s: %w", key, err)
	}
	return &sluice.WriteModel{
		Filter: bson.D{{Key: "_id", Value: key}},
		Update: bson.D{{Key: "$set", Value: bson.D{
			{Key: "nudge_master_id", Value: p.NudgeMasterID},
			{Key: "channel", Value: p.Channel},
			{Key: "priority", Value: p.Priority},
			{Key: "campaign_id", Value: p.CampaignID},
			{Key: "updated_at", Value: p.UpdatedAt},
		}}},
		Upsert: true,
	}, nil
}

func makePayload(nudgeMasterID string) []byte {
	b, _ := json.Marshal(inventoryPayload{
		NudgeMasterID: nudgeMasterID, Channel: "push", Priority: 3,
		CampaignID: "camp_integration_test", UpdatedAt: time.Now().UTC(),
	})
	return b
}

func buildIntegrationSluice(t *testing.T, namespace string) (*sluice.Sluice, *docdb.Sink) {
	t.Helper()
	ctx := context.Background()
	sk, err := docdb.New(ctx, docdb.Config{URI: mongoURI(), Database: "sluice_integration", Collection: namespace, MaxPoolSize: 50, MinPoolSize: 5})
	require.NoError(t, err)
	sl, err := sluice.New(namespace).
		WithRedis(sluice.RedisConfig{Addrs: []string{redisAddr()}, PoolSize: 20}).
		WithSink(sk).WithWriteContract(inventoryContract).
		WithFlushWindow(200 * time.Millisecond).WithMaxBatchSize(500).
		WithBandCount(8).WithKeyTTL(30 * time.Second).Build(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = sl.DrainAndClose(shutCtx)
	})
	return sl, sk
}

func mongoCollection(t *testing.T, namespace string) *mongo.Collection {
	t.Helper()
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURI()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	return client.Database("sluice_integration").Collection(namespace)
}

func countDocs(t *testing.T, coll *mongo.Collection, filter interface{}) int64 {
	t.Helper()
	n, err := coll.CountDocuments(context.Background(), filter)
	require.NoError(t, err)
	return n
}

func waitForCount(t *testing.T, coll *mongo.Collection, filter interface{}, expected int64, timeout time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool { return countDocs(t, coll, filter) >= expected },
		timeout, 100*time.Millisecond, "expected %d docs in MongoDB within %s", expected, timeout)
}

// buildHotIntegrationSluice creates a sluice instance configured with Hot/Cold regime,
// indexing, content deduplication, and a short activity window for fast test execution.
func buildHotIntegrationSluice(t *testing.T, namespace string) (*sluice.Sluice, *docdb.Sink) {
	t.Helper()
	ctx := context.Background()
	sk, err := docdb.New(ctx, docdb.Config{
		URI: mongoURI(), Database: "sluice_integration", Collection: namespace,
		MaxPoolSize: 50, MinPoolSize: 5,
	})
	require.NoError(t, err)

	// FIX: Instantiate the collection to pass to the ReadContract closure
	coll := mongoCollection(t, namespace)

	sl, err := sluice.New(namespace).
		WithRedis(sluice.RedisConfig{Addrs: []string{redisAddr()}, PoolSize: 20}).
		WithSink(sk).
		WithWriteContract(inventoryContract).
		WithReadContract(inventoryReadContract(coll)). // Shared connection pool!
		WithIndexContract(inventoryIndexContract).
		WithContentDedup(true).
		WithActivityWindow(1 * time.Minute). // Short TTL for testing
		WithIdempotencyTTL(1 * time.Minute).
		WithHotAwareFlush(true).
		WithFlushWindow(200 * time.Millisecond).
		WithMaxBatchSize(500).
		WithBandCount(4).
		WithKeyTTL(30 * time.Second).
		Build(ctx)
	require.NoError(t, err)

	t.Cleanup(func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = sl.DrainAndClose(shutCtx)
	})
	return sl, sk
}

// inventoryReadContract fetches the current state from MongoDB to warm up the Redis journal.
// It captures the shared *mongo.Collection to avoid opening a new connection per read.
func inventoryReadContract(coll *mongo.Collection) sluice.ReadContract {
	return func(correlationKey string) ([]byte, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		var result inventoryPayload
		err := coll.FindOne(ctx, bson.M{"_id": correlationKey}).Decode(&result)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				return nil, sluice.ErrRecordNotFound
			}
			return nil, fmt.Errorf("read contract: lookup %s: %w", correlationKey, err)
		}
		return json.Marshal(result)
	}
}

// inventoryIndexContract extracts secondary index fields for Redis SET/ZSET indexing.
func inventoryIndexContract(_ string, payload []byte) (map[string]interface{}, error) {
	var p inventoryPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, err
	}
	return map[string]interface{}{
		"channel":    p.Channel,                        // Equality index (SET)
		"priority":   float64(p.Priority),              // Range index (ZSET)
		"updated_at": float64(p.UpdatedAt.UnixMilli()), // Range index (ZSET)
	}, nil
}
