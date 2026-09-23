package docdb

import (
	"context"
	"testing"
	"time"

	"github.com/hussainpithawala/sluice-go/sink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func setupDocDBSink(t *testing.T) (*Sink, func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Use a unique database/collection name per test to allow parallel execution
	dbName := "test_sluice_sink_" + t.Name()
	collName := "test_collection"

	cfg := Config{
		URI:        "mongodb://localhost:27017",
		Database:   dbName,
		Collection: collName,
	}

	s, err := New(ctx, cfg)
	require.NoError(t, err, "Failed to connect to MongoDB")

	cleanup := func() {
		_ = s.collection.Drop(context.Background())
		_ = s.Close(context.Background())
	}

	return s, cleanup
}

func TestSink_BulkWrite_Success(t *testing.T) {
	t.Parallel()
	s, cleanup := setupDocDBSink(t)
	defer cleanup()

	models := []sink.WriteModel{
		{CorrelationKey: "key1", Filter: bson.M{"_id": "key1"}, Update: bson.M{"$set": bson.M{"status": "active"}}, Upsert: true},
		{CorrelationKey: "key2", Filter: bson.M{"_id": "key2"}, Update: bson.M{"$set": bson.M{"status": "active"}}, Upsert: true},
	}

	res, err := s.BulkWrite(context.Background(), models)
	require.NoError(t, err)
	assert.NotNil(t, res)
	assert.Equal(t, int64(2), res.UpsertedCount)
	assert.Empty(t, res.Errors)
}

func TestSink_BulkWrite_PartialFailure_DuplicateKey(t *testing.T) {
	t.Parallel()
	s, cleanup := setupDocDBSink(t)
	defer cleanup()

	// Create a unique index to trigger a duplicate key error
	_, err := s.collection.Indexes().CreateOne(context.Background(), mongo.IndexModel{
		Keys:    bson.D{{Key: "unique_field", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	require.NoError(t, err)

	models := []sink.WriteModel{
		{CorrelationKey: "key1", Filter: bson.M{"_id": "key1"}, Update: bson.M{"$set": bson.M{"unique_field": "A"}}, Upsert: true},
		{CorrelationKey: "key2", Filter: bson.M{"_id": "key2"}, Update: bson.M{"$set": bson.M{"unique_field": "A"}}, Upsert: true}, // Will fail with Code 11000
	}

	res, err := s.BulkWrite(context.Background(), models)
	require.NoError(t, err) // BulkWrite returns partial results, not a hard error
	assert.NotNil(t, res)
	assert.Len(t, res.Errors, 1)
	assert.Equal(t, "key2", res.Errors[0].CorrelationKey)
	assert.Equal(t, 11000, res.Errors[0].Code) // MongoDB duplicate key code
}

func TestSink_Write_DegradedMode(t *testing.T) {
	t.Parallel()
	s, cleanup := setupDocDBSink(t)
	defer cleanup()

	model := sink.WriteModel{
		CorrelationKey: "key1",
		Filter:         bson.M{"_id": "key1"},
		Update:         bson.M{"$set": bson.M{"status": "degraded"}},
		Upsert:         true,
	}

	err := s.Write(context.Background(), model)
	require.NoError(t, err)

	// Verify it was written
	var result bson.M
	err = s.collection.FindOne(context.Background(), bson.M{"_id": "key1"}).Decode(&result)
	require.NoError(t, err)
	assert.Equal(t, "degraded", result["status"])
}

func TestSink_PingAndClose(t *testing.T) {
	t.Parallel()
	s, cleanup := setupDocDBSink(t)
	defer cleanup() // cleanup calls Close

	err := s.Ping(context.Background())
	require.NoError(t, err)
}
