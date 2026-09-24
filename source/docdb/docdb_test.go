package docdb

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/hussainpithawala/sluice-go/source"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const testMongoURI = "mongodb://localhost:27017"

func setupDocDBSource(t *testing.T) (*Source, func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dbName := "test_sluice_source_" + t.Name()
	collName := "test_collection"

	cfg := Config{
		URI:        testMongoURI,
		Database:   dbName,
		Collection: collName,
	}

	s, err := NewSource(ctx, cfg)
	require.NoError(t, err, "Failed to connect to MongoDB")

	cleanup := func() {
		_ = s.collection.Drop(context.Background())
		_ = s.Close(context.Background())
	}

	return s, cleanup
}

func TestDocDBSource_ReadBulk_Success(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(testMongoURI))
	require.NoError(t, err)
	defer func(client *mongo.Client, ctx context.Context) {
		err := client.Disconnect(ctx)
		if err != nil {
			slog.Error(fmt.Sprintf("Error while disconnecting client %s", err))
		}
	}(client, ctx)

	dbName := "test_sluice_source_bulk_" + t.Name()
	collName := "test_collection"
	coll := client.Database(dbName).Collection(collName)

	// Seed data
	_, err = coll.InsertMany(ctx, []interface{}{
		bson.M{"_id": "item_1", "user_id": "user_123", "name": "Alice"},
		bson.M{"_id": "item_2", "user_id": "user_123", "name": "Bob"},
	})
	require.NoError(t, err)
	defer func(database *mongo.Database, ctx context.Context) {
		err := database.Drop(ctx)
		if err != nil {
			slog.Error(fmt.Sprintf("Error during dropping the database %s %e", database.Name(), err))
		}
	}(client.Database(dbName), ctx)

	s := NewSourceWithClient(client, dbName, collName)

	model := source.BulkReadModel{
		Query: DocDBBulkReadModel{
			Filter:  bson.M{"user_id": "user_123"},
			Options: options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}),
			Projector: func(cursor *mongo.Cursor) ([]source.BulkReadResult, error) {
				var results []source.BulkReadResult
				for cursor.Next(ctx) {
					var doc bson.M
					if err := cursor.Decode(&doc); err != nil {
						return nil, err
					}
					id := doc["_id"].(string)
					name := doc["name"].(string)
					payload, _ := json.Marshal(map[string]any{"name": name})
					results = append(results, source.BulkReadResult{
						CorrelationKey: id,
						Payload:        payload,
					})
				}
				return results, cursor.Err()
			},
		},
	}

	results, err := s.ReadBulk(ctx, model)
	require.NoError(t, err)
	assert.Len(t, results, 2)

	// Verify one of the results
	found := false
	for _, r := range results {
		if r.CorrelationKey == "item_1" {
			var payload map[string]any
			err := json.Unmarshal(r.Payload, &payload)
			require.NoError(t, err)
			assert.Equal(t, "Alice", payload["name"])
			found = true
		}
	}
	assert.True(t, found)
}

func TestSource_Read_Success(t *testing.T) {
	t.Parallel()
	s, cleanup := setupDocDBSource(t)
	defer cleanup()

	// Seed data
	_, err := s.collection.InsertOne(context.Background(), bson.M{"_id": "key1", "name": "test_user", "active": true})
	require.NoError(t, err)

	model := source.ReadModel{Filter: bson.M{"_id": "key1"}}
	data, err := s.Read(context.Background(), model)

	require.NoError(t, err)
	assert.NotNil(t, data)

	// Verify JSON structure
	var result map[string]any
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)
	assert.Equal(t, "test_user", result["name"])
	assert.Equal(t, true, result["active"])
}

func TestSource_Read_NotFound(t *testing.T) {
	t.Parallel()
	s, cleanup := setupDocDBSource(t)
	defer cleanup()

	model := source.ReadModel{Filter: bson.M{"_id": "non_existent_key"}}
	data, err := s.Read(context.Background(), model)

	assert.Nil(t, data)
	assert.ErrorIs(t, err, source.ErrRecordNotFound)
}
