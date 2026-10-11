// tests/integration/documentdb/docdb_sink_update_many_test.go
package documentdb

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/hussainpithawala/sluice-go/sink"
	"github.com/hussainpithawala/sluice-go/sink/docdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestDocDBBulkWrite_UpdateMany(t *testing.T) {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "mongo:6.0",
		ExposedPorts: []string{"27017/tcp"},
		WaitingFor:   wait.ForLog("Waiting for connections"),
	}
	mongoC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)
	defer func(mongoC testcontainers.Container, ctx context.Context, opts ...testcontainers.TerminateOption) {
		err := mongoC.Terminate(ctx, opts...)
		if err != nil {
			slog.Error(fmt.Sprintf("Error while terminating the MongoContainer %e", err))
		}
	}(mongoC, ctx)

	host, _ := mongoC.Host(ctx)
	port, _ := mongoC.MappedPort(ctx, "27017")
	uri := "mongodb://" + host + ":" + port.Port()

	s, err := docdb.New(ctx, docdb.DefaultConfig(uri, "testdb", "testcoll"))
	require.NoError(t, err)
	defer func(s *docdb.Sink, ctx context.Context) {
		err := s.Close(ctx)
		if err != nil {
			slog.Error(fmt.Sprintf("Error while closing the sink %e", err))
		}
	}(s, ctx)

	client := s.Client()
	coll := client.Database("testdb").Collection("testcoll")

	t.Run("Many: single model updates all 5 matching documents", func(t *testing.T) {
		// Drop and reseed to isolate this subtest
		_, err := coll.DeleteMany(ctx, bson.M{})
		require.NoError(t, err)

		// Insert 5 documents for user-1 that match the filter
		var docs []interface{}
		for i := 1; i <= 5; i++ {
			docs = append(docs, bson.M{
				"_id":       fmt.Sprintf("user1-camp-%d", i),
				"user_id":   "user-1",
				"campaign":  fmt.Sprintf("camp-%d", i),
				"fulfilled": false,
				"status":    "pending",
			})
		}
		// Also add a document for a different user (should NOT be touched)
		docs = append(docs, bson.M{
			"_id":       "user2-camp-99",
			"user_id":   "user-2",
			"campaign":  "camp-99",
			"fulfilled": false,
			"status":    "pending",
		})
		_, err = coll.InsertMany(ctx, docs)
		require.NoError(t, err)

		models := []sink.WriteModel{
			{
				CorrelationKey: "user-1",
				Filter:         bson.M{"user_id": "user-1", "fulfilled": false},
				Update:         bson.M{"$set": bson.M{"fulfilled": true, "status": "completed"}},
				Upsert:         false,
				Many:           true,
			},
		}

		result, err := s.BulkWrite(ctx, models)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Empty(t, result.Errors)

		// RFP #23 §7: Verify MatchedCount == 5, ModifiedCount == 5
		assert.Equal(t, int64(5), result.MatchedCount, "all 5 user-1 docs should match")
		assert.Equal(t, int64(5), result.ModifiedCount, "all 5 user-1 docs should be modified")

		// Verify all 5 user-1 documents are actually updated
		count, err := coll.CountDocuments(ctx, bson.M{
			"user_id":   "user-1",
			"fulfilled": true,
			"status":    "completed",
		})
		require.NoError(t, err)
		assert.Equal(t, int64(5), count)

		// Verify user-2 document was NOT touched
		count, err = coll.CountDocuments(ctx, bson.M{
			"user_id":   "user-2",
			"fulfilled": false,
		})
		require.NoError(t, err)
		assert.Equal(t, int64(1), count)
	})

	t.Run("Mixed batch: Many:true uses UpdateManyModel, Many:false uses UpdateOneModel", func(t *testing.T) {
		_, err := coll.DeleteMany(ctx, bson.M{})
		require.NoError(t, err)

		// Seed: 3 docs for user-A (Many target), 1 doc for user-B (single target)
		docs := []interface{}{
			bson.M{"_id": "a-1", "user_id": "user-A", "done": false},
			bson.M{"_id": "a-2", "user_id": "user-A", "done": false},
			bson.M{"_id": "a-3", "user_id": "user-A", "done": false},
			bson.M{"_id": "b-1", "user_id": "user-B", "done": false},
		}
		_, err = coll.InsertMany(ctx, docs)
		require.NoError(t, err)

		models := []sink.WriteModel{
			{
				CorrelationKey: "user-A",
				Filter:         bson.M{"user_id": "user-A"},
				Update:         bson.M{"$set": bson.M{"done": true}},
				Upsert:         false,
				Many:           true, // UpdateManyModel
			},
			{
				CorrelationKey: "user-B",
				Filter:         bson.M{"_id": "b-1"},
				Update:         bson.M{"$set": bson.M{"done": true}},
				Upsert:         true,
				Many:           false, // UpdateOneModel
			},
		}

		result, err := s.BulkWrite(ctx, models)
		require.NoError(t, err)
		assert.Empty(t, result.Errors)

		// Many model modified 3 docs; single model modified 1 doc → total 4
		assert.Equal(t, int64(4), result.ModifiedCount)

		// Verify user-A docs all updated
		count, err := coll.CountDocuments(ctx, bson.M{"user_id": "user-A", "done": true})
		require.NoError(t, err)
		assert.Equal(t, int64(3), count)

		// Verify user-B doc updated
		count, err = coll.CountDocuments(ctx, bson.M{"user_id": "user-B", "done": true})
		require.NoError(t, err)
		assert.Equal(t, int64(1), count)
	})

	t.Run("Degraded Write with Many:true calls UpdateMany", func(t *testing.T) {
		_, err := coll.DeleteMany(ctx, bson.M{})
		require.NoError(t, err)

		// Seed 4 documents
		var docs []interface{}
		for i := 1; i <= 4; i++ {
			docs = append(docs, bson.M{
				"_id":       fmt.Sprintf("degraded-%d", i),
				"group":     "degraded",
				"seq":       i,
				"processed": false,
			})
		}
		_, err = coll.InsertMany(ctx, docs)
		require.NoError(t, err)

		model := sink.WriteModel{
			CorrelationKey: "group-degraded",
			Filter:         bson.M{"group": "degraded"},
			Update:         bson.M{"$set": bson.M{"processed": true}},
			Upsert:         false,
			Many:           true,
		}

		err = s.Write(ctx, model)
		require.NoError(t, err)

		// All 4 documents must be updated (proves UpdateMany was used, not UpdateOne)
		count, err := coll.CountDocuments(ctx, bson.M{"group": "degraded", "processed": true})
		require.NoError(t, err)
		assert.Equal(t, int64(4), count, "degraded Write with Many:true must update all matching docs")
	})

	t.Run("Error attribution: failing Many model has correct CorrelationKey", func(t *testing.T) {
		_, err := coll.DeleteMany(ctx, bson.M{})
		require.NoError(t, err)

		// Create a unique index to force a duplicate key error
		_, err = coll.Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys:    bson.D{{Key: "unique_tag", Value: 1}},
			Options: options.Index().SetUnique(true),
		})
		require.NoError(t, err)
		defer func() {
			_ = coll.Indexes().DropOne(ctx, "unique_tag_1")
		}()

		// Seed a document occupying the unique value
		_, err = coll.InsertOne(ctx, bson.M{"_id": "existing", "unique_tag": "occupied"})
		require.NoError(t, err)

		models := []sink.WriteModel{
			{
				CorrelationKey: "many-fail-key",
				Filter:         bson.M{"_id": "many-fail-key"},
				Update:         bson.M{"$set": bson.M{"unique_tag": "occupied"}}, // conflicts
				Upsert:         true,
				Many:           true,
			},
		}

		result, err := s.BulkWrite(ctx, models)
		require.NoError(t, err) // BulkWrite itself succeeds; errors are per-model
		require.NotNil(t, result)
		require.Len(t, result.Errors, 1)

		assert.Equal(t, "many-fail-key", result.Errors[0].CorrelationKey)
		assert.Equal(t, 11000, result.Errors[0].Code)
		// A unique index violation on a non-_id index is permanent
		assert.Equal(t, sink.ClassPermanent, result.Errors[0].Class)
		assert.True(t, result.Errors[0].IsPermanent())
	})

	t.Run("MatchedCount exceeds model count: 1 model → 10 documents", func(t *testing.T) {
		_, err := coll.DeleteMany(ctx, bson.M{})
		require.NoError(t, err)

		// Insert 10 documents
		var docs []interface{}
		for i := 0; i < 10; i++ {
			docs = append(docs, bson.M{
				"_id":   fmt.Sprintf("overflow-%d", i),
				"batch": "overflow",
				"n":     i,
				"done":  false,
			})
		}
		_, err = coll.InsertMany(ctx, docs)
		require.NoError(t, err)

		// 1 model → 10 documents
		models := []sink.WriteModel{
			{
				CorrelationKey: "batch-overflow",
				Filter:         bson.M{"batch": "overflow"},
				Update:         bson.M{"$set": bson.M{"done": true}},
				Upsert:         false,
				Many:           true,
			},
		}

		result, err := s.BulkWrite(ctx, models)
		require.NoError(t, err)
		assert.Empty(t, result.Errors)
		assert.Equal(t, int64(10), result.MatchedCount, "1 model matched 10 docs")
		assert.Equal(t, int64(10), result.ModifiedCount, "1 model modified 10 docs")
	})

	t.Run("Idempotent reapplication: applying Many write twice is safe", func(t *testing.T) {
		_, err := coll.DeleteMany(ctx, bson.M{})
		require.NoError(t, err)

		docs := []interface{}{
			bson.M{"_id": "idem-1", "user": "idem", "val": 1, "done": false},
			bson.M{"_id": "idem-2", "user": "idem", "val": 2, "done": false},
		}
		_, err = coll.InsertMany(ctx, docs)
		require.NoError(t, err)

		model := sink.WriteModel{
			CorrelationKey: "idem",
			Filter:         bson.M{"user": "idem"},
			Update:         bson.M{"$set": bson.M{"done": true}},
			Upsert:         false,
			Many:           true,
		}

		// First application
		result1, err := s.BulkWrite(ctx, []sink.WriteModel{model})
		require.NoError(t, err)
		assert.Empty(t, result1.Errors)
		assert.Equal(t, int64(2), result1.ModifiedCount)

		// Second application: idempotent, no error
		result2, err := s.BulkWrite(ctx, []sink.WriteModel{model})
		require.NoError(t, err)
		assert.Empty(t, result2.Errors, "re-applying a Many write must not error")

		// Final state is still correct
		count, err := coll.CountDocuments(ctx, bson.M{"user": "idem", "done": true})
		require.NoError(t, err)
		assert.Equal(t, int64(2), count)
	})

	t.Run("Attribution: CheckAttribution passes for Many writes", func(t *testing.T) {
		_, err := coll.DeleteMany(ctx, bson.M{})
		require.NoError(t, err)

		// Seed documents
		var docs []interface{}
		for i := 1; i <= 3; i++ {
			docs = append(docs, bson.M{
				"_id":   fmt.Sprintf("attr-%d", i),
				"owner": "attr-user",
				"val":   i,
			})
		}
		_, err = coll.InsertMany(ctx, docs)
		require.NoError(t, err)

		models := []sink.WriteModel{
			{
				CorrelationKey: "attr-user",
				Filter:         bson.M{"owner": "attr-user"},
				Update:         bson.M{"$set": bson.M{"updated": true}},
				Upsert:         false,
				Many:           true,
			},
		}

		res, err := s.BulkWrite(ctx, models)
		require.NoError(t, err)

		err = sink.CheckAttribution(models, res)
		assert.NoError(t, err, "CheckAttribution should pass; no silent loss for Many writes")
	})
}
