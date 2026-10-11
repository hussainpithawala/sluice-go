package docdb

import (
	"context"
	"testing"
	"time"

	"github.com/hussainpithawala/sluice-go/sink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// TestDocDBSink_BulkWrite_ManyModel_UsesUpdateManyModel verifies that a mixed batch
// of Many:true and Many:false models produces the correct mongo.WriteModel types.
// RFP #23 §7: "Submit a mixed batch of Many:true and Many:false models. Verify the
// Many model uses UpdateManyModel and the non-Many uses UpdateOneModel."
func TestDocDBSink_BulkWrite_ManyModel_UsesUpdateManyModel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := testConfig(t)
	s, err := New(ctx, cfg)
	require.NoError(t, err)
	defer func() { _ = s.Close(ctx) }()

	// Clean collection
	_ = s.collection.Drop(ctx)

	// Seed documents for the Many model to match
	docs := []interface{}{
		bson.M{"user_id": "user-1", "campaign_id": "c1", "fulfilled": false},
		bson.M{"user_id": "user-1", "campaign_id": "c2", "fulfilled": false},
		bson.M{"user_id": "user-1", "campaign_id": "c3", "fulfilled": false},
		bson.M{"user_id": "user-2", "campaign_id": "c1", "fulfilled": false},
	}
	_, err = s.collection.InsertMany(ctx, docs)
	require.NoError(t, err)

	models := []sink.WriteModel{
		{
			CorrelationKey: "user-1",
			Filter: bson.M{
				"user_id":     "user-1",
				"campaign_id": bson.M{"$in": []string{"c1", "c2", "c3"}},
			},
			Update: bson.M{"$set": bson.M{"fulfilled": true, "status": "completed"}},
			Upsert: false,
			Many:   true,
		},
		{
			CorrelationKey: "user-2",
			Filter:         bson.M{"user_id": "user-2", "campaign_id": "c1"},
			Update:         bson.M{"$set": bson.M{"fulfilled": true}},
			Upsert:         true,
			Many:           false,
		},
	}

	result, err := s.BulkWrite(ctx, models)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Empty(t, result.Errors, "expected no errors in mixed batch")

	// Verify Many model updated all 3 matching documents
	assert.Equal(t, int64(4), result.ModifiedCount)

	// Verify the single-document model also worked
	var doc bson.M
	err = s.collection.FindOne(ctx, bson.M{"user_id": "user-2", "campaign_id": "c1"}).Decode(&doc)
	require.NoError(t, err)
	assert.Equal(t, true, doc["fulfilled"])
}

// TestDocDBSink_BulkWrite_ManyModel_UpdatesAllMatchingDocuments verifies that
// UpdateMany actually modifies all documents matching the filter.
// RFP #23 §7: "Insert 5 documents matching the filter, call BulkWrite, verify all 5
// are updated. Verify MatchedCount == 5, ModifiedCount == 5."
func TestDocDBSink_BulkWrite_ManyModel_UpdatesAllMatchingDocuments(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := testConfig(t)
	s, err := New(ctx, cfg)
	require.NoError(t, err)
	defer func() { _ = s.Close(ctx) }()

	_ = s.collection.Drop(ctx)

	// Insert 5 documents matching the filter
	var docs []interface{}
	for i := 1; i <= 5; i++ {
		docs = append(docs, bson.M{
			"user_id":   "user-many",
			"item_id":   i,
			"processed": false,
		})
	}
	_, err = s.collection.InsertMany(ctx, docs)
	require.NoError(t, err)

	models := []sink.WriteModel{
		{
			CorrelationKey: "user-many",
			Filter:         bson.M{"user_id": "user-many"},
			Update:         bson.M{"$set": bson.M{"processed": true}},
			Upsert:         false,
			Many:           true,
		},
	}

	result, err := s.BulkWrite(ctx, models)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Empty(t, result.Errors)

	// RFP #23 §7: Verify MatchedCount == 5, ModifiedCount == 5
	assert.Equal(t, int64(5), result.MatchedCount)
	assert.Equal(t, int64(5), result.ModifiedCount)

	// Verify all 5 documents are actually updated
	count, err := s.collection.CountDocuments(ctx, bson.M{"user_id": "user-many", "processed": true})
	require.NoError(t, err)
	assert.Equal(t, int64(5), count)
}

// TestDocDBSink_BulkWrite_ManyModel_ErrorAttribution verifies that when a Many model
// fails, the SinkError.CorrelationKey is correctly set.
// RFP #23 §7: "Submit a Many:true model that fails. Verify the SinkError.CorrelationKey
// is correctly set."
func TestDocDBSink_BulkWrite_ManyModel_ErrorAttribution(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := testConfig(t)
	s, err := New(ctx, cfg)
	require.NoError(t, err)
	defer func() { _ = s.Close(ctx) }()

	_ = s.collection.Drop(ctx)

	// Create a unique index to trigger a duplicate key error on upsert
	_, err = s.collection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "unique_field", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	require.NoError(t, err)

	// Seed a document with the unique field value
	_, err = s.collection.InsertOne(ctx, bson.M{"unique_field": "taken"})
	require.NoError(t, err)

	models := []sink.WriteModel{
		{
			CorrelationKey: "many-error-key",
			Filter:         bson.M{"nonexistent": "filter"},
			Update:         bson.M{"$set": bson.M{"unique_field": "taken"}},
			Upsert:         true,
			Many:           true,
		},
	}

	result, err := s.BulkWrite(ctx, models)
	require.NoError(t, err) // BulkWrite itself doesn't error; errors are in result
	require.NotNil(t, result)
	require.Len(t, result.Errors, 1)

	// Verify error attribution
	assert.Equal(t, "many-error-key", result.Errors[0].CorrelationKey)
	assert.Equal(t, sink.ClassPermanent, result.Errors[0].Class)
	assert.Contains(t, result.Errors[0].Err.Error(), "11000")
}

// TestDocDBSink_Write_Many_CallsUpdateMany verifies the degraded-mode Write method
// branches correctly for Many:true.
// RFP #23 §7: "Call Write with Many:true. Verify it calls UpdateMany, not UpdateOne."
func TestDocDBSink_Write_Many_CallsUpdateMany(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := testConfig(t)
	s, err := New(ctx, cfg)
	require.NoError(t, err)
	defer func() { _ = s.Close(ctx) }()

	_ = s.collection.Drop(ctx)

	// Seed 3 documents
	docs := []interface{}{
		bson.M{"group": "g1", "val": 1},
		bson.M{"group": "g1", "val": 2},
		bson.M{"group": "g1", "val": 3},
	}
	_, err = s.collection.InsertMany(ctx, docs)
	require.NoError(t, err)

	// Degraded Write with Many: true
	model := sink.WriteModel{
		CorrelationKey: "group-g1",
		Filter:         bson.M{"group": "g1"},
		Update:         bson.M{"$set": bson.M{"updated": true}},
		Upsert:         false,
		Many:           true,
	}

	err = s.Write(ctx, model)
	require.NoError(t, err)

	// Verify ALL 3 documents were updated (proves UpdateMany was called)
	count, err := s.collection.CountDocuments(ctx, bson.M{"group": "g1", "updated": true})
	require.NoError(t, err)
	assert.Equal(t, int64(3), count, "UpdateMany should have updated all 3 documents")
}

// TestDocDBSink_Write_ManyFalse_CallsUpdateOne verifies the degraded-mode Write method
// still uses UpdateOne for Many:false (default behavior preserved).
func TestDocDBSink_Write_ManyFalse_CallsUpdateOne(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := testConfig(t)
	s, err := New(ctx, cfg)
	require.NoError(t, err)
	defer func() { _ = s.Close(ctx) }()

	_ = s.collection.Drop(ctx)

	// Seed 3 documents with same filter
	docs := []interface{}{
		bson.M{"group": "g2", "val": 1},
		bson.M{"group": "g2", "val": 2},
		bson.M{"group": "g2", "val": 3},
	}
	_, err = s.collection.InsertMany(ctx, docs)
	require.NoError(t, err)

	model := sink.WriteModel{
		CorrelationKey: "group-g2",
		Filter:         bson.M{"group": "g2"},
		Update:         bson.M{"$set": bson.M{"updated": true}},
		Upsert:         false,
		Many:           false, // default
	}

	err = s.Write(ctx, model)
	require.NoError(t, err)

	// Verify only 1 document was updated (proves UpdateOne was called)
	count, err := s.collection.CountDocuments(ctx, bson.M{"group": "g2", "updated": true})
	require.NoError(t, err)
	assert.Equal(t, int64(1), count, "UpdateOne should have updated only 1 document")
}

// TestDocDBSink_BulkWrite_ManyModel_MatchedCountExceedsModelCount verifies that
// MatchedCount can exceed the number of WriteModels submitted (informational only).
// RFP #23 §3.4: "MatchedCount and ModifiedCount in BulkWriteResult may exceed the
// number of WriteModels submitted (one model, many documents)."
func TestDocDBSink_BulkWrite_ManyModel_MatchedCountExceedsModelCount(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := testConfig(t)
	s, err := New(ctx, cfg)
	require.NoError(t, err)
	defer func() { _ = s.Close(ctx) }()

	_ = s.collection.Drop(ctx)

	// Insert 10 documents
	var docs []interface{}
	for i := 0; i < 10; i++ {
		docs = append(docs, bson.M{"batch": "large", "n": i})
	}
	_, err = s.collection.InsertMany(ctx, docs)
	require.NoError(t, err)

	// Submit 1 model that matches 10 documents
	models := []sink.WriteModel{
		{
			CorrelationKey: "batch-large",
			Filter:         bson.M{"batch": "large"},
			Update:         bson.M{"$set": bson.M{"done": true}},
			Many:           true,
		},
	}

	result, err := s.BulkWrite(ctx, models)
	require.NoError(t, err)
	assert.Empty(t, result.Errors)

	// 1 model submitted, 10 documents matched
	assert.Equal(t, int64(10), result.MatchedCount)
	assert.Equal(t, int64(10), result.ModifiedCount)
}
