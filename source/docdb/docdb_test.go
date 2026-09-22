package docdb

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/hussainpithawala/sluice-go/source"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
)

func setupDocDBSource(t *testing.T) (*Source, func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dbName := "test_sluice_source_" + t.Name()
	collName := "test_collection"

	cfg := Config{
		URI:        "mongodb://localhost:27017",
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
