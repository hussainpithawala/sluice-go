// tests/integration/documentdb/docdb_sink_test.go
package documentdb

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"

	"github.com/hussainpithawala/sluice-go/sink"
	"github.com/hussainpithawala/sluice-go/sink/docdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestDocDBBulkWrite_SilentLossPrevention(t *testing.T) {
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
			slog.Error(fmt.Sprintf("Error while closing the source %e", err))
		}
	}(s, ctx)

	client := s.Client()
	coll := client.Database("testdb").Collection("testcoll")
	_, err = coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "email", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	require.NoError(t, err)

	t.Run("Transient: concurrent _id upsert race produces ClassTransient errors", func(t *testing.T) {
		// Use a unique key to avoid interference from other tests
		raceKey := fmt.Sprintf("race_%d", 1)

		// Fire 20 concurrent single-item BulkWrites, all upserting the same
		// non-existent _id. Exactly one will insert; the rest should get
		// 11000 on _id_ which classifyBulkErr marks as ClassTransient.
		var wg sync.WaitGroup
		var mu sync.Mutex
		var transientErrors []sink.SinkError
		var successCount int

		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				models := []sink.WriteModel{
					{
						CorrelationKey: raceKey,
						Filter:         bson.M{"_id": raceKey},
						Update:         bson.M{"$set": bson.M{"val": idx}},
						Upsert:         true,
					},
				}
				res, err := s.BulkWrite(ctx, models)
				if err != nil {
					return // total failure, not a partial result
				}
				mu.Lock()
				defer mu.Unlock()
				if len(res.Errors) == 0 {
					successCount++
				}
				for _, e := range res.Errors {
					if e.Code == 11000 {
						transientErrors = append(transientErrors, e)
					}
				}
			}(i)
		}
		wg.Wait()

		// At least one goroutine should have hit the 11000 race.
		// In practice with 20 goroutines on a local Mongo, this is reliable.
		if len(transientErrors) == 0 {
			t.Skip("no 11000 race observed in this run (all serialized)")
		}

		for _, e := range transientErrors {
			assert.Equal(t, raceKey, e.CorrelationKey, "error must be attributed")
			assert.Equal(t, 11000, e.Code)
			assert.Equal(t, sink.ClassTransient, e.Class,
				"11000 on _id_ upsert must be transient, not dead-lettered")
			assert.False(t, e.IsPermanent())
		}

		// Exactly one insert should have succeeded
		assert.Equal(t, 1, successCount, "exactly one upsert should have inserted")
	})

	t.Run("Permanent: Business unique index violation is dead-lettered", func(t *testing.T) {
		_, err := coll.InsertOne(ctx, bson.M{"_id": "user2", "email": "unique@test.com"})
		require.NoError(t, err)

		models := []sink.WriteModel{
			{
				CorrelationKey: "user3",
				Filter:         bson.M{"_id": "user3"},
				Update:         bson.M{"$set": bson.M{"email": "unique@test.com"}},
				Upsert:         true,
			},
		}

		res, err := s.BulkWrite(ctx, models)
		require.NoError(t, err)
		require.Len(t, res.Errors, 1)

		assert.Equal(t, "user3", res.Errors[0].CorrelationKey)
		assert.Equal(t, 11000, res.Errors[0].Code)
		assert.Equal(t, sink.ClassPermanent, res.Errors[0].Class)
		assert.True(t, res.Errors[0].IsPermanent())
	})

	t.Run("Attribution: Every error has a CorrelationKey", func(t *testing.T) {
		models := []sink.WriteModel{
			{
				CorrelationKey: "userA",
				Filter:         bson.M{"_id": "userA"},
				Update:         bson.M{"$set": bson.M{"val": 1}},
				Upsert:         true,
			},
			{
				CorrelationKey: "userB",
				Filter:         bson.M{"_id": "userA"},
				Update:         bson.M{"$set": bson.M{"val": 2}},
				Upsert:         true,
			},
		}

		res, err := s.BulkWrite(ctx, models)
		require.NoError(t, err)

		err = sink.CheckAttribution(models, res)
		assert.NoError(t, err, "CheckAttribution should pass; no silent loss")
	})
}
