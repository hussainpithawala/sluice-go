// Package main demonstrates the BulkRead functionality for the DocumentDB/MongoDB adapter.
//
// Run against local MongoDB + Redis:
//
//	MONGO_URI=mongodb://localhost:27017 REDIS_ADDRS=localhost:6379
//	go run ./examples/bulk_read_documentdb/main.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	sluice "github.com/hussainpithawala/sluice-go"
	"github.com/hussainpithawala/sluice-go/sink/docdb"
	"github.com/hussainpithawala/sluice-go/source"
	sourcedocdb "github.com/hussainpithawala/sluice-go/source/docdb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type CampaignPayload struct {
	CampaignID string `json:"campaign_id" bson:"campaign_id"`
	Name       string `json:"name" bson:"name"`
	Status     string `json:"status" bson:"status"`
	Priority   int    `json:"priority" bson:"priority"`
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mongoURI := getEnv("MONGO_URI", "mongodb://localhost:27017")
	dbName := "bulk_read_demo"
	collName := "campaigns"

	// 1. Setup MongoDB
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Error("failed to connect to mongo", "err", err)
	}
	defer func(client *mongo.Client, ctx context.Context) {
		err := client.Disconnect(ctx)
		if err != nil {
			slog.Error(fmt.Sprintf("An error occurred while disconnecting the client %e", err))
		}
	}(client, ctx)

	coll := client.Database(dbName).Collection(collName)
	_ = coll.Drop(ctx)

	// Seed data
	var docs []interface{}
	for i := 1; i <= 5; i++ {
		docs = append(docs, bson.M{
			"_id":      fmt.Sprintf("camp_%d", i),
			"user_id":  "user_123",
			"name":     fmt.Sprintf("Campaign %d", i),
			"status":   "active",
			"priority": i,
		})
	}
	_, _ = coll.InsertMany(ctx, docs)
	log.Info("seeded 5 campaigns for user_123")

	sk, err := docdb.New(ctx, docdb.Config{
		URI: mongoURI, Database: dbName, Collection: collName,
		MaxPoolSize: 100, MinPoolSize: 10,
	})
	if err != nil {
		slog.Error(fmt.Sprintf("Error while creating the doc-db sink %e", err))
	}

	src := sourcedocdb.NewSourceWithClient(client, dbName, collName)

	// 2. Initialize Sluice
	sl, err := sluice.New("bulk_read_demo").
		WithRedis(sluice.RedisConfig{Addrs: []string{getEnv("REDIS_ADDRS", "localhost:6379")}}).
		WithSink(sk).
		WithSource(src).
		WithReadBulkContract(func(lookupKey string) (*source.BulkReadModel, error) {
			return &source.BulkReadModel{
				Query: sourcedocdb.DocDBBulkReadModel{
					Filter: bson.M{"user_id": lookupKey},
					Projector: func(cursor *mongo.Cursor) ([]source.BulkReadResult, error) {
						var results []source.BulkReadResult
						for cursor.Next(ctx) {
							var doc bson.M
							if err := cursor.Decode(&doc); err != nil {
								return nil, err
							}
							id := doc["_id"].(string)
							payload, _ := json.Marshal(CampaignPayload{
								CampaignID: id,
								Name:       doc["name"].(string),
								Status:     doc["status"].(string),
								Priority:   int(doc["priority"].(int32)),
							})
							results = append(results, source.BulkReadResult{
								CorrelationKey: id,
								Payload:        payload,
							})
						}
						return results, cursor.Err()
					},
				},
			}, nil
		}).
		WithIndexBulkContract(func(results []source.BulkReadResult) (map[string]map[string]interface{}, error) {
			indexes := make(map[string]map[string]interface{})
			for _, res := range results {
				var p CampaignPayload
				if err := json.Unmarshal(res.Payload, &p); err != nil {
					continue
				}
				indexes[res.CorrelationKey] = map[string]interface{}{
					"status":   p.Status,
					"priority": float64(p.Priority),
				}
			}
			return indexes, nil
		}).
		Build(ctx)
	if err != nil {
		log.Error("failed to build sluice", "err", err)
	}
	defer func(sl *sluice.Sluice, ctx context.Context) {
		err := sl.DrainAndClose(ctx)
		if err != nil {
			slog.Error(fmt.Sprintf("An error occurred while sluice drain-and-close %e", err))
		}
	}(sl, ctx)

	// 3. Execute Bulk Read
	log.Info("executing ReadBulk for user_123...")
	start := time.Now()
	payloads, err := sl.ReadBulk(ctx, "user_123")
	if err != nil {
		log.Error("ReadBulk failed", "err", err)
	}
	log.Info("ReadBulk completed", "duration_ms", time.Since(start).Milliseconds(), "items_loaded", len(payloads))

	// 4. Verify L1/L2 Hydration
	log.Info("verifying individual Read() hits the journal...")
	for i := 1; i <= 5; i++ {
		crn := fmt.Sprintf("camp_%d", i)
		t0 := time.Now()
		p, err := sl.Read(ctx, crn)
		if err == nil {
			log.Info("individual read served from journal", "crn", crn, "latency_us", time.Since(t0).Microseconds(), "payload", string(p))
		}
	}

	log.Info("bulk read example completed successfully")
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
