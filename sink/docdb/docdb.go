// Package documentdb provides a FlushSink implementation for AWS DocumentDB and MongoDB.
package docdb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hussainpithawala/sluice-go/sink"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// Config holds connection parameters for DocumentDB or MongoDB.
type Config struct {
	URI                    string
	Database               string
	Collection             string
	MaxPoolSize            uint64
	MinPoolSize            uint64
	ConnectTimeout         time.Duration
	ServerSelectionTimeout time.Duration
}

// DefaultConfig returns safe defaults tuned for DocumentDB.
func DefaultConfig(uri, database, collection string) Config {
	return Config{
		URI:                    uri,
		Database:               database,
		Collection:             collection,
		MaxPoolSize:            100,
		MinPoolSize:            10,
		ConnectTimeout:         10 * time.Second,
		ServerSelectionTimeout: 5 * time.Second,
	}
}

// Sink implements sink.FlushSink against AWS DocumentDB or MongoDB.
type Sink struct {
	client     *mongo.Client
	collection *mongo.Collection
}

// New connects to DocumentDB and returns a ready FlushSink.
func New(ctx context.Context, cfg Config) (*Sink, error) {
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = 10 * time.Second
	}
	if cfg.ServerSelectionTimeout == 0 {
		cfg.ServerSelectionTimeout = 5 * time.Second
	}
	opts := options.Client().
		ApplyURI(cfg.URI).
		SetMaxPoolSize(cfg.MaxPoolSize).
		SetMinPoolSize(cfg.MinPoolSize).
		SetConnectTimeout(cfg.ConnectTimeout).
		SetServerSelectionTimeout(cfg.ServerSelectionTimeout)

	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("sluice/documentdb: connect: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx, readpref.Primary()); err != nil {
		_ = client.Disconnect(ctx)
		return nil, fmt.Errorf("sluice/documentdb: initial ping: %w", err)
	}
	return &Sink{
		client:     client,
		collection: client.Database(cfg.Database).Collection(cfg.Collection),
	}, nil
}

// BulkWrite executes all models as a single ordered=false BulkWrite call.
// Partial success is handled: failed records are returned in BulkWriteResult.Errors
// with their Class field populated to drive retry vs. dead-letter decisions.
func (s *Sink) BulkWrite(ctx context.Context, models []sink.WriteModel) (*sink.BulkWriteResult, error) {
	if len(models) == 0 {
		return &sink.BulkWriteResult{}, nil
	}

	mongoModels := make([]mongo.WriteModel, 0, len(models))
	for _, m := range models {
		upsert := m.Upsert
		mongoModels = append(mongoModels, &mongo.UpdateOneModel{
			Filter: m.Filter,
			Update: m.Update,
			Upsert: &upsert,
		})
	}

	res, err := s.collection.BulkWrite(ctx, mongoModels, options.BulkWrite().SetOrdered(false))
	if err != nil {
		var bwe mongo.BulkWriteException
		if errors.As(err, &bwe) {
			// 1. WriteConcernError is a total batch failure (transient).
			// The primary accepted it, but replication failed. Do not return partial results.
			if bwe.WriteConcernError != nil {
				return nil, fmt.Errorf("sluice/documentdb: write concern error (total failure): %w", err)
			}

			// 2. Process individual WriteErrors with strict attribution and classification.
			errs := make([]sink.SinkError, 0, len(bwe.WriteErrors))
			for _, we := range bwe.WriteErrors {
				key := ""
				isUpsert := false
				if we.Index >= 0 && we.Index < len(models) {
					key = models[we.Index].CorrelationKey
					isUpsert = models[we.Index].Upsert
				}

				errs = append(errs, sink.SinkError{
					CorrelationKey: key,
					Code:           we.Code,
					Class:          classifyBulkErr(we, isUpsert),
					Err:            fmt.Errorf("code %d: %s", we.Code, we.Message),
				})
			}

			result := &sink.BulkWriteResult{Errors: errs}
			if res != nil {
				result.InsertedCount = res.InsertedCount
				result.MatchedCount = res.MatchedCount
				result.ModifiedCount = res.ModifiedCount
				result.UpsertedCount = res.UpsertedCount
			}
			// Return partial result without a top-level error.
			// The engine will use CheckAttribution and IsPermanent() to decide the next action.
			return result, nil
		}

		// 3. Other errors (network, context cancel, etc.) are total transient failures.
		return nil, fmt.Errorf("sluice/documentdb: bulkwrite: %w", err)
	}

	return &sink.BulkWriteResult{
		InsertedCount: res.InsertedCount,
		MatchedCount:  res.MatchedCount,
		ModifiedCount: res.ModifiedCount,
		UpsertedCount: res.UpsertedCount,
	}, nil
}

// classifyBulkErr determines the ErrorClass for a MongoDB write error.
func classifyBulkErr(we mongo.BulkWriteError, isUpsert bool) sink.ErrorClass {
	if we.Code == 11000 {
		// 11000 on `_id_` for an upsert is transient (concurrent race with another pod).
		// Any other 11000 (e.g., unique index on a business field) is permanent.
		if isUpsert && strings.Contains(we.Message, "_id_") {
			return sink.ClassTransient
		}
		return sink.ClassPermanent
	}
	// All other write errors (e.g., validation, schema mismatch) are treated as permanent.
	return sink.ClassPermanent
}

// Write performs a single-document upsert. Used in degraded mode only.
func (s *Sink) Write(ctx context.Context, model sink.WriteModel) error {
	upsert := model.Upsert
	_, err := s.collection.UpdateOne(
		ctx, model.Filter, model.Update,
		options.Update().SetUpsert(upsert),
	)
	if err != nil {
		return fmt.Errorf("sluice/documentdb: single write: %w", err)
	}
	return nil
}

func (s *Sink) Ping(ctx context.Context) error  { return s.client.Ping(ctx, readpref.Primary()) }
func (s *Sink) Close(ctx context.Context) error { return s.client.Disconnect(ctx) }

// Client returns the underlying mongo.Client, allowing it to be shared
// with source/documentdb.Source to maintain a single connection pool.
func (s *Sink) Client() *mongo.Client {
	return s.client
}
