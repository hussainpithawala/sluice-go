package docdb

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/hussainpithawala/sluice-go/source"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// Config holds connection parameters for the DocumentDB / MongoDB read source.
type Config struct {
	URI                    string
	Database               string
	Collection             string
	MaxPoolSize            uint64
	MinPoolSize            uint64
	ConnectTimeout         time.Duration
	ServerSelectionTimeout time.Duration
}

// Source implements source.Source for MongoDB / DocumentDB.
type Source struct {
	client     *mongo.Client
	collection *mongo.Collection
}

// NewSource connects to DocumentDB/MongoDB and returns a ready Source.
func NewSource(ctx context.Context, cfg Config) (*Source, error) {
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
		return nil, fmt.Errorf("sluice/source/documentdb: connect: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx, readpref.Primary()); err != nil {
		_ = client.Disconnect(ctx)
		return nil, fmt.Errorf("sluice/source/documentdb: initial ping: %w", err)
	}
	return &Source{
		client:     client,
		collection: client.Database(cfg.Database).Collection(cfg.Collection),
	}, nil
}

// NewSourceWithClient creates a Source using an existing mongo.Client.
// Use this to share a connection pool with sink/documentdb.Sink.
func NewSourceWithClient(client *mongo.Client, database, collection string) *Source {
	return &Source{
		client:     client,
		collection: client.Database(database).Collection(collection),
	}
}

// Read executes a FindOne query based on the ReadModel's Filter.
func (s *Source) Read(ctx context.Context, model source.ReadModel) ([]byte, error) {
	var result bson.M
	err := s.collection.FindOne(ctx, model.Filter).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, source.ErrRecordNotFound
		}
		return nil, fmt.Errorf("sluice/source/documentdb: read: %w", err)
	}

	// Marshal the generic bson.M back to JSON bytes for the Redis journal.
	b, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("sluice/source/documentdb: marshal read result: %w", err)
	}
	return b, nil
}

// ReadBulk executes the Find query defined in DocDBBulkReadModel and delegates
// the result shaping entirely to the operator's Projector function.
func (s *Source) ReadBulk(ctx context.Context, model source.BulkReadModel) ([]source.BulkReadResult, error) {
	mongoModel, ok := model.Query.(DocDBBulkReadModel)
	if !ok {
		return nil, fmt.Errorf("documentdb source requires Query to be documentdb.DocDBBulkReadModel")
	}

	if mongoModel.Filter == nil {
		return nil, fmt.Errorf("documentdb source requires a non-nil Filter")
	}

	if mongoModel.Projector == nil {
		return nil, fmt.Errorf("documentdb source requires a non-nil Projector function")
	}

	// Execute the Find query using the collection
	cursor, err := s.collection.Find(ctx, mongoModel.Filter, mongoModel.Options)
	if err != nil {
		return nil, fmt.Errorf("documentdb: bulk read find execution failed: %w", err)
	}
	// Safety net to prevent cursor/connection leaks
	defer func(cursor *mongo.Cursor, ctx context.Context) {
		err := cursor.Close(ctx)
		if err != nil {
			slog.Debug("error while closing the documentdb source cursor", "err", err)
		}
	}(cursor, ctx)

	return mongoModel.Projector(cursor)
}

func (s *Source) Ping(ctx context.Context) error  { return s.client.Ping(ctx, readpref.Primary()) }
func (s *Source) Close(ctx context.Context) error { return s.client.Disconnect(ctx) }
