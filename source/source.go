// Package source defines the Source interface that all document store
// backends must implement to serve read paths (HotLoad/Read) for sluice.
package source

import (
	"context"
	"errors"
)

// ErrRecordNotFound indicates the requested document does not exist in the source.
var ErrRecordNotFound = errors.New("source: record not found")

// ReadModel is the resolved lookup instruction handed to the Source.
// It mirrors sink.WriteModel but for read operations.
type ReadModel struct {
	Filter interface{}
}

// Source is the read-side persistence contract for sluice.
// Implementations translate the generic ReadModel into datastore-specific
// queries (e.g., MongoDB FindOne, DynamoDB GetItem).
type Source interface {
	// Read fetches the raw JSON/document bytes for the given ReadModel.
	// Returns ErrRecordNotFound if the document does not exist.
	Read(ctx context.Context, model ReadModel) ([]byte, error)
	// ReadBulk executes a set-based query and returns multiple hydrated results.
	ReadBulk(ctx context.Context, model BulkReadModel) ([]BulkReadResult, error)

	Ping(ctx context.Context) error
	Close(ctx context.Context) error
}

// =============================================================================
// BULK READ FOUNDATION (Phase 1)
// =============================================================================

// BulkReadResult represents a single hydrated row/document from a bulk read operation.
type BulkReadResult struct {
	// CorrelationKey is the unique key for this specific item (e.g., "campaign_123").
	CorrelationKey string
	// Payload is the canonical JSON payload that will be stored in the Redis journal.
	Payload []byte
}

// BulkReadModel defines the execution plan for a bulk read.
type BulkReadModel struct {
	// Query is the datastore-specific statement.
	// For SQL: "SELECT campaign_id, json_build_object(...) FROM campaigns WHERE user_id = $1"
	// For NoSQL: A specific find query with projection.
	Query interface{}

	// Args are the parameters to bind to the query (e.g., []any{userID}).
	Args []any

	// Projector iterates over the returned rows and shapes them into BulkReadResults.
	// The exact type of the scanner depends on the adapter (e.g., pgx.Rows, mongo.Cursor).
	// We use 'any' here to keep the core source package datastore-agnostic.
	Projector func(scanner any) ([]BulkReadResult, error)
}
