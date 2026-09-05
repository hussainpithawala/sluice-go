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

	Ping(ctx context.Context) error
	Close(ctx context.Context) error
}
