// Package sink defines the FlushSink interface that all document store
// backends must implement to integrate with sluice.
package sink

import (
	"context"
	"errors"
)

// BulkWriteResult carries the outcome of a sink BulkWrite call.
type BulkWriteResult struct {
	InsertedCount int64
	MatchedCount  int64
	ModifiedCount int64
	UpsertedCount int64
	Errors        []SinkError
}

// SinkError ties a write failure back to its originating correlation key.
// Code carries the document-store error code when available (e.g. 11000 for
// a MongoDB/DocumentDB duplicate-key violation). Zero means unknown or
// not applicable.
type SinkError struct {
	CorrelationKey string
	Code           int
	Err            error
}

// ErrContractViolation indicates the write contract returned an error.
var ErrContractViolation = errors.New("sink: write contract returned error")

// FlushSink is the write-side persistence contract for sluice.
type FlushSink interface {
	BulkWrite(ctx context.Context, models []WriteModel) (*BulkWriteResult, error)
	Write(ctx context.Context, model WriteModel) error
	Ping(ctx context.Context) error
	Close(ctx context.Context) error
}

// WriteModel is the resolved upsert instruction handed to the FlushSink.
type WriteModel struct {
	CorrelationKey string
	Filter         interface{}
	Update         interface{}
	Upsert         bool
}
