package sink

import (
	"context"
	"errors"
)

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

// BulkWriteResult carries the outcome of a sink BulkWrite call.
type BulkWriteResult struct {
	InsertedCount int64
	MatchedCount  int64
	ModifiedCount int64
	UpsertedCount int64
	Errors        []SinkError
}

// SinkError ties a write failure back to its originating correlation key.
// It represents an error attributed to a specific correlation key.
// Code carries the document-store error code when available (e.g. 11000 for
// a MongoDB/DocumentDB duplicate-key violation). Zero means unknown or
// not applicable.
type SinkError struct {
	CorrelationKey string
	Code           int
	Message        string
	Class          ErrorClass
	Err            error // underlying error
}

// ErrorClass categorizes sink errors to drive retry vs. dead-letter decisions.
type ErrorClass int

const (
	ClassUnknown ErrorClass = iota
	ClassTransient
	ClassPermanent
)

// ErrUnattributedSinkError is returned when a sink fails to attribute an error to a specific key.
var ErrUnattributedSinkError = errors.New("sluice: sink returned an error with an empty or unattributed correlation key")
