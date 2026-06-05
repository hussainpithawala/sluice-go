package sluice

import "time"

// FlushRecord is the internal unit that travels through the pipeline.
// Payload is opaque bytes — the library never interprets it.
type FlushRecord struct {
	CorrelationKey string
	Payload        []byte
	ReceivedAt     time.Time
}

// WriteModel is the fully resolved upsert instruction returned by a WriteContract.
type WriteModel struct {
	Filter interface{}
	Update interface{}
	Upsert bool
}

// WriteContract is the only domain function the caller contributes.
// Called once per unique correlation key per flush cycle — never on Write().
type WriteContract func(correlationKey string, payload []byte) (*WriteModel, error)

// OnFlushCallback is invoked after every BulkWrite attempt — success or failure.
type OnFlushCallback func(correlationKeys []string, result *BulkWriteResult, err error)

// BulkWriteResult carries the outcome of a sink BulkWrite call.
type BulkWriteResult struct {
	InsertedCount int64
	MatchedCount  int64
	ModifiedCount int64
	UpsertedCount int64
	// Errors lists per-record failures. Empty on full success.
	// Records in Errors whose Code == ErrCodeDuplicateKey are routed
	// to the dead-letter set and will not be retried.
	Errors []SinkError
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

// ErrCodeDuplicateKey is the MongoDB / AWS DocumentDB error code for a
// unique-index violation. sluice uses this to distinguish permanent failures
// (route to dead-letter) from transient ones (leave in dirty set for retry).
const ErrCodeDuplicateKey = 11000

// DLQStrategy selects how dead-letter records are processed.
type DLQStrategy int

const (
	// DLQIgnore logs each dead-letter record and discards it.
	DLQIgnore DLQStrategy = iota
	// DLQUpsert re-runs the WriteContract with Upsert=true and BulkWrites.
	DLQUpsert
	// DLQReInsert mutates the key and pushes the record back to the dirty queue.
	DLQReInsert
)

// DLQResult carries the outcome of a ProcessDLQ invocation.
type DLQResult struct {
	Processed int // total records handled across all bands
	Succeeded int // successfully committed/re-queued
	Failed    int // records that failed during DLQ processing
}
