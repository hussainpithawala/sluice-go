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
// Filter and Update must be BSON-compatible values (bson.D, bson.M, etc.).
type WriteModel struct {
	Filter interface{}
	Update interface{}
	Upsert bool
}

// WriteContract is the only domain function the caller contributes.
// It converts a correlation key and raw payload into a WriteModel.
// Called once per unique correlation key per flush cycle — never on Write().
// Must be safe for concurrent use.
type WriteContract func(correlationKey string, payload []byte) (*WriteModel, error)

// OnFlushCallback is invoked after every BulkWrite attempt — success or failure.
type OnFlushCallback func(correlationKeys []string, result *BulkWriteResult, err error)

// BulkWriteResult carries the outcome of a sink BulkWrite call.
type BulkWriteResult struct {
	InsertedCount int64
	MatchedCount  int64
	ModifiedCount int64
	UpsertedCount int64
	Errors        []SinkError
}

// SinkError ties a write failure back to its originating correlation key.
type SinkError struct {
	CorrelationKey string
	Err            error
}
