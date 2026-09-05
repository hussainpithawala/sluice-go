package sluice

import (
	"time"

	"github.com/hussainpithawala/sluice-go/source"
)

// ─── Domain Contracts ────────────────────────────────────────────────────────

// WriteContract is the domain function the caller contributes for the write path.
// Called once per unique correlation key per flush cycle — never on Write().
type WriteContract func(correlationKey string, payload []byte) (*WriteModel, error)

// ReadContract translates a correlation key into a datastore-agnostic ReadModel.
// The caller only defines the Filter; the Source executes the query.
// Used by HotLoad() and cold Read() fallback.
type ReadContract func(correlationKey string) (*source.ReadModel, error)

// IndexContract extracts secondary index fields from a payload.
//
// Suggested value handling:
//   - string values       -> equality SET index
//   - int/int64/float64   -> range ZSET index
//   - time.Time           -> range ZSET index using Unix milliseconds
type IndexContract func(correlationKey string, payload []byte) (map[string]interface{}, error)

// ─── Write Path Models ───────────────────────────────────────────────────────

// WriteModel is the fully resolved upsert instruction returned by a WriteContract.
type WriteModel struct {
	Filter interface{}
	Update interface{}
	Upsert bool
}

// FlushRecord is the internal unit that travels through the pipeline.
// Payload is opaque bytes — the library never interprets it.
type FlushRecord struct {
	CorrelationKey string
	Payload        []byte
	ReceivedAt     time.Time
}

// OnFlushCallback is invoked after every BulkWrite attempt — success or failure.
type OnFlushCallback func(correlationKeys []string, result *BulkWriteResult, err error)

// BulkWriteResult carries the outcome of a sink BulkWrite call.
// This is a public wrapper around sink.BulkWriteResult to prevent internal package leakage.
type BulkWriteResult struct {
	InsertedCount int64
	MatchedCount  int64
	ModifiedCount int64
	UpsertedCount int64
	Errors        []SinkError
}

// SinkError ties a write failure back to its originating correlation key.
// Code carries the document-store error code when available (e.g. 11000 for
// a MongoDB/DocumentDB duplicate-key violation).
type SinkError struct {
	CorrelationKey string
	Code           int
	Err            error
}

// ErrCodeDuplicateKey is the MongoDB / AWS DocumentDB error code for a
// unique-index violation. sluice uses this to distinguish permanent failures
// (route to dead-letter) from transient ones (leave in dirty set for retry).
const ErrCodeDuplicateKey = 11000

// ─── DLQ Models ──────────────────────────────────────────────────────────────

// DLQStrategy selects how dead-letter records are processed.
type DLQStrategy int

const (
	DLQIgnore DLQStrategy = iota
	DLQUpsert
	DLQReInsert
)

// DLQResult carries the outcome of a ProcessDLQ invocation.
type DLQResult struct {
	Processed int // total records handled across all bands
	Succeeded int // successfully committed/re-queued
	Failed    int // records that failed during DLQ processing
}

// ─── Query Models ────────────────────────────────────────────────────────────

// Query defines the parameters for a compound lookup against the Redis journal.
type Query struct {
	Equality map[string]string  // Exact matches (SET intersection)
	RangeMin map[string]float64 // Minimum bounds (ZSET score)
	RangeMax map[string]float64 // Maximum bounds (ZSET score)
}

// QueryResult represents a single record returned by a Query operation.
type QueryResult struct {
	CorrelationKey string
	Payload        []byte
}
