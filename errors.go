package sluice

import (
	"errors"

	"github.com/hussainpithawala/sluice-go/internal/engine"
)

// ErrPayloadMissing is passed to the OnFlush callback for dirty keys whose
// payload was gone from Redis before it could be flushed. Unflushed payloads
// never expire, so this means data was lost (eviction, FLUSHDB, manual
// deletion) and should be treated as an incident.
var ErrPayloadMissing = engine.ErrPayloadMissing

var (
	ErrLibraryClosed        = errors.New("sluice: library is closed")
	ErrRedisUnavailable     = errors.New("sluice: redis unavailable")
	ErrSinkUnavailable      = errors.New("sluice: sink unavailable")
	ErrMissingSource        = errors.New("sluice: source unavailable")
	ErrContractViolation    = errors.New("sluice: write contract returned error")
	ErrEmptyCorrelationKey  = errors.New("sluice: correlation key must not be empty")
	ErrMissingNamespace     = errors.New("sluice: namespace is required")
	ErrMissingSink          = errors.New("sluice: sink is required — call WithSink()")
	ErrMissingWriteContract = errors.New("sluice: write contract is required — call WithWriteContract()")
	ErrMissingRedis         = errors.New("sluice: redis config is required — call WithRedis()")

	ErrDuplicateIdempotencyKey = errors.New("sluice: idempotency key already processed")
	ErrMissingReadContract     = errors.New("sluice: ReadContract is required for HotLoad/Read")
	ErrRecordNotFound          = errors.New("sluice: record not found in journal or sink")
)
