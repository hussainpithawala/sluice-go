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

// ErrDegradedWriteUnsafe is returned (wrapped together with
// ErrRedisUnavailable) when a Redis write fails and degraded mode declines
// to write directly to the datastore, because an older version of the key is
// still pending in Redis — or Redis is unreachable, so that cannot be ruled
// out. A direct write there could later be overwritten by the older version's
// flush. The write was not persisted; retry it (e.g. do not ack the upstream
// message) and it will go through Redis in order once Redis recovers.
var ErrDegradedWriteUnsafe = errors.New("sluice: degraded write refused: an older pending version could overwrite it")

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
