package sluice

import "errors"

var (
	ErrLibraryClosed       = errors.New("sluice: library is closed")
	ErrRedisUnavailable    = errors.New("sluice: redis unavailable")
	ErrSinkUnavailable     = errors.New("sluice: sink unavailable")
	ErrContractViolation   = errors.New("sluice: write contract returned error")
	ErrEmptyCorrelationKey = errors.New("sluice: correlation key must not be empty")
	ErrMissingNamespace    = errors.New("sluice: namespace is required")
	ErrMissingSink         = errors.New("sluice: sink is required — call WithSink()")
	ErrMissingContract     = errors.New("sluice: write contract is required — call WithWriteContract()")
	ErrMissingRedis        = errors.New("sluice: redis config is required — call WithRedis()")
)
