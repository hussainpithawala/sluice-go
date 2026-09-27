// Package sink defines the FlushSink interface that all document store
// backends must implement to integrate with sluice.
package sink

import (
	"errors"
	"fmt"
)

// ErrContractViolation indicates the write contract returned an error.
var ErrContractViolation = errors.New("sink: write contract returned error")

func (e SinkError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("sink error (key=%s, code=%d): %v", e.CorrelationKey, e.Code, e.Err)
	}
	return fmt.Sprintf("sink error (key=%s, code=%d): %s", e.CorrelationKey, e.Code, e.Message)
}

// IsPermanent determines if the error should be dead-lettered or retried.
// Preserves legacy behavior where ClassUnknown with Code 11000 is treated as permanent.
func (e SinkError) IsPermanent() bool {
	if e.Class == ClassPermanent {
		return true
	}
	if e.Class == ClassUnknown && e.Code == 11000 {
		return true
	}
	return false
}

// CheckAttribution validates that all errors in a partial-failure result have a valid CorrelationKey.
// If any error lacks a key, it returns ErrUnattributedSinkError, preventing the engine
// from silently committing keys that might have actually failed (success-by-elimination).
func CheckAttribution(models []WriteModel, result *BulkWriteResult) error {
	if result == nil || len(result.Errors) == 0 {
		return nil
	}
	for _, se := range result.Errors {
		if se.CorrelationKey == "" {
			return fmt.Errorf("%w: %v", ErrUnattributedSinkError, se)
		}
	}
	return nil
}
