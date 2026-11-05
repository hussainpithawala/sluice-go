package engine

import (
	"sync"
	"time"
)

// AdaptiveBatch adjusts the effective batch size based on recent flush durations.
// On timeout the size halves (floor at Min); on a fast flush it grows 10%
// (ceiling at Max); otherwise it holds steady.
type AdaptiveBatch struct {
	Max     int
	Min     int
	Target  time.Duration
	mu      sync.Mutex
	current int
}

// NewAdaptiveBatch creates an AdaptiveBatch starting at max.
func NewAdaptiveBatch(max, min int, target time.Duration) *AdaptiveBatch {
	if min <= 0 {
		min = 100
	}
	if max < min {
		max = min
	}
	return &AdaptiveBatch{
		Max:     max,
		Min:     min,
		Target:  target,
		current: max,
	}
}

// Size returns the current effective batch size.
func (a *AdaptiveBatch) Size() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.current
}

// Adjust recalculates the batch size after a flush.
func (a *AdaptiveBatch) Adjust(duration time.Duration, timedOut bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if timedOut {
		a.current /= 2
		if a.current < a.Min {
			a.current = a.Min
		}
		return
	}
	if duration < a.Target {
		next := a.current + a.current/10
		if next > a.Max {
			next = a.Max
		}
		if next == a.current && a.current < a.Max {
			next = a.current + 1
		}
		a.current = next
		return
	}
}
