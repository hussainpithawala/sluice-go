package engine

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFlushTimeout_CustomValue(t *testing.T) {
	e := &Engine{cfg: Config{
		FlushWindow:  250 * time.Millisecond,
		FlushTimeout: 10 * time.Second,
	}}
	assert.Equal(t, 10*time.Second, e.flushTimeout())
}

func TestFlushTimeout_FallbackToMultiplier(t *testing.T) {
	e := &Engine{cfg: Config{
		FlushWindow: 250 * time.Millisecond,
	}}
	assert.Equal(t, 1*time.Second, e.flushTimeout())
}

func TestFlushTimeout_ZeroFlushTimeout(t *testing.T) {
	e := &Engine{cfg: Config{
		FlushWindow:  5 * time.Second,
		FlushTimeout: 0,
	}}
	assert.Equal(t, 20*time.Second, e.flushTimeout())
}

func TestEffectiveBatchSize_NoAdaptive(t *testing.T) {
	e := &Engine{cfg: Config{MaxBatchSize: 500}}
	assert.Equal(t, 500, e.effectiveBatchSize())
}

func TestEffectiveBatchSize_WithAdaptive(t *testing.T) {
	ab := NewAdaptiveBatch(1000, 100, 3*time.Second)
	ab.current = 300
	e := &Engine{
		cfg:      Config{MaxBatchSize: 1000},
		adaptive: ab,
	}
	assert.Equal(t, 300, e.effectiveBatchSize())
}
