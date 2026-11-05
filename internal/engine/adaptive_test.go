package engine

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdaptiveBatch_NewDefaults(t *testing.T) {
	ab := NewAdaptiveBatch(5000, 100, 3*time.Second)
	require.Equal(t, 5000, ab.Size())
}

func TestAdaptiveBatch_HalveOnTimeout(t *testing.T) {
	ab := NewAdaptiveBatch(1000, 100, 3*time.Second)
	ab.Adjust(0, true)
	assert.Equal(t, 500, ab.Size())
	ab.Adjust(0, true)
	assert.Equal(t, 250, ab.Size())
	ab.Adjust(0, true)
	assert.Equal(t, 125, ab.Size())
}

func TestAdaptiveBatch_FloorAtMin(t *testing.T) {
	ab := NewAdaptiveBatch(200, 100, 3*time.Second)
	ab.Adjust(0, true) // 100
	ab.Adjust(0, true) // 50 → clamped to 100
	assert.Equal(t, 100, ab.Size())
}

func TestAdaptiveBatch_GrowOnFastFlush(t *testing.T) {
	ab := NewAdaptiveBatch(1000, 100, 3*time.Second)
	ab.current = 500
	ab.Adjust(1*time.Second, false) // under target → grow 10%
	assert.Equal(t, 550, ab.Size())
}

func TestAdaptiveBatch_CeilAtMax(t *testing.T) {
	ab := NewAdaptiveBatch(1000, 100, 3*time.Second)
	ab.current = 995
	ab.Adjust(1*time.Second, false) // 995 + 99 = 1094 → clamped to 1000
	assert.Equal(t, 1000, ab.Size())
}

func TestAdaptiveBatch_HoldSteadyAtTarget(t *testing.T) {
	ab := NewAdaptiveBatch(1000, 100, 3*time.Second)
	ab.current = 500
	ab.Adjust(3*time.Second, false) // at target → hold
	assert.Equal(t, 500, ab.Size())
}

func TestAdaptiveBatch_HoldSteadyAboveTarget(t *testing.T) {
	ab := NewAdaptiveBatch(1000, 100, 3*time.Second)
	ab.current = 500
	ab.Adjust(4*time.Second, false) // above target → hold
	assert.Equal(t, 500, ab.Size())
}

func TestAdaptiveBatch_MinGrowByOne(t *testing.T) {
	ab := NewAdaptiveBatch(1000, 1, 3*time.Second)
	ab.current = 5
	ab.Adjust(1*time.Second, false) // 5 + 0 (int div) → ensure at least +1
	assert.Equal(t, 6, ab.Size())
}

func TestAdaptiveBatch_RecoverFromTimeout(t *testing.T) {
	ab := NewAdaptiveBatch(1000, 100, 3*time.Second)
	// Timeout halves several times
	ab.Adjust(0, true) // 500
	ab.Adjust(0, true) // 250
	ab.Adjust(0, true) // 125
	assert.Equal(t, 125, ab.Size())

	// Successful fast flushes grow back
	for i := 0; i < 30; i++ {
		ab.Adjust(500*time.Millisecond, false)
	}
	assert.Greater(t, ab.Size(), 300)
	assert.LessOrEqual(t, ab.Size(), 1000)
}
