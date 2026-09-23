package documentdb

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/hussainpithawala/sluice-go/internal/shield"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestXAddBroadcast_PayloadMode(t *testing.T) {
	s := newTestShield(t, "test_broadcast")
	defer func(s *shield.Shield) {
		err := s.Close()
		if err != nil {
			slog.Info("Unable to close")
		}
	}(s)

	// Enable broadcast with payload mode
	s.EnableBroadcast(shield.BroadcastConfig{
		Mode:   shield.BroadcastPayload,
		MaxLen: 1000,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Write a key (non-batched path)
	key := "broadcast_test_001"
	payload := []byte(`{"broadcast":"test"}`)
	err := s.Write(ctx, key, payload)
	require.NoError(t, err)

	// Verify stream entry was created
	streamKey := shield.BroadcastKey("test_broadcast")
	entries, err := s.Client().XRange(ctx, streamKey, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, entries, 1, "exactly one stream entry expected")

	// Verify entry fields
	entry := entries[0]
	assert.Equal(t, key, entry.Values["crn"])
	assert.Equal(t, "upsert", entry.Values["kind"])
	assert.Equal(t, payload, []byte(entry.Values["payload"].(string)))

	// Verify ts is a valid UnixMilli timestamp
	tsStr := entry.Values["ts"].(string)
	ts, err := strconv.ParseInt(tsStr, 10, 64)
	require.NoError(t, err)
	assert.Positive(t, ts)

	// Verify band matches expected
	bandStr := entry.Values["band"].(string)
	band, err := strconv.Atoi(bandStr)
	require.NoError(t, err)
	assert.Equal(t, s.BandFor(key), band)
}

func TestXAddBroadcast_InvalidationMode(t *testing.T) {
	s := newTestShield(t, "test_broadcast_inv")
	defer func(s *shield.Shield) {
		err := s.Close()
		if err != nil {
			slog.Info("Unable to close")
		}
	}(s)

	s.EnableBroadcast(shield.BroadcastConfig{
		Mode:   shield.BroadcastInvalidation,
		MaxLen: 1000,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	key := "broadcast_inv_001"
	err := s.Write(ctx, key, []byte(`{"mode":"invalidation"}`))
	require.NoError(t, err)

	streamKey := shield.BroadcastKey("test_broadcast_inv")
	entries, err := s.Client().XRange(ctx, streamKey, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, entries, 1)

	// In invalidation mode, payload field must be absent
	_, hasPayload := entries[0].Values["payload"]
	assert.False(t, hasPayload, "invalidation mode must not include payload")

	// But crn/band/ts/kind must be present
	assert.NotEmpty(t, entries[0].Values["crn"])
	assert.NotEmpty(t, entries[0].Values["ts"])
	assert.Equal(t, "upsert", entries[0].Values["kind"])
}

func TestXAddBroadcast_DisabledIsNoOp(t *testing.T) {
	s := newTestShield(t, "test_broadcast_off")
	defer func(s *shield.Shield) {
		err := s.Close()
		if err != nil {
			slog.Info("Unable to close")
		}
	}(s)

	// Do NOT call EnableBroadcast

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := s.Write(ctx, "no_broadcast_key", []byte(`{"x":1}`))
	require.NoError(t, err)

	// Stream should not exist
	streamKey := shield.BroadcastKey("test_broadcast_off")
	exists, err := s.Client().Exists(ctx, streamKey).Result()
	require.NoError(t, err)
	assert.Zero(t, exists, "stream must not be created when broadcast is disabled")
}

func TestXAddBroadcast_BatchedPath(t *testing.T) {
	s := newTestShield(t, "test_broadcast_batched")
	defer func(s *shield.Shield) {
		err := s.Close()
		if err != nil {
			slog.Info("Unable to close")
		}
	}(s)

	s.EnableBatching(100, 10*time.Millisecond)
	s.EnableBroadcast(shield.BroadcastConfig{
		Mode:   shield.BroadcastPayload,
		MaxLen: 1000,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.StartBatcher(ctx)
	defer s.StopBatcher()

	// Write 5 keys through the batcher
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("batched_bcast_%03d", i)
		err := s.Write(ctx, key, []byte(fmt.Sprintf(`{"i":%d}`, i)))
		require.NoError(t, err)
	}

	// Wait for batch to flush
	time.Sleep(50 * time.Millisecond)

	// Verify all 5 entries are in the stream
	streamKey := shield.BroadcastKey("test_broadcast_batched")
	entries, err := s.Client().XRange(ctx, streamKey, "-", "+").Result()
	require.NoError(t, err)
	assert.Len(t, entries, 5, "all batched writes must produce stream entries")
}
