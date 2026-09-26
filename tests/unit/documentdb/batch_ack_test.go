package documentdb

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hussainpithawala/sluice-go/internal/shield"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// With batching enabled, Write must not return nil until the entry is in
// the journal: callers ack upstream on nil, so an acknowledged-but-buffered
// write would be lost if its batch then failed.

func newBatchingShield(t *testing.T, ns string, window time.Duration) *shield.Shield {
	t.Helper()
	s := newTestShield(t, ns)
	s.EnableBatching(100, window)
	s.StartBatcher(context.Background())
	t.Cleanup(func() {
		s.StopBatcher()
		_ = s.Close()
	})
	return s
}

func journalHas(t *testing.T, s *shield.Shield, ck string) bool {
	t.Helper()
	_, _, found, err := s.ReadWithTTL(context.Background(), ck)
	require.NoError(t, err)
	return found
}

func TestBatchedWrite_ReturnsOnlyAfterJournalWrite(t *testing.T) {
	// A long window makes "returned before the batch ran" observable.
	s := newBatchingShield(t, "test_batch_ack", 200*time.Millisecond)
	const ck = "batch_ack_001"

	start := time.Now()
	require.NoError(t, s.Write(context.Background(), ck, []byte(`{"v":1}`)))
	assert.GreaterOrEqual(t, time.Since(start), 150*time.Millisecond, "Write should wait for its batch")

	assert.True(t, journalHas(t, s, ck), "payload must be in the journal when Write returns")
	assert.True(t, isDirtyKey(t, s, ck), "key must be dirty when Write returns")
}

func TestBatchedWrite_FailureIsReturnedToItsWriterOnly(t *testing.T) {
	s := newBatchingShield(t, "test_batch_fail", 50*time.Millisecond)
	ctx := context.Background()

	// Two keys in different bands; poison one band's dirty set so ZADD
	// fails with WRONGTYPE for that key only.
	var bad, good string
	for i := 0; bad == "" || good == ""; i++ {
		ck := fmt.Sprintf("batch_fail_%03d", i)
		switch s.BandFor(ck) {
		case 0:
			if bad == "" {
				bad = ck
			}
		case 1:
			if good == "" {
				good = ck
			}
		}
	}
	require.NoError(t, s.Client().Set(ctx, shield.DirtyKey(s.Namespace(), 0), "not-a-zset", 0).Err())

	var wg sync.WaitGroup
	errs := make(map[string]error)
	var mu sync.Mutex
	for _, ck := range []string{bad, good} {
		wg.Add(1)
		go func(ck string) { // concurrent, so both ride the same batch
			defer wg.Done()
			err := s.Write(ctx, ck, []byte(`{"v":1}`))
			mu.Lock()
			errs[ck] = err
			mu.Unlock()
		}(ck)
	}
	wg.Wait()

	assert.Error(t, errs[bad], "the failed entry's writer must see the error")
	assert.Contains(t, fmt.Sprint(errs[bad]), "WRONGTYPE")
	assert.NoError(t, errs[good], "other entries in the batch are unaffected")
	assert.True(t, isDirtyKey(t, s, good))
}

func TestBatchedWrite_RecoversFromScriptCacheFlush(t *testing.T) {
	s := newBatchingShield(t, "test_batch_noscript", 10*time.Millisecond)
	ctx := context.Background()

	// Simulates a Redis restart or failover dropping the script cache.
	require.NoError(t, s.Client().ScriptFlush(ctx).Err())

	require.NoError(t, s.Write(ctx, "noscript_001", []byte(`{"v":1}`)))
	assert.True(t, journalHas(t, s, "noscript_001"))
}

func TestBroadcastWrite_RecoversFromScriptCacheFlush(t *testing.T) {
	s := newTestShield(t, "test_bcast_noscript")
	t.Cleanup(func() { _ = s.Close() })
	s.EnableBroadcast(shield.BroadcastConfig{Mode: shield.BroadcastPayload, MaxLen: 100})
	ctx := context.Background()

	require.NoError(t, s.Client().ScriptFlush(ctx).Err())

	require.NoError(t, s.Write(ctx, "bcast_noscript_001", []byte(`{"v":1}`)))
	assert.True(t, journalHas(t, s, "bcast_noscript_001"))
	n, err := s.Client().XLen(ctx, shield.BroadcastKey(s.Namespace())).Result()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, n, int64(1), "broadcast still emitted")
}

func TestBatchedWrite_AfterStopGoesDirect(t *testing.T) {
	s := newTestShield(t, "test_batch_stopped")
	t.Cleanup(func() { _ = s.Close() })
	s.EnableBatching(100, time.Hour) // would never flush on its own
	s.StartBatcher(context.Background())
	s.StopBatcher()
	s.StopBatcher() // idempotent

	done := make(chan error, 1)
	go func() { done <- s.Write(context.Background(), "stopped_001", []byte(`{"v":1}`)) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("Write after StopBatcher must not hang")
	}
	assert.True(t, journalHas(t, s, "stopped_001"))
}

func TestBatchedWrite_QueuedEntriesFlushedOnStop(t *testing.T) {
	s := newTestShield(t, "test_batch_stop_drain")
	t.Cleanup(func() { _ = s.Close() })
	s.EnableBatching(100, time.Hour) // only StopBatcher's drain flushes
	s.StartBatcher(context.Background())

	const n = 5
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			errs <- s.Write(context.Background(), fmt.Sprintf("drain_%d", i), []byte(`{"v":1}`))
		}(i)
	}
	time.Sleep(100 * time.Millisecond) // let them enqueue
	s.StopBatcher()

	for i := 0; i < n; i++ {
		select {
		case err := <-errs:
			require.NoError(t, err)
		case <-time.After(3 * time.Second):
			t.Fatal("queued writer never got its result")
		}
	}
	for i := 0; i < n; i++ {
		assert.True(t, journalHas(t, s, fmt.Sprintf("drain_%d", i)))
	}
}

func TestStartBatcher_ContextCancelDoesNotStopBatcher(t *testing.T) {
	s := newTestShield(t, "test_batch_ctx")
	t.Cleanup(func() {
		s.StopBatcher()
		_ = s.Close()
	})
	s.EnableBatching(100, 10*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	s.StartBatcher(ctx)
	cancel() // e.g. a short-lived Build context

	require.NoError(t, s.Write(context.Background(), "ctx_001", []byte(`{"v":1}`)))
	assert.True(t, journalHas(t, s, "ctx_001"))
}
