package documentdb

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"time"

	"github.com/hussainpithawala/sluice-go/internal/shield"
	"github.com/hussainpithawala/sluice-go/internal/testutil/faulthook"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// guardKey builds the idempotency guard key in the same format as sluice.go.
func guardKey(namespace string, s *shield.Shield, ck, idemKey string) string {
	return fmt.Sprintf("sl:%s:idem:{%d}:%s", namespace, s.BandFor(ck), idemKey)
}

// TestIdempotencyGuard_BasicFlow verifies the happy path: first write succeeds,
// same guard key is rejected, different guard key succeeds.
func TestIdempotencyGuard_BasicFlow(t *testing.T) {
	const namespace = "idem_basic"
	s := newTestShield(t, namespace)
	defer s.Close()

	ctx := context.Background()
	ck := "basic_001"
	payload := []byte(`{"basic":"flow"}`)
	idemKey := "txn_aaa"
	gk := guardKey(namespace, s, ck, idemKey)
	opts := &shield.WriteOpts{GuardKey: gk, GuardTTLMs: 5000}

	// First write — should succeed.
	wr, err := s.Write(ctx, ck, payload, opts)
	require.NoError(t, err)
	assert.True(t, wr.Written, "first write must be written")
	assert.Equal(t, shield.WriteCodeWritten, wr.Status)

	// Same guard key — should be rejected.
	wr, err = s.Write(ctx, ck, payload, opts)
	require.ErrorIs(t, err, shield.ErrDuplicateKey)
	assert.False(t, wr.Written, "duplicate guard key must not write")
	assert.Equal(t, shield.WriteCodeIdemDup, wr.Status)

	// Different guard key — should succeed (overwrites journal entry).
	otherGK := guardKey(namespace, s, ck, "txn_bbb")
	otherOpts := &shield.WriteOpts{GuardKey: otherGK, GuardTTLMs: 5000}
	wr, err = s.Write(ctx, ck, []byte(`{"basic":"flow_v2"}`), otherOpts)
	require.NoError(t, err)
	assert.True(t, wr.Written, "different guard key must succeed")
	assert.Equal(t, shield.WriteCodeWritten, wr.Status)
}

// TestIdempotencyGuard_NoGuardOnPreExecutionFailure verifies RFP #29 semantics:
// the guard key is SET as the LAST command in the Lua script, so a pre-execution
// failure (e.g. network error before Redis runs the script) leaves no guard key
// in Redis. Retrying with the same guard key must therefore succeed.
func TestIdempotencyGuard_NoGuardOnPreExecutionFailure(t *testing.T) {
	const namespace = "idem_prefail"
	s := newTestShield(t, namespace)
	defer s.Close()

	hook := faulthook.New()
	s.Client().(*redis.Client).AddHook(hook)

	ctx := context.Background()
	ck := "prefail_001"
	payload := []byte(`{"retry":"me"}`)
	idemKey := "txn_pre"
	gk := guardKey(namespace, s, ck, idemKey)
	opts := &shield.WriteOpts{GuardKey: gk, GuardTTLMs: 5000}

	// Inject pre-execution failure: the script never reaches Redis.
	hook.FailBefore("evalsha", 1)

	_, err := s.Write(ctx, ck, payload, opts)
	require.Error(t, err, "write should fail due to injected error")

	hook.Reset()

	// Retry with the same guard key — must succeed because no guard was set.
	wr, err := s.Write(ctx, ck, payload, opts)
	require.NoError(t, err)
	assert.True(t, wr.Written, "retry must succeed: guard was never set")
	assert.Equal(t, shield.WriteCodeWritten, wr.Status)
}

// TestIdempotencyGuard_GuardExistsAfterAmbiguousFailure verifies that when the
// script executes (Redis runs the Lua) but the response is lost (FailAfter),
// the guard key IS set because the script ran to completion. Retrying must see
// the duplicate.
func TestIdempotencyGuard_GuardExistsAfterAmbiguousFailure(t *testing.T) {
	const namespace = "idem_ambiguous"
	s := newTestShield(t, namespace)
	defer s.Close()

	hook := faulthook.New()
	s.Client().(*redis.Client).AddHook(hook)

	ctx := context.Background()
	ck := "ambiguous_001"
	payload := []byte(`{"ambiguous":"failure"}`)
	idemKey := "txn_amb"
	gk := guardKey(namespace, s, ck, idemKey)
	opts := &shield.WriteOpts{GuardKey: gk, GuardTTLMs: 5000}

	// Inject post-execution failure: script runs, but error returned to client.
	hook.FailAfter("evalsha", 1)

	_, err := s.Write(ctx, ck, payload, opts)
	require.Error(t, err, "write should return error despite script executing")

	hook.Reset()

	// Retry with the same guard key — should get ErrDuplicateKey because the
	// script ran to completion and set the guard.
	wr, err := s.Write(ctx, ck, payload, opts)
	require.ErrorIs(t, err, shield.ErrDuplicateKey)
	assert.False(t, wr.Written)
	assert.Equal(t, shield.WriteCodeIdemDup, wr.Status)

	// The journal entry should exist with v == 1.
	band := s.BandFor(ck)
	pkey := shield.PayloadKey(namespace, band, ck)
	v, vErr := s.Client().HGet(ctx, pkey, "v").Result()
	require.NoError(t, vErr)
	assert.Equal(t, "1", v, "journal entry must have v=1 from the executed script")
}

// TestIdempotencyGuard_ConcurrentExactlyOneWins verifies that among 100
// concurrent writes with the same guard key and correlation key, exactly one
// succeeds and 99 are rejected as duplicates.
func TestIdempotencyGuard_ConcurrentExactlyOneWins(t *testing.T) {
	const namespace = "idem_concurrent"
	s := newTestShield(t, namespace)
	defer s.Close()

	ctx := context.Background()
	ck := "concurrent_001"
	payload := []byte(`{"concurrent":"race"}`)
	idemKey := "txn_race"
	gk := guardKey(namespace, s, ck, idemKey)
	opts := &shield.WriteOpts{GuardKey: gk, GuardTTLMs: 5000}

	const goroutines = 100
	var (
		wg       sync.WaitGroup
		wins     atomic.Int64
		dups     atomic.Int64
		failures atomic.Int64
	)

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			_, err := s.Write(ctx, ck, payload, opts)
			switch {
			case err == nil:
				wins.Add(1)
			case err == shield.ErrDuplicateKey:
				dups.Add(1)
			default:
				failures.Add(1)
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, int64(1), wins.Load(), "exactly one goroutine must win")
	assert.Equal(t, int64(goroutines-1), dups.Load(), "all others must see ErrDuplicateKey")
	assert.Equal(t, int64(0), failures.Load(), "no unexpected failures")

	// Journal entry should have v == "1" (single successful write).
	band := s.BandFor(ck)
	pkey := shield.PayloadKey(namespace, band, ck)
	v, err := s.Client().HGet(ctx, pkey, "v").Result()
	require.NoError(t, err)
	assert.Equal(t, "1", v, "exactly one write should have incremented v")
}

// TestIdempotencyGuard_WithContentDedup verifies the interaction between the
// content-hash dedup path (WriteDedup) and the idempotency guard.
func TestIdempotencyGuard_WithContentDedup(t *testing.T) {
	const namespace = "idem_dedup"
	s := newTestShield(t, namespace)
	defer s.Close()

	ctx := context.Background()
	ck := "dedup_001"
	payload := []byte(`{"dedup":"data"}`)
	hash := "sha256_deadbeef"

	guard1 := "txn_dedup_1"
	gk1 := guardKey(namespace, s, ck, guard1)
	opts1 := &shield.WriteOpts{GuardKey: gk1, GuardTTLMs: 5000}

	// 1. First WriteDedup with guard — should succeed.
	wr, err := s.WriteDedup(ctx, ck, payload, hash, opts1)
	require.NoError(t, err)
	assert.True(t, wr.Written, "first WriteDedup must write")
	assert.Equal(t, shield.WriteCodeWritten, wr.Status)

	// 2. Same WriteDedup, same hash, NEW guard key — content dedup kicks in,
	//    guard is set for the new key, written=false (dedup, no state change).
	guard2 := "txn_dedup_2"
	gk2 := guardKey(namespace, s, ck, guard2)
	opts2 := &shield.WriteOpts{GuardKey: gk2, GuardTTLMs: 5000}

	wr, err = s.WriteDedup(ctx, ck, payload, hash, opts2)
	require.NoError(t, err)
	assert.False(t, wr.Written, "content dedup: same hash should not rewrite")
	assert.Equal(t, shield.WriteCodeDedup, wr.Status)

	// 3. Same WriteDedup, same hash, FIRST guard key again — guard1 was set in
	//    step 1, so this is an idempotency duplicate.
	wr, err = s.WriteDedup(ctx, ck, payload, hash, opts1)
	require.ErrorIs(t, err, shield.ErrDuplicateKey)
	assert.False(t, wr.Written)
	assert.Equal(t, shield.WriteCodeIdemDup, wr.Status)
}

// ── Cross-RFP Tests (#29 × #31) ────────────────────────────────────────────

// TestCrossRFP_GuardWinsOverIndexPending verifies the ordering guarantee:
// when BOTH the idempotency guard AND the ixp marker are present, the script
// returns code 2 (idem dup), NOT code 3 (index pending). The guard check
// runs first and short-circuits.
func TestCrossRFP_GuardWinsOverIndexPending(t *testing.T) {
	cfg := shield.RedisConfig{
		Addrs:        []string{testRedisAddr()},
		ClusterMode:  false,
		DB:           testRedisDB,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     10,
	}
	s, err := shield.New(cfg, "test_cross_guard_ix", 16, 30*time.Second, 4*time.Hour, 0, true)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	cleanRedisKeys(t, s.Client(), "test_cross_guard_ix")

	ctx := context.Background()
	ck := "cross_001"
	payload := []byte(`{"cross":"test"}`)
	hash := "cross_hash"
	gk := guardKey("test_cross_guard_ix", s, ck, "txn_cross")
	opts := &shield.WriteOpts{GuardKey: gk, GuardTTLMs: 5000}

	// WriteDedup with guard and indexEnabled — succeeds, guard set, ixp set
	wr, err := s.WriteDedup(ctx, ck, payload, hash, opts)
	require.NoError(t, err)
	assert.Equal(t, shield.WriteCodeWritten, wr.Status)

	// Do NOT clear ixp (simulating index failure). Now both guard and ixp exist.

	// Same guard key, same hash — guard must win (code 2), not index pending (code 3)
	wr, err = s.WriteDedup(ctx, ck, payload, hash, opts)
	require.ErrorIs(t, err, shield.ErrDuplicateKey)
	assert.Equal(t, shield.WriteCodeIdemDup, wr.Status, "guard (code 2) must take priority over ixp (code 3)")
}

// TestCrossRFP_DifferentGuardTriggersIndexRepair verifies that when the guard
// key is different (new idempotency key) but the content hash matches and the
// ixp marker is set, the script returns code 3 so the caller can repair indexes.
func TestCrossRFP_DifferentGuardTriggersIndexRepair(t *testing.T) {
	cfg := shield.RedisConfig{
		Addrs:        []string{testRedisAddr()},
		ClusterMode:  false,
		DB:           testRedisDB,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     10,
	}
	s, err := shield.New(cfg, "test_cross_repair", 16, 30*time.Second, 4*time.Hour, 0, true)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	cleanRedisKeys(t, s.Client(), "test_cross_repair")

	ctx := context.Background()
	ck := "cross_repair_001"
	payload := []byte(`{"repair":"me"}`)
	hash := "repair_hash"

	gk1 := guardKey("test_cross_repair", s, ck, "txn_orig")
	opts1 := &shield.WriteOpts{GuardKey: gk1, GuardTTLMs: 5000}

	// Original write with guard1 — succeeds, ixp set
	wr, err := s.WriteDedup(ctx, ck, payload, hash, opts1)
	require.NoError(t, err)
	assert.Equal(t, shield.WriteCodeWritten, wr.Status)

	// Do NOT clear ixp. Now a NEW idempotency key comes in (different message ID).
	gk2 := guardKey("test_cross_repair", s, ck, "txn_retry")
	opts2 := &shield.WriteOpts{GuardKey: gk2, GuardTTLMs: 5000}

	// Different guard key, same hash, ixp present → code 3 (index repair)
	wr, err = s.WriteDedup(ctx, ck, payload, hash, opts2)
	require.NoError(t, err)
	assert.Equal(t, shield.WriteCodeIndexPend, wr.Status,
		"different guard + same hash + ixp present must trigger index repair (code 3)")
	assert.False(t, wr.Written)
}
