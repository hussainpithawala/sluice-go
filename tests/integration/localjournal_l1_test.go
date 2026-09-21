package integration

import (
	"context"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hussainpithawala/sluice-go"
	"github.com/hussainpithawala/sluice-go/internal/localjournal"
	"github.com/hussainpithawala/sluice-go/internal/shield"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── Test Doubles ────────────────────────────────────────────────────────────

// countingRecorder is a minimal MetricsRecorder that counts L1 events.
// It satisfies the embedded localjournal.MetricsRecorder interface and
// stubs every other method so it compiles against the full interface.
type countingRecorder struct {
	mu                    sync.Mutex
	hits                  int
	misses                int
	missReasons           map[string]int
	setSizeCalls          int
	lastLagRecordDuration time.Duration
	reads                 int
	// spy: if set, called on every RecordRedisOp so we can detect
	// whether the L2 Redis tier was touched during a Read.
	redisOpHook func(op string)
}

func newCountingRecorder() *countingRecorder {
	return &countingRecorder{missReasons: make(map[string]int)}
}

func (r *countingRecorder) RecordLocalCacheHit(string) {
	r.mu.Lock()
	r.hits++
	r.mu.Unlock()
}
func (r *countingRecorder) RecordLocalCacheMiss(_ string, reason localjournal.MissReason) {
	r.mu.Lock()
	r.misses++
	r.missReasons[string(reason)]++
	r.mu.Unlock()
}
func (r *countingRecorder) RecordLocalSetSize(string, int) {
	r.mu.Lock()
	r.setSizeCalls++
	r.mu.Unlock()
}

func (r *countingRecorder) RecordBroadcastLag(_ string, lag time.Duration) {
	r.mu.Lock()
	r.lastLagRecordDuration = lag
	r.mu.Unlock()
}

// ── Stubs for the rest of MetricsRecorder (compile-time satisfaction) ──────
func (r *countingRecorder) RecordRead(string, time.Duration, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reads++
}
func (r *countingRecorder) RecordWrite(string)                        {}
func (r *countingRecorder) RecordDegradedWrite(string, error)         {}
func (r *countingRecorder) RecordWarmUp(string, time.Duration, error) {}
func (r *countingRecorder) RecordHotSetSize(string, int)              {}
func (r *countingRecorder) RecordRedisOp(_ string, op string, _ time.Duration, _ error) {
	if r.redisOpHook != nil {
		r.redisOpHook(op)
	}
}
func (r *countingRecorder) RecordFlush(string, string, int, time.Duration, error) {}
func (r *countingRecorder) RecordDirtyQueueDepth(string, string, int)             {}
func (r *countingRecorder) RecordContractError(string, string, error)             {}
func (r *countingRecorder) RecordDeadLetter(string, string, int)                  {}
func (r *countingRecorder) RecordDLQProcess(string, string, int, int, int)        {}

// snapshot returns a thread-safe copy of the counters.
func (r *countingRecorder) snapshot() (hits, misses int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.hits, r.misses
}

// ── Helpers ─────────────────────────────────────────────────────────────────

const l1TestDB = 15

func l1RedisAddr() string {
	if a := os.Getenv("REDIS_ADDR"); a != "" {
		return a
	}
	return "localhost:6379"
}

// buildTestSluice wires a minimal Sluice with a real shield (Redis on DB 15)
// and an L1 cache. Adjust the field names to match your actual Sluice struct —
// the public surface exercised here is only Write() and Read().
func buildTestSluice(t *testing.T, namespace string, mode localjournal.LocalCacheMode, ttl time.Duration) (*sluice.Sluice, *countingRecorder, *shield.Shield) {
	t.Helper()

	sh, err := shield.New(shield.RedisConfig{
		Addrs:        []string{l1RedisAddr()},
		ClusterMode:  false,
		DB:           l1TestDB,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     10,
	}, namespace, 16, 30*time.Second, 4*time.Hour)
	require.NoError(t, err, "shield.New")

	// Isolate: wipe the dedicated test DB
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, sh.Client().FlushDB(ctx).Err())

	rec := newCountingRecorder()

	var local *localjournal.Cache
	if mode != localjournal.LocalCacheOff {
		local = localjournal.New(localjournal.Config{
			Namespace:  namespace,
			MaxEntries: 1024,
			LocalTTL:   ttl,
			Metrics:    rec,
		})
	}

	// Construct via your builder if Build() supports injecting a pre-built
	// shield; otherwise assemble the struct directly. The fields below must
	// match your sluice.Sluice definition:

	s := sluice.NewForTest(sluice.TestConfig{
		Namespace: namespace,
		Shield:    sh,
		Local:     local,
		Metrics:   rec,
		// HotAwareFlush, ActivityWindow etc. default to safe zero values
	})

	return s, rec, sh
}

// ── The Proof ───────────────────────────────────────────────────────────────

// TestL1_WriteThenRead_HitsL1 proves the write-through contract:
//
//  1. Write() synchronously Puts the payload into L1.
//  2. Read() on the same pod is served entirely from L1:
//     a. RecordLocalCacheHit fires exactly once.
//     b. NO Redis "read"-class op fires (ReadJournal is never called).
//     c. The returned payload is byte-identical to what was written.
//
// This is the strongest form of the assertion: a positive metric signal AND
// a negative network signal, both observed independently.
func TestL1_WriteThenRead_HitsL1(t *testing.T) {
	s, rec, sh := buildTestSluice(t, "test_l1_write_read", localjournal.LocalCacheLazy, 60*time.Second)
	defer sh.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Spy: count every Redis op classified as a read.
	var redisReads int64
	rec.redisOpHook = func(op string) {
		if op == "read" || op == "readjournal" {
			atomic.AddInt64(&redisReads, 1)
		}
	}

	const key = "user_session_001"
	payload := []byte(`{"plan":"pro","seats":5}`)

	// ── Act 1: Write ─────────────────────────────────────────────────────
	require.NoError(t, s.Write(ctx, key, payload), "Write must succeed")

	// Sanity: the write reached Redis (the source of truth)
	jr, err := sh.ReadJournal(ctx, key)
	require.NoError(t, err)
	require.True(t, jr.Found, "payload must be journaled in L2")
	require.Equal(t, payload, jr.Payload)
	require.Positive(t, jr.Version, "journal must carry a UnixMilli version")

	// ── Act 2: Read (should hit L1, never touch Redis) ───────────────────
	got, err := s.Read(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, payload, got, "Read must return the exact written payload")

	// ── Assert: positive metric signal ───────────────────────────────────
	hits, misses := rec.snapshot()
	assert.Equal(t, 1, hits, "RecordLocalCacheHit must fire exactly once")
	assert.Equal(t, 0, misses, "no L1 miss should be recorded on the hot path")

	// ── Assert: negative network signal ──────────────────────────────────
	assert.Zero(t, atomic.LoadInt64(&redisReads),
		"Read() must NOT call ReadJournal when L1 holds the key")
}

// TestL1_SecondRead_AlsoHitsL1 proves repeated reads stay in L1 and that the
// hit counter monotonically increments (no silent fall-through).
func TestL1_SecondRead_AlsoHitsL1(t *testing.T) {
	s, rec, sh := buildTestSluice(t, "test_l1_repeat_read", localjournal.LocalCacheLazy, 60*time.Second)
	defer sh.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var redisReads int64
	rec.redisOpHook = func(op string) {
		if op == "read" || op == "readjournal" {
			atomic.AddInt64(&redisReads, 1)
		}
	}

	const key = "user_session_002"
	require.NoError(t, s.Write(ctx, key, []byte(`{"v":1}`)))

	for i := 1; i <= 5; i++ {
		_, err := s.Read(ctx, key)
		require.NoError(t, err, "read %d", i)
	}

	hits, misses := rec.snapshot()
	assert.Equal(t, 5, hits, "every read must hit L1")
	assert.Zero(t, misses)
	assert.Zero(t, atomic.LoadInt64(&redisReads), "Redis must never be read")
}

// TestL1_OffMode_BehavesLikeV107 proves the zero-behaviour-change guarantee:
// with Mode=Off, every Read falls through to L2 and no L1 metric ever fires.
func TestL1_OffMode_BehavesLikeV107(t *testing.T) {
	s, rec, sh := buildTestSluice(t, "test_l1_off", localjournal.LocalCacheOff, 0)
	defer func(sh *shield.Shield) {
		err := sh.Close()
		if err != nil {
			log.Fatal("Unable to close the suite")
		}
	}(sh)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var redisReads int64
	rec.redisOpHook = func(op string) {
		if op == "read" || op == "readjournal" {
			atomic.AddInt64(&redisReads, 1)
		}
	}

	const key = "user_session_003"
	require.NoError(t, s.Write(ctx, key, []byte(`{"v":1}`)))

	got, err := s.Read(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, []byte(`{"v":1}`), got)

	hits, _ := rec.snapshot()
	assert.Zero(t, hits, "Mode=Off must never record an L1 hit")
	assert.Equal(t, int64(1), atomic.LoadInt64(&redisReads),
		"Mode=Off must read from Redis exactly as v1.0.7 did")
}

// TestL1_TTLExpiry_FallsThroughToL2AndHeals proves the lazy-heal backstop:
// when an L1 entry expires, Read() falls through to ReadJournal, re-heals L1
// with the journal's authoritative version, and subsequent reads hit L1 again.
func TestL1_TTLExpiry_FallsThroughToL2AndHeals(t *testing.T) {
	s, rec, sh := buildTestSluice(t, "test_l1_expiry", localjournal.LocalCacheLazy, 50*time.Millisecond)
	defer sh.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var redisReads int64
	rec.redisOpHook = func(op string) {
		if op == "read" || op == "readjournal" {
			atomic.AddInt64(&redisReads, 1)
		}
	}

	const key = "user_session_004"
	require.NoError(t, s.Write(ctx, key, []byte(`{"v":1}`)))

	// First read: L1 hit
	_, err := s.Read(ctx, key)
	require.NoError(t, err)

	// Let the L1 entry expire
	time.Sleep(80 * time.Millisecond)

	// Second read: L1 miss (expired) → L2 heal
	_, err = s.Read(ctx, key)
	require.NoError(t, err)

	hits, misses := rec.snapshot()
	assert.Equal(t, 1, hits, "first read hits L1")
	assert.Equal(t, 1, misses, "expired read records exactly one miss")
	assert.Equal(t, 1, rec.missReasons["expired"], "miss reason must be 'expired'")
	assert.Equal(t, int64(1), atomic.LoadInt64(&redisReads),
		"exactly one ReadJournal call for the heal")

	// Third read: L1 healed, hits again
	_, err = s.Read(ctx, key)
	require.NoError(t, err)

	hits, _ = rec.snapshot()
	assert.Equal(t, 2, hits, "post-heal read hits L1 again")
	assert.Equal(t, int64(1), atomic.LoadInt64(&redisReads),
		"no additional Redis read after heal")
}

// TestL1_StaleVersion_RejectedByPut proves Principle C3: a Put with an older
// version than the resident entry is rejected. This guards against
// out-of-order broadcasts or heals regressing local state.
//
// Note: this exercises localjournal.Cache directly (the version gate is a
// Cache-level invariant, independent of the Sluice wiring).
func TestL1_StaleVersion_RejectedByPut(t *testing.T) {
	cache := localjournal.New(localjournal.Config{
		Namespace:  "test_l1_version",
		MaxEntries: 16,
		LocalTTL:   time.Minute,
	})

	const key = "k"
	require.True(t, cache.Put(key, []byte("new"), 200), "v200 applies")

	// Older version must be rejected
	assert.False(t, cache.Put(key, []byte("old"), 100), "v100 must be rejected")
	// Equal version must also be rejected (strict >)
	assert.False(t, cache.Put(key, []byte("same"), 200), "equal version rejected")

	// Newer version applies
	require.True(t, cache.Put(key, []byte("newer"), 300))

	got, v, ok := cache.Get(key)
	require.True(t, ok)
	assert.Equal(t, int64(300), v)
	assert.Equal(t, []byte("newer"), got)
}
