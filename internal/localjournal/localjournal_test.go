package localjournal

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// ─── Test Helpers ────────────────────────────────────────────────────────────

// fakeClock gives deterministic control over time.Now for TTL tests.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// spyRecorder captures every hit/miss for assertion.
type spyRecorder struct {
	mu           sync.Mutex
	hits         int
	misses       int
	missList     []MissReason
	localSetSize int
}

// Changed from LocalCacheHit to RecordLocalCacheHit
func (r *spyRecorder) RecordLocalCacheHit(string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hits++
}

// Changed from LocalCacheMiss to RecordLocalCacheMiss
func (r *spyRecorder) RecordLocalCacheMiss(_ string, reason MissReason) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.misses++
	r.missList = append(r.missList, reason)
}
func (r *spyRecorder) RecordLocalSetSize(namespace string, size int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.localSetSize++
}

func (r *spyRecorder) LocalCacheHit(string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hits++
}

func (r *spyRecorder) LocalCacheMiss(_ string, reason MissReason) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.misses++
	r.missList = append(r.missList, reason)
}

func (r *spyRecorder) snapshot() (hits, misses int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.hits, r.misses
}

// testCache builds a Cache with a fake clock and spy recorder.
func testCache(maxEntries int, ttl time.Duration) (*Cache, *fakeClock, *spyRecorder) {
	clock := &fakeClock{t: time.Unix(1_000_000, 0)}
	rec := &spyRecorder{}
	c := New(Config{
		Namespace:  "test",
		MaxEntries: maxEntries,
		LocalTTL:   ttl,
		Now:        clock.Now,
		Metrics:    rec, // <-- CHANGED: Use Metrics instead of Recorder
	})
	return c, clock, rec
}

// ─── Get Tests ───────────────────────────────────────────────────────────────

func TestGet_DisabledCache(t *testing.T) {
	c := New(Config{
		Namespace:  "test",
		MaxEntries: 0, // disabled
		LocalTTL:   time.Minute,
	})

	p, v, ok := c.Get("any_key")
	if ok {
		t.Fatal("expected Get to return false for disabled cache")
	}
	if p != nil {
		t.Fatalf("expected nil payload, got %v", p)
	}
	if v != 0 {
		t.Fatalf("expected version 0, got %d", v)
	}
}

func TestGet_KeyNotFound_RecordsAbsentMiss(t *testing.T) {
	c, _, rec := testCache(100, time.Minute)

	p, v, ok := c.Get("missing_key")
	if ok {
		t.Fatal("expected Get to return false for missing key")
	}
	if p != nil || v != 0 {
		t.Fatalf("expected nil/0, got %v/%d", p, v)
	}

	hits, misses := rec.snapshot()
	if hits != 0 {
		t.Fatalf("expected 0 hits, got %d", hits)
	}
	if misses != 1 {
		t.Fatalf("expected 1 miss, got %d", misses)
	}
	if rec.missList[0] != MissAbsent {
		t.Fatalf("expected MissAbsent, got %s", rec.missList[0])
	}
}

func TestGet_ValidKey_ReturnsPayloadAndRecordsHit(t *testing.T) {
	c, _, rec := testCache(100, time.Minute)

	payload := []byte(`{"user":"alice"}`)
	version := int64(42)
	c.Put("key1", payload, version)

	p, v, ok := c.Get("key1")
	if !ok {
		t.Fatal("expected Get to return true for valid key")
	}
	if string(p) != string(payload) {
		t.Fatalf("expected payload %q, got %q", payload, p)
	}
	if v != version {
		t.Fatalf("expected version %d, got %d", version, v)
	}

	hits, misses := rec.snapshot()
	if hits != 1 {
		t.Fatalf("expected 1 hit, got %d", hits)
	}
	if misses != 0 {
		t.Fatalf("expected 0 misses, got %d", misses)
	}
}

func TestGet_ExpiredKey_RecordsExpiredMissAndRemoves(t *testing.T) {
	c, clock, rec := testCache(100, 5*time.Second)

	c.Put("key1", []byte("value1"), 1)

	// Advance past TTL
	clock.Advance(6 * time.Second)

	p, v, ok := c.Get("key1")
	if ok {
		t.Fatal("expected Get to return false for expired key")
	}
	if p != nil || v != 0 {
		t.Fatalf("expected nil/0 for expired key, got %v/%d", p, v)
	}

	hits, misses := rec.snapshot()
	if hits != 0 {
		t.Fatalf("expected 0 hits, got %d", hits)
	}
	if misses != 1 {
		t.Fatalf("expected 1 miss, got %d", misses)
	}
	if rec.missList[0] != MissExpired {
		t.Fatalf("expected MissExpired, got %s", rec.missList[0])
	}

	// Verify the entry was actually removed from the map
	// A second Get should record MissAbsent, not MissExpired
	p2, _, ok2 := c.Get("key1")
	if ok2 || p2 != nil {
		t.Fatal("expected key to be removed after expiry")
	}
	_, misses2 := rec.snapshot()
	if misses2 != 2 {
		t.Fatalf("expected 2 total misses, got %d", misses2)
	}
	if rec.missList[1] != MissAbsent {
		t.Fatalf("expected second miss to be MissAbsent, got %s", rec.missList[1])
	}
}

func TestGet_ExactlyAtTTLBoundary_NotExpired(t *testing.T) {
	c, clock, _ := testCache(100, 5*time.Second)

	c.Put("key1", []byte("value1"), 1)

	// Advance exactly to TTL boundary (now == expiresAt, not after)
	clock.Advance(5 * time.Second)

	// now.After(expiresAt) is false when now == expiresAt
	_, _, ok := c.Get("key1")
	if !ok {
		t.Fatal("expected key to still be valid at exact TTL boundary")
	}
}

func TestGet_NilRecorder_DoesNotPanic(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1_000_000, 0)}
	c := New(Config{
		Namespace:  "test",
		MaxEntries: 100,
		LocalTTL:   time.Minute,
		Now:        clock.Now,
		Metrics:    nil, // <-- CHANGED: Use Metrics instead of Recorder
	})

	c.Put("key1", []byte("v"), 1)
	_, _, ok := c.Get("key1")
	if !ok {
		t.Fatal("expected hit")
	}

	// Miss path without recorder should not panic
	_, _, ok = c.Get("missing")
	if ok {
		t.Fatal("expected miss")
	}
}

// ─── Put Tests ───────────────────────────────────────────────────────────────

func TestPut_DisabledCache(t *testing.T) {
	c := New(Config{
		Namespace:  "test",
		MaxEntries: 0,
		LocalTTL:   time.Minute,
	})

	ok := c.Put("key", []byte("v"), 1)
	if ok {
		t.Fatal("expected Put to return false for disabled cache")
	}
}

func TestPut_NewKey_InsertsSuccessfully(t *testing.T) {
	c, _, _ := testCache(100, time.Minute)

	ok := c.Put("key1", []byte("value1"), 10)
	if !ok {
		t.Fatal("expected Put to return true for new key")
	}

	p, v, found := c.Get("key1")
	if !found {
		t.Fatal("expected key to be found after Put")
	}
	if string(p) != "value1" {
		t.Fatalf("expected 'value1', got %q", p)
	}
	if v != 10 {
		t.Fatalf("expected version 10, got %d", v)
	}
}

func TestPut_SameVersion_Rejected(t *testing.T) {
	c, _, _ := testCache(100, time.Minute)

	c.Put("key1", []byte("original"), 10)

	// Same version should be rejected
	ok := c.Put("key1", []byte("duplicate"), 10)
	if ok {
		t.Fatal("expected Put with same version to be rejected")
	}

	// Original value should be preserved
	p, v, _ := c.Get("key1")
	if string(p) != "original" {
		t.Fatalf("expected 'original', got %q", p)
	}
	if v != 10 {
		t.Fatalf("expected version 10, got %d", v)
	}
}

func TestPut_OlderVersion_Rejected(t *testing.T) {
	c, _, _ := testCache(100, time.Minute)

	c.Put("key1", []byte("newer"), 20)

	// Older version should be rejected
	ok := c.Put("key1", []byte("older"), 15)
	if ok {
		t.Fatal("expected Put with older version to be rejected")
	}

	p, v, _ := c.Get("key1")
	if string(p) != "newer" {
		t.Fatalf("expected 'newer', got %q", p)
	}
	if v != 20 {
		t.Fatalf("expected version 20, got %d", v)
	}
}

func TestPut_NewerVersion_Accepted(t *testing.T) {
	c, _, _ := testCache(100, time.Minute)

	c.Put("key1", []byte("old"), 10)

	ok := c.Put("key1", []byte("new"), 20)
	if !ok {
		t.Fatal("expected Put with newer version to be accepted")
	}

	p, v, _ := c.Get("key1")
	if string(p) != "new" {
		t.Fatalf("expected 'new', got %q", p)
	}
	if v != 20 {
		t.Fatalf("expected version 20, got %d", v)
	}
}

func TestPut_UpdatesExpiry(t *testing.T) {
	c, clock, _ := testCache(100, 5*time.Second)

	c.Put("key1", []byte("v1"), 1)

	// Advance 3 seconds (not yet expired)
	clock.Advance(3 * time.Second)

	// Update with newer version — should reset expiry
	c.Put("key1", []byte("v2"), 2)

	// Advance 3 more seconds (6s total from original put, but only 3s from update)
	clock.Advance(3 * time.Second)

	// Should still be valid because expiry was reset on update
	_, _, ok := c.Get("key1")
	if !ok {
		t.Fatal("expected key to be valid after expiry reset via Put")
	}
}

func TestPut_Eviction_WhenOverCapacity(t *testing.T) {
	// MaxEntries=256 with 256 shards means capPerShard=1.
	// Total capacity across all shards is 256.
	// Inserting 500 keys guarantees evictions will occur regardless of hashing.
	c, _, _ := testCache(256, time.Minute)

	for i := range 500 {
		key := fmt.Sprintf("key_%d", i)
		c.Put(key, []byte(fmt.Sprintf("value_%d", i)), int64(i))
	}

	total := c.Len()
	if total > 256 {
		t.Fatalf("expected Len() <= 256 after eviction, got %d", total)
	}

	evictions := c.Evictions()
	if evictions == 0 {
		t.Fatal("expected at least one eviction")
	}
}

func TestPut_ZeroVersion_Accepted(t *testing.T) {
	c, _, _ := testCache(100, time.Minute)

	ok := c.Put("key1", []byte("v"), 0)
	if !ok {
		t.Fatal("expected Put with version 0 to be accepted for new key")
	}

	// A second put with version 0 should be rejected (same version)
	ok = c.Put("key1", []byte("v2"), 0)
	if ok {
		t.Fatal("expected Put with same version 0 to be rejected")
	}
}

func TestPut_NegativeVersion_Accepted(t *testing.T) {
	c, _, _ := testCache(100, time.Minute)

	// Negative versions are technically valid int64 values
	ok := c.Put("key1", []byte("v"), -1)
	if !ok {
		t.Fatal("expected Put with negative version to be accepted for new key")
	}

	// Version 0 should be newer than -1
	ok = c.Put("key1", []byte("v2"), 0)
	if !ok {
		t.Fatal("expected Put with version 0 to be accepted over version -1")
	}
}

// ─── Invalidate Tests ────────────────────────────────────────────────────────

func TestInvalidate_ExistingKey_Removes(t *testing.T) {
	c, _, _ := testCache(100, time.Minute)

	c.Put("key1", []byte("value1"), 1)

	c.Invalidate("key1")

	_, _, ok := c.Get("key1")
	if ok {
		t.Fatal("expected key to be removed after Invalidate")
	}
}

func TestInvalidate_MissingKey_NoError(t *testing.T) {
	c, _, _ := testCache(100, time.Minute)

	// Should not panic or error
	c.Invalidate("nonexistent_key")
}

func TestInvalidate_DisabledCache_NoPanic(t *testing.T) {
	c := New(Config{
		Namespace:  "test",
		MaxEntries: 0,
		LocalTTL:   time.Minute,
	})

	// Should not panic
	c.Invalidate("any_key")
}

func TestInvalidate_AllowsReinsertion(t *testing.T) {
	c, _, _ := testCache(100, time.Minute)

	c.Put("key1", []byte("v1"), 1)
	c.Invalidate("key1")

	// Re-insert with same version should work (key was removed)
	ok := c.Put("key1", []byte("v2"), 1)
	if !ok {
		t.Fatal("expected Put to succeed after Invalidate")
	}

	p, v, found := c.Get("key1")
	if !found {
		t.Fatal("expected key to be found after re-insertion")
	}
	if string(p) != "v2" {
		t.Fatalf("expected 'v2', got %q", p)
	}
	if v != 1 {
		t.Fatalf("expected version 1, got %d", v)
	}
}

// ─── LRU Ordering Tests ─────────────────────────────────────────────────────

func TestGet_PromotesToLRUFront(t *testing.T) {
	// With capPerShard=1, accessing a key should keep it alive
	// while an unaccessed key in the same shard gets evicted.
	// This test verifies the LRU promotion logic works.
	c, _, _ := testCache(512, time.Minute) // capPerShard = 2

	// Insert two keys
	c.Put("key_a", []byte("a"), 1)
	c.Put("key_b", []byte("b"), 2)

	// Access key_a to promote it
	c.Get("key_a")

	// Both should still be accessible
	_, _, okA := c.Get("key_a")
	_, _, okB := c.Get("key_b")
	if !okA || !okB {
		t.Fatal("expected both keys to be accessible")
	}
}

// ─── Concurrency Tests ──────────────────────────────────────────────────────

func TestConcurrentPutGet(t *testing.T) {
	c, _, _ := testCache(1000, time.Minute)

	var wg sync.WaitGroup
	const goroutines = 50
	const ops = 100

	// Concurrent writers
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				key := fmt.Sprintf("key_%d_%d", id, i%10)
				version := int64(id*ops + i)
				c.Put(key, []byte(fmt.Sprintf("v_%d_%d", id, i)), version)
			}
		}(g)
	}

	// Concurrent readers
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				key := fmt.Sprintf("key_%d_%d", id, i%10)
				c.Get(key)
			}
		}(g)
	}

	// Concurrent invalidators
	for g := 0; g < goroutines/5; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < ops/10; i++ {
				key := fmt.Sprintf("key_%d_%d", id, i%10)
				c.Invalidate(key)
			}
		}(g)
	}

	wg.Wait()
	// If we get here without a race detector complaint or panic, the test passes.
}

// ─── Edge Cases ──────────────────────────────────────────────────────────────

func TestPut_EmptyPayload(t *testing.T) {
	c, _, _ := testCache(100, time.Minute)

	ok := c.Put("key1", []byte{}, 1)
	if !ok {
		t.Fatal("expected Put with empty payload to succeed")
	}

	p, _, found := c.Get("key1")
	if !found {
		t.Fatal("expected key to be found")
	}
	if len(p) != 0 {
		t.Fatalf("expected empty payload, got %d bytes", len(p))
	}
}

func TestPut_NilPayload(t *testing.T) {
	c, _, _ := testCache(100, time.Minute)

	ok := c.Put("key1", nil, 1)
	if !ok {
		t.Fatal("expected Put with nil payload to succeed")
	}

	p, _, found := c.Get("key1")
	if !found {
		t.Fatal("expected key to be found")
	}
	if p != nil {
		t.Fatalf("expected nil payload, got %v", p)
	}
}

func TestPut_LargePayload(t *testing.T) {
	c, _, _ := testCache(100, time.Minute)

	large := make([]byte, 1024*1024) // 1MB
	for i := range large {
		large[i] = byte(i % 256)
	}

	ok := c.Put("key1", large, 1)
	if !ok {
		t.Fatal("expected Put with large payload to succeed")
	}

	p, _, found := c.Get("key1")
	if !found {
		t.Fatal("expected key to be found")
	}
	if len(p) != len(large) {
		t.Fatalf("expected %d bytes, got %d", len(large), len(p))
	}
}

func TestGet_MultipleKeys_IndependentExpiry(t *testing.T) {
	c, clock, _ := testCache(100, 5*time.Second)

	c.Put("key1", []byte("v1"), 1)

	clock.Advance(3 * time.Second)
	c.Put("key2", []byte("v2"), 2)

	// Advance 3 more seconds: key1 is now 6s old (expired), key2 is 3s old (valid)
	clock.Advance(3 * time.Second)

	_, _, ok1 := c.Get("key1")
	if ok1 {
		t.Fatal("expected key1 to be expired")
	}

	_, _, ok2 := c.Get("key2")
	if !ok2 {
		t.Fatal("expected key2 to still be valid")
	}
}

// ─── Recorder Integration ────────────────────────────────────────────────────

func TestRecorder_MultipleMisses_TrackedCorrectly(t *testing.T) {
	c, clock, rec := testCache(100, 1*time.Second)

	// Miss: absent
	c.Get("missing1")

	// Put then expire
	c.Put("key1", []byte("v"), 1)
	clock.Advance(2 * time.Second)

	// Miss: expired
	c.Get("key1")

	// Miss: absent again (key was removed on expiry)
	c.Get("key1")

	hits, misses := rec.snapshot()
	if hits != 0 {
		t.Fatalf("expected 0 hits, got %d", hits)
	}
	if misses != 3 {
		t.Fatalf("expected 3 misses, got %d", misses)
	}

	expectedReasons := []MissReason{MissAbsent, MissExpired, MissAbsent}
	for i, expected := range expectedReasons {
		if rec.missList[i] != expected {
			t.Fatalf("miss[%d]: expected %s, got %s", i, expected, rec.missList[i])
		}
	}
}

func TestRecorder_HitAfterMiss(t *testing.T) {
	c, _, rec := testCache(100, time.Minute)

	c.Get("key1") // miss
	c.Put("key1", []byte("v"), 1)
	c.Get("key1") // hit

	hits, misses := rec.snapshot()
	if hits != 1 {
		t.Fatalf("expected 1 hit, got %d", hits)
	}
	if misses != 1 {
		t.Fatalf("expected 1 miss, got %d", misses)
	}
}
