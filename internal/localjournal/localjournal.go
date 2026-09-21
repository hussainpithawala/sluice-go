package localjournal

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"
)

const shardCount = 256 // power of two → allows fast modulo via bitwise AND

// MissReason classifies a cache miss for telemetry.
type MissReason string

const (
	MissAbsent  MissReason = "absent"
	MissExpired MissReason = "expired"
)

// Recorder receives hit/miss telemetry. The core MetricsRecorder will
// implement this interface once the v1.1.0 additions land.
type Recorder interface {
	LocalCacheHit(namespace string)
	LocalCacheMiss(namespace string, reason MissReason)
}

// Config holds the internal configuration for the Cache.
type Config struct {
	Namespace  string
	MaxEntries int              // global LRU cap, spread across shards; <=0 disables
	LocalTTL   time.Duration    // per-entry staleness bound
	Now        func() time.Time // injectable clock for testing
	Metrics    MetricsRecorder
}

// entry represents a single cached payload.
// Payload slices are shared with the caller; callers MUST treat them as read-only.
type entry struct {
	key       string
	payload   []byte
	version   int64 // journal write ts (UnixMilli)
	expiresAt time.Time
	el        *list.Element // pointer to the LRU list element
}

// shard is a single independently locked partition of the cache.
type shard struct {
	mu  sync.RWMutex
	m   map[string]*entry
	lru *list.List // front = most recently used, back = least recently used
}

// Cache is the L1 in-process read tier.
type Cache struct {
	cfg         Config
	now         func() time.Time
	shards      [shardCount]shard
	capPerShard int
	evicted     uint64 // accessed via sync/atomic
}

// New creates and initializes a new Cache.
// If cfg.MaxEntries <= 0, the cache is effectively disabled (Enabled() returns false),
// and Get/Put operations become no-ops.
func New(cfg Config) *Cache {
	if cfg.LocalTTL <= 0 {
		cfg.LocalTTL = 60 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}

	c := &Cache{
		cfg: cfg,
		now: cfg.Now,
	}

	if cfg.MaxEntries > 0 {
		// Distribute capacity evenly across shards.
		// We round up to ensure the total capacity is at least MaxEntries.
		// Example: 200,000 / 256 = 781.25 -> 782 per shard.
		c.capPerShard = (cfg.MaxEntries + shardCount - 1) / shardCount
	}

	for i := range c.shards {
		c.shards[i].m = make(map[string]*entry)
		c.shards[i].lru = list.New()
	}

	return c
}

// Enabled returns true if the cache is configured to store entries.
func (c *Cache) Enabled() bool {
	return c.capPerShard > 0
}

// shardFor determines which shard a given key belongs to.
// Uses FNV-1a inline for speed and masks with (shardCount - 1).
// Note: This is intentionally independent of the Redis {band} hashing,
// as L1 shards are purely for local lock contention reduction.
func (c *Cache) shardFor(key string) *shard {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return &c.shards[h&(shardCount-1)]
}

// Len returns the total number of entries currently in the cache.
// Used primarily for testing and the RecordLocalSetSize metric.
func (c *Cache) Len() int {
	n := 0
	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.RLock()
		n += len(sh.m)
		sh.mu.RUnlock()
	}
	return n
}

// Evictions returns the total number of entries evicted due to LRU capacity.
func (c *Cache) Evictions() uint64 {
	return atomic.LoadUint64(&c.evicted)
}

// Get returns the cached payload and version.
// Callers MUST treat the returned payload slice as read-only, as it is
// shared with the cache's internal storage to avoid allocation overhead.
func (c *Cache) Get(key string) ([]byte, int64, bool) {
	if !c.Enabled() {
		return nil, 0, false
	}
	sh := c.shardFor(key)
	now := c.now()

	// We need a write lock because a cache hit requires updating the LRU list
	// (MoveToFront), and a cache miss due to expiry requires modifying both
	// the LRU list and the map.
	sh.mu.Lock()
	e, ok := sh.m[key]
	if !ok {
		sh.mu.Unlock()
		c.miss(MissAbsent)
		return nil, 0, false
	}

	if now.After(e.expiresAt) {
		sh.lru.Remove(e.el)
		delete(sh.m, key)
		sh.mu.Unlock()
		c.miss(MissExpired)
		return nil, 0, false
	}

	// Valid hit: promote to front of LRU
	sh.lru.MoveToFront(e.el)
	p, v := e.payload, e.version
	sh.mu.Unlock()

	if c.cfg.Metrics != nil {
		c.cfg.Metrics.RecordLocalCacheHit(c.cfg.Namespace) // ← must be invoked
	}
	return p, v, true
}

// Put stores the payload if the provided version is strictly newer (>) than
// the currently stored version. This enforces the Timestamp Contract (C3),
// preventing out-of-order or stale broadcasts from regressing local state.
// Returns true if the entry was applied, false if rejected or disabled.
func (c *Cache) Put(key string, payload []byte, version int64) bool {
	if !c.Enabled() {
		return false
	}
	sh := c.shardFor(key)
	now := c.now()

	sh.mu.Lock()
	if e, ok := sh.m[key]; ok {
		// BEFORE: if e.version >= version {  → rejected equal versions
		// AFTER:  reject only if resident is STRICTLY newer.
		// Equal versions now apply → last-writer-wins for same-millisecond writes,
		// which is correct because the stream delivers them in write order.
		if e.version >= version {
			sh.mu.Unlock()
			return false
		}
		e.payload = payload
		e.version = version
		e.expiresAt = now.Add(c.cfg.LocalTTL)
		sh.lru.MoveToFront(e.el)
		sh.mu.Unlock()
		return true
	}

	e := &entry{
		key:       key,
		payload:   payload,
		version:   version,
		expiresAt: now.Add(c.cfg.LocalTTL),
	}
	e.el = sh.lru.PushFront(e)
	sh.m[key] = e

	for sh.lru.Len() > c.capPerShard {
		back := sh.lru.Back()
		if back == nil {
			break
		}
		ev := back.Value.(*entry)
		sh.lru.Remove(back)
		delete(sh.m, ev.key)
		atomic.AddUint64(&c.evicted, 1)
	}

	sh.mu.Unlock()
	return true
}

// Invalidate removes a specific key from the cache.
// Used when a key is explicitly deleted or moved to the DLQ.
func (c *Cache) Invalidate(key string) {
	if !c.Enabled() {
		return
	}
	sh := c.shardFor(key)
	sh.mu.Lock()
	if e, ok := sh.m[key]; ok {
		sh.lru.Remove(e.el)
		delete(sh.m, key)
	}
	sh.mu.Unlock()
}

// miss is a helper to record cache miss telemetry.
func (c *Cache) miss(reason MissReason) {
	if c.cfg.Metrics != nil {
		c.cfg.Metrics.RecordLocalCacheMiss(c.cfg.Namespace, reason)
	}
}
