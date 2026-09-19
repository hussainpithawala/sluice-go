package localjournal

import "time"

type LocalCacheMode int

const (
	LocalCacheOff      LocalCacheMode = iota // Default: zero behavior change
	LocalCacheLazy                           // Phase 1: Demand-filled, TTL-bounded
	LocalCachePushPull                       // Phase 2: Stream-broadcast (future)
)

// LocalCacheConfig configures the in-process L1 read tier.
type LocalCacheConfig struct {
	Mode       LocalCacheMode
	MaxEntries int           // Global LRU cap (default 200,000)
	LocalTTL   time.Duration // Staleness bound (default 60s)
}
