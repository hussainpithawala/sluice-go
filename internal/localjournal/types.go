package localjournal

import (
	"time"

	"github.com/hussainpithawala/sluice-go/internal/shield"
)

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
	Broadcast  shield.BroadcastMode
	Retention  int64
}

type MetricsRecorder interface {
	// RecordLocalCacheHit is called when a Read() is served from L1 memory.
	RecordLocalCacheHit(namespace string)

	// RecordLocalCacheMiss is called when a Read() falls through L1 to L2/L3.
	// reason is typically "absent" or "expired".
	RecordLocalCacheMiss(namespace string, reason MissReason)

	// RecordLocalSetSize is used to track L1 memory footprint (entries count).
	RecordLocalSetSize(namespace string, size int)
}
