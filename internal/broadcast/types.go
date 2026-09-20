package broadcast

import "time"

// Kind distinguishes broadcast message intent in the stream.
// The subscriber treats upsert and seed uniformly (version-checked Put),
// but the kind is preserved for telemetry and future differentiation.
type Kind string

// MetricsRecorder defines the telemetry interface required by the Subscriber.
// This is typically satisfied by the core sluice MetricsRecorder via interface embedding.
type MetricsRecorder interface {
	// RecordBroadcastLag emits the time difference between the stream's
	// last-generated ID and the subscriber's current cursor.
	RecordBroadcastLag(namespace string, lag time.Duration)
}

const (
	// KindUpsert is emitted on every normal write-through.
	KindUpsert Kind = "upsert"

	// KindSeed is emitted on HotLoad promotion, signaling peers that
	// this key was just promoted to hot and they should cache it.
	KindSeed Kind = "seed"

	// KindTouch is reserved for optional TTL-extension messages.
	// Deferred per RFC §11.3 — LocalTTL handles expiry naturally.
	KindTouch Kind = "touch"
)

// Mode selects what the broadcaster emits per message and how the
// subscriber reacts.
type Mode int

const (
	// ModePayload includes the full payload in each stream entry (~1KB).
	// Subscribers can Put directly into L1 without refetching from Redis.
	// Maximum read offload; use when read:write ratio is high.
	ModePayload Mode = iota

	// ModeInvalidation sends only crn/band/ts/kind (~40B).
	// Subscribers invalidate the local entry; the next Read() miss
	// falls through to ReadJournal and heals L1 naturally.
	// Use when write velocity approaches read velocity.
	ModeInvalidation
)

// Config holds parameters for both the broadcaster and subscriber.
type Config struct {
	// Namespace is the sluice namespace, used for metrics tagging.
	Namespace string

	// Mode selects payload vs. invalidation broadcasting.
	Mode Mode

	// Stream is the Redis stream key. Default: "sl:{ns}:bcast".
	// Set by the builder; subscribers and broadcasters must agree.
	Stream string

	// MaxLen is the XADD MAXLEN ~ N retention bound (broadcaster only).
	// Default: 200_000 (≈30s of hot-write traffic at reference scale).
	MaxLen int64

	// BlockMS is the XREAD BLOCK duration for the subscriber loop.
	// Default: 1s. Lower values reduce convergence lag but increase
	// idle connection churn; higher values do the opposite.
	BlockMS time.Duration

	// Now is an injectable clock for testing. Default: time.Now.
	Now func() time.Time
}

// applyDefaults fills zero-value Config fields with production defaults.
func (c *Config) applyDefaults() {
	if c.BlockMS <= 0 {
		c.BlockMS = 1 * time.Second
	}
	if c.MaxLen <= 0 {
		c.MaxLen = 200_000
	}
	if c.Now == nil {
		c.Now = time.Now
	}
}
