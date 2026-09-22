package documentdb

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/hussainpithawala/sluice-go/internal/broadcast"
	"github.com/hussainpithawala/sluice-go/internal/localjournal"
	"github.com/hussainpithawala/sluice-go/internal/shield"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── Test Infrastructure ─────────────────────────────────────────────────────

const bcastTestDB = 15

func bcastRedisAddr() string {
	if a := os.Getenv("REDIS_ADDR"); a != "" {
		return a
	}
	return "localhost:6379"
}

// broadcastTestEnv sets up a Redis client and a local cache for subscriber tests.
type broadcastTestEnv struct {
	client redis.UniversalClient
	local  *localjournal.Cache
	stream string
}

func newBroadcastTestEnv(t *testing.T, namespace string) *broadcastTestEnv {
	t.Helper()

	client := redis.NewClient(&redis.Options{
		Addr: bcastRedisAddr(),
		DB:   bcastTestDB,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, client.Ping(ctx).Err(), "Redis must be reachable")
	require.NoError(t, client.FlushDB(ctx).Err(), "isolate test DB")

	local := localjournal.New(localjournal.Config{
		Namespace:  namespace,
		MaxEntries: 1024,
		LocalTTL:   60 * time.Second,
	})

	return &broadcastTestEnv{
		client: client,
		local:  local,
		stream: shield.BroadcastKey(namespace),
	}
}

func (e *broadcastTestEnv) Close() {
	_ = e.client.Close()
}

// emitMessage writes a broadcast message directly to the stream (simulates a peer pod).
func (e *broadcastTestEnv) emitMessage(t *testing.T, ctx context.Context, crn string, ts int64, kind broadcast.Kind, payload []byte) {
	values := []interface{}{
		"crn", crn,
		"band", 0,
		"ts", ts,
		"kind", string(kind),
	}
	if payload != nil {
		values = append(values, "payload", payload)
	}
	err := e.client.XAdd(ctx, &redis.XAddArgs{
		Stream: e.stream,
		MaxLen: 1000,
		Approx: true,
		Values: values,
	}).Err()
	require.NoError(t, err, "emitMessage at %v", t)
}

// subscriberTestMetrics is a minimal MetricsRecorder for subscriber tests.
type subscriberTestMetrics struct {
	mu       sync.Mutex
	lagCalls int
	lastLag  time.Duration
}

func (m *subscriberTestMetrics) RecordBroadcastLag(_ string, lag time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lagCalls++
	m.lastLag = lag
}

func (m *subscriberTestMetrics) snapshot() (int, time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lagCalls, m.lastLag
}

// ── Lifecycle Tests ─────────────────────────────────────────────────────────

func TestSubscriber_StartStop(t *testing.T) {
	env := newBroadcastTestEnv(t, "test_bcast_lifecycle")
	defer env.Close()

	metrics := &subscriberTestMetrics{}
	sub := broadcast.NewSubscriber(
		env.client,
		env.stream,
		env.local,
		metrics,
		broadcast.Config{
			Namespace:           "test_bcast_lifecycle",
			Mode:                broadcast.ModePayload,
			Block_in_milli_secs: 100 * time.Millisecond, // short block for fast test
		},
	)

	// Start should be non-blocking
	sub.Start()

	// Give the loop a moment to start
	time.Sleep(50 * time.Millisecond)

	// Stop should block until the goroutine exits (bounded by BlockMS + margin)
	done := make(chan struct{})
	go func() {
		sub.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Success: Stop returned
	case <-time.After(3 * time.Second):
		t.Fatal("Stop() did not return within 3s — goroutine leak or deadlock")
	}
}

func TestSubscriber_StopIsIdempotent(t *testing.T) {
	env := newBroadcastTestEnv(t, "test_bcast_stop_idem")
	defer env.Close()

	sub := broadcast.NewSubscriber(
		env.client,
		env.stream,
		env.local,
		nil,
		broadcast.Config{
			Namespace:           "test_bcast_stop_idem",
			Mode:                broadcast.ModePayload,
			Block_in_milli_secs: 50 * time.Millisecond,
		},
	)

	sub.Start()
	time.Sleep(20 * time.Millisecond)

	// Calling Stop multiple times must not panic or deadlock
	sub.Stop()
	sub.Stop()
	sub.Stop()
}

// ── Message Application Tests ───────────────────────────────────────────────

func TestSubscriber_AppliesUpsertMessage(t *testing.T) {
	env := newBroadcastTestEnv(t, "test_bcast_upsert")
	defer env.Close()

	sub := broadcast.NewSubscriber(
		env.client,
		env.stream,
		env.local,
		nil,
		broadcast.Config{
			Namespace:           "test_bcast_upsert",
			Mode:                broadcast.ModePayload,
			Block_in_milli_secs: 100 * time.Millisecond,
		},
	)
	sub.Start()
	defer sub.Stop()

	ctx := context.Background()
	payload := []byte(`{"user":"alice","plan":"pro"}`)

	// Emit a message as if from a peer pod
	env.emitMessage(t, ctx, "session_001", time.Now().UnixMilli(), broadcast.KindUpsert, payload)

	// Wait for the subscriber to process
	require.Eventually(t, func() bool {
		p, _, ok := env.local.Get("session_001")
		return ok && string(p) == string(payload)
	}, 3*time.Second, 10*time.Millisecond, "subscriber must apply upsert to L1")
}

func TestSubscriber_AppliesSeedMessage(t *testing.T) {
	env := newBroadcastTestEnv(t, "test_bcast_seed")
	defer env.Close()

	sub := broadcast.NewSubscriber(
		env.client,
		env.stream,
		env.local,
		nil,
		broadcast.Config{
			Namespace:           "test_bcast_seed",
			Mode:                broadcast.ModePayload,
			Block_in_milli_secs: 100 * time.Millisecond,
		},
	)
	sub.Start()
	defer sub.Stop()

	ctx := context.Background()
	payload := []byte(`{"hot":"loaded"}`)

	env.emitMessage(t, ctx, "hot_key_001", time.Now().UnixMilli(), broadcast.KindSeed, payload)

	require.Eventually(t, func() bool {
		p, _, ok := env.local.Get("hot_key_001")
		return ok && string(p) == string(payload)
	}, 3*time.Second, 10*time.Millisecond, "subscriber must apply seed to L1")
}

// ── Version Gating Tests ────────────────────────────────────────────────────

func TestSubscriber_VersionGating_RejectsOlder(t *testing.T) {
	env := newBroadcastTestEnv(t, "test_bcast_version")
	defer env.Close()

	// Pre-populate L1 with a newer version
	env.local.Put("versioned_key", []byte(`{"v":2}`), 200)

	sub := broadcast.NewSubscriber(
		env.client,
		env.stream,
		env.local,
		nil,
		broadcast.Config{
			Namespace:           "test_bcast_version",
			Mode:                broadcast.ModePayload,
			Block_in_milli_secs: 100 * time.Millisecond,
		},
	)
	sub.Start()
	defer sub.Stop()

	ctx := context.Background()

	// Emit an OLDER version (ts=100 < resident ts=200)
	env.emitMessage(t, ctx, "versioned_key", 100, broadcast.KindUpsert, []byte(`{"v":1}`))

	// Wait a bit for the subscriber to process (or reject)
	time.Sleep(500 * time.Millisecond)

	// L1 must still hold the newer version
	p, v, ok := env.local.Get("versioned_key")
	require.True(t, ok)
	assert.Equal(t, int64(200), v, "resident version must not regress")
	assert.Equal(t, []byte(`{"v":2}`), p, "payload must not regress")
}

func TestSubscriber_VersionGating_AcceptsNewer(t *testing.T) {
	env := newBroadcastTestEnv(t, "test_bcast_version_newer")
	defer env.Close()

	// Pre-populate L1 with an older version
	env.local.Put("versioned_key", []byte(`{"v":1}`), 100)

	sub := broadcast.NewSubscriber(
		env.client,
		env.stream,
		env.local,
		nil,
		broadcast.Config{
			Namespace:           "test_bcast_version_newer",
			Mode:                broadcast.ModePayload,
			Block_in_milli_secs: 100 * time.Millisecond,
		},
	)
	sub.Start()
	defer sub.Stop()

	ctx := context.Background()

	// Emit a NEWER version (ts=300 > resident ts=100)
	env.emitMessage(t, ctx, "versioned_key", 300, broadcast.KindUpsert, []byte(`{"v":3}`))

	require.Eventually(t, func() bool {
		p, v, ok := env.local.Get("versioned_key")
		return ok && v == 300 && string(p) == `{"v":3}`
	}, 3*time.Second, 10*time.Millisecond, "newer version must be applied")
}

// ── Invalidation Mode Tests ─────────────────────────────────────────────────

func TestSubscriber_InvalidationMode(t *testing.T) {
	env := newBroadcastTestEnv(t, "test_bcast_invalidation")
	defer env.Close()

	// Pre-populate L1
	env.local.Put("inv_key", []byte(`{"stale":true}`), 100)

	sub := broadcast.NewSubscriber(
		env.client,
		env.stream,
		env.local,
		nil,
		broadcast.Config{
			Namespace:           "test_bcast_invalidation",
			Mode:                broadcast.ModeInvalidation, // <-- invalidation mode
			Block_in_milli_secs: 100 * time.Millisecond,
		},
	)
	sub.Start()
	defer sub.Stop()

	ctx := context.Background()

	// In invalidation mode, messages carry no payload. The subscriber should
	// call local.Invalidate(crn) so the next Read() falls through to Redis.
	env.emitMessage(t, ctx, "inv_key", 200, broadcast.KindUpsert, nil)

	require.Eventually(t, func() bool {
		_, _, ok := env.local.Get("inv_key")
		return !ok // entry must be invalidated (removed)
	}, 3*time.Second, 10*time.Millisecond, "invalidation mode must remove L1 entry")
}

// ── Cross-Pod Convergence Test ──────────────────────────────────────────────

func TestSubscriber_CrossPodConvergence(t *testing.T) {
	env := newBroadcastTestEnv(t, "test_bcast_convergence")
	defer env.Close()

	// Simulate Pod B (subscriber only)
	sub := broadcast.NewSubscriber(
		env.client,
		env.stream,
		env.local,
		nil,
		broadcast.Config{
			Namespace:           "test_bcast_convergence",
			Mode:                broadcast.ModePayload,
			Block_in_milli_secs: 100 * time.Millisecond,
		},
	)
	sub.Start()
	defer sub.Stop()

	ctx := context.Background()

	// Simulate Pod A writing (journal + broadcast)
	// In production this would be shield.Write() which pipelines XADD.
	// Here we simulate by writing the broadcast message directly.
	payload := []byte(`{"convergence":"test","v":1}`)
	ts := time.Now().UnixMilli()
	env.emitMessage(t, ctx, "converge_key", ts, broadcast.KindUpsert, payload)

	// Pod B must converge within broadcast lag (< 3s in test, < 10ms in prod)
	require.Eventually(t, func() bool {
		p, _, ok := env.local.Get("converge_key")
		return ok && string(p) == string(payload)
	}, 3*time.Second, 10*time.Millisecond,
		"Pod B must converge to Pod A's write via broadcast stream")
}

// ── Stream Trim / Lazy Heal Tests ───────────────────────────────────────────

func TestSubscriber_StreamTrimDoesNotCrash(t *testing.T) {
	env := newBroadcastTestEnv(t, "test_bcast_trim")
	defer env.Close()

	sub := broadcast.NewSubscriber(
		env.client,
		env.stream,
		env.local,
		nil,
		broadcast.Config{
			Namespace:           "test_bcast_trim",
			Mode:                broadcast.ModePayload,
			Block_in_milli_secs: 100 * time.Millisecond,
		},
	)
	sub.Start()
	defer sub.Stop()

	ctx := context.Background()

	// Emit several messages
	for i := 0; i < 10; i++ {
		env.emitMessage(t, ctx, fmt.Sprintf("trim_key_%d", i), time.Now().UnixMilli(),
			broadcast.KindUpsert, []byte(fmt.Sprintf(`{"i":%d}`, i)))
	}

	// Wait for subscriber to process at least some
	time.Sleep(300 * time.Millisecond)

	// Aggressively trim the stream to 1 entry (simulates MAXLEN expiry or manual trim)
	err := env.client.XTrimMaxLen(ctx, env.stream, 1).Err()
	require.NoError(t, err)

	// Subscriber must NOT crash. Emit a new message and verify it's still processing.
	env.emitMessage(t, ctx, "post_trim_key", time.Now().UnixMilli(),
		broadcast.KindUpsert, []byte(`{"after":"trim"}`))

	require.Eventually(t, func() bool {
		p, _, ok := env.local.Get("post_trim_key")
		return ok && string(p) == `{"after":"trim"}`
	}, 3*time.Second, 10*time.Millisecond,
		"subscriber must recover after stream trim and process new messages")
}

// ── Lag Metric Tests ────────────────────────────────────────────────────────

func TestSubscriber_LagMetric(t *testing.T) {
	env := newBroadcastTestEnv(t, "test_bcast_lag")
	defer env.Close()

	metrics := &subscriberTestMetrics{}
	sub := broadcast.NewSubscriber(
		env.client,
		env.stream,
		env.local,
		metrics,
		broadcast.Config{
			Namespace:           "test_bcast_lag",
			Mode:                broadcast.ModePayload,
			Block_in_milli_secs: 100 * time.Millisecond,
		},
	)
	sub.Start()
	defer sub.Stop()

	ctx := context.Background()

	// Emit a message to establish a cursor
	env.emitMessage(t, ctx, "lag_key", time.Now().UnixMilli(),
		broadcast.KindUpsert, []byte(`{"lag":"test"}`))

	// Wait for the lag monitor to fire at least once (it ticks every 1s)
	require.Eventually(t, func() bool {
		calls, _ := metrics.snapshot()
		return calls > 0
	}, 5*time.Second, 100*time.Millisecond,
		"lag metric must be emitted at least once")

	// Verify lag is non-negative
	_, lastLag := metrics.snapshot()
	assert.GreaterOrEqual(t, lastLag, time.Duration(0),
		"lag must be non-negative")
}

// ── Graceful Shutdown Under Load ────────────────────────────────────────────

func TestSubscriber_GracefulShutdownUnderLoad(t *testing.T) {
	env := newBroadcastTestEnv(t, "test_bcast_shutdown_load")
	defer env.Close()

	sub := broadcast.NewSubscriber(
		env.client,
		env.stream,
		env.local,
		nil,
		broadcast.Config{
			Namespace:           "test_bcast_shutdown_load",
			Mode:                broadcast.ModePayload,
			Block_in_milli_secs: 500 * time.Millisecond, // longer block to test unblocking
		},
	)
	sub.Start()

	ctx := context.Background()

	// Flood with messages
	var emitWg sync.WaitGroup
	for i := 0; i < 50; i++ {
		emitWg.Add(1)
		go func(idx int) {
			defer emitWg.Done()
			env.emitMessage(t, ctx, fmt.Sprintf("flood_%d", idx), time.Now().UnixMilli(),
				broadcast.KindUpsert, []byte(fmt.Sprintf(`{"flood":%d}`, idx)))
		}(i)
	}
	emitWg.Wait()

	// Give subscriber a moment to start processing
	time.Sleep(100 * time.Millisecond)

	// Stop must return promptly even with a 500ms BlockMS
	start := time.Now()
	sub.Stop()
	elapsed := time.Since(start)

	assert.Less(t, elapsed, 2*time.Second,
		"Stop() must unblock XREAD and return promptly, took %v", elapsed)
}

// ── Multiple Subscribers (Multi-Pod Simulation) ─────────────────────────────

func TestSubscriber_MultiplePods(t *testing.T) {
	env := newBroadcastTestEnv(t, "test_bcast_multipod")
	defer env.Close()

	// Create 3 independent L1 caches simulating 3 pods
	locals := make([]*localjournal.Cache, 3)
	subs := make([]*broadcast.Subscriber, 3)
	for i := 0; i < 3; i++ {
		locals[i] = localjournal.New(localjournal.Config{
			Namespace:  "test_bcast_multipod",
			MaxEntries: 1024,
			LocalTTL:   60 * time.Second,
		})
		subs[i] = broadcast.NewSubscriber(
			env.client,
			env.stream,
			locals[i],
			nil,
			broadcast.Config{
				Namespace:           "test_bcast_multipod",
				Mode:                broadcast.ModePayload,
				Block_in_milli_secs: 100 * time.Millisecond,
			},
		)
		subs[i].Start()
	}
	defer func() {
		for _, s := range subs {
			s.Stop()
		}
	}()

	ctx := context.Background()
	payload := []byte(`{"multi":"pod"}`)
	env.emitMessage(t, ctx, "multi_pod_key", time.Now().UnixMilli(),
		broadcast.KindUpsert, payload)

	// All 3 pods must converge
	for i := 0; i < 3; i++ {
		podIdx := i
		require.Eventually(t, func() bool {
			p, _, ok := locals[podIdx].Get("multi_pod_key")
			return ok && string(p) == string(payload)
		}, 3*time.Second, 10*time.Millisecond,
			"pod %d must converge", podIdx)
	}
}

// ── Broadcast + Subscriber Integration (Shield → Stream → Subscriber) ───────

func TestSubscriber_ShieldWriteBroadcastsToSubscriber(t *testing.T) {
	env := newBroadcastTestEnv(t, "test_bcast_shield")
	defer env.Close()

	// Create a real Shield with broadcast enabled
	sh, err := shield.New(shield.RedisConfig{
		Addrs:        []string{bcastRedisAddr()},
		ClusterMode:  false,
		DB:           bcastTestDB,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     10,
	}, "test_bcast_shield", 16, 30*time.Second, 4*time.Hour)
	require.NoError(t, err)
	defer func(sh *shield.Shield) {
		err := sh.Close()
		if err != nil {
			slog.Debug("Unable to close shield")
		}
	}(sh)

	sh.EnableBroadcast(shield.BroadcastConfig{
		Mode:   shield.BroadcastPayload,
		MaxLen: 1000,
	})

	// Start a subscriber listening on the same stream
	sub := broadcast.NewSubscriber(
		env.client,
		shield.BroadcastKey("test_bcast_shield"),
		env.local,
		nil,
		broadcast.Config{
			Namespace:           "test_bcast_shield",
			Mode:                broadcast.ModePayload,
			Block_in_milli_secs: 100 * time.Millisecond,
		},
	)
	sub.Start()
	defer sub.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Write via Shield (which should pipeline XADD)
	payload := []byte(`{"shield":"broadcast"}`)
	err = sh.Write(ctx, "shield_bcast_key", payload)
	require.NoError(t, err)

	// Subscriber must converge
	require.Eventually(t, func() bool {
		p, _, ok := env.local.Get("shield_bcast_key")
		return ok && string(p) == string(payload)
	}, 3*time.Second, 10*time.Millisecond,
		"subscriber must converge from Shield.Write broadcast")
}
