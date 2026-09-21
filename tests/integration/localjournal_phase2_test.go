package integration

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hussainpithawala/sluice-go"
	"github.com/hussainpithawala/sluice-go/internal/broadcast"
	"github.com/hussainpithawala/sluice-go/internal/localjournal"
	"github.com/hussainpithawala/sluice-go/internal/shield"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── 2.4 ─ ReadFresh bypasses L1 ─────────────────────────────────────────────

// TestReadFresh_BypassesL1 proves the strong-consistency escape hatch:
// after L1 is seeded with a stale value and the journal advances, Read()
// keeps serving the stale L1 value while ReadFresh() returns the journal's
// authoritative value.
func TestReadFresh_BypassesL1(t *testing.T) {
	const namespace = "test_readfresh"

	sh, err := shield.New(shield.RedisConfig{
		Addrs:        []string{l1RedisAddr()},
		ClusterMode:  false,
		DB:           l1TestDB,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     10,
	}, namespace, 16, 30*time.Second, 4*time.Hour)
	require.NoError(t, err)
	defer sh.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, sh.Client().FlushDB(ctx).Err())

	rec := newCountingRecorder()
	local := localjournal.New(localjournal.Config{
		Namespace:  namespace,
		MaxEntries: 1024,
		LocalTTL:   60 * time.Second,
		Metrics:    rec,
	})

	s := sluice.NewForTest(sluice.TestConfig{
		Namespace: namespace,
		Shield:    sh,
		Local:     local,
		Metrics:   rec,
	})

	const key = "readfresh_key"
	stalePayload := []byte(`{"v":1,"state":"stale"}`)
	freshPayload := []byte(`{"v":2,"state":"fresh"}`)

	// Write via Sluice: journals to Redis AND write-through seeds L1.
	require.NoError(t, s.Write(ctx, key, stalePayload))

	// Read() hits L1 → stale payload.
	got, err := s.Read(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, stalePayload, got)
	hits, _ := rec.snapshot()
	assert.Equal(t, 1, hits, "first Read must hit L1")

	// Advance the journal directly (simulates another pod's write landing).
	require.NoError(t, sh.Write(ctx, key, freshPayload))

	// Read() STILL hits L1 → returns the stale value (bounded staleness).
	got, err = s.Read(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, stalePayload, got, "Read() must serve the L1 (stale) value")
	hits, _ = rec.snapshot()
	assert.Equal(t, 2, hits, "second Read must also hit L1")

	// ReadFresh() bypasses L1 → returns the journal's authoritative value.
	got, err = s.ReadFresh(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, freshPayload, got, "ReadFresh must return the journal's fresh value")
}

// ── 2.5 ─ HotLoad emits kind=seed ──────────────────────────────────────────

// TestHotLoad_EmitsSeedBroadcast proves HotLoad emits exactly one broadcast
// message with kind=seed (not a duplicate upsert), carrying the payload and
// a valid UnixMilli ts.
func TestHotLoad_EmitsSeedBroadcast(t *testing.T) {
	const namespace = "test_hotload_seed"

	sh, err := shield.New(shield.RedisConfig{
		Addrs:        []string{l1RedisAddr()},
		ClusterMode:  false,
		DB:           l1TestDB,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     10,
	}, namespace, 16, 30*time.Second, 4*time.Hour)
	require.NoError(t, err)
	defer sh.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, sh.Client().FlushDB(ctx).Err())

	sh.EnableBroadcast(shield.BroadcastConfig{
		Mode:   shield.BroadcastPayload,
		MaxLen: 1000,
	})

	const key = "hotload_seed_key"
	payload := []byte(`{"promoted":"hot"}`)

	// HotLoad seeds the journal, sets the hot marker, and emits kind=seed.
	require.NoError(t, sh.HotLoad(ctx, key, payload))

	// Hot marker must be set.
	isHot, err := sh.IsHot(ctx, key)
	require.NoError(t, err)
	assert.True(t, isHot, "HotLoad must set the hot marker")

	// Exactly one stream entry, kind=seed, with the payload.
	streamKey := shield.BroadcastKey(namespace)
	entries, err := sh.Client().XRange(ctx, streamKey, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, entries, 1, "HotLoad must emit exactly one broadcast (no duplicate upsert)")

	entry := entries[0]
	assert.Equal(t, key, entry.Values["crn"])
	assert.Equal(t, "seed", entry.Values["kind"], "HotLoad must emit kind=seed")
	assert.Equal(t, payload, []byte(entry.Values["payload"].(string)))

	ts, err := strconv.ParseInt(entry.Values["ts"].(string), 10, 64)
	require.NoError(t, err)
	assert.Positive(t, ts, "ts must be a valid UnixMilli timestamp")
}

// ── 2.7 ─ PushPull end-to-end, two pods ────────────────────────────────────

// TestPushPull_EndToEnd_TwoPods is the RFC §12 exit criterion for Phase 2:
// Pod A writes, Pod B (which never wrote) converges via the broadcast stream,
// and Pod B's subsequent Read() is served from L1 with zero Redis reads.
func TestPushPull_EndToEnd_TwoPods(t *testing.T) {
	const namespace = "test_pushpull_e2e"

	redisCfg := shield.RedisConfig{
		Addrs:        []string{l1RedisAddr()},
		ClusterMode:  false,
		DB:           l1TestDB,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     10,
	}

	// Isolate: flush the test DB before building any pod.
	{
		tmp, err := shield.New(redisCfg, namespace, 16, 30*time.Second, 4*time.Hour)
		require.NoError(t, err)
		fctx, fcancel := context.WithTimeout(context.Background(), 5*time.Second)
		require.NoError(t, tmp.Client().FlushDB(fctx).Err())
		fcancel()
		require.NoError(t, tmp.Close())
	}

	// buildPod assembles a full PushPull pod: shield+broadcaster, L1 cache,
	// subscriber, and a Sluice engine wired together.
	buildPod := func(t *testing.T) (*sluice.Sluice, *broadcast.Subscriber, *countingRecorder, *localjournal.Cache, *shield.Shield) {
		t.Helper()

		sh, err := shield.New(redisCfg, namespace, 16, 30*time.Second, 4*time.Hour)
		require.NoError(t, err)

		sh.EnableBroadcast(shield.BroadcastConfig{
			Mode:   shield.BroadcastPayload,
			MaxLen: 1000,
		})

		rec := newCountingRecorder()
		local := localjournal.New(localjournal.Config{
			Namespace:  namespace,
			MaxEntries: 1024,
			LocalTTL:   60 * time.Second,
			Metrics:    rec,
		})

		sub := broadcast.NewSubscriber(
			sh.Client(),
			shield.BroadcastKey(namespace),
			local,
			nil, // subscriber lag metrics not asserted in this e2e test
			broadcast.Config{
				Namespace: namespace,
				Mode:      broadcast.ModePayload,
				Stream:    shield.BroadcastKey(namespace),
				BlockMS:   100 * time.Millisecond,
			},
		)
		sub.Start()

		s := sluice.NewForTest(sluice.TestConfig{
			Namespace: namespace,
			Shield:    sh,
			Local:     local,
			Metrics:   rec,
		})

		return s, sub, rec, local, sh
	}

	podA, subA, _, _, shA := buildPod(t)
	defer shA.Close()
	defer subA.Stop()

	podB, subB, recB, localB, shB := buildPod(t)
	defer shB.Close()
	defer subB.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const key = "pushpull_session"
	payload := []byte(`{"cross":"pod","v":1}`)

	// Pod A writes. This journals to Redis, write-through seeds Pod A's L1,
	// and emits a broadcast to the shared stream.
	require.NoError(t, podA.Write(ctx, key, payload))

	// Pod B (which never wrote) must converge via the broadcast stream.
	require.Eventually(t, func() bool {
		p, _, ok := localB.Get(key)
		return ok && string(p) == string(payload)
	}, 3*time.Second, 10*time.Millisecond,
		"Pod B's L1 must converge from Pod A's broadcast")

	// Install a Redis-read spy on Pod B BEFORE reading.
	var redisReads int64
	recB.redisOpHook = func(op string) {
		if op == "read" || op == "readjournal" || op == "readfresh" {
			atomic.AddInt64(&redisReads, 1)
		}
	}

	// Pod B Read() must be served from L1 — zero Redis reads.
	got, err := podB.Read(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, payload, got, "Pod B Read must return the converged payload")
	assert.Zero(t, atomic.LoadInt64(&redisReads),
		"Pod B Read must be served from L1, not Redis")

	hitsB, _ := recB.snapshot()
	assert.GreaterOrEqual(t, hitsB, 1, "Pod B must record an L1 hit")
}
