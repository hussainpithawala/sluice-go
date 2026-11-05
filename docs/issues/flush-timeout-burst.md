# RFP #34: Flush Timeout Resilience Under Burst Load

**Status:** Proposed  
**Target Release:** v1.1.0  
**Dependencies:** RFP #18 (silent-loss fixes must ship first; the attribution guard and error classification are prerequisites for safe partial-commit work)  
**Blocks:** None

---

## 1. Context & Problem Statement

The engine hard-codes the flush context timeout as `4 × FlushWindow` (`internal/engine/engine.go:195-200`). A single context covers the entire flush path:

1. `ZRANGEBYSCORE` the dirty set, then pipelined `HMGET`s of up to `MaxBatchSize` payloads;
2. apply the `WriteContract` to each record;
3. `sink.BulkWrite(ctx, models)` — the sink upserts;
4. `CommitKeys` — the `ZREM` from the dirty set.

This creates a cascade failure under burst load. When an event spike pushes a band's dirty set above `MaxBatchSize`, every pod drains the same oldest N records and sends them concurrently to the sink with no coordination. The combined write pressure causes flushes to exceed the fixed timeout.

A timed-out flush commits nothing (two-phase protocol: `ZREM` only runs after a successful `BulkWrite`). The same records retry every cycle, forming a head-of-line block. The front of the queue only advances when payloads expire at `KeyTTL`, turning a transient burst into a sustained delivery delay equal to `KeyTTL` (e.g. 10 minutes with a 600 s TTL).

### 1.1 Failure Characteristics

- **Delivery latency degrades to `KeyTTL`.** Records are not flushed; they age out.
- **Write amplification.** Every pod retries the full batch every cycle, multiplying sink load and Valkey command volume.
- **Silent data loss.** Payloads that expire before being flushed are dropped. Without the `RecordUnflushedExpiry` metric, this is completely invisible.
- **No alerting surface.** Flush timeouts produce no log or dedicated metric. The only signal is the `OnFlush` callback, which most consumers do not wire up.

### 1.2 Current Mitigation

`MaxBatchSize` is kept low (e.g. 500) so each flush fits within the timeout. This caps throughput and does not address the multi-pod stampede or the lack of observability.

---

## 2. The Goal

1. **The flush timeout adapts to the actual work.** A burst that grows the batch does not cause a cascade of full-batch retries.
2. **Concurrent pod flushes for the same band do not duplicate work.** Pods that arrive while another pod holds the band skip or flush a disjoint slice.
3. **Flush failures are observable.** Operators see backlog depth, timeout rate, and per-flush duration without needing a custom `OnFlush` callback.
4. **No silent data loss from queue head-of-line blocking.** A record that cannot be flushed within a bounded time is surfaced, not silently expired.

## 3. Non-Goals

- Changing the two-phase flush/commit protocol itself (drain → BulkWrite → conditional ZREM).
- Changing the band-hashing scheme or the number of bands.
- Cross-pod coordination beyond a per-band advisory lock (no distributed consensus).

---

## 4. Proposed Design

### 4.1 Configurable Flush Timeout (`Config.FlushTimeout`)

Add `FlushTimeout time.Duration` to `engine.Config`. When set, it replaces the hard-coded `FlushWindow * 4` multiplier. When zero, the current `FlushWindow * 4` behaviour is preserved for backward compatibility.

```go
// engine.go — runBand
func (e *Engine) flushTimeout() time.Duration {
    if e.cfg.FlushTimeout > 0 {
        return e.cfg.FlushTimeout
    }
    return e.cfg.FlushWindow * 4
}
```

Both the ticker and volume-signal paths in `runBand` call `e.flushTimeout()` instead of inlining the multiplier.

### 4.2 Adaptive Batch Sizing

Add an optional adaptive batch sizer that adjusts the effective batch size based on recent flush durations. This prevents a single burst from permanently degrading throughput.

```go
type AdaptiveBatch struct {
    Max       int           // ceiling (= Config.MaxBatchSize)
    Min       int           // floor, e.g. 100
    Target    time.Duration // target flush duration, e.g. FlushTimeout * 0.6
    current   int
}

// Adjust is called after each flush with the observed duration.
func (a *AdaptiveBatch) Adjust(duration time.Duration, timedOut bool)

// Size returns the current effective batch size.
func (a *AdaptiveBatch) Size() int
```

Behaviour:
- If the flush timed out, halve `current` (floor at `Min`).
- If the flush completed in < `Target`, increase `current` by 10% (ceiling at `Max`).
- Otherwise, hold steady.

This is opt-in: `Config.AdaptiveBatch bool`. When off, `MaxBatchSize` is used directly as today.

### 4.3 Per-Band Flush Lock (Advisory, Redis-Based)

A lightweight advisory lock so that only one pod flushes a given band at a time. Other pods skip the flush cycle for that band (the next tick or volume signal retries).

```go
// shield/lock.go
func (s *Shield) TryFlushLock(ctx context.Context, band int, ttl time.Duration) (acquired bool, err error)
func (s *Shield) ReleaseFlushLock(ctx context.Context, band int) error
```

Implementation: `SET sluice:{ns}:flush_lock:{band} {pod_id} NX PX {ttl_ms}`. The TTL is set to `FlushTimeout + 5s` so a crashed pod's lock auto-expires. Release uses a Lua `GET + DEL` to avoid releasing another pod's lock.

`runBand` wraps the flush:

```go
acquired, err := e.shield.TryFlushLock(ctx, band, e.flushTimeout()+5*time.Second)
if err != nil || !acquired {
    continue // next tick
}
defer e.shield.ReleaseFlushLock(ctx, band)
```

This eliminates the multi-pod stampede: one pod flushes, others skip. The dirty set is drained faster per record (no duplicate work), and the sink sees a fraction of the concurrent writes.

### 4.4 Flush Failure Observability

The engine already calls `MetricsRecorder.RecordFlush` with an error on timeout. This proposal adds:

1. **`RecordFlushTimeout(namespace, band string, batchSize int, deadline time.Duration)`** — a dedicated counter so operators can alert on timeout rate without parsing error strings.
2. **Structured log on timeout** — `slog.Warn("sluice: flush timed out", ...)` with namespace, band, batch size, and deadline. Today a timeout produces no log.
3. **`RecordFlushLockSkip(namespace, band string)`** — counts how often a pod skips because another pod holds the lock. A sustained high rate means one pod is slow or stuck.

### 4.5 Backlog Starvation Guard

When the entire dirty set is older than `KeyTTL`, records expire from Redis before they are flushed. The `startPreEvictionFlusher` sends a volume signal when the oldest entry approaches `KeyTTL`, but if that flush also times out (e.g. under sustained burst load), the signal has no effect.

With the flush lock and adaptive batching, this path is much less likely. As a safety net, if a flush timeout occurs and the oldest record in the band is within 20% of `KeyTTL`, the engine should:

1. Log at `slog.Error` level (imminent data loss).
2. Fire `RecordFlushTimeout` with a `critical=true` label.
3. Halve the batch size immediately (via the adaptive sizer) for the next cycle.

---

## 5. Implementation Plan

Each step ends with `gofmt`, `go build`, `go vet`, and a green `go test ./...`.

1. **`Config.FlushTimeout` and `flushTimeout()` helper.** Replace the two `FlushWindow*4` call sites in `runBand`. Unit test: a custom `FlushTimeout` is honoured; zero falls back to `FlushWindow*4`.
2. **Flush failure logging.** Add `slog.Warn` on context deadline exceeded in `flushBand`. Add `RecordFlushTimeout` to `MetricsRecorder`. Unit test: a fake sink that sleeps past the deadline triggers the log and metric.
3. **Per-band flush lock.** `shield/lock.go` with `TryFlushLock` / `ReleaseFlushLock`. Integration test against Redis: two goroutines contend; only one acquires. Wire into `runBand` behind `Config.FlushLock bool`.
4. **Adaptive batch sizing.** `internal/engine/adaptive.go`. Unit test: halves on timeout, grows on fast flush, respects min/max. Wire into `flushBand` behind `Config.AdaptiveBatch bool`.
5. **Backlog starvation guard.** Check oldest score after a timeout. Unit test: a band with oldest score near `KeyTTL` triggers the error log and metric.
6. **Docs.** CHANGELOG entry, README section on burst-load tuning, migration note for `FlushTimeout`.

---

## 6. Success Criteria / Test Plan

**Unit tests** (`tests/unit/`, fake sink + fake shield):

| Test | Asserts |
|---|---|
| Custom FlushTimeout | Context deadline equals configured value, not `FlushWindow*4` |
| Zero FlushTimeout fallback | Context deadline equals `FlushWindow*4` |
| Timeout logging | `slog.Warn` emitted with correct fields on deadline exceeded |
| RecordFlushTimeout metric | Counter incremented on timeout |
| Flush lock acquire | First goroutine acquires, second does not |
| Flush lock release | After release, second goroutine acquires |
| Flush lock auto-expire | A lock whose holder crashes is acquirable after TTL |
| Adaptive halve on timeout | `Size()` halves after a timed-out flush, floors at Min |
| Adaptive grow on fast flush | `Size()` grows 10% after a sub-target flush, caps at Max |
| Starvation guard | Oldest score near KeyTTL + timeout → error log + critical metric |

**Integration tests** (Redis, multi-goroutine):

| Test | Asserts |
|---|---|
| Lock contention under load | 10 goroutines, only 1 holds lock at any time |
| Burst recovery | Inject 10K records into a band, set MaxBatchSize=500, FlushTimeout=5s. All records flushed within 30 s with zero timeouts |
| Adaptive recovery | Inject 10K records, start with MaxBatchSize=5000. Adaptive sizer reduces batch until flushes complete, then grows back |

**Load test** (staging, multi-pod):

| Scenario | Pass criteria |
|---|---|
| Event burst exceeding `MaxBatchSize` across N pods | Dirty set drains within a bounded time. Zero `RecordFlushTimeout` after initial adaptation. Zero `RecordUnflushedExpiry` |
| Sustained high-throughput ingestion | Steady-state batch size within 20% of Max. Flush lock skip rate < 5% |

---

## 7. Open Questions

1. **Flush lock scope.** Should the lock cover only `BulkWrite + CommitKeys`, or the entire flush including `DrainBand`? Locking only the write path lets multiple pods drain concurrently (reading is idempotent), but they still send duplicate upserts. Locking the full flush is simpler and eliminates duplicates entirely.
2. **Adaptive sizer state.** Per-band or global? Per-band is more accurate (one slow sink shard doesn't penalize all bands) but adds memory and complexity. Global is simpler and good enough if all bands hit the same sink.
3. **`FlushTimeout` default.** Should the default change from `FlushWindow*4` to a more conservative value (e.g. `FlushWindow*8` or a fixed 60 s)? The `4×` multiplier is aggressive for large batches.
4. **Interaction with `startPreEvictionFlusher`.** The pre-eviction flusher sends a volume signal, which without a flush lock can compound the multi-pod contention. With the flush lock, the signal is harmless (only one pod responds). Should the pre-eviction flusher be removed or kept as a belt-and-suspenders measure?

---

## 8. Migration Notes

- `Config.FlushTimeout`, `Config.FlushLock`, and `Config.AdaptiveBatch` are all opt-in with zero-value defaults that preserve current behaviour.
- `MetricsRecorder` gains `RecordFlushTimeout` and `RecordFlushLockSkip`. Existing implementations must add these methods (breaking change). A no-op default adapter will be provided.
- A conservative `MaxBatchSize` (e.g. 500) should be kept until adaptive batching is validated in staging.
