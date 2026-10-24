# RFP #30: Context-Aware Graceful Shutdown & Drain Loop

**Status:** Approved with review amendments (2026-10-17), validated against `b6966c4` (v1.1.0)
**Target Release:** next minor after RFP #29 (behavior change: an incomplete drain now returns an error)
**Sequence:** 2 of 3 (RFP #29 → #30 → #31)
**Dependencies:** None technically; lands after RFP #29 to keep the patch and minor releases separate. Reuses the shared fault-injection helper from RFP #29 section 8.
**Blocks:** None. If RFP #31 adds a background index worker, it must integrate with this drain.

---

## 1. Context & Problem Statement

On shutdown, `runBand` (`internal/engine/engine.go`) performs a single `flushBand` under a hard-coded 30s timeout and discards the result. `flushBand` drains at most `MaxBatchSize` records. `Engine.DrainAndStop()` takes no context and blocks on `wg.Wait()`. `Sluice.DrainAndClose(ctx)` uses `ctx` only for `sk.Close`.

Consequences:

- A backlog larger than one batch is **left in Redis** while `DrainAndClose` returns `nil`.
- A slow or hung sink can hold shutdown past the caller's deadline, because the caller's context never reaches the engine.
- Final flush errors are discarded.

Records left in Redis are not lost: unflushed payloads are persistent and are picked up by the next instance. That holds only if Redis itself is durable, and the caller currently has no signal that anything was left behind.

### 1.1 Defects

| Where | Defect | Effect |
|---|---|---|
| `runBand` stop branch | One `flushBand`, error discarded (`_ = e.flushBand(...)`) | Backlog > `MaxBatchSize` is left dirty with no error |
| `Engine.DrainAndStop` | No `context.Context`; blocks on `wg.Wait()` | Caller's deadline ignored |
| `runBand` stop branch | Fixed 30s timeout | Neither configurable nor tied to the caller |
| `Sluice.DrainAndClose` | Returns `nil` regardless of drain outcome | Callers cannot tell complete from incomplete |
| `flushBand` | Returns only `error` | No count of records flushed |

---

## 2. The Goal

1. **Drain until empty or the deadline**, whichever comes first, per band.
2. **Respect the caller's deadline**: shutdown returns shortly after `ctx` expires.
3. **Report the outcome** with a `DrainResult`, so incomplete drains are visible.
4. **No new loss paths.** Anything not flushed stays dirty and persistent in Redis for the next instance.

## 3. Non-Goals

- Redis durability or failover (AOF/replication requirements stay as documented).
- A cross-pod flush lease.
- Changing the two-phase flush/commit protocol.

---

## 4. Proposed Design

### 4.1 Types

```go
// DrainResult describes the outcome of a shutdown drain.
type DrainResult struct {
    RecordsFlushed      int64 // committed to the sink during the drain
    RecordsDeadLettered int64 // moved to the DLQ during the drain (permanent failures)
    RecordsAbandoned    int64 // still dirty in Redis when the drain stopped (recoverable)
    LedgerRecordsDropped int64 // ledger records not persisted at shutdown; only when WithLedger is set
    FinalError       error // last flush error, or ctx error on timeout; nil if complete
}

var ErrIncompleteDrain = errors.New("sluice: drain incomplete")
```

"Abandoned" means *left in Redis for the next instance*, not lost. `RecordsDeadLettered` keeps the accounting identity `Flushed + Abandoned + DeadLettered == seeded` visible to callers: a drain that dead-letters 400 poison records and flushes 0 is a different outcome from one that flushes 0 and abandons 400, and operators can tell them apart at a glance.

### 4.2 Engine (`internal/engine/engine.go`)

- `DrainAndStop(ctx context.Context) DrainResult`. It closes `stopCh`, then waits for the band goroutines or `ctx.Done()`, whichever is first.
- Each band's stop branch becomes a loop:

```go
for {
    if err := ctx.Err(); err != nil { final = err; break }
    r, err := e.flushBand(ctx, band)   // now returns per-batch counts
    flushed += r.Flushed; deadLettered += r.DeadLettered
    if err != nil { final = err; sleepOrDone(ctx, backoff); continue }
    depth, derr := e.shield.DirtyQueueDepth(ctx, band)
    if derr == nil && depth == 0 { break }
    // no-progress guard: r.Flushed == 0 with depth > 0 counts toward backoff
}
abandoned += DirtyQueueDepth(fresh short context)
```

- `flushBand` returns per-batch counts: keys committed by `CommitFlushed` (flushed) and keys moved by the dead-letter path (dead-lettered).
- Drain reuses `flushBand` and therefore the same `CommitFlushed` hot/cold path: with `HotAwareFlush` on, committed hot keys keep `ActivityWindow` and cold keys get `KeyTTL`. A cold key left abandoned expires `KeyTTL` after the next instance flushes it, a hot one lives `ActivityWindow`. This is coherent with the rest of the design and needs no drain-specific handling; a test pins it (section 6).
- The loop must not spin when the sink is down. On error or no progress it waits with a context-aware **exponential backoff (50ms doubling to a 1s cap, with jitter)** until the deadline. Redis load is bounded too: each retry tick costs only the depth probe, `flushBand`'s own depth probe and one `DrainBand` read, and the backoff caps the number of ticks.
- **Ordering:** `Sluice.closed` is set (CAS) before `DrainAndStop`, so new `Write` calls are rejected first. A `Write` that passed the check just before the CAS can still land dirty mid-drain. It is either flushed or counted as abandoned, so nothing is lost; the accounting test in section 6 covers it.
- If the caller's `ctx` has no deadline, apply a default `ShutdownTimeout` (30s, preserving today's bound) so shutdown cannot hang forever.
- `abandoned` is read from `DirtyQueueDepth` after the loop. With several pods flushing the same band, counts are approximate and the doc says so.

### 4.3 Library (`sluice.go`)

- `DrainAndCloseWithResult(ctx) (DrainResult, error)` is the new method.
- `DrainAndClose(ctx) error` keeps its signature and calls it. When the drain is incomplete it returns an error wrapping `ErrIncompleteDrain` that exposes the `DrainResult` (`errors.As` to a `*DrainError`).
- Redis and sink close still run on an incomplete drain. `sk.Close` gets a **fresh, detached grace context (5s)**, not the caller's possibly expired `ctx`: closing a connection pool with an expired context can leak sockets and make a graceful story messy exactly when the operator is already handling a timeout. The 5s is documented.
- **Doc comment (must be stated):** the first `DrainAndClose` call may return an error wrapping `ErrIncompleteDrain`; a second call returns `nil` because the CAS on `closed` fails early. Callers who `defer s.DrainAndClose(ctx)` are unaffected; callers who asserted `nil` after a backlog were asserting something that was never true.

### 4.4 Interaction with the event ledger (GitHub #18)

When the ledger is configured, `DrainAndClose` gets a longer sequence, and all of it shares the caller's deadline:

1. Stop accepting writes (`closed` CAS), as today.
2. Engine drain loop (this RFP), sink still open.
3. Close the sink.
4. Drain the ledger: final Pebble flush, final WAL segment upload (releasing `Required`-mode waiters), then close the object store last.

- A `Write` waiting on a `Required`-mode WAL upload at shutdown is released by the final segment upload or by the deadline, with `ErrLedgerUnavailable`.
- `DrainResult.LedgerRecordsDropped` mirrors `ledger_dropped_total{reason="shutdown_timeout"}`. A dropped ledger record weakens reconciliation coverage but loses no data, so it is **reported, not an error**: it does not by itself produce `ErrIncompleteDrain`. A ledger drain error is wrapped into `FinalError`.
- Strict mode's never-expiring DLQ does not change the accounting: dead-lettered records are still counted in `RecordsDeadLettered`.

---

## 5. Implementation Plan

1. Change `flushBand` to return per-batch counts plus an error; update the ticker and volume-signal callers to ignore the counts.
2. Add `DrainResult`, `ErrIncompleteDrain`, and the engine drain loop with backoff.
3. Change `DrainAndStop` to accept a context and return a result; update the sole caller in `sluice.go`.
4. Add `DrainAndCloseWithResult` and the `DrainError` wrapper.
5. Document the new behavior and add a migration note: `DrainAndClose` can now return an error after an incomplete drain.

## 6. Success Criteria / Test Plan

Tests use `BandCount ≥ 4` and a controllable fake sink.

| # | Test | Assertion |
|---|---|---|
| 1 | Seed `3 × MaxBatchSize`, healthy sink, generous deadline | `RecordsFlushed == 3×Max`, `RecordsAbandoned == 0`, `FinalError == nil`; Redis dirty depth 0 |
| 2 | Seed `3 × MaxBatchSize`; sink completes one `BulkWrite` then blocks; short deadline | Returns within deadline + 250ms; `RecordsFlushed == MaxBatchSize`, `RecordsAbandoned == 2×MaxBatchSize`; `errors.Is(err, ErrIncompleteDrain)`; remaining keys still dirty with payloads |
| 3 | After test 2, start a new instance on the same namespace and drain | All `3×Max` records eventually reach the sink (restart recovery) |
| 4 | Sink fails transiently, then recovers before the deadline | Drain completes; `FinalError == nil` |
| 5 | Sink down for the entire drain | Returns at the deadline; `BulkWrite` call count bounded by the backoff schedule (`≤ ticks + 1`); **Redis command count also bounded**: `≤ 4 × ticks` (no per-tick extra reads) |
| 6 | Permanent errors on some records | Drain terminates; those keys dead-lettered and counted in `RecordsDeadLettered`, not flushed or abandoned |
| 7 | Uneven backlogs across bands | `Flushed + Abandoned + DeadLettered == seeded` |
| 8 | All of the above under `-race` | No races |
| 9 | Hammer `Write` from many goroutines concurrently with `DrainAndClose` | `Flushed + Abandoned + DeadLettered` equals the number of writes that returned success (rejected writes excluded); under `-race` |
| 10 | `HotAwareFlush` on; drain a mix of hot and cold keys | Hot keys committed with `ActivityWindow` TTL, cold keys with `KeyTTL` (assert `PTTL`) |
| 11 | Second `DrainAndClose` after an incomplete first | Returns `nil`; first returned `ErrIncompleteDrain` |
| 12 | `ctx` already expired when `sk.Close` runs | Close still gets a 5s grace context and completes |
| 13 | Ledger in `Required` mode; writes blocked on WAL upload when shutdown starts, short deadline | All waiters released by the deadline; `Flushed + Abandoned + DeadLettered` still equals acknowledged writes; `LedgerRecordsDropped` matches the metric |
| 14 | Ledger store failing during shutdown | `DrainAndClose` returns by the deadline; `FinalError` wraps the ledger error; no `ErrIncompleteDrain` unless Redis records were abandoned |

Numeric gates:

- Shutdown returns within **deadline + 250ms in 50/50 runs** of test 2.
- **`Flushed + Abandoned + DeadLettered == seeded`** in every test.
- No leaked goroutines after `DrainAndClose` returns (compare `runtime.NumGoroutine` before and after).

## 7. Resolved Decisions

1. **API shape:** additive `DrainAndCloseWithResult` plus a wrapping error. `DrainAndClose` changing from unconditional `nil` to possibly-error is a behavior change, not a signature break, and belongs in a minor release and the CHANGELOG.
2. **Dead-lettered records:** counted in `RecordsDeadLettered` (section 4.1).
3. **Close after expiry:** `sk.Close` uses a fresh 5s detached grace context.
4. **Batched writes:** `StopBatcher` runs before the engine drain. Implementation must confirm that writes still queued in the batcher reach Redis before the drain starts; the concurrent-write test (test 9) exercises this.

## 8. Cross-Cutting

Fixes the "flush-then-discard" instance of the shared pattern described in RFP #29 section 8.2 (the discarded final-flush error). Fault tests use the shared helper from RFP #29 section 8.1.
