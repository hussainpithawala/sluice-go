# Sluice-Go: Recommendations

Repository: <https://github.com/hussainpithawala/sluice-go>
Validated against commit `b6966c4` (2026-10-12) by reading the code and tests. The test suites were not run (they need Redis and MongoDB via Docker).

Items from the earlier draft that the repository already covers have been removed (see [Removed as already covered](#removed-as-already-covered)).

---

## 1. Confirmed Issues

### P0 — Idempotency guard is set before the journal write

**Finding:** `WriteIdempotent` (`sluice.go`) runs `SetNX` on the guard key and then calls `s.Write`. If `Write` fails, nothing removes the guard. A client retry then receives `ErrDuplicateIdempotencyKey` for a write that was never journaled, so the write is lost. The README describes this as "exactly-once delivery".

**Existing tests:** `TestWriteIdempotent_ExactlyOnce` (unit and integration) covers only the happy path.

**Recommended actions:**

- Add a fault-injection test that fails the journal write after the guard is set, then retries the same key.
- Fix with either:
  - a single Lua script that claims the guard and writes the journal entry atomically (the guard key already shares the `{band}` hash tag, so cluster slots are satisfied), or
  - recoverable guard states (`pending` → `accepted`), where a `pending` guard does not block a retry.
- On ambiguous Redis outcomes (timeout after the command may have been applied), do not blindly delete the guard.
- Correct the README wording to match the actual guarantee.

**Acceptance criteria:**

- A failed journal write does not suppress a legitimate retry.
- A repeated delivery is never journaled twice.
- The documented failure model states what happens on Redis timeout and failover.

### P0 — Shutdown flushes at most one batch per band

**Finding:** On stop, `runBand` (`internal/engine/engine.go`) calls `flushBand` once under a 30s timeout and discards the error (`_ = e.flushBand(...)`). `flushBand` drains at most `MaxBatchSize` records. A larger backlog is left behind while `DrainAndClose` returns success.

**Impact:** The leftover records stay in Redis because unflushed payloads do not expire. This is a silent incomplete drain, and it is only recoverable if the Redis deployment is durable.

**Existing tests:** None. `flush_durability_test.go` calls `DrainAndClose` only in cleanup.

**Recommended actions:**

- Loop `flushBand` on shutdown until the dirty queue is empty, the context expires, or a non-progress error occurs.
- Add a regression test with more pending records than `MaxBatchSize` across multiple bands.
- Add a test with transient sink failures during drain.

**Acceptance criteria:**

- All pending records are persisted, or shutdown reports incomplete drainage explicitly.
- Final flush errors are not discarded.

### P1 — Shutdown ignores the caller's deadline

**Finding:** `DrainAndClose(ctx)` passes `ctx` only to `sk.Close`. `Engine.DrainAndStop()` takes no context and blocks on `wg.Wait()`, so a slow sink can hold shutdown past the caller's deadline.

**Recommended actions:**

- Change `DrainAndStop` to accept a context and stop waiting when it expires.
- Return a structured drain result: remaining dirty-queue depth per band and final flush errors.
- Test shutdown with the database unavailable.

**Acceptance criteria:**

- Shutdown returns by the deadline.
- Callers can distinguish a complete drain from a timed-out or incomplete one.
- Undrained records remain recoverable in Redis.

> This item and the previous one share a fix: a context-aware drain loop.

### P1 — Index failures are never repaired

**Finding:** In `Write`, index maintenance is best-effort (`_ = s.shield.UpdateIndexes(...)`) and runs only when the record was actually written. With `ContentDedup` on, an identical payload is deduplicated and skips indexing. A failed index update therefore stays inconsistent across retries.

**Existing tests:** None for index failure or reconciliation.

**Recommended actions:**

- Add a test that injects an index-update failure, repeats the same payload with dedup on, and checks the index.
- Choose a design: re-run indexing on the dedup path, track failed index updates for retry, or document the index as best-effort and provide a rebuild path.

**Acceptance criteria:**

- An index failure does not permanently corrupt `Query` results, or the limitation and its rebuild procedure are documented.

---

## 2. Remaining Verification Work

### P2 — L1 cache: equal-version behavior and multi-pod concurrency

Stale-version rejection is already tested (`TestL1_StaleVersion_RejectedByPut`, `TestSubscriber_VersionGating_RejectsOlder`). What remains:

- Resolve the mismatch in `internal/localjournal/localjournal.go`: the comment on `Put` says only strictly older versions are rejected, but the code is `if e.version >= version`, which also rejects equal versions.
- Add a concurrent-writer test across multiple goroutines and pods against the authoritative Redis payload.
- State the consistency guarantees of `Read` (L1, bounded by `LocalTTL`) and `ReadFresh` (bypasses L1) next to the `Query` guarantees in the docs.

### P2 — Sink failure cases not yet shown as tested

Attribution guard, conditional commit by sequence, `DeadLetterIfUnchanged`, and permanent/transient classification are already implemented. the silent-loss RFP (`docs/issues/sink-silent-loss.md`) tracks the rest. Confirm tests exist for:

- Database success followed by a lost response (the batch re-flushes; upserts must stay idempotent).
- Redis commit failure after database success (keys stay dirty by design).
- DLQ replay not overwriting newer state, for each sink.

### P2 — Redis failover and recovery point

The README already requires `noeviction`, AOF, and replicas or MemoryDB. What is missing:

- A failover test showing behavior for acknowledged but unflushed writes.
- An explicit recovery-point objective in the docs (with `appendfsync everysec`, up to about one second of acknowledged writes can be lost on a primary crash).

---

## 3. Test Execution Notes

- `make test-unit` already runs with `-race`; `make test-integration` runs separately (`-tags=integration`, `-p 1`).
- `make check` runs only unit tests, so integration regressions are not caught by it.
- Before adding tests, find existing coverage:

```bash
grep -RInE \
  'DrainAndClose|WriteIdempotent|CommitFlushed|ProcessDLQ|ContentDedup' \
  --include='*_test.go' .
```

For each fix: add a deterministic regression test, run unit and integration suites, and record the commit.

---

## 4. Targeted Benchmarks

- Shutdown duration versus backlog size (validates the drain-loop fix).
- Backlog recovery time after a database outage.
- Redis memory behavior near capacity with `noeviction`.

---

## 5. Gates Before Relying on Acknowledged-Write Durability

- [ ] `WriteIdempotent` failure window fixed or proven safe.
- [ ] Multi-batch shutdown drain verified, with errors surfaced.
- [ ] Shutdown honors the caller's deadline and reports incomplete drains.
- [ ] Index failure behavior fixed or documented.
- [ ] Redis failover tested and recovery point documented.

---

## Removed as already covered

| Earlier item | Why removed |
|---|---|
| Idempotency: same Redis Cluster slot | Guard key already uses the `{band}` hash tag |
| Sink partial-failure and DLQ safety (as written) | Attribution guard, conditional commit, and error classification already exist; narrowed above |
| Cache version gating (as written) | Stale-version tests already exist; narrowed above |
| Redis configuration requirements | Already documented in the README; narrowed to failover and recovery point |
| Generic benchmarks and readiness gates | Not specific to this codebase; replaced with targeted ones |

## Final Recommendation

Fix the idempotency guard first, then the context-aware drain loop, which resolves both shutdown findings. Then address index repair, and finish with the Redis recovery-point documentation and the remaining P2 verification.
