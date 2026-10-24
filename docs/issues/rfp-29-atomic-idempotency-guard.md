# RFP #29: Atomic Idempotency Guard via Lua Script

**Status:** Approved with review amendments (2026-10-17), validated against `b6966c4` (v1.1.0)
**Target Release:** next patch after v1.1.0
**Sequence:** 1 of 3 (RFP #29 → #30 → #31)
**Dependencies:** None
**Blocks:** RFP #31 (both change `atomicDedupWriteLua`; landing this first avoids a script-signature conflict). Also delivers the shared test and contributor items in section 8, which RFPs #30 and #31 reuse.

---

## 1. Context & Problem Statement

`WriteIdempotent` (`sluice.go`) claims the idempotency guard with `SetNX` and only then calls `s.Write`. Nothing releases the guard if `Write` fails. A client retry then receives `ErrDuplicateIdempotencyKey` for an operation that was never journaled, so the write is lost without any signal. The README presents this as "exactly-once delivery".

> **Note on the original sketch:** the journal write is not an `RPUSH`. It is `atomicWriteLua` (`HSET` payload, `HINCRBY v`, `PERSIST`, `ZADD` dirty set) or `atomicDedupWriteLua` (`internal/shield/types.go`). The new script extends these; it does not add a list push.

### 1.1 Defects

| Where | Defect | Effect |
|---|---|---|
| `WriteIdempotent` | Guard is set before the journal write; no rollback on failure | A Redis error, context cancel, or `ErrRedisUnavailable` after `SetNX` leaves a guard with no journal entry |
| `WriteIdempotent` | Guard lives for `IdempotencyTTL` | Every retry in that window is rejected as a duplicate, so the loss is silent and lasts the whole TTL |
| `WriteIdempotent` | Two round trips (`SETNX`, then the write) | Extra latency, plus the ambiguity window between them |
| README | "exactly-once delivery" | Overstated for the current behavior |

### 1.2 Write paths that must honor the guard

`Write` reaches Redis through four paths, and all must take the guard atomically:

1. Batched writes: `enqueueBatched` → `pipelineWrites` (`EvalSha` in a pipeline).
2. Direct write without broadcast: `writeScript.Run`.
3. Direct write with broadcast: `pipelineWrites` for a single entry.
4. Content dedup: `WriteDedup` (`atomicDedupWriteLua`).

---

## 2. The Goal

1. **No guard without a journal entry.** A failed journal write never leaves a guard behind, so a legitimate retry is never suppressed. **Exactly-once acceptance holds when the script returns success.** Redis does not roll back a script, so a mid-script abort degrades to at-least-once (section 4.4).
2. **One Redis round trip** for guard and journal write.
3. **Ambiguous outcomes are safe.** If a client timeout hides a write that succeeded, the retry sees the guard and is correctly rejected as a duplicate.
4. **All three keys share the `{band}` hash slot**, so this works in Redis Cluster mode.

## 3. Non-Goals

- Changing the guard key format or `IdempotencyTTL` semantics.
- Exactly-once delivery to the sink. The guarantee is exactly-once acceptance into the journal.
- Redesigning `DegradedModeDirect`; its behavior is fixed in section 4.5.

---

## 4. Proposed Design

### 4.1 Script changes (`internal/shield/types.go`)

Extend both scripts with an optional third key. Existing callers pass two keys and are unaffected.

```lua
-- KEYS[3] = idempotency guard (optional); extra ARGV = guard TTL in ms
if KEYS[3] and redis.call('EXISTS', KEYS[3]) == 1 then
    return 2                       -- duplicate: nothing written
end

-- ... existing HSET / HINCRBY / PERSIST / ZADD ...

if KEYS[3] then
    redis.call('SET', KEYS[3], '1', 'PX', <guardTtlMs>)   -- LAST command
end
return 1
```

Rules:

- The guard `SET` is the **last** command, so any failure before it leaves no guard.
- In `atomicDedupWriteLua`, the "content hash matches" branch must also set the guard before `return 0`. The operation was accepted even though no state changed.
- **Reply shape:** both scripts reply `{code, e, v}`, where `v` is the payload hash's sequence after the call (`0` if none) and `e` is the hash's epoch field if present (`0` if absent). This RFP only *returns* `e`; it does not write it. The event ledger (GitHub #18) Phase 0 adds `HSETNX e` and starts populating it, so the ledger needs no second signature change. Defining the shape here also lets #31 return the current `v` with its new code without changing the signature again.
- Return codes are defined once here, as constants in `shield`, for both scripts. #31 must not renumber them:

| Code | Meaning |
|---|---|
| `0` | Deduplicated, accepted (no state change) |
| `1` | Written |
| `2` | Idempotency duplicate (nothing written) |
| `3` | Reserved for RFP #31: deduplicated, index pending |
- The guard TTL is a new trailing `ARGV` (`ARGV[4]` for `atomicWriteLua`, `ARGV[6]` for `atomicDedupWriteLua`).

### 4.2 Go changes

- `shield`: add the guard key and TTL to `writeEntry`. The batcher's `done` channel currently carries only an `error`, which cannot express "duplicate" or "dedup", and `Write`'s post-processing branches on `written`. Replace it with a per-entry result struct `writeResult{written bool, status int, version Version, err error}` (the version is what the ledger records and what strict mode returns from `WriteIf`) and map status `2` to a shield-level `ErrDuplicateKey`.
- `WriteDedup` currently builds `redis.NewScript(atomicDedupWriteLua)` on every call. Make it a preloaded script like `writeScript` (a field set in `New`), since it is now on the idempotent hot path with a third key.
- `sluice.go`: refactor `Write` into an internal `write(ctx, ck, payload, idem *idemArgs)` so L1 write-through, indexing, and hot-regime signaling run only when the record was journaled, never on a duplicate. `WriteIdempotent` calls it and maps `ErrDuplicateKey` to `ErrDuplicateIdempotencyKey`. `SetNX` is no longer called from this path.
- README: replace "exactly-once delivery" with "exactly-once acceptance into the journal within `IdempotencyTTL`", and document the ambiguity behavior.

### 4.3 Compatibility

The guard key format (`sl:<ns>:idem:{band}:<key>`) and value (`1`) are unchanged. During a rolling deploy, old pods (`SETNX` then write) and new pods (Lua) interoperate on the same keys.

### 4.4 Residual gap: Redis does not roll back scripts

"Guard exists only if the journal entry exists" holds when the script fails *before* its writes (for example, the early duplicate check). It is not absolute. If the script aborts **after** the `HSET`/`ZADD` ran but before the final guard `SET`, Redis keeps the earlier writes. The result is a journal entry with no guard and an error returned to the caller. The caller does not ack and retries, so the write is journaled twice (`v` bumps, the key is re-dirtied) and flushed twice.

This is safe only because every sink write is an upsert and `Many` writes must be idempotent by documented requirement. The guarantee is therefore:

- **Script returns success:** exactly-once acceptance within `IdempotencyTTL`.
- **Script aborts mid-way:** at-least-once. The dangerous direction (guard without journal entry, silent loss) is still impossible.
- **Dedup-hit path self-heals:** if the guard `SET` fails on a hash match, the retry re-enters the dedup branch, sees no guard, sets it, and returns `0`. Only the fresh-write path cannot self-heal.

Likely triggers include OOM under `noeviction`, our documented deployment mode, and script kills. Whether OOM can actually abort a script after its first write depends on the Redis/Valkey version, so the test plan verifies this on the deployed version; the answer shows how narrow the gap really is. The README states the guarantee in these terms.

### 4.5 Degraded mode

`WriteIdempotent` in degraded mode behaves exactly like `Write` in degraded mode: if the script fails and `DegradedModeDirect` is enabled, the write goes to the sink. Refusing it would force the consumer to hold the event through a Redis outage, trading the library's core durability property for a guarantee that cannot hold once the journal is bypassed. Duplicate application is covered by the existing upsert/idempotent-`Many` contract.

- The exactly-once-acceptance guarantee **explicitly excludes the degraded path**.
- When an idempotent write takes the degraded path, log at `warn` and increment a counter (`degraded_write_total{idem="true"}`), because this is the one case where the idempotency contract is silently weakened.

### 4.6 Interactions with the event ledger (GitHub #18) and strict mode (GitHub #21)

- **Script family rule.** Every journal-write script (`atomicWriteLua`, `atomicDedupWriteLua`, and strict mode's future `casWriteLua`) uses the `{code, e, v}` reply, this code enum, and the optional guard-key convention. `casWriteLua` must accept the optional guard key or document why it does not.
- **Landing order.** This RFP is a patch for a silent-loss bug and should not wait on the larger programs: **#29 first**, then the ledger's Phase 0 (which then shrinks to populating `e`, propagating `FlushRecord.Epoch`, the degraded `Seq: 0` rule and the `\x00` key check), then strict mode Phase 2.
- **One `Version` type.** The ledger defines `ledger.Version{Epoch, Seq}` and strict mode defines `sink.Version{Epoch, Seq}`. Define it once in a leaf package (for example `internal/version`) and alias it elsewhere; `writeResult.version` uses that type.
- **Durable ack and duplicates.** Strict mode pipelines `WAIT`/`WAITAOF` after the write script. If the script succeeds (guard set, entry journaled) and the wait fails, the caller gets `ErrDurabilityUnconfirmed` and retries the same key. Without handling, the retry returns code `2` and `ErrDuplicateIdempotencyKey`, and durability is never confirmed. Rule: in a durable namespace the duplicate path must also issue the `WAIT`/`WAITAOF`; it returns `ErrDurabilityUnconfirmed` while unconfirmed and maps to `ErrDuplicateIdempotencyKey` only once confirmed.
- **Degraded mode.** Strict mode forces `WithDegradedModeDirect(false)`, so in a strict namespace the exactly-once-acceptance guarantee holds with no degraded exception. The exclusion in 4.5 applies to non-strict namespaces only.
- **Ledger recording.** `WriteIdempotent` records `EventID = idempotencyKey` (per #18). A record is enqueued only when the script returned `1` (written), never for `2` (duplicate); `0` (dedup-accepted) is recorded only under `PerEvent`.

---

## 5. Implementation Plan

1. Add the optional-guard branches to both scripts and unit-test them directly against Redis.
2. Thread the guard through `writeEntry`, `pipelineWrites`, and `WriteDedup`.
3. Refactor `Write` / `WriteIdempotent` as in 4.2.
4. Add the fault-injection and concurrency tests (section 6).
5. Update README and `CHANGELOG.md`.

## 6. Success Criteria / Test Plan

Fault injection uses a go-redis `Hook` that fails or delays the write script call.

| # | Test | Assertion |
|---|---|---|
| 1 | Hook fails the write script (error before execution), then retry the same key | Guard key absent after the failure; retry returns `nil`; dirty set contains the key; `v == 1` |
| 2 | Hook lets the script run, then returns a timeout error (ambiguous) | Guard exists; retry returns `ErrDuplicateIdempotencyKey`; journal has exactly one write (`v == 1`) |
| 3 | 100 goroutines, same idempotency key | Exactly 1 `nil`, 99 duplicates, `v == 1` |
| 4 | `ContentDedup` on, identical payload, new idempotency key | Returns `nil`, guard set, no second dirty-queue entry |
| 5 | Run tests 1 and 3 on each write path: batched, direct, direct with broadcast, dedup | Same assertions on every path |
| 6 | Cluster mode: guard, payload, and dirty keys in one slot | No `CROSSSLOT` error |
| 7 | Existing `TestWriteIdempotent_ExactlyOnce` (unit and integration) | Still passes unchanged |
| 8 | Test-only script variant that raises an error just before the guard `SET`, fresh-write path | Journal entry exists, no guard, error returned; retry journals again (`v == 2`) and succeeds: at-least-once, as documented |
| 9 | Same injected abort on the dedup-hit path | Retry re-enters the dedup branch, sets the guard, returns `0` (self-heals); `v` unchanged |
| 10 | On the deployed Redis/Valkey version, fill memory under `noeviction` after the first write in a script | Records whether the final `SET` can fail on OOM (sizes the gap in 4.4) |
| 11 | `DegradedModeDirect` on, Redis script fails, `WriteIdempotent` | Write reaches the sink; `warn` logged; `degraded_write_total{idem="true"}` incremented |
| 12 | #29 × #31: `ContentDedup` + `IndexContract`, index update fails on the original write, same idempotency key redelivered | Returns duplicate (`2`), no re-index. This is a deliberate silent non-repair: the original acceptance owned repair, and the #31 failed-index metric is what surfaces it |
| 13 | Durable ack: script succeeds, `WAIT` fails, same idempotency key retried (strict-mode Phase 2) | Retry runs the `WAIT` again; returns `ErrDurabilityUnconfirmed` until confirmed, then `ErrDuplicateIdempotencyKey` |
| 14 | Ledger configured (ledger Phase 4): fresh write, then duplicate of the same key | Exactly one ledger record, with `EventID == idempotencyKey`; none for the duplicate |

Numeric gates:

- **0 leaked guards** over 1,000 injected pre-execution failures.
- `WriteIdempotent` uses **1 Redis round trip** (was 2).
- Plain `Write` p99 latency regression **≤ 5%** against v1.1.0 on the existing benchmark.

## 7. Resolved Decisions

1. **Degraded mode:** keep the degraded write; the guarantee excludes that path; log and count it (4.5).
2. **Duplicate outcome:** a bare `ErrDuplicateIdempotencyKey` sentinel. Storing the original outcome adds state and a cleanup problem for a need nobody has; "accepted" is the only outcome that matters.

---

## 8. Shared Deliverables (used by RFPs #30 and #31)

### 8.1 Shared fault-injection test helper

RFPs #29, #30 and #31 each need the same go-redis `Hook` fault injection. Build it once, here, as a shared test helper (for example `internal/testutil/faulthook`), with these modes, each addressable **per command and per script name**:

- **fail-before-execution:** the command never reaches Redis.
- **fail-after-execution:** the command runs, then the client sees an error (the ambiguous case).
- **delay:** the command is held for a set duration.

Each mode takes a count ("fail the next N matching calls") so a test can fail once and then recover. This RFP's tests 1, 2, 8 and 9 are its first consumers.

### 8.2 Contributor principle

All three RFPs fix the same bug: a side effect issued after a decision, with its error discarded. Guard-before-write (#29), flush-then-discard (#30), and index-after-write (#31). The 1.0.8 audit found the same shape inside the flush engine. Add to the contributor docs:

> Every fire-and-forget side effect must have either a repair path or a loud metric; preferably both. New side effects ship with a fault-injection test using the shared helper.
