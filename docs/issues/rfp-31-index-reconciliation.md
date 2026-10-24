# RFP #31: Index Reconciliation and Consistency Guarantees

**Status:** Approved with review amendments (2026-10-17), validated against `b6966c4` (v1.1.0). Option C is the decision (section 4.1).
**Target Release:** after RFP #30
**Sequence:** 3 of 3 (RFP #29 → #30 → #31)
**Dependencies:** RFP #29 (shares `atomicDedupWriteLua`, its return-code enum and reply shape, and the shared fault-injection helper); RFP #30 for phase 2 (the sweeper must drain on shutdown)
**Blocks:** None

---

## 1. Context & Problem Statement

Secondary-index maintenance in `Sluice.Write` is best-effort and runs only when the record was actually written:

```go
if written {
    ...
    if s.cfg.IndexContract != nil {
        _ = s.shield.UpdateIndexes(ctx, correlationKey, indexes, s.cfg.ActivityWindow)
    }
}
```

With `ContentDedup` on, a payload identical to the stored one returns `written == false` from `WriteDedup`, so indexing is skipped. If the first attempt's index update failed, no retry of the same payload ever repairs it, and `Query` silently omits the record.

Scope of the problem:

- **Affected:** `ContentDedup` on, index update failed, identical payload re-sent.
- **Not affected:** `ContentDedup` off (the retry is written and re-indexed), or a different payload (written and re-indexed).
- **Also affected, and not repairable by retries:** the hydration path. `applyHydration` (`sluice.go:467` at `b6966c4`; refer to it by function name, since line numbers drift) calls `UpdateIndexes` and discards the failure with only a metric. Hydration fills journal misses only, and writers do not re-send payloads that came from the store, so no identical retry will ever occur for a hydrated key. A phase 1 scoped to the write path alone would repair the less vulnerable of the two paths.

### 1.1 Defects

| Where | Defect | Effect |
|---|---|---|
| `Write` | `UpdateIndexes` error discarded | A failed index update is invisible |
| `Write` + `WriteDedup` | Indexing skipped on a dedup hit | A failed index update is never retried by identical retries |
| `applyHydration` | `UpdateIndexes` failure discarded (metric only); no retry of a hydrated key ever happens | Hydrated key stays missing from `Query` with no repair path |
| Index layer | No record of which keys have a pending index update | Nothing to reconcile from |
| Docs | `Query` consistency is not stated | Callers assume indexes are always current |

---

## 2. The Goal

1. **An index failure converges.** The index becomes correct on the next identical write (v1), and optionally without any retry (v2).
2. **No change to the primary payload path.** Repair does not require the payload to change.
3. **Explicit guarantees** for `Query` published next to `Read` and `ReadFresh`.
4. **Bounded overhead** on the write path.

## 3. Non-Goals

- Making index updates transactional with the journal write.
- Changing index key layout or `Query` semantics.
- Rebuilding indexes from the sink (out of scope; Redis is the index store).

---

## 4. Proposed Design

### 4.1 Options evaluated

| Option | How it works | Heals without a retry? | Cost / risk |
|---|---|---|---|
| **A. Async index worker** | `Write` enqueues index work; a per-band worker applies it with retries | Yes | New goroutines and backpressure; `Query` lags writes; must drain on shutdown (RFP #30); largest change |
| **B. Dedup-path repair** | A pending marker is set with the journal write; a dedup hit that finds the marker re-runs indexing | No, only when the same payload is re-sent | Small: one Lua branch plus one clear command |
| **C. B first, A-lite later** | Ship B; add a sweeper that reuses the same marker only if needed | Yes, in phase 2 | Phase 1 is small; phase 2 is optional |

**Recommendation: C.** B closes the specific gap the issue describes at minimal cost. The marker is the same state a worker would need, so phase 2 builds on it rather than replacing it. Decide on phase 2 after measuring how often index failures occur without a retry.

### 4.2 Phase 1 design (Option B)

**Marker.** When `IndexContract` is configured, the write scripts set `ixp = v` (the new per-key sequence) in the payload hash, in the same atomic script as the journal write. No extra round trip. **`hydrateLua` sets `ixp` too** (one extra `ARGV` flag and one `HSET`, only when `IndexContract` is configured), so hydration failures are repairable. Confirm during implementation whether `hydrateLua` stores the content hash `h` and a `v`: if it does not store `h`, a later identical write takes the normal written path and re-indexes there (the same post-index clear removes the marker); if it does not assign `v`, the marker value is `0` and the conditional clear compares against `0`.

**Dedup script.** Conditions are evaluated in a fixed order: **(1) idempotency guard check, (2) hash match with marker, (3) hash match without marker.** The guard check always wins when both apply, because duplicate rejection is the stronger contract. Do not "fix" the order so the marker wins. In branch (2):

```lua
if redis.call('HEXISTS', payloadKey, 'ixp') == 1 then
    return {3, v}   -- deduplicated, index pending: caller must re-index; v = current sequence
end
-- existing TTL refresh, return 0
```

(Code `3` is reserved for this in RFP #29's return-code enum: `0` dedup-accepted, `1` written, `2` idempotency duplicate.)

**Go side.** On `3`, `WriteDedup` reports `indexPending` and the `v` returned with it; `Write` recomputes the index fields from the payload via `IndexContract`, then **re-reads `v` and skips the repair if it changed**. Without this check, a newer, different-payload write landing between the script's return and `UpdateIndexes` would have its index values overwritten by the older repair. The window is milliseconds and self-heals on the next write to that key, so the cost is one extra round trip on the repair path only (the rare path) and zero on the hot path.

**Clear.** After a *successful* `UpdateIndexes` (checked after `Exec`, not inside the same pipeline), run a small script:

```lua
-- KEYS[1]=payload hash; ARGV[1]=v that was indexed
if redis.call('HGET', KEYS[1], 'v') == ARGV[1] then
    redis.call('HDEL', KEYS[1], 'ixp')
end
```

The `v` check means a newer write that landed during repair keeps its own marker.

**Cost.** One extra Redis command per write when `IndexContract` is set, zero otherwise. The marker lives in the payload hash, so it follows the payload's TTL; index pruning still goes through the existing index expiry queue.

**Visibility.** Count and log failed index updates instead of discarding the error: new counter `index_update_failed_total{band}`, emitted at the original failure on both the write and hydration paths. Add an alert on it to `monitoring/prometheus/rules/sluice.rules.yml`. Index health is not coupled to the primary write path; the alert is the loud signal without the coupling.

### 4.3 Phase 2 (optional, Option A-lite)

Pinned down now so phase 1 does not paint itself into a corner:

- **`ixpend` is written by the Go side when `UpdateIndexes` fails**, not derived from the marker. The marker dies with its payload (cold TTL), but a sweeper must still find the failure after that. The ZSET lives in the same `{band}` slot and is scored by failure time.
- **Expired-payload rule:** if the payload has since expired (no journal entry), the sweeper drops the entry from `ixpend` and moves on. The record left the journal, its index entries are pruned by the existing expiry machinery, and there is nothing left to reconcile. Without this rule, phase 2 resurrects the memory-growth problem the index expiry queue was built to fix.
- The sweeper re-indexes, clears the marker with the conditional-`v` script, and removes the `ixpend` entry. It must stop and drain under the RFP #30 shutdown path.
- Build it only if `index_update_failed_total` shows failures that are not followed by retries; the phase 1 metric is the measurement instrument.

### 4.4 Interaction with RFP #29

If an idempotency key is redelivered after the original write's index update failed (`ContentDedup` + `IndexContract`), the guard exists and the write returns `2` (idempotency duplicate) with no re-index. This is defensible, since the original acceptance owned repair, but it is a silent non-repair. The failed-index metric from 4.2 is what surfaces it, so it must be emitted at the original failure, not at retry. Phase 2's sweeper would repair this case.

### 4.5 Documented guarantees

State in the README: index maintenance is best-effort on the write path; after this RFP, a failed update is repaired by the next write of the same key (v1) or within `ReconcileInterval` (v2, if built); `Query` is eventually consistent with writes. The sentence to write: **`Query` is to live state what `Read` is to the journal, not what `ReadFresh` is.** (`Read` serves L1/L2; `ReadFresh` bypasses L1.)

### 4.6 Interactions with the event ledger (GitHub #18) and strict mode (GitHub #21)

- **`hydrateLua` has two extenders.** This RFP adds the `ixp` marker; strict mode Phase 3 extends it to set `e` and `v` from the store's version. Coordinate the `ARGV` layout so each flag is optional. Once strict mode lands, hydrated entries carry `v`, which resolves the "confirm whether `hydrateLua` stores `v`" item in 4.2; until then the marker value is `0`.
- **Redis loss also loses indexes.** After a Redis loss, `Query` returns nothing for affected keys until they are re-indexed. The ledger's `Reconcile{Correct: true}` re-`Write`s `Behind` keys through the journal, which recreates the hash and re-indexes (a fresh hash is a written, not deduplicated, write). New writes repair the rest. State this in 4.5 as part of the `Query` guarantee.
- **Script family rule.** The marker is set by every journal-write script, including strict mode's `casWriteLua`, using the shared `{code, e, v}` reply from RFP #29.

---

## 5. Implementation Plan

1. Add the `ixp` marker to both write scripts (coordinate with RFP #29's script changes).
2. Add the dedup-branch return code and the Go handling in `WriteDedup` / `Write`.
3. Add the conditional clear script; call it after a successful `UpdateIndexes`.
4. Add the failed-index-update metric; review the hydration call site (`sluice.go:467`).
5. Write the guarantees section in the README.
6. Decide on phase 2 using the metric.

## 6. Success Criteria / Test Plan

Index failure is injected with a go-redis `Hook` that fails the index pipeline once.

| # | Test | Assertion |
|---|---|---|
| 1 | `ContentDedup` on, `IndexContract` set; inject an index failure on write 1; send the identical payload | After the second write, `Query` returns the record; `ixp` absent |
| 2 | Same as 1 with `ContentDedup` off (control) | Passes before and after the change (retry re-indexes) |
| 3 | Different payload after a failure | Index correct for the new payload; stale entries removed |
| 4 | Newer write lands while repair for `v1` is running | `v1` clear is skipped; marker for `v2` retained |
| 5 | 10,000 successful writes | `ixp` absent on all keys (no marker leaks) |
| 6 | Cluster mode | Marker, payload, and index keys use one `{band}` slot; no `CROSSSLOT` |
| 7 | Phase 2 only: failure with no retry | Sweeper repairs within `ReconcileInterval + 1s` |
| 8 | #29 × #31: idempotency key redelivered after an index failure | Returns idempotency duplicate; no re-index; failed-index metric incremented once at the original failure |
| 9 | Hydration: inject an index failure in `applyHydration` with `ContentDedup` on, then write the identical payload | Marker was set by `hydrateLua`; the write repairs the index and clears the marker |
| 10 | Stale-reindex race: dedup hit with marker, a different payload lands before the repair's `UpdateIndexes` | Repair re-reads `v`, sees it changed, and skips; index reflects the newer payload |
| 11 | Dedup ordering: idempotency key already present and a marker present | Returns `2` (guard wins), not `3` |
| 12 | Phase 2 only: payload expires while its `ixpend` entry exists | Sweeper drops the entry; `ixpend` is empty afterwards |
| 13 | Failure on the write and hydration paths | `index_update_failed_total{band}` increments once per failure; alert rule fires in the rules test |
| 14 | After the ledger lands: `FLUSHALL`, then `Reconcile{Correct: true}` with `IndexContract` | Replayed keys appear in `Query`; markers absent after repair |
| 15 | After strict mode Phase 3: hydrate a key through `hydrateLua` with `IndexContract` and an injected index failure | Marker set, `v` present, and a later identical write repairs the index |

Numeric gates:

- Repair happens on the **first** identical retry in 100% of 1,000 injected failures.
- Extra Redis commands per write: **≤ 1** with `IndexContract`, **0** without.
- Write p99 latency regression **≤ 5%** with `ContentDedup` and `IndexContract` on, against v1.1.0.

## 7. Resolved Decisions

1. **Phase 2:** yes, after the metric. Its cost (goroutines, backpressure, shutdown integration, `Query` lag) is justified only by observed failure-without-retry rates, and `index_update_failed_total` is the instrument.
2. **Persistent failures:** stay best-effort plus metric and alert; do not couple the primary write path to index health.
3. **Consistency:** eventual `Query` consistency is acceptable; stated in 4.5.

## 8. Cross-Cutting

Fixes the "index-after-write" instance of the shared pattern in RFP #29 section 8.2 (a side effect after the decision with its error discarded): it now has a repair path (marker, dedup/hydration repair, optional sweeper) and a loud metric. Fault tests use the shared helper from RFP #29 section 8.1.
