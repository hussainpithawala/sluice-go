# RFP #19: Strict Mode — Payment-Grade Namespaces

**Status:** Proposed (2026-09-27)  
**Target Release:** after RFP #17  
**Dependencies:** RFP #17 Phase 0 (per-key `Version{Epoch, Seq}`), RFP #18 (sink error classification, merged)  
**Related:** RFP #17 ledger in `PerEvent` + `Required` mode, for transaction history

---

## 1. Context & Problem Statement

`sluice` was built for high-velocity state that tolerates coalescing, such as banner inventory. The
same engine (journal, bulk flush, DLQ, live reads) is attractive for **high-cardinality transaction
records such as payments**. Batching alone turns ~100K inserts/s into ~100 bulk writes.

Coalescing isn't the obstacle: keying by transaction ID gives every transaction its own journal
entry. The obstacles are in the durability and ordering contract:

| Gap | Today | Why it matters for payments |
|---|---|---|
| **Ack durability** | `Write` returns nil once the entry is on the Redis **primary** | A failover to a replica that hadn't received it loses an acknowledged payment, with no signal |
| **Guarded state transitions** | Latest arrival wins | A late `authorized` can overwrite `captured` |
| **Store ordering** | Flushes are unguarded upserts (see §1.1) | An older version can overwrite a newer one in the store, permanently |
| **Degraded mode** | Writes straight to the store when Redis fails and nothing is pending | Bypasses the durable journal, the guards and the history |
| **DLQ retention** | Dead-lettered payloads expire after 7 days, and `drainAction` discards them silently | A payment must never expire out of the DLQ |
| **DLQ strategies** | `Ignore` deletes; `ReInsert` changes the key | Neither is acceptable for a transaction |
| **L1 staleness** | `Read` may be up to `LocalTTL` stale | Payment status must be current |

### 1.1 Finding: a stale flush can permanently regress the store (all namespaces)

Found while designing this RFP. It affects **every namespace today**, not only strict ones.

Nothing stops two flushes of the same band from overlapping:

- Pods have no per-band lease, so every pod flushes every band.
- A `BulkWrite` that times out on the client (the flush context is `4 × FlushWindow`) can still
  complete on the server afterwards. That happens even with a single pod.

The sequence:

1. Flush A drains key `k` at version 5 and sends `BulkWrite(v5)`.
2. A new write moves `k` to version 6.
3. Flush B (another pod, or A's next cycle after a timeout) drains v6, writes it, and commits:
   `CommitFlushed` sees an unchanged `v`, removes `k` from the dirty set and applies the post-flush TTL.
4. Flush A's `BulkWrite(v5)` lands **after** B's. The store now holds v5. A's commit is skipped
   (`v` changed), but the key is already clean.

Nothing flushes `k` again. The journal serves v6 until the post-flush TTL expires, then reads fall
back to the store and return **v5**. No metric fires. The conditional commit protects the dirty set,
not the store.

**Fix (Phase 1, available to every namespace):** a store-side **version guard**. The sink applies a
write only if the stored version is older than the one being written (§4.2).

---

## 2. Goals

1. **Ack means durable.** In a strict namespace, `Write` returns nil only once the entry survives the
   loss of the Redis primary.
2. **The store never goes backwards.** A flush never overwrites a newer stored version. This part is
   available to every namespace.
3. **Guarded transitions.** Callers can make a write conditional on the current version, or on a
   state-machine check.
4. **Nothing expires or is silently discarded** in a strict namespace: journal, DLQ or history.
5. **Fail loudly instead of degrading.** A strict namespace returns an error rather than bypass any of the above.

## 3. Non-Goals

- Multi-key transactions: moving money between two accounts atomically. Model a transfer as one
  record, or use the datastore's transactions.
- Replacing the system of record. The datastore stays authoritative; strict mode makes the path to it safe.
- A per-band flush lease. The version guard makes overlapping flushes harmless, so a lease becomes an
  optimisation (fewer wasted writes), not a correctness requirement.

---

## 4. Proposed Design

### 4.1 Durable acknowledgement

```go
type Durability interface{ isDurability() }

// WAIT after each journal write, on the same connection (pipelined with the script).
type DurableAckReplicas struct {
    Replicas int           // minimum replicas that must acknowledge
    Timeout  time.Duration
}

// WAITAOF (Redis 7.2+ / Valkey): fsynced to the local AOF and/or to replicas' AOFs.
type DurableAckAOF struct {
    Local    bool // fsync on the primary
    Replicas int
    Timeout  time.Duration
}

// The operator asserts the journal store acknowledges only durable writes
// (e.g. Amazon MemoryDB's multi-AZ transaction log). No extra command is sent.
type DurableStore struct{}

var ErrDurabilityUnconfirmed = errors.New("sluice: write reached the redis primary but durability was not confirmed")
```

- The `WAIT`/`WAITAOF` is pipelined **after** the write script on the same connection, so it costs
  latency but no extra round-trip. Under `WithBatchedWrites`, one `WAIT` covers the whole pipeline, and every waiter gets its result.
- Fewer confirmations than required, or a timeout, returns `ErrDurabilityUnconfirmed`. The write
  **is** in the journal and will be flushed, but must not be acked upstream. A retry is safe:
  `WriteIdempotent` dedupes, and a plain rewrite of the same payload is harmless.
- Only the write needs durable acknowledgement. Losing a commit (`ZREM` + TTL) in a failover makes
  the key dirty again and re-flushes it, which the version guard makes harmless. Losing a dead-letter move makes it retry.

### 4.2 Version guard in the sinks (all namespaces, opt-in; required in strict mode)

`sink.WriteModel` gains an optional field, a non-breaking addition:

```go
type WriteModel struct {
    CorrelationKey string
    Filter, Update interface{}
    Upsert         bool
    Version        *Version // nil = unguarded (today's behaviour)
}

type Version struct{ Epoch, Seq int64 }

// Sinks that can guard implement this; Build checks it when a guard is required.
type VersionGuarder interface{ SupportsVersionGuard() bool }

// BulkWriteResult gains a counter. A superseded write is a success: the store
// already holds a newer version, so the key is committed.
type BulkWriteResult struct {
    // ...
    SupersededCount int64
}
```

With `WithVersionGuard()`, the engine, DLQ `Upsert` and `WriteIf` set `Version`. Each bundled sink
**stamps** the version into the record (fields named in its `Config.VersionFields`, default
`_sluice_e` / `_sluice_v`) and **applies it only if the stored version is older**:

| Sink | Guarded write | "Store is newer" outcome |
|---|---|---|
| `sink/postgres` | `INSERT … ON CONFLICT (…) DO UPDATE SET … WHERE (t._sluice_e, t._sluice_v) < (EXCLUDED._sluice_e, EXCLUDED._sluice_v)` | Row skipped by the `WHERE`; not an error |
| `sink/docdb` | `UpdateOne({_id, $or: [{_sluice_e: {$lt: e}}, {_sluice_e: e, _sluice_v: {$lt: v}}]}, $set, upsert)` | 11000 on `_id_`: the doc exists and failed the guard, *or* a concurrent insert won. The sink re-issues that model once **without** upsert: matched → applied; not matched → superseded |
| `sink/dynamodb` | Per-item `PutItem` with `attribute_not_exists(pk) OR _sluice_e < :e OR (_sluice_e = :e AND _sluice_v < :v)`, run in parallel. `BatchWriteItem` can't be conditional | `ConditionalCheckFailedException` → superseded |

Superseded writes are counted in `sluice_<ns>_flush_superseded_total{band}`. A steady non-zero rate
means overlapping flushes are common, and a lease would save wasted writes.

**Relation to RFP #17.** Stamping in the sink makes RFP #17's `VersionedWriteContract` unnecessary
for bundled sinks: the version reaches the store without contract changes. `VersionOf` is still
needed to read the version back through `Source` (§9.1).

### 4.3 Conditional writes

```go
type Condition interface{ isCondition() }

func IfAbsent() Condition                // no record exists (journal or store)
func IfVersion(v Version) Condition      // current version is exactly v

// Optimistic state-machine check: read current, call fn, then CAS on the version read.
// fn returns an error to reject the transition (e.g. captured → authorized).
func IfTransition(fn func(current []byte, v Version) error) Condition

func (s *Sluice) WriteIf(ctx context.Context, key string, payload []byte, c Condition) (Version, error)

type VersionConflictError struct{ Current Version } // errors.Is(err, ErrVersionConflict)
var ErrVersionConflict = errors.New("sluice: version conflict")
var ErrTransitionRejected = errors.New("sluice: transition rejected")
```

- **Journal CAS.** A new `casWriteLua` compares the hash's `{e, v}` with the expected version and
  writes only if they match (`IfAbsent` requires no hash). Otherwise it returns the current version.
  It is the same single-slot script shape as `atomicWriteLua`.
- **Journal miss.** A cold key's hash may have expired after its flush, while the record lives in
  the store. Before the CAS, `WriteIf` reads it through `Source` + `ReadContract`, extracts its
  version with `VersionOf`, and **hydrates the hash with that version** (`hydrateLua` extended to
  set `e` and `v`; it still only fills a miss). The CAS then runs against the real current version,
  so `IfAbsent` can't succeed for a record that exists only in the store.
- `IfTransition` retries on `ErrVersionConflict` up to `StrictConfig.MaxCASRetries` (default 3), re-reading each time.
- `WriteIf` goes through the same durable ack (§4.1), ledger (RFP #17) and indexing path as `Write`.
- Without `VersionOf`, `WriteIf` returns an error on a journal miss rather than guess.

### 4.4 Strict mode

```go
type StrictConfig struct {
    Durability    Durability // required
    VersionOf     VersionOf  // required: WriteIf miss hydration and reconciliation
    MaxCASRetries int        // default 3
}

func (b *Builder) WithStrictMode(cfg StrictConfig) *Builder
```

`Build` enforces these rules and returns `ErrStrictModeConfig` naming each violation, rather than silently overriding the configuration:

| Rule | Why |
|---|---|
| A `Durability` is set | Goal 1 |
| The sink implements `VersionGuarder`; the version guard is on | Goal 2 |
| `WithDegradedModeDirect(false)` | A direct store write bypasses the durable journal, the guard and the ledger. Callers get `ErrRedisUnavailable` and retry |
| No `WithLocalCache` | `Read` must not be stale; with L1 off, `Read` reads L2 and then L3 |
| DLQ entries never expire (TTL 0 → `PERSIST`) | Goal 4. Needs `deadLetterIfUnchangedLua` and `MoveToDeadLetter` to treat 0 as `PERSIST`, since `PEXPIRE 0` deletes |
| `WithDLQAutoProcess` uses only `DLQUpsert`; `ProcessDLQ` refuses `DLQIgnore` and `DLQReInsert` | Ignore deletes a transaction; ReInsert changes its key |
| `drainAction` never discards an entry silently; it logs and counts | Goal 4, and also worth doing for every namespace |

**Transaction history** is not built into strict mode. A strict namespace coalesces status changes per
transaction to its latest state, which is right for current status. For an audit trail, combine it
with the RFP #17 ledger in `PerEvent` + `Required` mode: every accepted write, with its version, is
then recorded outside Redis before `Write` returns.

### 4.5 Metrics

- `RecordDurableAck(namespace string, d time.Duration, err error)`: `WAIT` latency and failures.
- `RecordSuperseded(namespace, band string, count int)`.
- `RecordCASConflict(namespace string)` and `RecordTransitionRejected(namespace string)`.

These are breaking additions for custom `MetricsRecorder`s, released together with RFP #17's.

---

## 5. Implementation Plan

Each phase ends with gofmt, `go build`, `go vet` (also `-tags integration`), and a green full `go test ./...`. Mutation-check each new test.

### Phase 0: prerequisite
- RFP #17 Phase 0: `Version{Epoch, Seq}` returned by the write scripts and read by `DrainBand`/`DrainDLQ`.

### Phase 1: version guard (fixes §1.1 for every namespace)
- `WriteModel.Version`, `VersionGuarder`, `SupersededCount`; the engine and DLQ `Upsert` set `Version` when `WithVersionGuard()` is on.
- Guarded writes in `sink/postgres`, `sink/docdb` (with the one-shot retry without upsert) and `sink/dynamodb` (conditional `PutItem`).
- Superseded metric. Recommend enabling the guard by default in a later release.

### Phase 2: durable acknowledgement
- `DurableAckReplicas`, `DurableAckAOF` and `DurableStore`; `WAIT`/`WAITAOF` pipelined in the plain,
  dedup, batched and idempotent write paths; `ErrDurabilityUnconfirmed`.
- docker-compose: a Redis primary with one replica, for tests.

### Phase 3: conditional writes
- `casWriteLua`; `hydrateLua` setting `e`/`v`; `WriteIf` with `IfAbsent`, `IfVersion` and `IfTransition`.

### Phase 4: strict mode
- `WithStrictMode` and the `Build` rules (§4.4); DLQ TTL 0 → `PERSIST`; the `drainAction` discard log and metric; restricted DLQ strategies.
- README "Strict mode" section; CHANGELOG; `examples/payments/<adapter>/` (authorize → capture → refund, with a rejected out-of-order transition).

---

## 6. Success Criteria / Test Plan

| Test | Asserts |
|---|---|
| **Stale flush (§1.1), reproduced** | Two engines on one band, with a sink that delays v5's `BulkWrite` until v6 is written and committed. **Without** the guard, the store ends at v5 (this test documents the bug). **With** it, the store ends at v6 and `SupersededCount == 1`. Run once per sink. |
| Guard per sink | Postgres: the `WHERE` skips an older row. DocDB: an older version on an existing doc → superseded; a concurrent insert race → the re-issue applies the newer version. DynamoDB: `ConditionalCheckFailed` → superseded. |
| Unguarded is unchanged | With `Version == nil`, every sink behaves exactly as today. |
| Durable ack, success | A primary with 1 replica and `Replicas: 1`: `Write` returns nil, and the entry is on the replica. |
| Durable ack, failure | Replica stopped: `Write` returns `ErrDurabilityUnconfirmed` within `Timeout`; the entry is still journaled and flushes. |
| Batched durable ack | 500 concurrent writes under `WithBatchedWrites` issue one `WAIT` per pipeline; each caller gets the result. |
| `IfVersion` | A stale expected version returns `VersionConflictError` with the current version, and nothing is written. |
| `IfAbsent` across expiry | Write, flush, let the hash expire: `IfAbsent` fails, because the store record is hydrated with its version. |
| `IfTransition` | `captured → authorized` returns `ErrTransitionRejected`. 50 concurrent valid transitions all apply in some serial order, with no lost update. |
| Strict `Build` | Each rule in §4.4, violated alone, fails `Build` with `ErrStrictModeConfig` naming it. |
| DLQ never expires | A strict dead-letter has `PTTL == -1`; `DLQIgnore` and `DLQReInsert` are refused. |
| **Failover** | Primary + replica with `Replicas: 1`: write 10,000 keys, kill the primary mid-stream, promote the replica. Every `Write` that returned nil is in the store after the flush. |

---

## 7. Known Limits

1. **`WAIT` is not consensus.** Redis's own documentation notes that `WAIT` doesn't make it strongly
   consistent: a failover can still, in edge cases, promote a replica that missed an acknowledged
   write. `DurableStore` on a store with a durable transaction log (MemoryDB) is the stronger option.
2. **Latency.** `WAIT` adds a replication round-trip to every write, roughly 1–5 ms within one
   region. `WithBatchedWrites` amortises it across concurrent writers.
3. **DynamoDB guarded writes** use one conditional `PutItem` per item instead of `BatchWriteItem`.
   WCU cost is the same, but call count and client concurrency rise.
4. **Epoch skew** (RFP #17 §8.2) also applies to the guard. Within one hash lifetime `Seq` decides,
   and hydration on miss (§4.3) carries the store's version forward, so a new lifetime continues from it.
5. **Single-record atomicity only** (§3).

## 8. Out of Scope

- Cross-region replication of the journal.
- Holding a transaction in Redis until an external event (auth hold expiry, etc.). That is application logic on top of `WriteIf`.

## 9. Open Questions

1. **Version stamping: sink vs. contract.** With §4.2, bundled sinks stamp the version themselves.
   Should RFP #17 drop `VersionedWriteContract` and use sink stamping (plus `VersionOf` for reads),
   keeping the contract only for custom sinks? Recommended: yes.
2. **Guard on by default.** §1.1 affects every namespace. Should `WithVersionGuard()` become the
   default once Phase 1 is proven, with an opt-out? Existing store records have no version, so the
   first guarded write must treat "no version" as older (all three guards above do).
3. **Managed-service support.** Verify `WAIT`/`WAITAOF` on ElastiCache (Redis OSS and Valkey
   engines) and on MemoryDB, where `WAIT` may be unnecessary or unsupported. Document the recommended `Durability` per service.
4. **Default `Replicas`.** Require an explicit value, or default to 1?
