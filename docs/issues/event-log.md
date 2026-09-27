# RFP #17: Event Ledger — An Opt-in, Redis-Independent Record of Acknowledged Writes for Reconciliation

**Status:** Proposed (revised 2026-09-27; replaces the earlier "second journal in the same Redis" draft)  
**Target Release:** next minor after v1.0.8  
**Dependencies:** RFP #18 (sink silent-loss fixes, merged), durability hardening in `[Unreleased]`, RFP #16 (Prometheus metrics)

---

## 1. Context & Problem Statement

`sluice` acknowledges a write once it is in the Redis journal, and flushes it to the datastore later.
Between the ack and the flush, Redis holds the only copy.

Upstream systems already cover every failure that **returns an error**:

- **Synchronous hot writes** (mobile app, web) are retried by the client when `Write` fails.
- **Asynchronous writes** (webhooks, SQS, Kafka) are redelivered when the consumer doesn't ack.

Since the durability hardening, `Write` returns nil only after the entry is journaled, so a failed
`Write` is always retried by someone. What upstream retries **cannot** cover is a loss *after* the ack:

| Silent loss after ack | Why no one retries |
|---|---|
| Redis node loss, or a failover to a lagging replica | The caller already got nil; the dirty entry vanishes with the payload, so no loss metric fires |
| Eviction, `FLUSHDB`, manual deletion | Same |
| A flush/commit bug that commits a key it did not write | The key leaves the dirty set as if it were written |

The goal of this RFP is to **check and correct** these cases: keep an independent record of what was
acknowledged, and let a reconciler compare it with the datastore.

### 1.1 Why the earlier draft was rejected

The earlier draft kept events in a second journal **in the same Redis**, and made `WriteEvent` fail
if either the event or the state write failed.

1. **It shared Redis's failures.** The losses above take the event journal with them, so the
   reconciler would see nothing wrong. A record kept for reconciliation must take a path that shares
   no component with the journal.
2. **It gated the write path.** A side feature could fail the primary write.
3. **It was the most complex option**: a derived Shield, a second engine, an event DLQ that never
   expires, and a reordered `DrainAndClose`.

### 1.2 Not a reporting source

Recording every event for analytics (the coalescing problem: two events for one key in one flush
window reach the store as one) is **not** a goal. Reporting should consume the stream or an
analytics pipeline directly. The ledger records acknowledged state so that it can be verified and
repaired.

---

## 2. Goals

1. **Opt-in per instance.** Off by default, with no cost when off. When on, every acknowledged write on that instance is recorded.
2. **Independent of Redis.** The ledger path never reads or writes Redis.
3. **Never breaks the write flow** in the default mode: a ledger failure never changes `Write`'s result.
4. **The ledger's own losses are loud**: a metric and a log line, never a silent drop (except the
   crash window in §8.1, which is documented).
5. **Enough to correct.** After a Redis loss, a reconciler can find keys whose datastore state is
   behind an acknowledged write and replay them.

## 3. Non-Goals

- Exactly-once recording of every event for reporting (§1.2).
- Replacing Kafka/SQS as a durable log. For async paths the topic already *is* the ledger; the
  ledger is aimed mainly at the synchronous path, which has no upstream log.
- A datastore-side version guard (compare-and-set on the state document).

---

## 4. Resolved Decisions

| Question | Decision |
|---|---|
| Where the ledger lives | Behind a pluggable `ledger.Ledger` interface. Bundled: DocumentDB, PostgreSQL, DynamoDB tables and a `ledger.Func` adapter (Kafka, Firehose, S3, …). **Never Redis.** |
| What is recorded | Only **acknowledged** writes: those where `Write` returns nil, including degraded direct writes. |
| Effect on `Write` | `BestEffort` (default): none. `Required` (opt-in): `Write` also waits for the ledger. |
| Granularity | `LatestPerKey` (default): within one ledger flush window, a pod keeps only the latest record per key. `PerEvent`: one record per write. |
| Event identity | Optional. `WriteIdempotent` uses its `idempotencyKey`; the new `WriteWith` accepts `WithEventID(id)`. |
| Scope | **Per instance, all writes.** With `WithLedger` configured, every acknowledged `Write`, `WriteIdempotent` and `WriteWith` is recorded. There is no per-call opt-out. To leave a path unrecorded (e.g. an async path whose topic is already replayable), run it on an instance without `WithLedger`. |
| API compatibility | `Write`'s signature and behaviour are unchanged. Per-call options go through a new `WriteWith` method. |
| Per-key version | **Recorded** as `Version{Epoch, Seq}` (§5.2): the existing sequence `v`, plus a hash-creation epoch, because `v` restarts at 1 whenever a key's payload hash is recreated. An opt-in `VersionedWriteContract` lets the version reach the datastore document so reconciliation can compare exactly. `Write` and `WriteContract` are unchanged. |

`LatestPerKey` is the default because reconciliation only needs the latest acknowledged state per
key. `PerEvent` brings the velocity problem back to the ledger store (one row per event).

---

## 5. Proposed Design

### 5.1 Public API

```go
package ledger

type Record struct {
    ID             string    // event ID if given, else generated once at enqueue; stable across retries
    Namespace      string
    CorrelationKey string
    EventID        string    // optional
    Payload        []byte    // nil when IncludePayload is false
    ContentHash    uint64    // xxHash64 of Payload
    Version        Version   // journal version of this write (§5.2)
    AckedAt        time.Time // pod clock when Write acknowledged; informational, not used for ordering
    Pod            string    // instance ID
}

// Version orders writes to one key. Compare Epoch first, then Seq.
type Version struct {
    Epoch int64 // ms timestamp of the write that created the key's current payload hash
    Seq   int64 // per-key sequence 'v' within that hash; 0 for a degraded direct write
}

func (a Version) Less(b Version) bool

// Ledger appends records to a store that does not depend on Redis.
// Append must be idempotent on Record.ID: a retried batch must not duplicate rows.
// An error means the whole batch may have failed and will be retried.
type Ledger interface {
    Append(ctx context.Context, records []Record) error
    Close(ctx context.Context) error
}

// Reader is optional; the reconciler (§5.7) needs it.
type Reader interface {
    Scan(ctx context.Context, namespace string, from, to time.Time, fn func(Record) error) error
}

// Func adapts a function (e.g. a Kafka producer) to Ledger.
type Func func(ctx context.Context, records []Record) error

type Mode int
const (
    BestEffort Mode = iota // default
    Required
)

type Granularity int
const (
    LatestPerKey Granularity = iota // default
    PerEvent
)
```

```go
package sluice

func (b *Builder) WithLedger(l ledger.Ledger, opts ...LedgerOption) *Builder

// LedgerOptions (defaults in brackets):
//   LedgerMode(m)             [BestEffort]
//   LedgerGranularity(g)      [LatestPerKey]
//   LedgerFlushWindow(d)      [1s]
//   LedgerBatchSize(n)        [1000]
//   LedgerBufferSize(n)       [100_000]   records held in memory before BestEffort drops
//   LedgerIncludePayload(b)   [true]      needed to correct, not just detect
//   LedgerRetry(max, backoff) [5, 100ms exponential]
//   LedgerAckTimeout(d)       [2s]        Required mode only

// Write is unchanged. WriteWith is Write plus per-call options; with no
// options it behaves exactly like Write.
func (s *Sluice) WriteWith(ctx context.Context, key string, payload []byte, opts ...WriteOption) error
func WithEventID(id string) WriteOption // stored as Record.EventID and used as Record.ID

// Optional, instead of WithWriteContract. Receives the version of the payload
// being flushed so it can be stored in the document (e.g. _sluice_e, _sluice_v).
type VersionedWriteContract func(key string, payload []byte, v ledger.Version) (*WriteModel, error)
func (b *Builder) WithVersionedWriteContract(fn VersionedWriteContract) *Builder

var ErrLedgerUnavailable = errors.New("sluice: ledger append failed; the write is journaled but not ledgered")
```

### 5.2 Write version

Every write already bumps a per-key sequence `v` in the payload hash, which the conditional commit
compares against. On its own, `v` is **not** monotonic per key. It restarts at 1 whenever the hash is
recreated:

- after a cold key's post-flush TTL (`KeyTTL`, default 30s) expires;
- after hydration seeds a hash (hydration doesn't set `v`);
- after Redis is lost, which is exactly the case reconciliation is for.

Comparing a ledger record's `v` with the store's `v` across one of these resets would call a stale
record "newer" and replay it. The fix is an **epoch**: the timestamp of the write that created the
current hash. The version is `(Epoch, Seq)`, compared lexicographically.

- **Within one hash lifetime**, `Seq` gives the exact arrival order, with no clock involved.
- **Across lifetimes**, `Epoch` orders them. A hash is only recreated after it expires (at least
  `KeyTTL` after its last flush) or after a Redis loss, so consecutive epochs are far apart compared with clock skew between pods.

**Script changes** (internal only):

- `atomicWriteLua` and `atomicDedupWriteLua` add `HSETNX <hash> e <ts>` (one command) and return
  `{e, v}` instead of `1`. A deduplicated write still returns `0`.
- `DrainBand` and `DrainDLQ` read `e` alongside `p`, `ts` and `v`, so the engine and DLQ replay know
  the version of each payload they flush.
- A hash written before this change gets `e` on its next write. Its `v` continues from the old value,
  which is still correct within that lifetime.
- The returned version is propagated through every write path: plain, dedup, batched (`EvalSha`
  pipeline, handed back to each waiter) and broadcast.
- A **degraded direct write** has no hash, so its version is `{Epoch: now, Seq: 0}`. Degraded mode
  writes directly only when nothing is pending for the key, so no journal version competes with it.

**Getting the version into the store.** `WithVersionedWriteContract` is an opt-in alternative to
`WithWriteContract`. The engine, DLQ `Upsert` and degraded mode pass it the version of the payload they
are writing, and it stores that version in the document. `WriteContract` keeps working unchanged;
without a versioned contract, reconciliation falls back to a user `VerifyContract` (§5.7).

### 5.3 Write path

`Write` and `WriteIdempotent` delegate to the same internal path as `WriteWith`, so all three are
recorded identically. The ledger step runs **after** that path has decided to return nil: after a
successful journal write or a successful degraded direct write. It never runs for a write that returns an error, because
upstream retries that write.

| Mode | Behaviour |
|---|---|
| `BestEffort` | Non-blocking enqueue. If the buffer is full, drop the record and count `ledger_dropped_total{reason="buffer_full"}`. `Write` returns nil either way. |
| `Required` | Enqueue, then wait for the batch holding this record to be appended (the ack-on-commit pattern already used by `WithBatchedWrites`). On failure or `LedgerAckTimeout`, return `ErrLedgerUnavailable`. The journal write has already happened; a retry is safe, because the journal is latest-wins and the ledger is idempotent on `Record.ID`. |

With `WithContentDedup`, a write that is skipped as unchanged is ledgered only under `PerEvent`, with
the version of the payload already in the journal.

### 5.4 Ledger writer

- One writer per `Sluice` instance, entirely in process: a bounded buffer and a goroutine that
  appends every `LedgerFlushWindow`, or sooner once `LedgerBatchSize` records are waiting.
- `LatestPerKey` keeps a `map[key]Record` per window and replaces an entry only when the new record's
  `Version` is greater, so concurrent writers on one pod can't make an older version win.
  Required-mode waiters of a replaced record are released by the append of the record that replaced it.
- A failed `Append` is retried with backoff. When retries run out:
  - `BestEffort`: the batch is dropped, counted as `ledger_dropped_total{reason="append_failed"}` and logged at error level with the key count;
  - `Required`: the waiters get `ErrLedgerUnavailable`.
- **`DrainAndClose`** stops accepting writes, flushes the ledger buffer, then closes the ledger. The
  ledger has no Redis dependency, so it doesn't matter whether this runs before or after the Redis
  client closes.

### 5.5 Bundled ledgers

`sluice` still emits no DDL. Each package documents the schema and a retention mechanism.

| Package | Append | Idempotency | Retention |
|---|---|---|---|
| `ledger/docdb` | unordered `InsertMany` | `_id = Record.ID`; 11000 on `_id_` counts as success | TTL index on `acked_at` |
| `ledger/postgres` | multi-row `INSERT … ON CONFLICT (id) DO NOTHING` | primary key `id` | time-partitioned table; drop old partitions |
| `ledger/dynamodb` | `BatchWriteItem`, 25-item chunks, `UnprocessedItems` retried | `PK = namespace#key`, `SK = acked_at#id` | DynamoDB TTL attribute |
| `ledger.Func` | your function | yours | yours |

**Isolation.** The bundled ledgers are isolated from Redis, but they share the datastore server when
pointed at the same cluster. That's acceptable: the ledger verifies the Redis → datastore path. If
you need it isolated from the datastore as well, use a separate cluster or `ledger.Func` to Kafka/S3.

### 5.6 Metrics

New `MetricsRecorder` methods (a breaking change for custom recorders, as with `RecordUnflushedExpiry`):

- `RecordLedgerAppend(namespace string, records int, d time.Duration, err error)`
- `RecordLedgerDrop(namespace, reason string, records int)`, where reason is `buffer_full`, `append_failed` or `shutdown_timeout`
- `RecordLedgerBufferDepth(namespace string, depth int)`

A reference alert fires on any non-zero `ledger_dropped_total`. That's a warning, not critical: a
drop weakens the reconciliation's coverage but doesn't lose data by itself.

### 5.7 Reconciliation

```go
// With a VersionedWriteContract: extract the version the contract stored.
// ok is false for a document written without one (e.g. before it was enabled).
type VersionOf func(stored []byte) (v ledger.Version, ok bool)

// Without one: decide from the document itself.
type VerifyContract func(key string, rec ledger.Record, stored []byte) (Verdict, error)

type Verdict int
const (
    InSync Verdict = iota
    Behind  // the store is older than rec: correct it
    Unknown // can't tell: report it, don't correct it
)

type ReconcileOptions struct {
    From, To  time.Time
    VersionOf VersionOf      // set this or Verify; VersionOf is preferred
    Verify    VerifyContract // store documents are shaped by the contract, so bytes can't be compared directly
    Correct   bool           // false = report only
    Settle   time.Duration  // ignore records newer than now-Settle (default 2×FlushWindow + KeyTTL)
}

func (s *Sluice) Reconcile(ctx context.Context, opts ReconcileOptions) (*ReconcileResult, error)
```

1. `Scan` the ledger over `[From, To]` and keep the record with the greatest `Version` per key.
2. Skip keys still pending in Redis (`HasPendingVersion`), if Redis is reachable. Those are unflushed, not lost.
3. Read the stored document through `Source` + `ReadContract`, then decide:
   - with `VersionOf`: a missing document or `stored.Less(rec.Version)` is `Behind`; an equal or
     greater version is `InSync`; `ok == false` is `Unknown`;
   - otherwise, call `Verify`.
4. With `Correct`, re-`Write` the ledger payload for each `Behind` key. It goes through the journal,
   so ordering against new traffic follows normal latest-arrival semantics.
5. Report `checked`, `in_sync`, `behind`, `corrected` and `unknown` counts, and record them as metrics.

`Reconcile` is a one-shot call, like `ProcessDLQ`. Schedule it yourself (a ticker, Asynq, a cron
job), typically after a Redis incident or on a slow periodic cadence.

---

## 6. Implementation Plan

Each phase ends with gofmt, `go build`, `go vet` (also `-tags integration`), and a green full `go test ./...`. Mutation-check each new test.

### Phase 0: write version
Useful on its own, so it can ship before the ledger.
- `ledger.Version`; the epoch `e` and the `{e, v}` return in both write scripts; the version
  propagated through the plain, dedup, batched and broadcast write paths, and read by `DrainBand` and `DrainDLQ` (§5.2).
- `WithVersionedWriteContract`, wired into the engine, DLQ `Upsert` and degraded mode.

### Phase 1: ledger core
- `ledger` package: `Record`, `Ledger`, `Reader`, `Func`, `Mode`, `Granularity`.
- The ledger writer (§5.4); `WithLedger` and its options; `WriteWith` and `WithEventID`; `WriteIdempotent` passing its key as the event ID. `Write` keeps its signature.
- Metrics (§5.6), including the noop, log and Prometheus recorders.
- `DrainAndClose` flushes the ledger.

### Phase 2: bundled ledgers
- `ledger/docdb`, `ledger/postgres`, `ledger/dynamodb`, each with a `Reader`.

### Phase 3: reconciliation
- `Reconcile` (§5.7), with an example under `examples/reconcile/<adapter>/`.

### Phase 4: docs
- README "Event ledger and reconciliation" section; CHANGELOG with the breaking note (new `MetricsRecorder` methods).

---

## 7. Success Criteria / Test Plan

| Test | Asserts |
|---|---|
| Version survives hash recreation | Write, flush, let the post-flush TTL expire, write again: the second version is greater than the first (`Seq` restarts at 1, `Epoch` increases). The same holds after `FLUSHALL`. |
| Version is arrival order | 200 concurrent writes to one key: the returned versions are distinct and totally ordered, and the journal holds the one with the greatest version. |
| Pre-versioning hash | A hash with `v` but no `e` gets `e` on its next write, and `Seq` continues. |
| Versioned contract | The engine, DLQ `Upsert` and degraded mode each pass the version of the payload they write; a plain `WriteContract` still works unchanged. |
| Off by default | Without `WithLedger`, `Write` does no ledger work and allocates nothing for it. |
| No Redis dependency | With Redis stopped after a write was acked, the ledger buffer still appends. The ledger package does not import `internal/shield` (checked in a test). |
| BestEffort never fails `Write` | A ledger whose `Append` always fails: `Write` returns nil; `ledger_dropped_total{reason="append_failed"}` counts every record. |
| Buffer full | Buffer 10, a blocked ledger, 100 writes: all return nil, 90 are counted as dropped. |
| Required | A failing ledger makes `Write` return `ErrLedgerUnavailable`; after the ledger recovers, a retry returns nil and leaves one record per `Record.ID`. |
| LatestPerKey | 200 writes to one key in one window produce 1 record with the last payload. `PerEvent` produces 200. |
| Only acked writes | A write that returns an error (Redis down, degraded refused) produces no record. |
| All write methods recorded | On a ledgered instance, `Write`, `WriteIdempotent` and `WriteWith` each produce a record. `WriteIdempotent`'s record has `EventID == idempotencyKey`; `WriteWith(…, WithEventID(id))` has `EventID == id`. |
| `Write` unchanged | Existing `Write` tests pass untouched, and `WriteWith` with no options matches `Write` on an instance without a ledger. |
| Idempotent append | Each bundled ledger: appending the same batch twice leaves one row per ID. |
| Shutdown | `DrainAndClose` appends everything buffered. |
| **Redis loss end to end** | Write 1,000 keys with a 1h flush window, wait for the ledger to append, `FLUSHALL` Redis, run `Reconcile{Correct: true}`: all 1,000 keys reach the store with the latest payload, and `behind == corrected == 1000`. |
| Reconcile skips pending keys | A key still dirty in Redis is neither counted as behind nor corrected. |
| No regression across a reset | Flush version `{E1, 5}`, lose Redis, write and flush `{E2, 1}` with `E2 > E1`: an old ledger record `{E1, 5}` is `InSync`, not `Behind`, and nothing is replayed. |

---

## 8. Known Limits

1. **BestEffort crash window.** Records buffered in a pod that crashes (up to `LedgerFlushWindow`)
   are lost without a metric. Use `Required` where that window matters.
2. **Epoch and clock skew.** `Epoch` is the writing pod's clock. It matters only when a hash is
   recreated within the skew window of the previous one, which normal expiry can't cause (at least
   `KeyTTL` apart). After an abrupt Redis loss, a hash created within that window of the loss could be
   mis-ordered. `Seq` ordering within one lifetime uses no clock.
3. **Replay race.** Between `Reconcile` deciding `Behind` and its re-`Write` landing, a new write for
   the key could land and flush; the replay then overwrites it. Narrow it by re-checking the journal
   just before replaying. Without `VersionOf`, `Verify` must return `Behind` only when it can tell the
   store is older, and `Unknown` otherwise.
4. **Volume.** Under `LatestPerKey` the ledger sees at most one record per key per pod per window. At
   the design envelope (~80K unique keys/s) that is still a large append stream. For high-volume
   namespaces, use `ledger.Func` to Kafka or Firehose, or put only the synchronous path on a ledgered
   instance and keep async ingesters on an instance without `WithLedger`.

## 9. Decisions Log

Resolved 2026-09-27:

1. **`Write` is not changed.** Per-call options go through a new `WriteWith` method, so existing
   callers, and code that stores `s.Write` as a function value or uses it to satisfy an interface, are unaffected.
2. **The per-key version is recorded** (§5.2). The sequence `v` already exists, so returning it adds
   no real compute and doesn't undo coalescing. Because `v` restarts when a hash is recreated, it is
   paired with a hash-creation epoch. The version reaches the store through the opt-in
   `VersionedWriteContract`; `WriteContract` is unchanged.
3. **A ledgered instance records all writes.** Enabling the ledger is an instance-level decision, not a per-call choice.
