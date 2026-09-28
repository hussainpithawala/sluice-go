# RFP #17: Event Ledger — An Opt-in, Redis-Independent Record of Acknowledged Writes for Reconciliation

**Status:** Proposed (revised 2026-09-29; the ledger store changes from bundled datastore adapters to an embedded Pebble store backed by object storage; earlier revision 2026-09-27 replaced the "second journal in the same Redis" draft)  
**Target Release:** next minor after v1.0.8  
**Dependencies:** RFP #18 (sink silent-loss fixes, merged), durability hardening in `[Unreleased]`, RFP #16 (Prometheus metrics). Sink-side version stamping is RFP #21 (§5.2).
 
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

### 1.1 Why the earlier drafts were rejected

**Draft 1: a second journal in the same Redis.** `WriteEvent` failed if either the event or the state write failed.

1. **It shared Redis's failures.** The losses above take the event journal with them, so the
   reconciler would see nothing wrong. A record kept for reconciliation must take a path that shares
   no component with the journal.
2. **It gated the write path.** A side feature could fail the primary write.
3. **It was the most complex option**: a derived Shield, a second engine, an event DLQ that never
   expires, and a reordered `DrainAndClose`.
   **Draft 2: bundled ledgers in DocumentDB, PostgreSQL and DynamoDB (revised 2026-09-27).** This fixed the
   Redis dependency, but it put a per-key append stream on a datastore the operator then had to size,
   index, partition and expire, and each adapter needed its own schema, idempotency and retention story.
   The revised design keeps the ledger local and cheap to write (an embedded Pebble store) and uses
   object storage only for durability and for cross-pod reads. There is one storage engine, one record
   format and one conformance suite, instead of three datastore adapters.

### 1.2 Not a reporting source

Recording every event for analytics (the coalescing problem: two events for one key in one flush
window reach the store as one) is **not** a goal. Reporting should consume the stream or an
analytics pipeline directly. The ledger records acknowledged state so that it can be verified and
repaired.
 
---

## 2. Goals

1. **Opt-in per instance.** Off by default, with no cost when off. When on, every acknowledged write on that instance is recorded.
2. **Independent of Redis.** The ledger path never reads or writes Redis, and the `ledger` package does not import `internal/shield`.
3. **Never breaks the write flow** in the default mode: a ledger failure never changes `Write`'s result.
4. **The ledger's own losses are loud**: a metric and a log line, never a silent drop (except the
   crash window in §8.1, which is documented).
5. **Enough to correct.** After a Redis loss, a reconciler can find keys whose datastore state is
   behind an acknowledged write and replay them.
6. **Durable on request.** In `Required` mode a record is in object storage before `Write` returns, so a pod crash after the ack loses nothing the caller was told was recorded.
## 3. Non-Goals

- Exactly-once recording of every event for reporting (§1.2).
- Replacing Kafka/SQS as a durable log. For async paths the topic already *is* the ledger; the
  ledger is aimed mainly at the synchronous path, which has no upstream log.
- A datastore-side version guard (compare-and-set on the state document).
- Stamping the version into datastore documents. That is RFP #21's sink-side stamping; this RFP only records and compares versions (§5.2).
- Bundled datastore ledgers (DocumentDB, PostgreSQL, DynamoDB). Removed in this revision (§9).
---

## 4. Resolved Decisions

| Question | Decision |
|---|---|
| Where the ledger lives | An embedded **Pebble** store on each pod, with SSTables and (in `Required` mode) WAL segments in **object storage** behind a small `objstore.Storage` interface. Adapters: S3, GCS, Azure Blob, local filesystem. **Never Redis.** |
| Escape hatch | `ledger.Func` is kept for forwarding records to Kafka, Firehose or raw S3. |
| What is recorded | Only **acknowledged** writes: those where `Write` returns nil, including degraded direct writes. |
| Effect on `Write` | `BestEffort` (default): none. `Required` (opt-in): `Write` also waits for the record's WAL segment to be uploaded to object storage. |
| Granularity | `LatestPerKey` (default): one record per key; within one ledger flush window a pod keeps only the latest, and Pebble compaction keeps the latest version. `PerEvent`: one record per write, never merged. |
| Event identity | Optional. `WriteIdempotent` uses its `idempotencyKey`; the new `WriteWith` accepts `WithEventID(id)`. Under `PerEvent` the event ID is part of the key. |
| Scope | **Per instance, all writes.** With `WithLedger` configured, every acknowledged `Write`, `WriteIdempotent` and `WriteWith` is recorded. There is no per-call opt-out. To leave a path unrecorded (e.g. an async path whose topic is already replayable), run it on an instance without `WithLedger`. |
| API compatibility | `Write`'s signature and behaviour are unchanged. Per-call options go through a new `WriteWith` method. |
| Per-key version | **Recorded** as `Version{Epoch, Seq}` (§5.2): the existing sequence `v`, plus a hash-creation epoch, because `v` restarts at 1 whenever a key's payload hash is recreated. `VersionedWriteContract` is **dropped**; getting the version into the datastore document is deferred to RFP #21's sink-side stamping. |
| Correlation keys | A `\x00` byte is rejected in correlation keys at `Write`/`WriteIdempotent`/`WriteWith` validation (`ErrInvalidCorrelationKey`), because it is the ledger key separator (§5.4). |
| Cross-pod reads | The reconciler reads other pods' SSTables and WAL segments directly from object storage; there is no `ledger.Reader` interface. |

`LatestPerKey` is the default because reconciliation only needs the latest acknowledged state per
key. `PerEvent` keeps every write, so its storage grows with event volume rather than key count.
 
---

## 5. Proposed Design

### 5.1 Public API

```go
package ledger
 
type Record struct {
    Namespace      string
    CorrelationKey string
    EventID        string    // optional; part of the key under PerEvent
    Payload        []byte    // nil when IncludePayload is false
    ContentHash    uint64    // xxHash64 of Payload
    Version        Version   // journal version of this write (§5.2)
    AckedAt        time.Time // pod clock when Write acknowledged; informational, not used for ordering
    Pod            string    // instance ID
}
 
// Version orders writes to one key. Compare Epoch first, then Seq.
type Version struct {
    Epoch int64 // ms timestamp of the write that created the key's current payload hash
    Seq   int64 // per-key sequence 'v' within that hash; 0 is reserved for degraded direct writes
}
 
func (a Version) Less(b Version) bool
 
// Func adapts a function (e.g. a Kafka producer) to a record sink. Escape hatch only.
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
 
// Config is passed to WithLedger. Defaults are listed in §5.4 and §5.5.
type Config struct {
    Storage objstore.Storage // required: S3, GCS, Azure or FS adapter
    Sync    SyncConfig       // Required mode only
    // Mode, Granularity, FlushWindow, BatchSize, BufferSize, IncludePayload and
    // Retry are set through the LedgerOption values below.
}
 
type SyncConfig struct {
    SyncWindow    time.Duration // [10ms]
    SyncBatchSize int           // [500]
    SyncRetries   int           // [3]
    SyncBackoff   time.Duration // [50ms], exponential
    SyncTimeout   time.Duration
}
```

```go
package objstore
 
// Storage is the only thing the ledger needs from a bucket.
type Storage interface {
    // Put, Get (returns ErrNotFound), Delete (idempotent), List (ordered by key),
    // and an ObjectReader with ReadAt and Size for Pebble's remote SSTable reads.
    // Put is atomic: a concurrent Get never sees a partial object.
}
var ErrNotFound, ErrAlreadyClosed error
```

```go
package sluice
 
func (b *Builder) WithLedger(cfg ledger.Config, opts ...LedgerOption) *Builder
 
// LedgerOptions (defaults in brackets):
//   LedgerMode(m)             [BestEffort]
//   LedgerGranularity(g)      [LatestPerKey]
//   LedgerFlushWindow(d)      [1s]
//   LedgerBatchSize(n)        [1000]
//   LedgerBufferSize(n)       [100_000]   records held in memory before BestEffort drops
//   LedgerIncludePayload(b)   [true]      needed to correct, not just detect
//   LedgerRetry(max, backoff)
 
// Write is unchanged. WriteWith is Write plus per-call options; with no
// options it behaves exactly like Write.
func (s *Sluice) WriteWith(ctx context.Context, key string, payload []byte, opts ...WriteOption) error
func WithEventID(id string) WriteOption // stored as Record.EventID
 
var ErrLedgerUnavailable     = errors.New("sluice: ledger sync failed; the write is journaled but not durably ledgered")
var ErrInvalidCorrelationKey = errors.New("sluice: correlation key is empty or contains a null byte")
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
  **Script changes** (internal only; this ships as Phase 0, independently of the ledger):

- `atomicWriteLua` and `atomicDedupWriteLua` add `HSETNX <hash> e <ts>` (one command) and return
  `{e, v}` instead of `1`. A deduplicated write still returns `0`.
- `FlushRecord` gains `Epoch` alongside its existing `Seq`.
- `DrainBand` and `DrainDLQ` read `e` alongside `p`, `ts` and `v`, so the engine and DLQ replay know
  the version of each payload they flush.
- A hash written before this change gets `e` on its next write. Its `v` continues from the old value,
  which is still correct within that lifetime.
- The returned version is propagated through every write path: plain, dedup, batched (`EvalSha`
  pipeline, handed back to each waiter) and broadcast.
- A **degraded direct write** has no hash, so its version is `{Epoch: now, Seq: 0}`. `Seq: 0` is
  reserved for degraded writes, and this invariant is documented on `Version`. Degraded mode writes
  directly only when nothing is pending for the key, so no journal version competes with it.
- A `\x00` byte is rejected in correlation keys, next to the existing empty-key check, because the
  ledger key format (§5.4) uses it as a separator.
  **Getting the version into the store.** This RFP does not add a versioned write contract. Stamping the
  version into datastore documents is deferred to RFP #21 (sink-side stamping). Until then, reconciliation
  compares versions only where a `VersionOf` can read one from the stored document, and otherwise falls back to a user-supplied
  `VerifyContract` (§5.7).

### 5.3 Write path

`Write` and `WriteIdempotent` delegate to the same internal path as `WriteWith`, so all three are
recorded identically. The ledger step runs **after** that path has decided to return nil: after a
successful journal write or a successful degraded direct write. It never runs for a write that returns an error, because
upstream retries that write. The version is captured from the write script's return value.

| Mode | Behaviour |
|---|---|
| `BestEffort` | Non-blocking enqueue into the writer buffer. If the buffer is full, drop the record and count `ledger_dropped_total{reason="buffer_full"}`. `Write` returns nil either way. |
| `Required` | The record is written to the local Pebble store as in `BestEffort`, then enqueued to the sync buffer, and `Write` blocks until the WAL segment holding it has been uploaded to object storage. On exhausted retries or `SyncTimeout`, return `ErrLedgerUnavailable`. The journal write has already happened; a retry is safe, because the journal is latest-wins and the ledger key makes the re-append idempotent. |

With `WithContentDedup`, a write that is skipped as unchanged is ledgered only under `PerEvent`, with
the version of the payload already in the journal.

### 5.4 Ledger store (Pebble) and writer

**Record encoding.** A record is a 44-byte fixed header followed by its variable-length fields
(namespace, correlation key, event ID, payload).

**Key encoding.**

| Granularity | Key |
|---|---|
| `LatestPerKey` | `{ns}\x00{correlationKey}` |
| `PerEvent` | `{ns}\x00{acked_at_ms_BE_8B}{correlationKey}\x00{eventID}` |

Keys sort by namespace, then correlation key (`LatestPerKey`) or timestamp, key and event ID (`PerEvent`).
Under `LatestPerKey`, Pebble compaction reduces repeated writes of one key to the record with the
greatest `Version`. Under `PerEvent`, nothing is merged.

**Store.** `ledger.Store` owns the Pebble lifecycle:

- `Open` creates an ephemeral local WAL directory and opens Pebble with a `PebbleFactory` (in `objstore`)
  that bridges `objstore.Storage` to Pebble's remote-storage interface, so SSTables live in object storage.
- `WriteBatch` writes a slice of records as one atomic Pebble batch.
- `Scan(namespace, from, to, fn)` iterates records in key order.
  **Writer.** One writer per `Sluice` instance, in process:

- A bounded buffer (`LedgerBufferSize`, default 100,000) and a flusher goroutine that drains it into
  Pebble every `LedgerFlushWindow` (1s), or sooner once `LedgerBatchSize` (1,000) records are waiting.
- `LatestPerKey` keeps a `map[key]Record` per window and replaces an entry only when the new record's
  `Version` is greater, so concurrent writers on one pod can't make an older version win.
- `Enqueue(ctx, Record) error` never blocks in `BestEffort` and always returns nil.
- A failed Pebble write is retried with backoff (`LedgerRetry`). When retries run out in `BestEffort`,
  the batch is dropped, counted as `ledger_dropped_total{reason="append_failed"}` and logged at error
  level with the key count.
- `Drain(ctx)` stops the flusher, does a final Pebble flush, triggers a best-effort compaction, and closes Pebble.
- A **compaction monitor** goroutine tracks which WAL segments are covered by uploaded SSTables and
  deletes covered segments from object storage (a watermark: once compaction covers segment N, segments 1..N are removed).
  **`Required` mode: WAL sync.** A sync goroutine makes each record durable before `Write` returns:

- Required-mode records are collected in a sync buffer, each with a waiter channel.
- Every `SyncWindow` (10ms) or `SyncBatchSize` (500) records, the batch is encoded as a segment and
  uploaded to `{ns}/{pod-id}/wal/{segment_id}.wal`. A segment is a 16-byte header (format version,
  count, CRC64) followed by repeated key-value entries.
- The upload is retried with exponential backoff (`SyncRetries` 3, `SyncBackoff` 50ms). On success, all
  waiters are released with nil; when retries run out, they get `ErrLedgerUnavailable`.
- Re-uploading a segment overwrites the same object, so a retry is idempotent.
- `Drain` stops the sync goroutine only after the final segment is uploaded, then releases any remaining waiters.
  **Lifecycle.** `WithLedger` opens Pebble in `Build`, validates object-store connectivity, and starts the
  ledger goroutines on a context independent of `Build`'s. `DrainAndClose` stops accepting writes, closes
  the sink, then drains the ledger, and closes the object store last. The ledger has no Redis dependency,
  so the position of the Redis client close doesn't matter.

### 5.5 Object storage

`objstore.Storage` is small on purpose (`Put`, `Get`, `Delete`, `List`, and an `ObjectReader` with
`ReadAt` and `Size`), with helpers `DeletePrefix`, `PutBytes` and `GetBytes`. Four adapters ship:

| Package | Backend | Test strategy |
|---|---|---|
| `ledger/objstore/s3` | AWS S3 | Integration test against LocalStack (already in docker-compose) |
| `ledger/objstore/gcs` | Google Cloud Storage | Unit test with a fake; integration skipped without credentials |
| `ledger/objstore/azure` | Azure Blob Storage | Unit test with a fake; integration skipped without credentials |
| `ledger/objstore/fs` | Local filesystem | Unit test; the default in every other phase's tests |

All four pass one shared conformance suite (`RunConformance`): put/get roundtrip, get of a missing
object, idempotent delete, list order, empty list, `ObjectReader.ReadAt`, `ObjectReader.Size`, and put
atomicity (a concurrent `Put` and `Get` never sees a partial object).

**Layout** (all under the object store the operator configures):

```
{ns}/{pod-id}/wal/{segment_id}.wal     Required-mode WAL segments
{ns}/{pod-id}/...                      Pebble SSTables
{ns}/_reconcile/current.json           reconciliation checkpoint (§5.7)
{ns}/_reconcile/history/{run_id}.json  completed run summaries
```

**Isolation.** The ledger shares no component with Redis. It does not depend on the datastore either,
so a datastore incident does not take the ledger with it. Retention of old pods' data is handled by
the reconciler's dead-pod pruning (§5.7), not by a per-backend TTL mechanism.

### 5.6 Metrics

New `MetricsRecorder` methods (a breaking change for custom recorders, as with `RecordUnflushedExpiry`):

- `RecordLedgerAppend(namespace string, records int, d time.Duration, err error)`
- `RecordLedgerDrop(namespace, reason string, records int)`, where reason is `buffer_full`, `append_failed` or `shutdown_timeout`
- `RecordLedgerBufferDepth(namespace string, depth int)`
  `noopMetrics` and the Prometheus recorder are updated, and the Grafana dashboard
  (`dashboards/sluice-overview.json`) gains a ledger panel.

A reference alert fires on any non-zero `ledger_dropped_total`. That's a warning, not critical: a
drop weakens the reconciliation's coverage but doesn't lose data by itself.

### 5.7 Reconciliation

```go
// Extracts the version stored in a datastore document, if the document carries one.
// ok is false for a document without a stamped version.
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
    From, To           time.Time
    VersionOf          VersionOf      // set this or Verify; VersionOf is preferred
    Verify             VerifyContract // store documents are shaped by the contract, so bytes can't be compared directly
    Correct            bool           // false = report only
    Settle             time.Duration  // ignore records newer than now-Settle (default 2×FlushWindow + KeyTTL)
    PruneStaleAfter    time.Duration  // default 24h; 0 disables
    CheckpointEvery    int            // default 10000 keys
    CheckpointInterval time.Duration  // default 30s
    VerifyWorkers      int            // default 16
}
 
type ReconcileResult struct {
    Checked   int64
    InSync    int64
    Behind    int64
    Corrected int64
    Unknown   int64
    Skipped   int64
    Pruned    int  // pod prefixes deleted
    Resumed   bool // true if resumed from a checkpoint
}
 
func (s *Sluice) Reconcile(ctx context.Context, opts ReconcileOptions) (*ReconcileResult, error)
```

**Read side.**

1. **Pod discovery.** List the namespace's pod prefixes in object storage and classify each pod as
   alive or stale. The pod list is **frozen** at the start of a run: a pod that starts mid-run is
   picked up by the next run.
2. **Per-pod iterator** (`iterator.go`). Scan the pod's SSTables and decode its WAL segments, merged so
   that the SSTable entry wins on a duplicate. This is how records that were synced to object storage
   but not yet compacted (for example, after a pod crash) are still seen.
3. **K-way merge** (`heap.go`). A min-heap across the pod iterators keeps the record with the greatest
   `Version` per key. Under `PerEvent`, events are grouped by key and the latest version per key is verified.
   **Verify side.** The merge goroutine feeds a bounded channel drained by `VerifyWorkers` workers. For each key:

1. Skip records newer than `now - Settle`.
2. Skip keys still pending in Redis (`HasPendingVersion`) when Redis is reachable. Those are unflushed, not lost. When Redis is unreachable this filter is skipped.
3. Read the stored document through `Source` + `ReadContract`, then decide:
    - with `VersionOf`: a missing document or `stored.Less(rec.Version)` is `Behind`; an equal or
      greater version is `InSync`; `ok == false` is `Unknown`;
    - otherwise, call `Verify`.
4. With `Correct`, re-`Write` the ledger payload for each `Behind` key. It goes through the journal,
   so ordering against new traffic follows normal latest-arrival semantics.
5. Count `Checked`, `InSync`, `Behind`, `Corrected`, `Unknown` and `Skipped`, and record them as metrics.
   **Checkpoints.** A run is resumable. The checkpoint (`{ns}/_reconcile/current.json`) holds `RunID`,
   `LastKey`, `Phase`, the frozen pod list, running counters, `LockedBy` and `Heartbeat`. It is written every
   `CheckpointEvery` keys or `CheckpointInterval`, whichever comes first.

- **Phases:** `PhaseMerge` → `PhaseVerify` → `PhasePrune`.
- **Concurrent runs:** a second reconciler sees a fresh heartbeat and returns `ErrReconcileInProgress`.
  If the heartbeat is stale (twice the checkpoint interval), the second reconciler takes over and resumes from the checkpoint (`Resumed: true`).
- **History:** a completed run's summary is written to `{ns}/_reconcile/history/{run_id}.json`.
  **Pruning.** A pod's prefix is deleted (`DeletePrefix`) only when the pod has been stale for at least
  `PruneStaleAfter` **and** every key read from it was verified as `InSync` or `Corrected`. One `Unknown`
  or unverified key keeps the whole prefix.

`Reconcile` is a one-shot call, like `ProcessDLQ`. Schedule it yourself (a ticker, Asynq, a cron
job), typically after a Redis incident or on a slow periodic cadence.
 
---

## 6. Implementation Plan

Every phase ends with gofmt, `go build`, `go vet` (also `-tags integration`), and a green full `go test ./...`. Mutation-check each new test.

### Dependency graph

```
Phase 0 (Version) ─────────────────────┐
                                       │
Phase 1 (objstore + adapters) ─────┐   │
                                   ▼   ▼
                              Phase 2 (Pebble store, record encoding,
                                       writer, BestEffort)
                                   │
                            ┌──────┴──────┐
                            ▼             ▼
                        Phase 3       Phase 4
                        (Required     (Builder integration,
                         WAL sync)     metrics, WriteWith)
                            │             │
                            └──────┬──────┘
                                   ▼
                              Phase 5 (Reconciler)
                                   │
                                   ▼
                              Phase 6 (Examples, docs)
```

Phases 0 and 1 have no dependency on each other and can proceed in parallel. Phases 3 and 4 both depend on
Phase 2 and are independent of each other. Phase 5 depends on both Phase 3 and Phase 4.

### Phase 0: Write version (`ledger.Version`)

Ships independently. Prerequisite for the ledger (this RFP) and for RFP #21. No ledger code.

- `ledger.Version{Epoch, Seq}` and `Less`.
- `HSETNX <hash> e <ts>` in `atomicWriteLua` and `atomicDedupWriteLua`; both return `{e, v}` instead of `1`; a deduplicated write still returns `0`.
- `FlushRecord` gains `Epoch`.
- Version propagated through the plain, dedup, batched and broadcast write paths; `DrainBand` and `DrainDLQ` read `e`.
- Degraded direct writes produce `{Epoch: now, Seq: 0}`; document that `Seq: 0` is reserved for them.
- `\x00` rejected in correlation keys at `Write`/`WriteIdempotent` validation, alongside the empty-key check.
- Does **not** include a versioned write contract: deferred to RFP #21's sink-side stamping.
  Files:

```
ledger/version.go              NEW  — Version type, Less
internal/shield/shield.go           — Lua scripts, DrainBand, DrainDLQ
internal/shield/types.go            — FlushRecord gains Epoch
internal/engine/engine.go           — propagate Version through flush
sluice.go                           — \x00 validation, version capture in Write
errors.go                           — ErrInvalidCorrelationKey
```

### Phase 1: Object storage interface and adapters

No Pebble, no ledger logic. Pure storage abstraction.

- `ledger/objstore/objstore.go`: `Storage`, `ObjectReader`, `Entry`, `ErrNotFound`, `ErrAlreadyClosed`.
- `ledger/objstore/helpers.go`: `DeletePrefix`, `PutBytes`, `GetBytes`.
- `ledger/objstore/pebble.go`: `PebbleFactory`, the bridge to Pebble's `remote.StorageFactory`.
- Four adapters (§5.5) and the shared conformance suite.
  Files:

```
ledger/objstore/
  ├── objstore.go          NEW  — Storage, ObjectReader, Entry, errors
  ├── helpers.go           NEW  — DeletePrefix, PutBytes, GetBytes
  ├── pebble.go            NEW  — PebbleFactory bridge
  ├── conformance_test.go  NEW  — shared test suite
  ├── s3/    s3.go, s3_test.go        NEW
  ├── gcs/   gcs.go, gcs_test.go      NEW
  ├── azure/ azure.go, azure_test.go  NEW
  └── fs/    fs.go, fs_test.go        NEW
```

### Phase 2: Pebble ledger store and record encoding

The core storage engine. `BestEffort` mode only; no object-store WAL sync yet.

- `ledger/record.go`: `Record`, `Encode`/`Decode` (44-byte fixed header + variable fields).
- `ledger/keys.go`: key encoding for both granularities (§5.4).
- `ledger/store.go`: `Open`, `Close`, `WriteBatch`, `Scan`.
- `ledger/writer.go`: bounded buffer, flusher, `LatestPerKey` dedup, `Enqueue`, `Drain`.
- Compaction monitor: tracks SSTable coverage and deletes covered WAL segments from object storage via a watermark.
  Files:

```
ledger/
  ├── record.go        NEW  — Record, Encode, Decode
  ├── keys.go          NEW  — key encoding, both granularities
  ├── store.go         NEW  — Pebble open/close/write/scan
  ├── writer.go        NEW  — buffer, flusher, LatestPerKey dedup, Drain
  ├── config.go        NEW  — Config, defaults
  ├── record_test.go, keys_test.go   NEW
  ├── store_test.go, writer_test.go  NEW  — use objstore/fs
```

### Phase 3: Required mode — object-store WAL sync

Builds on Phase 2. Adds synchronous durability.

- `ledger/sync.go`: sync goroutine and segment encoding (§5.4).
- `ledger/writer.go`: `Enqueue` in `Required` mode writes to Pebble, enqueues to the sync buffer, and blocks on the waiter; `Drain` stops the sync goroutine after the final segment is uploaded.
- `SyncConfig` added to `ledger.Config`.
  Files:

```
ledger/
  ├── sync.go        NEW  — sync goroutine, segment encoding
  ├── sync_test.go   NEW  — uses objstore/fs
  └── writer.go           — Required-mode enqueue + drain changes
```

### Phase 4: Builder integration, metrics, `WriteWith`

Wires the ledger into sluice's public API and lifecycle.

- `sluice.go`: `WithLedger(ledger.Config)`; `Build` opens Pebble, validates object-store connectivity and starts the ledger goroutines on a context independent of `Build`'s; `DrainAndClose` drains the ledger after the sink closes and closes the object store last; `Write`/`WriteIdempotent` capture the version from the script return and enqueue after journal success; `WriteWith` and `WithEventID`.
- `types.go`: `Sluice` gains `ledgerWriter`, `ledgerDB`, `objStore`; `Builder` gains `ledgerCfg`; the `LedgerOption` types (`LedgerMode`, `LedgerGranularity`, `LedgerFlushWindow`, `LedgerBatchSize`, `LedgerBufferSize`, `LedgerIncludePayload`, `LedgerRetry`), `WriteOption`, `WithEventID`, `ErrLedgerUnavailable`.
- Metrics (§5.6): the three new `MetricsRecorder` methods, `noopMetrics`, the Prometheus recorder, and the Grafana ledger panel.
  Files:

```
sluice.go                              — WithLedger, Build, DrainAndClose, Write, WriteWith
types.go                               — struct changes, LedgerOption, WriteOption, errors
metrics.go                             — new MetricsRecorder methods, noopMetrics
errors.go                              — ErrLedgerUnavailable, ErrInvalidCorrelationKey
metrics/prometheus/prometheus.go       — ledger counters and gauges
metrics/prometheus/prometheus_test.go
dashboards/sluice-overview.json
```

### Phase 5: Reconciler

The payoff. Reads across pods, verifies against the datastore, corrects losses (§5.7).

Files:

```
ledger/
  ├── reconcile.go       NEW  — Reconcile, K-way merge, verify pipeline
  ├── checkpoint.go      NEW  — Checkpoint, load/save, lock, resume
  ├── prune.go           NEW  — dead pod cleanup
  ├── iterator.go        NEW  — per-pod iterator (SSTable + WAL merge)
  ├── heap.go            NEW  — min-heap for K-way merge
  ├── reconcile_test.go  NEW  — uses objstore/fs, multi-pod scenarios
  └── checkpoint_test.go NEW
sluice.go                     — Reconcile method on Sluice
```

### Phase 6: Examples and documentation

- `examples/reconcile/{documentdb,dynamodb,postgres}/main.go`: write N keys, simulate Redis loss, run `Reconcile{Correct: true}`, print the result. (The names refer to the datastore being reconciled, not to a ledger backend.)
- README: "Event ledger and reconciliation" section.
- CHANGELOG: breaking note (`MetricsRecorder` gains ledger methods) and the new features.
- Update `docs/issues/event-log.md` status from Proposed to Implemented, noting the design changes (Pebble + object storage replaces the bundled ledger adapters).
---

## 7. Success Criteria / Test Plan

### Phase 0: version

| Test | Asserts |
|---|---|
| Version survives hash recreation | Write, flush, let the post-flush TTL expire, write again: the second version is greater (`Seq` restarts at 1, `Epoch` increases). The same holds after `FLUSHALL`. |
| Version is arrival order | 200 concurrent writes to one key: the returned versions are distinct and totally ordered; the journal holds the greatest. |
| Pre-versioning hash | A hash with `v` but no `e` gets `e` on its next write; `Seq` continues. |
| Degraded write version | A degraded direct write produces `{Epoch: now, Seq: 0}`. |
| Null byte rejection | `Write("key\\x00bad", payload)` returns `ErrInvalidCorrelationKey`. |

### Phase 1: object storage (every adapter passes `RunConformance`)

| Test | Asserts |
|---|---|
| `PutGetRoundtrip` | A put object reads back identical. |
| `GetNotFound` | Get of a missing object returns `ErrNotFound`. |
| `DeleteIdempotent` | Deleting a missing object succeeds. |
| `ListOrder` / `ListEmpty` | List returns keys in order; an empty prefix lists nothing. |
| `ObjectReaderReadAt` / `ObjectReaderSize` | Range reads and size are correct. |
| `PutAtomicity` | A concurrent `Put` and `Get` never sees a partial object. |

### Phase 2: Pebble store and writer

| Test | Asserts |
|---|---|
| Record encode/decode roundtrip | All field types, including empty `EventID` and nil `Payload`. |
| Key sort order (`LatestPerKey`) | Lexicographic sort matches namespace, then correlation key. |
| Key sort order (`PerEvent`) | Lexicographic sort matches namespace, timestamp, key, event ID. |
| `LatestPerKey` compaction | Write 50 versions of one key, compact: one record remains. |
| `PerEvent` no merge | Write 50 events for one key, compact: all 50 remain. |
| Writer `BestEffort` buffer full | Buffer 10, blocked flusher, 100 enqueues: all return nil, 90 counted as dropped. |
| Writer `LatestPerKey` dedup | 200 writes to one key in one window produce 1 Pebble write. |
| Writer `Drain` flushes everything | 500 buffered records, `Drain`: all in Pebble. |
| Compaction monitor | After compaction covers segment N, WAL segments 1..N are deleted from object storage. |

### Phase 3: `Required` mode

| Test | Asserts |
|---|---|
| Required returns after object PUT | `Enqueue` blocks until the PUT completes, then returns nil. |
| Required batch amortization | 50 concurrent enqueues, one PUT, all 50 released. |
| Required store failure | A failing store: waiters get `ErrLedgerUnavailable` after retries are exhausted. |
| Required store recovery | The store fails twice and succeeds the third time: waiters get nil. |
| Segment CRC integrity | Encode a segment, flip one byte: decode detects the corruption. |
| Drain uploads final segment | 10 records enqueued, immediate `Drain`: the final segment is in the store, all waiters released. |
| Segment idempotent re-upload | The same segment uploaded twice leaves one object. |

### Phase 4: integration

| Test | Asserts |
|---|---|
| Off by default | Without `WithLedger`, `Write` does no ledger work and allocates nothing for it. |
| No Redis dependency | With Redis stopped after an acked write, the ledger buffer still appends. The `ledger` package does not import `internal/shield` (checked in a test). |
| `BestEffort` never fails `Write` | A ledger whose store always fails: `Write` returns nil; `ledger_dropped_total{reason="append_failed"}` counts every record. |
| Required fails `Write` | A failing store: `Write` returns `ErrLedgerUnavailable`. |
| All write methods recorded | `Write`, `WriteIdempotent` and `WriteWith` each produce a ledger record. |
| `WriteWith` EventID | `WriteWith(ctx, k, p, WithEventID("evt_1"))`: the record has `EventID == "evt_1"`. |
| `WriteIdempotent` EventID | `WriteIdempotent(ctx, k, p, "idem_1")`: the record has `EventID == "idem_1"`. |
| `Write` signature unchanged | Existing `Write` tests pass unmodified. |
| Only acked writes recorded | A write that returns an error produces no record. |
| `DrainAndClose` order | The ledger is drained after the sink is closed; all buffered records are in Pebble; the object store is closed last. |
| Context independence | Cancelling `Build`'s context does not stop the ledger goroutines. |

### Phase 5: reconciler

| Test | Asserts |
|---|---|
| **Redis loss end to end** | Write 1,000 keys with a 1h flush window, wait for the ledger append, `FLUSHALL` Redis, run `Reconcile{Correct: true}`: all 1,000 keys reach the store, and `Behind == Corrected == 1000`. |
| Reconcile skips pending keys | A key still dirty in Redis is neither `Behind` nor corrected. |
| No regression across a reset | Flush `{E1, 5}`, lose Redis, write and flush `{E2, 1}` with `E2 > E1`: an old ledger record `{E1, 5}` is `InSync`, not `Behind`. |
| Cross-pod merge | 3 pods write overlapping keys: the reconciler keeps the highest version per key. |
| WAL recovery | A pod crashes after WAL sync but before compaction: the reconciler reads the WAL segments and verifies all records. |
| Checkpoint resume | Kill the reconciler at key 25,000 of 50,000. Resume: it starts from the checkpoint and the final counts are correct. |
| Checkpoint heartbeat lock | Two concurrent reconcilers: the second returns `ErrReconcileInProgress`. |
| Stale lock takeover | Reconciler A's heartbeat is stale by 2× the interval: reconciler B takes over and resumes from A's checkpoint. |
| Frozen pod list | A new pod starts mid-reconciliation: it is not included in the current run and is picked up by the next. |
| Dead pod pruning | A dead pod's keys are all `InSync`: its prefix is deleted. One key `Unknown`: the prefix is preserved. |
| `PerEvent` reconciliation | 500 events across 3 pods, grouped by key: the latest version per key is verified. |
 
---

## 8. Known Limits

1. **Crash window.** In `BestEffort` mode, a pod that crashes loses the records not yet covered by an
   uploaded SSTable, so the window is bounded by the compaction cadence, not by `LedgerFlushWindow`.
   This loss is not counted by a metric. In `Required` mode the window is effectively zero from the
   caller's perspective: `Write` returns only after the record's WAL segment is in object storage. Use
   `Required` where the window matters.
2. **`Required` latency and cost.** `Write` waits for an object-store PUT, batched over `SyncWindow`
   (10ms) or `SyncBatchSize` (500). Concurrent writers share a PUT, but a lone write still pays one
   PUT round trip, and PUT count grows with the number of pods and the sync window.
3. **Epoch and clock skew.** `Epoch` is the writing pod's clock. It matters only when a hash is
   recreated within the skew window of the previous one, which normal expiry can't cause (at least
   `KeyTTL` apart). After an abrupt Redis loss, a hash created within that window of the loss could be
   mis-ordered. `Seq` ordering within one lifetime uses no clock.
4. **Replay race.** Between `Reconcile` deciding `Behind` and its re-`Write` landing, a new write for
   the key could land and flush; the replay then overwrites it. Narrow it by re-checking the journal
   just before replaying. Without `VersionOf`, `Verify` must return `Behind` only when it can tell the
   store is older, and `Unknown` otherwise.
5. **No version in the store until RFP #21.** Until sink-side stamping exists, `VersionOf` has nothing
   to read from documents, and reconciliation depends on a user-written `VerifyContract`.
6. **Local disk.** Each pod keeps an ephemeral local Pebble WAL directory. Its contents are not the
   durable copy (object storage is), but a pod that dies before its records are compacted or, in
   `Required` mode, synced, cannot recover them from local disk.
7. **Volume.** Under `LatestPerKey` the ledger holds at most one record per key per pod per window,
   and Pebble absorbs the write rate locally. Object storage sees SSTable uploads and, in `Required`
   mode, WAL segment PUTs, rather than one request per record. `PerEvent` storage grows with event
   volume. To keep an async path out of the ledger entirely, run it on an instance without `WithLedger`.
8. **Breaking change.** `MetricsRecorder` gains three methods; custom recorders must implement them.
## 9. Decisions Log

Resolved 2026-09-27:

1. **`Write` is not changed.** Per-call options go through a new `WriteWith` method, so existing
   callers, and code that stores `s.Write` as a function value or uses it to satisfy an interface, are unaffected.
2. **The per-key version is recorded** (§5.2). The sequence `v` already exists, so returning it adds
   no real compute and doesn't undo coalescing. Because `v` restarts when a hash is recreated, it is
   paired with a hash-creation epoch.
3. **A ledgered instance records all writes.** Enabling the ledger is an instance-level decision, not a per-call choice.
   Resolved 2026-09-29:

4. **Pebble on object storage replaces the bundled ledgers.** `ledger/docdb`, `ledger/postgres`,
   `ledger/dynamodb`, the `ledger.Ledger` interface and `ledger.Reader` are removed. Reads go through
   `ledger.Store.Scan`, and cross-pod reads through the reconciler's per-pod iterators. `ledger.Func` stays as
   an escape hatch for Kafka, Firehose and raw S3.
5. **Object storage is a four-adapter abstraction** (S3, GCS, Azure Blob, local filesystem) with a shared conformance suite, so no phase other than Phase 1 depends on a particular cloud.
6. **`VersionedWriteContract` is dropped.** Getting the version into datastore documents is deferred to
   RFP #21's sink-side stamping. `Version` and the epoch script changes still ship as Phase 0.
7. **`Required` mode is synchronous over an object-store WAL**, replacing the in-memory ack-on-commit
   wait. The in-memory buffer is no longer the only pre-append storage: records go to local Pebble first
   (`BestEffort`), and additionally to an uploaded WAL segment (`Required`).
8. **Correlation keys may not contain `\x00`**, because it separates the namespace and key in the ledger key encoding.
9. **The reconciler is resumable and prunes dead pods**: checkpointed in object storage, protected by a heartbeat lock, with a frozen pod list per run.
10. **Crash window changes.** Was: up to `LedgerFlushWindow` of in-memory records. Now: `BestEffort` is bounded by compaction cadence; `Required` is effectively zero from the caller's perspective.