# RFP #17: Event Log — Recording Every Incoming Event in `sluice-go`

**Status:** Proposed (design agreed 2026-09-27); **blocked on RFP #18**  
**Target Release:** next minor after v1.0.8  
**Dependencies:** **RFP #18 (Closing silent-loss paths in the sink and engine layers)**, durability hardening in `[Unreleased]` (conditional commit via per-key sequence `v`, `DeadLetterIfUnchanged`, ack-on-commit batching, NOSCRIPT recovery, safe degraded mode), RFP #16 (Prometheus metrics)

---

## 1. Context & Problem Statement

Banner-inventory events for the same user_id arrive **concurrently from two sources**: a Kafka/SQS stream and the synchronous API. The platform uses them to hyper-personalise ads and banners, and growth teams report on them. **Every event is a fact**: an impression, dismissal or click that is missing from the store produces wrong growth reports.

`sluice` is built to **collapse** writes per correlation key, which is the opposite of what reporting needs:

- The journal stores one payload per user_id. `atomicWriteLua` does `HSET p <payload>`, so a later `Write` for the same user_id **replaces** the unflushed payload. The dirty set's `ZADD` collapses to one member (see README, *The coalescing mechanism*).
- The engine flushes **only the latest payload per key**, as an upsert (Mongo `$set`, Postgres `ON CONFLICT DO UPDATE`, DynamoDB `PutRequest`).
- "Latest" means **latest to arrive at Redis**. `ts` is `time.Now()` on the writing pod at journal-write time, not the event time.

So when the stream and the API each emit an event for user_id X within one flush window (250ms by default), only one event reaches the datastore, **even when nothing fails**. The other event is lost from reporting.

A datastore-side version guard was considered first and rejected. It only decides *which single version* survives, so it still keeps one event of the two.

### 1.1 Prerequisite: silent-loss paths in the sink/engine layer

While designing this, the review found that the engine and the DLQ processor decide success **by elimination**, and the sinks produce unattributed or misclassified errors, so records can be committed without having been written. These defects affect state writes today and are tracked separately in **RFP #18** (`sink-silent-loss.md`).

The event log reuses the same flush, commit and DLQ machinery, and relies on RFP #18 for:
- `CheckAttribution` in the engine and in `handleUpsert` (no success by elimination);
- `SinkError.Class` / `IsPermanent()`, so 11000 on `_id_` during an upsert race is retried rather than dead-lettered;
- DynamoDB duplicate-key and Postgres duplicate-conflict-key pre-checks returning permanent per-row errors (see §7, trap 2);
- Postgres `Config.OnConflict: DoNothing` (§4.5).

RFP #18 must ship before Phase 1 below starts.

---

## 2. The Goal

1. **Record every event exactly once.** Every `WriteEvent` that returns nil is eventually in the datastore's event log, once per event ID, however many times it is redelivered.
2. **Keep the per-user_id state document working as today**: the current-state inventory that `Read`/`Query`/`HotLoad` serve, latest wins.
3. **Work on all three sinks**: DocumentDB, Postgres and DynamoDB.
4. **Stay available for events during a full Redis outage** by writing them directly to the datastore.
5. **Inherit the silent-loss fixes** from RFP #18 for events, with no event-specific success-by-elimination path.

## 3. Resolved Decisions

| Question | Decision |
|---|---|
| Event identity | **Producers attach a unique, stable event ID** (event_id, SQS MessageId, Kafka offset, API idempotency key); redeliveries and retries reuse it. |
| State doc | **Keep it.** `WriteEvent` appends the event *and* updates the user_id state doc (latest wins, as today). Reports read the event log. |
| Sinks | **All three** in this change. |
| Full Redis outage | **Write events directly** to the datastore. An immutable event keyed by its identity cannot overwrite, or be overwritten by, anything. |

---

## 4. Proposed Design

### 4.1 Public API (new `eventlog.go`)

```go
type Event struct {
    CorrelationKey string // user_id
    EventID        string // producer-supplied, stable across redeliveries
    Payload        []byte
}

// Key is the canonical, injective event identity. Use it as the event
// document's _id / primary key so journal identity == sink identity.
func (e Event) Key() string

type EventContract func(ev Event) (*WriteModel, error) // must be a pure function of ev

type EventLogOptions struct {
    Namespace      string        // default "<ns>:ev" (reserved suffix)
    BandCount      int           // default: state BandCount
    KeyTTL         time.Duration // post-flush journal TTL; default 5s; must be > 0
    DegradedDirect bool          // default true: direct sink write on journal failure
    DLQStrategy    DLQStrategy   // default DLQUpsert; DLQIgnore only with AllowIgnore
    AllowIgnore    bool
    Metrics        MetricsRecorder // default: event decorator over the main recorder
    OnFlush        OnFlushCallback
}

func (b *Builder) WithEventLog(sk sink.FlushSink, ec EventContract, opts ...EventLogOption) *Builder
func (s *Sluice) WriteEvent(ctx context.Context, userID, eventID string, payload []byte) error
func (s *Sluice) ProcessEventDLQ(ctx context.Context, strategy DLQStrategy, opts ...DLQOption) (*DLQResult, error)

// Returned when either half of WriteEvent fails. Unwrap() []error keeps
// errors.Is(err, ErrDegradedWriteUnsafe) etc. working.
type PartialWriteError struct {
    EventRecorded      bool
    EventErr, StateErr error
}

var ErrEmptyEventID          = errors.New("sluice: event id must not be empty")
var ErrEventLogNotConfigured = errors.New("sluice: event log not configured — call WithEventLog()")
```

### 4.2 Event journal: a second Shield on the same Redis

- **`(*Shield).Derive(namespace, DeriveOptions{BandCount, KeyTTL, DLQTTL})`** returns a **fresh** `Shield` that shares the Redis client and scripts. It must **not** copy `*s`, which contains a `sync.Once`, a `sync.RWMutex` and a `sync.WaitGroup`. It uses the event namespace (default `<ns>:ev`) and validates `{}` in it.
  - It gets its own payload, dirty-set and DLQ keys. None match the state scan patterns (`sl:<ns>:hot:*`, `sl:<ns>:idx:*`), and vice versa.
  - New field `ownsClient`: `Close()` on a derived Shield does nothing.
  - No broadcast, index sweeper, gauge sampler or hot markers on the event Shield.
- **Journal key** = `EncodeKey(userID, eventID)`, an injective encoding (`len(userID):userID:eventID`). It lives in the event's own `{band}` slot, so all scripts stay single-slot.
- **Journal payload** = a versioned binary envelope `0x01 | uvarint len(user_id) | user_id | uvarint len(eid) | eid | payload`, so contract identity never depends on parsing the key. Don't use JSON: it would base64 the payload and add about 33%.
- **Redelivery before flush**: the same event ID writes the same journal key, the `HSET` replaces it with identical content, and there is one flush. There is no double count.

### 4.3 Event flushing: a second engine

- `engine.New` with the event Shield, the event sink, and a wrapper: `Decode` the envelope, call `EventContract`, and build a `sink.WriteModel{CorrelationKey: Event.Key()}`.
- Settings: `HotAwareFlush=false`, a positive `KeyTTL` (a TTL of 0 would leave committed events in Redis forever), and a nil state callback.
- It reuses the existing machinery unchanged: `CommitFlushed` (conditional on seq `v`), `DeadLetterIfUnchanged`, the missing-payload surfacing, and the pre-eviction flusher.
- **Volume signalling**: the non-batched `WriteEvent` path checks the event Shield's `DirtyQueueDepth`, like `Write` does. The batched path uses `SetVolumeSignaler` on the event Shield.

### 4.4 `WriteEvent` semantics

1. Validate `userID`, `eventID` and the configuration.
2. Run the event journal write **concurrently** with `s.Write(ctx, userID, payload)` (when a state `WriteContract` is set), so batched mode doesn't pay two batch windows.
3. Event path: `evShield.Write(key, envelope)`. On error with `DegradedDirect`, write straight to the event sink via `EventContract` with **no pending-version probe**: an immutable, identity-keyed event is safe to write directly. In non-batched mode, run the volume check.
4. Return nil **only if both writes succeeded**; otherwise return a `*PartialWriteError`. Retrying is idempotent: the event is deduplicated by identity, and the state write is latest-wins.

| Case | Result |
|---|---|
| Both ok | nil. Safe to ack upstream. |
| Event ok, state fails | `PartialWriteError{EventRecorded:true}`. Don't ack; the retry dedupes the event. |
| Event fails, state ok | `PartialWriteError{EventRecorded:false}`. Don't ack; the retry records it. |
| Full Redis outage | The event is written directly (`EventRecorded:true`). State gets `ErrDegradedWriteUnsafe` (safe degraded mode can't rule out pending versions). The error is returned, the upstream message is **not** acked, and the event is already recorded. |

### 4.5 Recommended event contracts per sink (insert-if-absent where possible)

| Sink | Contract / config | Why |
|---|---|---|
| DocumentDB | `Filter {_id: ev.Key()}`, `Update {$setOnInsert: {...}}`, `Upsert: true` | The first write wins, and replays are no-ops. An 11000 on `_id_` (an upsert race) is retryable (RFP #18). |
| Postgres | The event sink uses `OnConflict: DoNothing`; `PRIMARY KEY (user_id, event_id)` | No dead tuples or WAL on replay; the first write wins. |
| DynamoDB | `PK=user_id`, `SK=event_id`, `PutRequest` | `BatchWriteItem` can't be conditional, so it is idempotent only because the item is deterministic (a pure contract). Also serves queries by user_id. The degraded single write may use `attribute_not_exists(PK)`, with `ConditionalCheckFailed` counted as success. |

### 4.6 Event DLQ

- The event DLQ has its **own strategy**, defaulting to **Upsert** through the event contract. **Ignore** is refused unless `AllowIgnore` is set, because ignoring a dead-lettered event deletes a fact.
- **ReInsert is routed to Upsert** (`dlq.Config.ReInsertAsUpsert`). **Never** use the existing `handleReInsert` for events: with an identity key mapping, it writes the new key and then `CommitDLQKeys` DELs that same payload hash, and the next flush reports it as lost.
- **Event DLQ entries never expire** (`DLQTTL = 0`). This requires changing `deadLetterIfUnchangedLua` and `MoveToDeadLetter` so that TTL 0 means `PERSIST`. Today `PEXPIRE 0` would **delete** the key.
- `drainAction` currently discards expired DLQ entries with no signal. Add a warning log and a metric.
- `ProcessEventDLQ` works without a state `WriteContract` (event-only builds). The auto-processor runs both DLQs.

### 4.7 Metrics

- The Prometheus recorder ignores the namespace argument, so a second engine sharing it would **collide**: `dirty_queue_depth{band="3"}` would jump between state and event values.
- Default: an `eventMetrics` decorator over the main recorder. Band labels become `ev-<band>`, op names become `ev_<op>`, and the per-event `correlation_key` label on `contract_error_total` is replaced with a constant, to avoid unbounded cardinality.
- Or pass `EventLogOptions.Metrics`, e.g. `sluiceprom.NewRecorder("banner_inventory_events")`.

### 4.8 Lifecycle and resources

`DrainAndClose` order:
1. Stop both batchers.
2. Drain both engines, in parallel.
3. Stop the DLQ, gauge and sweeper goroutines.
4. Close the Redis client **once**.
5. Close the state sink, then the event sink if it is a different instance.

Today `DrainAndClose` closes the shared client right after the state engine, which would break the event engine's drain with "client is closed".

With the event log enabled, the default Redis `PoolSize` rises to `max(20, 2*BandCount+8)`, since there are twice as many flush goroutines plus two batchers.

---

## 5. Implementation Plan

Each phase ends with gofmt, `go build`, `go vet` (also `-tags integration`), and a green full `go test ./...`. Mutation-check each new test (stash the fix and confirm the test fails).

Prerequisite: RFP #18 (sink and engine correctness) is merged.

### Phase 1: shield support
- `Derive`, `ownsClient`, a no-op `Close` for derived Shields.
- `PERSIST` when the DLQ TTL is 0.
- Log and count the entries `drainAction` discards.
- `internal/eventlog/envelope.go` (`EncodeKey`, `Encode`, `Decode`, versioned).

### Phase 2: event log
- `eventlog.go` (the API in §4.1).
- `Build` wiring: ping the event sink, `Derive`, the second engine, the event batcher, pool size.
- `WriteEvent` (§4.4), the event DLQ (§4.6), `eventmetrics.go` (§4.7), `DrainAndClose` (§4.8).
- Validation: a nil sink or contract is rejected; `KeyTTL` must be > 0.

### Phase 3: docs and example
- README "Event log" section:
  - the identity requirement and pure-contract rule;
  - the per-sink contracts;
  - `PartialWriteError` handling (don't ack unless nil);
  - the known limits.
- CHANGELOG with breaking notes (DLQ TTL 0 = never).
- Extend `examples/banner_write_dual_read_hot/documentdb` with two concurrent sources writing events.

---

## 6. Success Criteria / Test Plan

**`tests/unit/documentdb/event_log_test.go`** (real Redis and Mongo; namespaces `ns` and `ns:ev`):

| Test | Asserts |
|---|---|
| Concurrent events for one user_id | 200 goroutines × distinct event IDs, split across two "sources", within a 2s flush window, with and without batching. **200 event docs**. The state doc equals the final journal payload. Before the flush: 200 event dirty members, 1 state dirty member. |
| Redelivery | The same event ID ×10 concurrently, then again after flush plus TTL expiry with a different payload. **Exactly 1 doc**, first write kept. |
| Full Redis outage | Via an in-test TCP proxy to Redis that is then closed. The error has `EventRecorded:true` and matches `ErrDegradedWriteUnsafe`; the event doc exists immediately. The proxy is reopened and the call retried: nil, still 1 event, and the state doc exists. |
| Journal failure without degraded mode | An error is returned; after the fix and a retry, the event is recorded exactly once. |
| Event-only build | No state contract: events flush, and `Write` returns `ErrMissingWriteContract`. |
| Event DLQ | A contract failure dead-letters the event with DLQ `PTTL == -1`. Upsert and ReInsert both keep the event with no loss metric. Ignore is refused by default. |
| Shutdown | `DrainAndClose` with a 1h flush window flushes pending events and produces no "client is closed" errors. A derived `Close` leaves the client usable. |
| Isolation | The state Shield's hot-marker, index and pending probes ignore event keys. |
| Metrics | A Prometheus registry shows separate `dirty_queue_depth` series for `3` and `ev-3`. |

**Event contract tests** (sink-level attribution and classification tests live in RFP #18):
- **DocDB**: a `$setOnInsert` replay leaves the doc unchanged.
- **Postgres**: a `DoNothing` replay keeps the first row.
- **DynamoDB**: a replayed `PutRequest` from a pure contract leaves the item unchanged; the degraded single write with `attribute_not_exists(PK)` counts `ConditionalCheckFailed` as success.

**Live run:** two concurrent sources write the same user_ids for about 60s.
- The event-doc count equals the number of `WriteEvent` calls that returned nil.
- `unflushed_expiry_total` stays 0.
- Redis memory levels off.

---

## 7. Design Traps Found in Review (avoid these)

1. **The shared Redis client is closed too early.** `DrainAndClose` calls `shield.Close()` → `client.Close()` while the event engine is still draining. Fix: a non-owning derived Shield, and close the client once, last.
2. **Journal identity must equal sink identity.** If the journal key is (user_id, eventID) but the sink key is the eventID alone, two user_ids sharing an event ID silently overwrite one row. If both land in one batch, Postgres raises `21000` and DynamoDB a ValidationException, which retry forever and **stall the band**, because `DrainBand` always picks the lowest scores. Fix: `Event.Key()` end to end, plus sink duplicate pre-checks that return permanent per-row errors.
3. **`handleReInsert` with an identity key mapping deletes the payload it just wrote** (`CommitDLQKeys` DEL). For events, route ReInsert to Upsert.
4. **`PEXPIRE 0` deletes the key.** A "never expire" DLQ TTL must be `PERSIST`.
5. **DLQ payloads expire after 7 days** and `drainAction` drops them with no signal. Events need a TTL of 0 plus a log and metric on discard.
6. **Prometheus collisions and cardinality.** The recorder ignores the namespace, and `correlation_key` labels become per-event.
7. **The engine treats every 11000 as permanent.** Multiple pods flush the same band with no lease, so upsert races on `_id` happen. They must be retried, not dead-lettered. Fixed by RFP #18.
8. **A post-flush TTL of 0** leaves committed events in Redis forever (`commitFlushedLua` skips the PEXPIRE). The event `KeyTTL` must be > 0.

## 8. Known Limits (not solved by this RFP)

1. **State-doc arrival order.** "Latest wins" still means *latest to reach Redis*. With two concurrent sources, and retries after a partial failure, the state doc can briefly go backwards. Reports must read the event log. A state version guard (datastore compare-and-set on an event-time version) is a separate follow-up.
2. **Async replication.** A nil return means the event is on the Redis primary. If a failover promotes a lagging replica, recently acked events can vanish, together with their dirty entries, so no loss metric fires. Follow-up: an opt-in `ReplicaAck{N, Timeout}` (a `WAIT` after the journal write; check ElastiCache and MemoryDB support), or a durable journal store (MemoryDB).
3. **Contract purity.** DynamoDB idempotency relies on the event contract being a pure function of the event (a deterministic item). A contract that stamps ingestion time would make replays rewrite the item.
