# RFP #18: Closing Silent-Loss Paths in the Sink and Engine Layers

**Status:** In progress on branch `silent-loss-fixes` (split out of RFP #17 on 2026-09-27)  
**Target Release:** next patch after v1.0.8 (ships ahead of RFP #17)  
**Dependencies:** Durability hardening in `[Unreleased]` (conditional commit via per-key sequence `v`, `DeadLetterIfUnchanged`)  
**Blocks:** RFP #17 (Event log), which reuses the same flush, commit and DLQ machinery

---

## 1. Context & Problem Statement

While designing the event log (RFP #17), the review found that the engine and the DLQ processor decide success **by elimination**: every key not named in a `SinkError` is committed. The sinks produce unattributed or missing errors, so records are committed without having been written.

These defects affect **state writes today**, independent of any event-log work. A record committed without being written is removed from the dirty set and gets the post-flush TTL, so Redis drops the only copy and no loss metric fires. This violates the no-data-loss requirement: loud failure is always preferred over a silent drop.

### 1.1 Defects

| Where | Defect | Effect |
|---|---|---|
| `sink/dynamodb` `flushBatch` | A failed `Update.(map[string]any)` assertion or a marshal failure returns early | The rest of the chunk is never written but is counted as success |
| `sink/dynamodb` | Context cancel or retries exhausted return **zero-value** `SinkError`s (empty `CorrelationKey`) | Unprocessed items are committed as success |
| `sink/dynamodb` | `BulkWrite` never returns an error; all Codes are 0 | No permanent/transient distinction |
| `sink/docdb` | `BulkWriteException.WriteConcernError` is ignored; value type assertion instead of `errors.As` | A write-concern failure is committed as success |
| `sink/docdb` | 11000 is always permanent | A concurrent-upsert race on `_id` is wrongly dead-lettered |
| `sink/postgres` | Columns and conflict target come from the **first row only** | Later rows get NULLs or dropped fields |
| `sink/postgres` | 23xxx become ints ≠ 11000, so they are treated as transient; `42P01`/`42703` fail `Atoi` | A poison batch retries forever and stalls its band |
| `internal/engine` | Every key not named in `result.Errors` is committed; classification is `Code == 11000` | Any of the sink defects above becomes a silent commit |
| `internal/dlq` `handleUpsert` | The same success-by-elimination logic | The same risk on DLQ replay |

---

## 2. The Goal

1. **No key is committed unless the sink positively accounted for it.** An error the engine can't attribute to a key in the batch aborts the commit for the whole batch; keys stay dirty and are retried.
2. **Every sink classifies its errors** as transient or permanent, so retry vs. dead-letter no longer depends on a MongoDB-specific code.
3. **A poison record never stalls its band.** Permanent errors are isolated to the offending rows and dead-lettered; the rest of the batch commits.
4. **Concurrent upsert races are retried, not dead-lettered.** Multiple pods flush the same band with no lease, so 11000 on `_id` during an upsert is expected and transient.

## 3. Non-Goals

- Recording individual events, or anything else in RFP #17.
- Changing the flush/commit protocol itself (two-phase drain, conditional commit on seq `v`).
- A per-band flush lease across pods.

---

## 4. Proposed Design

### 4.1 Error classification (`sink/types.go`, `sink/sink.go`)

```go
type ErrorClass int

const (
    ClassUnknown ErrorClass = iota // sink did not classify; legacy behaviour
    ClassTransient
    ClassPermanent
)

type SinkError struct {
    CorrelationKey string
    Code           int
    Message        string
    Class          ErrorClass
    Err            error
}

// IsPermanent reports whether the error should be dead-lettered.
// Permanent, or Unknown with Code 11000, which keeps today's behaviour for
// third-party sinks that don't set Class.
func (e SinkError) IsPermanent() bool

var ErrUnattributedSinkError = errors.New("sluice: sink returned an error with an empty or unattributed correlation key")

// CheckAttribution returns ErrUnattributedSinkError if any error in res has an
// empty CorrelationKey or one that is not among models.
func CheckAttribution(models []WriteModel, res *BulkWriteResult) error
```

### 4.2 Engine and DLQ

- `internal/engine/engine.go` `flushBand`: after a nil `flushErr`, run `CheckAttribution`. On failure, log, invoke the flush callback with the error, and **commit nothing**; all keys stay in the dirty set. Classify with `IsPermanent()` instead of `Code == 11000`. The dead-letter reason becomes `permanent_sink_error`.
- `internal/dlq/dlq.go` `handleUpsert`: the same attribution guard before `CommitDLQKeys`, and the same `IsPermanent()` classification.

### 4.3 DocumentDB (`sink/docdb`)

- Use `errors.As` for `mongo.BulkWriteException`.
- A `WriteConcernError` is a **total failure**: return an error, commit nothing.
- A pure `classifyBulkErr(we, isUpsert)` helper:
  - 11000 on `_id_` for an upsert → `ClassTransient` (race with another pod);
  - any other 11000 (unique index on a business field) → `ClassPermanent`;
  - other codes: see open question §7.1.

### 4.4 DynamoDB (`sink/dynamodb`)

- An `API` interface and `NewSinkWithAPI`, so tests can use a fake.
- Key attributes from an option, or `DescribeTable` loaded once.
- Per-item permanent errors instead of returning early on a bad `Update` type or marshal failure.
- Duplicate keys in one chunk become permanent per-item errors (`BatchWriteItem` rejects the whole call otherwise).
- `UnprocessedItems`, context cancel and retries-exhausted are mapped back to real keys as transient. An item that can't be mapped makes `BulkWrite` return an error.
- On a whole-call `ValidationException`, fall back to per-item `PutItem` to isolate the bad item.

### 4.5 Postgres (`sink/postgres`)

- `Config.OnConflict`: `DoUpdate` (default) or `DoNothing` (needed by RFP #17 for insert-if-absent event rows).
- Every row in a batch must have the same columns and conflict target; a mismatched row gets a permanent per-row error instead of NULL-overwriting fields.
- Duplicate conflict keys within one batch become permanent per-row errors (avoids `21000`, which fails the whole statement).
- SQLSTATE classes `23xxx`, `42P01`, `42703` and `21000` are `ClassPermanent`; codes are kept as strings, not `Atoi`'d.
- A permanent multi-row error is retried row by row to isolate the bad rows; the good rows commit.

---

## 5. Implementation Plan

Each step ends with gofmt, `go build`, `go vet` (also `-tags integration`), and a green full `go test ./...`. Mutation-check each new test (stash the fix and confirm the test fails).

1. **Core types and engine**: `ErrorClass`, `SinkError.Class`, `IsPermanent()`, `ErrUnattributedSinkError`, `CheckAttribution`; engine attribution guard and classification. *(in progress)*
2. **DocumentDB**: `errors.As`, write-concern handling, `classifyBulkErr`. *(in progress)*
3. **DLQ**: attribution guard in `handleUpsert`.
4. **DynamoDB**: §4.4.
5. **Postgres**: §4.5.
6. **Docs**: README section on error classification for custom sinks; CHANGELOG with breaking notes (`SinkError.Class` semantics, unattributed errors now abort the commit, `duplicate_key` dead-letter reason renamed).

---

## 6. Success Criteria / Test Plan

**Engine and DLQ** (`tests/unit/`, fake sink):

| Test | Asserts |
|---|---|
| Unattributed error | A fake sink returning `SinkError{CorrelationKey:""}` commits nothing; all keys stay dirty. |
| Unknown key | A `SinkError` for a key not in the batch commits nothing. |
| Classification | `ClassPermanent` with 23505 dead-letters; `ClassTransient` with 11000 retries; `ClassUnknown` with 11000 dead-letters (legacy). |
| DLQ replay | The same three cases through `handleUpsert` leave the DLQ entries in place. |

**Sink tests:**
- **DocDB**:
  - a write-concern exception returns an error;
  - 11000 on `_id_` for an upsert is transient, 11000 on a unique field is permanent.
- **DynamoDB, fake API**:
  - `UnprocessedItems` for items 3 and 7 map to exactly {k3, k7} as transient;
  - context cancel is mapped to keys, with none empty;
  - an item that can't be mapped makes `BulkWrite` return an error;
  - a `ValidationException` falls back to per-item puts.
- **DynamoDB Local**:
  - a 30-item chunk with one non-map item and one unmarshalable item writes the other 28, with 2 permanent errors carrying the correct keys;
  - a duplicate PK in one batch gives a permanent error for the duplicate, and the rest are written.
- **Postgres**:
  - a column mismatch gives a permanent error with no NULL overwrite;
  - a duplicate key in a batch fails only that row;
  - a 23505 in a 5-row batch fails 1 row and commits 4;
  - a `DoNothing` replay keeps the first row.

**Live run:** a multi-pod load test against each sink with injected failures (write-concern, throttling, a poison row) shows the sink row count equals the number of `Write` calls that returned nil, and `unflushed_expiry_total` stays 0.

---

## 7. Open Questions

1. **Default class for non-11000 Mongo write errors.** Today they are retried (transient). Marking them all permanent dead-letters codes that are really retryable (e.g. `ShutdownInProgress`, `NotWritablePrimary`, `ExceededTimeLimit`). Proposal: an explicit permanent list (validation `121`, `2`, `9`, `52`, `66`, …), everything else transient.
2. **Row-by-row isolation cost in Postgres.** A large batch with one poison row becomes N single-row statements. Consider bisection instead of linear retry.
