# RFP #23: UpdateMany Support in WriteModel and Sinks (DocumentDB, MongoDB, PostgreSQL)

**Status:** In-progress
**Target Release:** v1.0.9 (ships with RFP #20)
**Dependencies:** RFP #22 (mongo-driver v2 migration), implemented first so `sink/docdb` is touched only once
**Related:** RFP #19 (version guard), see §6

> **Scope note.** The original proposal deferred PostgreSQL `Many` support to a follow-up patch and
> returned a permanent error in v1.0.9. During implementation the scope was expanded: PostgreSQL now
> supports `Many` natively in this release, giving a unified abstraction across all sinks except
> DynamoDB. §3.6 and §9 reflect the implemented behavior.

---

## 1. Context & Problem Statement

`sluice.WriteModel` originally resolved to a single-document write in every sink:

| Sink | MongoDB operation | DynamoDB operation | PostgreSQL operation |
|---|---|---|---|
| `sink/docdb` | `UpdateOneModel` | n/a | n/a |
| `sink/dynamodb` | n/a | `PutItem` / `PutRequest` | n/a |
| `sink/postgres` | n/a | n/a | `INSERT ... ON CONFLICT` (single row) |

This fits the original design assumption: one correlation key maps to one document. The correlation
key is typically the document's `_id` or a unique business key, so `UpdateOne` with an upsert is the
natural operation.

However, real-world use cases exist where **a single logical write affects multiple documents**.

### 1.1 Motivating example: one-to-many state transitions

Consider a notification or campaign platform that processes conversion events from Kafka, keyed by
`user_id`. A single user has multiple campaign documents in the store, one per `campaign_id`. When a
conversion event arrives, the processor marks **all eligible campaigns for that user** as fulfilled:

```go
filter := bson.M{
    "user_id":     userID,
    "campaign_id": bson.M{"$in": campaignIDs},
    "$or": []bson.M{
        {"fulfilled": bson.M{"$exists": false}},
        {"fulfilled": bson.M{"$ne": true}},
    },
}
update := bson.M{"$set": bson.M{"fulfilled": true, "status": "completed"}}
collection.UpdateMany(ctx, filter, update)
```

If sluice replaces this write path, the `WriteContract` produces a `WriteModel` with the same
filter/update, but the sink executes it as `UpdateOneModel`, which only modifies the **first**
matching document. The remaining campaign documents stay unfulfilled.

### 1.2 Why not restructure the correlation key?

Changing the correlation key from `user_id` to `campaign_id` would make each write single-document,
but has consequences:

- **Coalescing breaks down.** Two conversion events for the same user within a flush window would
  generate N writes (one per campaign) instead of one `UpdateMany`. The write amplification defeats
  the purpose of sluice.
- **The write contract must enumerate documents.** Today the `WriteContract` receives the payload and
  produces a single `WriteModel`; it doesn't need to know how many documents exist. Keying by
  `campaign_id` would require the contract to query the document list. A read in a write contract is
  a design violation.

### 1.3 Scope beyond MongoDB

- **DynamoDB** has no `UpdateMany` equivalent; its model is inherently single-item.
- **PostgreSQL's** `UPDATE ... WHERE` naturally affects multiple rows.

The abstraction lets each sink map the `Many` flag to its native behavior.

---

## 2. Goals

1. Allow a `WriteModel` to express "this write affects multiple documents/rows matching the filter."
2. Preserve the default single-document behavior, with no change for existing consumers.
3. Keep the change minimal: one field on `WriteModel`, one branch in each sink.
4. Document the interaction with the RFP #19 version guard (§6).
5. *(Added during implementation)* Provide native multi-row support in the PostgreSQL sink in the
   same release, rather than deferring it.

---

## 3. Design

### 3.1 `sink.WriteModel`: add `Many` field

```go
// sink/types.go

type WriteModel struct {
    CorrelationKey string
    Filter         interface{}
    Update         interface{}
    Upsert         bool
    Many           bool // When true, the sink applies UpdateMany / multi-row UPDATE.
}
```

Default is `false`; the zero value preserves existing `UpdateOne` behavior. No existing consumer
breaks.

### 3.2 `sluice.WriteModel`: add `Many` field

```go
// types.go

type WriteModel struct {
    Filter interface{}
    Update interface{}
    Upsert bool
    Many   bool
}
```

The engine copies fields from the public `WriteModel` to `sink.WriteModel`:

```go
models = append(models, sink.WriteModel{
    CorrelationKey: rec.CorrelationKey,
    Filter:         wm.Filter,
    Update:         wm.Update,
    Upsert:         wm.Upsert,
    Many:           wm.Many, // new
})
```

**Four call sites** propagate the field: `internal/engine/engine.go:265`, `sluice.go:233`,
`sluice.go:888`, and `sluice.go:1152` (degraded-mode `Write`).

### 3.3 `sink/docdb`: branch on `Many` (mongo-driver v2)

```go
// sink/docdb/docdb.go: in the BulkWrite loop

for _, m := range models {
    upsert := m.Upsert
    if m.Many {
        mongoModels = append(mongoModels, &mongo.UpdateManyModel{
            Filter: m.Filter,
            Update: m.Update,
            Upsert: &upsert,
        })
    } else {
        mongoModels = append(mongoModels, &mongo.UpdateOneModel{
            Filter: m.Filter,
            Update: m.Update,
            Upsert: &upsert,
        })
    }
}
```

Both `UpdateOneModel` and `UpdateManyModel` implement `mongo.WriteModel`, so `BulkWrite` accepts a
mixed batch.

The degraded-mode `Write` method branches the same way, using the v2 per-operation option builders
(`options.UpdateOne()` / `options.UpdateMany()` replace the v1 `options.Update()`):

```go
func (s *Sink) Write(ctx context.Context, model sink.WriteModel) error {
    upsert := model.Upsert
    if model.Many {
        _, err := s.collection.UpdateMany(
            ctx, model.Filter, model.Update,
            options.UpdateMany().SetUpsert(upsert),
        )
        return err
    }
    _, err := s.collection.UpdateOne(
        ctx, model.Filter, model.Update,
        options.UpdateOne().SetUpsert(upsert),
    )
    return err
}
```

### 3.4 Error attribution for `Many` writes

`UpdateMany` in MongoDB's `BulkWrite` reports errors at the operation level, not per-document. A
single `UpdateManyModel` that matches 50 documents and fails on 3 produces one `BulkWriteError` with
the index of that model. The existing attribution logic (`models[we.Index].CorrelationKey`) works
unchanged: the error maps back to the correlation key that produced the model.

What changes: `MatchedCount` and `ModifiedCount` in `BulkWriteResult` may exceed the number of
`WriteModel`s submitted (one model, many documents). This is informational and does not affect engine
behavior. The engine uses `Errors` for commit/dead-letter decisions, not counts.

### 3.5 `sink/dynamodb`

DynamoDB has no multi-item update. If `Many` is `true`, the sink returns a permanent `SinkError`:

```go
if m.Many {
    preErrors = append(preErrors, sink.SinkError{
        CorrelationKey: m.CorrelationKey,
        Code:           0,
        Class:          sink.ClassPermanent,
        Err:            errors.New("dynamodb: UpdateMany not supported"),
    })
    continue
}
```

This dead-letters the record immediately. Consumers that need `Many` must not use the DynamoDB sink.
This is an expected constraint, not a bug.

### 3.6 `sink/postgres` *(scope expanded; implemented natively)*

The original proposal returned a permanent `SinkError` for `Many` in v1.0.9 and deferred real support.
Native support was implemented instead.

**Contract.** When `Many: true`, the `WriteModel` fields are repurposed:

| Field | Meaning when `Many: true` |
|---|---|
| `Update` | A raw SQL `UPDATE` string |
| `Filter` | A `[]any` slice of query arguments (`$1`, `$2`, ...) |
| `Upsert` | Ignored |

**Execution.** `Many` models are executed via `pool.Exec`, independently of the bulk
`INSERT ... ON CONFLICT` matrix. This avoids column-mismatch validation errors that would occur if a
raw `UPDATE` were forced through the insert path.

**Error handling.** Reuses the existing `isPermanentSQLState` classifier, so constraint violations
(e.g. `23505`) map to `ClassPermanent`, and errors are attributed to the correct `CorrelationKey`.

```go
if m.Many {
    args, _ := m.Filter.([]any)
    sqlStr, _ := m.Update.(string)
    _, err := s.pool.Exec(ctx, sqlStr, args...)
    // classify err via isPermanentSQLState, attribute to m.CorrelationKey
}
```

---

## 4. Interaction with Content Dedup

Content dedup (`ContentDedup: true`) uses xxHash64 of the payload to skip unchanged writes. This works
at the correlation-key level: if the same user sends the same event payload twice, the second write
is a no-op in Redis.

`Many` does not affect this. The dedup decision happens before the `WriteContract` runs; the engine
compares payload hashes in the Redis journal. Whether the resulting `WriteModel` uses `UpdateOne` or
`UpdateMany` is irrelevant to dedup.

---

## 5. RFP #22 Prerequisite: mongo-driver v2 Migration

RFP #22 was implemented as a hard cut to `go.mongodb.org/mongo-driver/v2`, eliminating dual-driver
overhead and type-incompatibility risks (e.g. `bson.D` mismatches) for newer consumers. It lands
before RFP #23 so the sink code is changed once.

- **Dependency swap:** `go.mod` updated to `go.mongodb.org/mongo-driver/v2`. All v1 imports removed
  across 26+ files (`sink/docdb`, `source/docdb`, tests, examples).
- **Connection lifecycle:** `mongo.Connect(ctx, opts)` became `mongo.Connect(opts)`; the context is
  now passed to `Ping`.
- **Options builders:** `options.Update()` became `options.UpdateOne()` / `options.UpdateMany()`.
  `options.Find()` now returns a builder implementing the `options.Lister` interface.
- **Error hierarchy:** `classifyBulkErr` and `classify_test.go` updated for the v2
  `mongo.BulkWriteError` struct, which now embeds `mongo.WriteError`. All 4 classification test cases
  pass.

---

## 6. Interaction with Version Guard (RFP #19)

The version guard (RFP #19 Phase 1) adds a per-document `{_sluice_e, _sluice_v}` version and wraps
every write in a conditional:

```
UpdateOne({_id, $or: [{_sluice_e: {$lt: e}}, {_sluice_e: e, _sluice_v: {$lt: v}}]}, ...)
```

This is inherently single-document: the version is per-`_id`. For `Many` writes there are two options:

**Option A: Skip the version guard for `Many` writes.**
The guard's purpose is preventing stale flushes from regressing a single document's state. An
`UpdateMany` with a multi-document filter is typically idempotent and convergent: applying it twice
produces the same result. Version regression is not a concern when the update sets terminal state
(e.g. `fulfilled=true, status=completed`) rather than an intermediate version.

**Option B: Apply the version guard per-document within the `UpdateMany`.**
This would require the filter to include the version condition, which would skip documents already at
a newer version. It is correct but adds complexity to the `WriteContract`, which must include version
fields in the filter.

**Decision: Option A (confirmed in implementation).** The version guard conditional logic
(`_sluice_e`, `_sluice_v`) is explicitly bypassed for `Many: true` writes **across all sinks**.
`WithVersionGuard()` applies to `Many: false` writes only. Consumers who need per-document version
protection on multi-document writes should key by document ID instead.

---

## 7. Files Changed

| File | Change |
|---|---|
| `go.mod` and 26+ files | RFP #22: swap to mongo-driver v2 |
| `sink/types.go` | Add `Many bool` to `WriteModel` |
| `types.go` | Add `Many bool` to `WriteModel` |
| `internal/engine/engine.go:265` | Copy `Many` field to `sink.WriteModel` |
| `sluice.go:233` | Copy `Many` field in wrapped contract |
| `sluice.go:888` | Copy `Many` field in `WriteBatch` wrapped contract |
| `sluice.go:1152` | Pass `Many` field in degraded-mode `Write` |
| `sink/docdb/docdb.go` | Branch on `Many` in `BulkWrite` and `Write` (v2 builders) |
| `sink/dynamodb/dynamodb.go` | Return permanent error for `Many` |
| `sink/postgres/postgres.go` | Native `Many` execution via `pool.Exec` *(was: permanent error)* |
| `sink/docdb/docdb_test.go` | Test cases for `Many` writes |
| `sink/docdb/classify_test.go` | Verify classification is unchanged for `Many`; v2 error types |

---

## 8. Verification

### 8.1 Planned test matrix

1. **Unit, `sink/docdb`:** Submit a mixed batch of `Many: true` and `Many: false` models. Verify the
   `Many` model uses `UpdateManyModel` and the non-Many uses `UpdateOneModel`.
2. **Unit, `sink/docdb` degraded mode:** Call `Write` with `Many: true`. Verify it calls `UpdateMany`,
   not `UpdateOne`.
3. **Unit, `sink/dynamodb`:** Submit a `Many: true` model. Verify it returns a permanent `SinkError`
   and does not call DynamoDB.
4. **Unit, error attribution:** Submit a `Many: true` model that fails. Verify `SinkError.CorrelationKey`
   is correctly set.
5. **Integration:** Write a `Many: true` model against a real MongoDB. Insert 5 matching documents,
   call `BulkWrite`, verify all 5 are updated (`MatchedCount == 5`, `ModifiedCount == 5`).
6. **Engine test:** Verify a `WriteContract` returning `Many: true` propagates through the engine to
   the sink unchanged.
7. **Regression:** No existing test regresses; the zero value of `Many` is `false`.

### 8.2 Reported results

- [x] `go build ./...` and `go vet ./...` pass cleanly.
- [x] Unit tests for `classifyBulkErr` pass with v2 error types (4/4).
- [x] Postgres `BulkWrite` correctly executes `Many` models and attributes errors to the correct
  `CorrelationKey`.
- [x] Zero regressions in existing single-document write behavior (default `Many: false`).

---

## 9. Migration Guide for Consumers

### 9.1 MongoDB / DocumentDB

```go
// Before: WriteContract returns UpdateOne (default)
func myContract(key string, payload []byte) (*sluice.WriteModel, error) {
    return &sluice.WriteModel{
        Filter: bson.M{"_id": key},
        Update: bson.M{"$set": bson.M{"value": payload}},
        Upsert: true,
    }, nil
}

// After: WriteContract can return UpdateMany
func myContract(key string, payload []byte) (*sluice.WriteModel, error) {
    var p FulfillmentPayload
    json.Unmarshal(payload, &p)
    return &sluice.WriteModel{
        Filter: bson.M{
            "user_id":     key,
            "campaign_id": bson.M{"$in": p.CampaignIDs},
        },
        Update: bson.M{"$set": bson.M{"fulfilled": true, "status": "completed"}},
        Upsert: false,
        Many:   true, // <-- new
    }, nil
}
```

### 9.2 PostgreSQL: fulfilling all campaigns for a user

```go
func myPostgresContract(key string, payload []byte) (*sluice.WriteModel, error) {
    var p FulfillmentPayload
    json.Unmarshal(payload, &p)

    // The raw SQL update
    sql := `UPDATE campaigns SET fulfilled = true, status = 'completed'
            WHERE user_id = $1 AND campaign_id = ANY($2)`

    // The arguments slice
    args := []interface{}{key, p.CampaignIDs}

    return &sluice.WriteModel{
        Filter: args, // Args go in Filter for Many
        Update: sql,  // SQL goes in Update for Many
        Upsert: false, // Ignored for Many
        Many:   true,
    }, nil
}
```

### 9.3 DynamoDB

`Many: true` is rejected with a permanent error and dead-lettered. Use a different sink, or key by
item ID.

No other consumer changes are needed. The sink handles the rest.

---

## 10. Behavior Summary by Sink

| Sink | `Many: false` | `Many: true` | Version guard on `Many` |
|---|---|---|---|
| `sink/docdb` | `UpdateOneModel` | `UpdateManyModel` (mixed batches allowed) | Bypassed |
| `sink/dynamodb` | `PutItem` / `PutRequest` | Permanent `SinkError` (DLQ) | n/a |
| `sink/postgres` | `INSERT ... ON CONFLICT` | Raw `UPDATE` via `pool.Exec` (`Update`=SQL, `Filter`=args) | Bypassed |

---

## 11. Notes for Reviewers

These are observations from merging the two documents, not part of the original RFP:

- **Overloaded fields in Postgres `Many`.** Reusing `Filter` for args and `Update` for SQL is
  type-unsafe (both are `interface{}`). The sink should return a permanent `SinkError` with a clear
  message if `Update` is not a `string` or `Filter` is not `[]any`, rather than failing at runtime.
- **Ordering within a batch.** Postgres `Many` models run via `pool.Exec` outside the bulk insert
  matrix. If a single batch mixes inserts and `Many` updates touching the same rows, confirm the
  intended execution order and whether they share a transaction.
- **Raw SQL provenance.** Because the SQL string comes from the `WriteContract`, it must never be
  built by concatenating payload values; always use `$n` placeholders via the args slice.
- **Counts are informational.** `MatchedCount`/`ModifiedCount` can exceed the number of submitted
  models; nothing in the engine should depend on them for commit decisions.