# RFP #23: UpdateMany Support in WriteModel and Sinks for DocumentdB and MongoDB

**Status:** Proposed (2026-10-05)  
**Target Release:** v1.1.0 (ships with RFP #20)  
**Dependencies:** RFP #22 (mongo-driver v2 migration — avoids touching sink/docdb twice)  
**Related:** RFP #19 (version guard — interacts with the per-document version semantics)

---

## 1. Context & Problem Statement

`sluice.WriteModel` currently resolves to a single-document write in every sink:

| Sink | MongoDB operation | DynamoDB operation | PostgreSQL operation |
|---|---|---|---|
| `sink/docdb` | `UpdateOneModel` | — | — |
| `sink/dynamodb` | — | `PutItem` / `PutRequest` | — |
| `sink/postgres` | — | — | `INSERT ... ON CONFLICT` (single row) |

This is correct for the original design assumption: one correlation key maps to one document. The
correlation key is typically the document's `_id` or a unique business key, so `UpdateOne` with an
upsert is the natural operation.

However, real-world use cases exist where **a single logical write affects multiple documents**:

### 1.1 Motivating example: one-to-many state transitions

Consider a notification or campaign platform that processes conversion events from Kafka, keyed by
`user_id`. A single user has multiple campaign documents in the store — one per `campaign_id`. When
a conversion event arrives, the processor marks **all eligible campaigns for that user** as
fulfilled:

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
filter/update, but the sink executes it as `UpdateOneModel` — which only modifies the **first**
matching document. The remaining campaign documents stay unfulfilled.

### 1.2 Why not restructure the correlation key?

Changing the correlation key from `user_id` to `campaign_id` would make each write single-document.
But this has consequences:

- **Coalescing breaks down.** Two conversion events for the same user within a flush window would
  generate N writes (one per campaign) instead of one `UpdateMany`. The write amplification defeats
  the purpose of sluice.
- **The write contract must enumerate documents.** Today the `WriteContract` receives the payload
  and produces a single `WriteModel`; it doesn't need to know how many documents exist. Keying by
  `campaign_id` would require the contract to query the document list — a read in a write
  contract is a design violation.

### 1.3 Scope beyond MongoDB

DynamoDB has no `UpdateMany` equivalent — its model is inherently single-item. PostgreSQL's
`UPDATE ... WHERE` naturally affects multiple rows. The abstraction should allow each sink to map
the `Many` flag to its native behavior.

---

## 2. Goals

1. Allow a `WriteModel` to express "this write affects multiple documents matching the filter."
2. Preserve the default single-document behavior — no change for existing consumers.
3. Keep the change minimal: one field on `WriteModel`, one branch in each sink.
4. Document the interaction with RFP #19 version guard (§5).

---

## 3. Design

### 3.1 `sink.WriteModel` — add `Many` field

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

Default is `false` — zero-value preserves existing `UpdateOne` behavior. No existing consumer
breaks.

### 3.2 `sluice.WriteModel` — add `Many` field

```go
// types.go

type WriteModel struct {
    Filter interface{}
    Update interface{}
    Upsert bool
    Many   bool
}
```

The engine already copies fields from the public `WriteModel` to `sink.WriteModel` at
`internal/engine/engine.go:265` and `sluice.go:233,888`:

```go
models = append(models, sink.WriteModel{
    CorrelationKey: rec.CorrelationKey,
    Filter:         wm.Filter,
    Update:         wm.Update,
    Upsert:         wm.Upsert,
    // Add:
    Many:           wm.Many,
})
```

Three call sites to update — `engine.go:265`, `sluice.go:233`, `sluice.go:888`.

### 3.3 `sink/docdb` — branch on `Many`

```go
// sink/docdb/docdb.go — in BulkWrite loop

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

Both `UpdateOneModel` and `UpdateManyModel` implement `mongo.WriteModel`, so the `BulkWrite` call
accepts a mixed batch.

The degraded-mode `Write` method similarly branches:

```go
func (s *Sink) Write(ctx context.Context, model sink.WriteModel) error {
    upsert := model.Upsert
    if model.Many {
        _, err := s.collection.UpdateMany(
            ctx, model.Filter, model.Update,
            options.Update().SetUpsert(upsert),
        )
        return err
    }
    _, err := s.collection.UpdateOne(
        ctx, model.Filter, model.Update,
        options.Update().SetUpsert(upsert),
    )
    return err
}
```

### 3.4 Error attribution for `Many` writes

`UpdateMany` in MongoDB's `BulkWrite` reports errors at the operation level, not per-document. A
single `UpdateManyModel` that matches 50 documents and fails on 3 produces one `BulkWriteError`
with the index of that model. The existing attribution logic (`models[we.Index].CorrelationKey`)
works unchanged — the error maps back to the correlation key that produced the model.

What changes: `MatchedCount` and `ModifiedCount` in `BulkWriteResult` may exceed the number of
`WriteModel`s submitted (one model, many documents). This is informational and does not affect
engine behavior — the engine uses `Errors` for commit/dead-letter decisions, not counts.

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

### 3.6 `sink/postgres`

PostgreSQL naturally supports multi-row updates. The current `INSERT ... ON CONFLICT` pattern is
single-row by design (the conflict target is the primary key). For `Many`:

```go
if m.Many {
    // Execute a direct UPDATE ... WHERE for this model.
    _, err := s.pool.Exec(ctx, updateSQL, args...)
}
```

The `Many` write model for Postgres would carry the raw SQL + args in `Filter`/`Update` (as it
already does for the insert path). This needs further design — defer to a follow-up if no Postgres
consumer needs `Many` immediately.

For v1.1.0, the Postgres sink can return a permanent `SinkError` for `Many` writes (same as
DynamoDB), and add support in a later patch.

---

## 4. Interaction with Content Dedup

Content dedup (`ContentDedup: true`) uses xxHash64 of the payload to skip unchanged writes. This
works at the correlation-key level: if the same user sends the same event payload twice, the
second write is a no-op in Redis.

`Many` does not affect this. The dedup decision happens before the `WriteContract` runs — the engine
compares payload hashes in the Redis journal. Whether the resulting `WriteModel` uses `UpdateOne` or
`UpdateMany` is irrelevant to dedup.

---

## 5. Interaction with Version Guard (RFP #19)

The version guard (RFP #19 Phase 1) adds a per-document `{_sluice_e, _sluice_v}` version and wraps
every write in a conditional:

```
UpdateOne({_id, $or: [{_sluice_e: {$lt: e}}, {_sluice_e: e, _sluice_v: {$lt: v}}]}, ...)
```

This is inherently single-document: the version is per-`_id`. For `Many` writes, there are two
options:

**Option A: Skip version guard for `Many` writes.**  
The version guard's purpose is preventing stale flushes from regressing a single document's state.
An `UpdateMany` with a multi-document filter is typically idempotent and convergent — applying it
twice produces the same result. Version regression is not a concern when the update sets terminal
state (e.g. `fulfilled=true, status=completed`), not an intermediate version.

**Option B: Apply version guard per-document within the `UpdateMany`.**  
This would require the filter to include the version condition, which would skip documents that are
already at a newer version. This is correct but adds complexity to the `WriteContract` — the
contract must include version fields in the filter.

**Recommendation:** Option A. The version guard config (`WithVersionGuard()`) applies to `Many:
false` writes only. Document this clearly. Consumers who need per-document version protection on
multi-document writes should key by document ID instead.

---

## 6. Files Changed

| File | Change |
|---|---|
| `sink/types.go` | Add `Many bool` to `WriteModel` |
| `types.go` | Add `Many bool` to `WriteModel` |
| `internal/engine/engine.go:265` | Copy `Many` field to `sink.WriteModel` |
| `sluice.go:233` | Copy `Many` field in wrapped contract |
| `sluice.go:888` | Copy `Many` field in `WriteBatch` wrapped contract |
| `sluice.go:1152` | Pass `Many` field in degraded-mode `Write` |
| `sink/docdb/docdb.go` | Branch on `Many` in `BulkWrite` and `Write` |
| `sink/dynamodb/dynamodb.go` | Return permanent error for `Many` |
| `sink/postgres/postgres.go` | Return permanent error for `Many` (defer support) |
| `sink/docdb/docdb_test.go` | Add test cases for `Many` writes |
| `sink/docdb/classify_test.go` | Verify classification is unchanged for `Many` |

---

## 7. Verification

1. **Unit test — `sink/docdb`:** Submit a mixed batch of `Many: true` and `Many: false` models.
   Verify the `Many` model uses `UpdateManyModel` and the non-Many uses `UpdateOneModel`. Mock the
   collection to inspect the generated `mongo.WriteModel` slice.
2. **Unit test — `sink/docdb` degraded mode:** Call `Write` with `Many: true`. Verify it calls
   `UpdateMany`, not `UpdateOne`.
3. **Unit test — `sink/dynamodb`:** Submit a `Many: true` model. Verify it returns a permanent
   `SinkError` and does not call DynamoDB.
4. **Unit test — error attribution:** Submit a `Many: true` model that fails. Verify the
   `SinkError.CorrelationKey` is correctly set.
5. **Integration test:** Write a `Many: true` model against a real MongoDB. Insert 5 documents
   matching the filter, call `BulkWrite`, verify all 5 are updated. Verify `MatchedCount == 5`,
   `ModifiedCount == 5`.
6. **Engine test:** Verify that a `WriteContract` returning `Many: true` propagates through the
   engine to the sink unchanged.
7. **Existing tests pass:** No regression in any existing test — the zero-value of `Many` is
   `false`, preserving current behavior.

---

## 8. Migration Guide for Consumers

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
        Many:   true,  // <-- new
    }, nil
}
```

No other changes needed. The sink handles the rest.
