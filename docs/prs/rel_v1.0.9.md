# sluice-go — RFP #22 & RFP #23 Implementation Summary

> **Repository:** `github.com/hussainpithawala/sluice-go`

> **Target Release:** `v1.0.9`

> **Status:** ✅ Implemented & Tested

> **Date:** 2026-10-12

---

## 1. Executive Summary

| RFP | Title | Status |
|-----|-------|--------|
| **#22** | Upgrade MongoDB Driver to `go.mongodb.org/mongo-driver/v2` | ✅ Complete |
| **#23** | UpdateMany Support in `WriteModel` and Sinks (DocumentDB / DynamoDB / PostgreSQL) | ✅ Complete |

Both RFPs ship together in **v1.1.0**. RFP #23 explicitly depends on RFP #22 to avoid touching `sink/docdb` twice. The PostgreSQL scope for RFP #23 was **expanded** during implementation to include native multi-row support instead of deferring to a follow-up.

---

## 2. RFP #22 — MongoDB Driver v2 Migration

### 2.1 Key Changes
- **Dependency Swap:** `go.mod` updated to `go.mongodb.org/mongo-driver/v2`; all v1 imports removed across 26+ files.
- **Connection Lifecycle:** `mongo.Connect(ctx, opts)` → `mongo.Connect(opts)`; context now passed to `Ping`.
- **Options Builders:** Migrated to v2 functional builders:
    - `options.Update()` → `options.UpdateOne()` / `options.UpdateMany()`
    - `options.Find()` now returns a builder implementing `options.Lister`.
- **Error Hierarchy:** `classifyBulkErr` and `classify_test.go` updated for the v2 `mongo.BulkWriteError` struct (which embeds `mongo.WriteError`). All 4 classification cases pass.

### 2.2 Breaking Changes
| Consumer Impact | Detail |
|---|---|
| `sink.Client()` / `source.NewSourceWithClient()` | Now returns a v2 `*mongo.Client` |
| `bson.D` / `bson.M` filters | Must switch to `go.mongodb.org/mongo-driver/v2/bson` |

---

## 3. RFP #23 — UpdateMany Support

### 3.1 Core Abstraction
- Added `Many bool` to both `sluice.WriteModel` and `sink.WriteModel`.
- Propagated through the engine boundary at all 4 call sites (`engine.go:265`, `sluice.go:233`, `sluice.go:888`, `sluice.go:1152`).
- Default is `false` — zero-value preserves existing `UpdateOne` behavior (no regression).

### 3.2 Sink Implementations
| Sink | Behavior for `Many: true` |
|------|---------------------------|
| **DocumentDB** (`sink/docdb`) | Branches between `UpdateManyModel` and `UpdateOneModel` using v2 builders. |
| **DynamoDB** (`sink/dynamodb`) | Hard-rejects with `ClassPermanent` error (no multi-item update). |
| **PostgreSQL** (`sink/postgres`) — *expanded scope* | Executes raw SQL `UPDATE`. `Update` = SQL string, `Filter` = `[]any` args. Reuses `isPermanentSQLState` for error classification. |

### 3.3 Version Guard Interaction (RFP #19)
Confirmed **Option A**: the Version Guard conditional logic is **bypassed** for `Many: true` writes across all sinks. Multi-row state transitions are expected to be idempotent/convergent.

---

## 4. Files Changed

### 4.1 Core Implementation
| # | File Path | RFP | Change Type | Description |
|---|-----------|-----|-------------|-------------|
| 1 | `go.mod` | #22 | Modified | Swap driver v1.15.0 → `v2` |
| 2 | `sink/docdb/docdb.go` | #22, #23 | Modified | v2 migration + `Many` branching |
| 3 | `source/docdb/docdb.go` | #22 | Modified | v2 imports; `mongo.Connect` signature |
| 4 | `source/docdb/types.go` | #22 | Modified | v2 import paths |
| 5 | `sink/types.go` | #23 | Modified | Add `Many bool` to `sink.WriteModel` |
| 6 | `types.go` | #23 | Modified | Add `Many bool` to `sluice.WriteModel` |
| 7 | `internal/engine/engine.go` | #23 | Modified | Copy `Many` field (~line 265) |
| 8 | `sluice.go` | #23 | Modified | Copy `Many` at 3 call sites |
| 9 | `sink/dynamodb/dynamodb.go` | #23 | Modified | Permanent error for `Many:true` |
| 10 | `sink/postgres/postgres.go` | #23 | Modified | Raw SQL `UPDATE` for `Many:true` |

### 4.2 Documentation
| # | File Path | RFP | Change Type | Description |
|---|-----------|-----|-------------|-------------|
| 11 | `README.md` | #22 | Modified | Installation instructions (new import path) |
| 12 | `CHANGELOG.md` | #22 | Modified | Breaking-change entry (v1.1.0) |

### 4.3 New Test Files
| # | File Path | RFP | Change Type | Description |
|---|-----------|-----|-------------|-------------|
| 13 | `sink/docdb/docdb_update_many_test.go` | #23 | Created | 6 unit tests |
| 14 | `sink/postgres/postgres_update_many_test.go` | #23 | Created | 8 unit tests |
| 15 | `sink/dynamodb/dynamodb_update_many_test.go` | #23 | Created | 6 unit tests |
| 16 | `tests/integration/documentdb/docdb_sink_update_many_test.go` | #23 | Created | 7 subtests (testcontainers: `mongo:6.0`) |
| 17 | `tests/integration/postgres/postgres_sink_update_many_test.go` | #23 | Created | 9 subtests (testcontainers: `postgres:16-alpine`) |
| 18 | `tests/integration/dynamodb/dynamodb_sink_update_many_test.go` | #23 | Created | 7 subtests (testcontainers: DynamoDB Local) |

---

## 5. Unit Tests

| # | Test File | Test Name | Scenario | Key Assertion(s) | RFP §7 Mapping |
|---|-----------|-----------|----------|------------------|----------------|
| 1 | `sink/docdb` | `TestDocDBSink_BulkWrite_ManyModel_UsesUpdateManyModel` | Mixed batch `Many:true` + `Many:false` | `Many` uses `UpdateManyModel`; non-`Many` uses `UpdateOneModel` | §7.1 |
| 2 | `sink/docdb` | `TestDocDBSink_BulkWrite_ManyModel_UpdatesAllMatchingDocuments` | 5 docs match filter | `MatchedCount==5`, `ModifiedCount==5` | §7.5 |
| 3 | `sink/docdb` | `TestDocDBSink_BulkWrite_ManyModel_ErrorAttribution` | Failing `Many` model | `CorrelationKey` set, `ClassPermanent` | §7.4 |
| 4 | `sink/docdb` | `TestDocDBSink_Write_Many_CallsUpdateMany` | Degraded `Write` `Many:true` | All docs updated | §7.2 |
| 5 | `sink/docdb` | `TestDocDBSink_Write_ManyFalse_CallsUpdateOne` | Degraded `Write` `Many:false` | Only 1 doc updated | Regression |
| 6 | `sink/docdb` | `TestDocDBSink_BulkWrite_ManyModel_MatchedCountExceedsModelCount` | 1 model → 10 docs | `MatchedCount==10` | §3.4 |
| 7 | `sink/postgres` | `TestPostgresSink_BulkWrite_Many_ExecutesRawSQL` | Raw SQL + args | `pool.Exec` called; rows updated | §3.6 |
| 8 | `sink/postgres` | `TestPostgresSink_BulkWrite_Many_InvalidUpdateType` | `Update` not a string | `ClassPermanent`, "raw SQL string" | §3.6 |
| 9 | `sink/postgres` | `TestPostgresSink_BulkWrite_Many_InvalidFilterType` | `Filter` not `[]any` | `ClassPermanent`, "[]any slice" | §3.6 |
| 10 | `sink/postgres` | `TestPostgresSink_BulkWrite_MixedManyAndUpsert` | `Many` + `Upsert` in batch | Both paths execute | §7.1 |
| 11 | `sink/postgres` | `TestPostgresSink_BulkWrite_Many_SQLStateError` | Bad table/column | `ClassPermanent` via `isPermanentSQLState` | §7.4 |
| 12 | `sink/postgres` | `TestPostgresSink_Write_Many_ExecutesRawSQL` | Degraded `Write` `Many:true` | Raw SQL executed | §7.2 |
| 13 | `sink/postgres` | `TestPostgresSink_Write_Many_InvalidUpdateType` | Degraded bad `Update` | Returns error | §3.6 |
| 14 | `sink/postgres` | `TestPostgresSink_BulkWrite_Many_NilFilter` | `Filter` is `nil` | SQL runs without args | §3.6 |
| 15 | `sink/dynamodb` | `TestDynamoDBSink_BulkWrite_Many_ReturnsPermanentError` | `Many:true` submitted | `ClassPermanent`, "UpdateMany not supported" | §7.3 |
| 16 | `sink/dynamodb` | `TestDynamoDBSink_BulkWrite_Many_NoAPICall` | All `Many:true`, nil client | No panic; API never invoked | §7.3 |
| 17 | `sink/dynamodb` | `TestDynamoDBSink_BulkWrite_MixedManyAndValid` | `Many` + valid in batch | `Many` rejected; valid proceeds | §7.3 |
| 18 | `sink/dynamodb` | `TestDynamoDBSink_Write_Many_ReturnsError` | Degraded `Write` `Many:true` | Returns error | §7.3 |
| 19 | `sink/dynamodb` | `TestDynamoDBSink_BulkWrite_ManyFalse_DefaultBehaviorPreserved` | `Many:false` default | No errors; default works | Regression |
| 20 | `sink/dynamodb` | `TestDynamoDBSink_BulkWrite_AllMany_EmptyBatch` | Entire batch `Many:true` | All rejected with `ClassPermanent` | §7.3 |

---

## 6. Integration Tests

| # | Test File | Subtest | Scenario | Key Assertion(s) | RFP §7 Mapping |
|---|-----------|---------|----------|------------------|----------------|
| 1 | `tests/integration/documentdb` | `Many: single model updates all 5 matching documents` | 5 docs user-1 + 1 doc user-2 | `MatchedCount==5`, `ModifiedCount==5`; user-2 untouched | §7.5 |
| 2 | `tests/integration/documentdb` | `Mixed batch` | 3 docs (Many) + 1 doc (single) | Total `ModifiedCount==4` | §7.1 |
| 3 | `tests/integration/documentdb` | `Degraded Write with Many:true` | 4 docs, degraded `Write` | All 4 updated | §7.2 |
| 4 | `tests/integration/documentdb` | `Error attribution` | Unique index violation | `CorrelationKey` set, `Code==11000`, `ClassPermanent` | §7.4 |
| 5 | `tests/integration/documentdb` | `MatchedCount exceeds model count` | 10 docs, 1 model | `MatchedCount==10` | §3.4 |
| 6 | `tests/integration/documentdb` | `Idempotent reapplication` | Apply same write twice | Second apply no error | §5 Option A |
| 7 | `tests/integration/documentdb` | `Attribution: CheckAttribution passes` | `CheckAttribution` on result | No silent loss | §7.4 |
| 8 | `tests/integration/postgres` | `Many: single model updates all matching rows via raw SQL` | 5 rows user-1 + 1 row user-2 | All 5 updated; user-2 untouched | §3.6 |
| 9 | `tests/integration/postgres` | `Degraded Write with Many:true executes raw SQL` | 4 rows, degraded `Write` | All 4 updated | §7.2 |
| 10 | `tests/integration/postgres` | `SQL error in Many model is classified as permanent` | Non-existent column (42703) | `ClassPermanent` | §7.4 |
| 11 | `tests/integration/postgres` | `Invalid Update type` | `Update` not string | `ClassPermanent` | §3.6 |
| 12 | `tests/integration/postgres` | `Invalid Filter type` | `Filter` not `[]any` | `ClassPermanent` | §3.6 |
| 13 | `tests/integration/postgres` | `Nil Filter executes SQL without arguments` | `Filter` is `nil` | Rows updated | §3.6 |
| 14 | `tests/integration/postgres` | `Idempotent reapplication` | Apply same write twice | Second apply no error | §5 Option A |
| 15 | `tests/integration/postgres` | `Mixed batch: Many:true raw SQL + Many:false upsert` | Segregated tables | Both paths execute | §7.1 |
| 16 | `tests/integration/postgres` | `Many:false default behavior preserved` | Upsert via `ON CONFLICT` | Row inserted | Regression |
| 17 | `tests/integration/dynamodb` | `Many:true returns permanent SinkError` | `Many:true` submitted | `ClassPermanent` | §7.3 |
| 18 | `tests/integration/dynamodb` | `Many:true does not write any item` | Rejected `Many` | `GetItem` returns empty | §7.3 |
| 19 | `tests/integration/dynamodb` | `Mixed batch: Many rejected, valid proceeds` | `Many` + valid in batch | `Many` rejected; valid written | §7.3 |
| 20 | `tests/integration/dynamodb` | `Degraded Write with Many:true returns error` | Degraded `Write` | Returns error | §7.3 |
| 21 | `tests/integration/dynamodb` | `Many:false default behavior preserved` | Default single-item | Item written | Regression |
| 22 | `tests/integration/dynamodb` | `All Many batch` | Entire batch `Many:true` | All 3 rejected | §7.3 |
| 23 | `tests/integration/dynamodb` | `Attribution: CheckAttribution passes` | Rejected `Many` result | Attribution passes | §7.4 |

---

## 7. Coverage by RFP #23 §7 Verification Requirement

| RFP §7 Requirement | Unit Test Coverage | Integration Test Coverage |
|--------------------|--------------------|---------------------------|
| **§7.1** Mixed batch → correct model types | docdb #1, postgres #10, dynamodb #17 | docdb #2, postgres #15, dynamodb #19 |
| **§7.2** Degraded `Write` with `Many:true` | docdb #4, postgres #12, dynamodb #18 | docdb #3, postgres #9, dynamodb #20 |
| **§7.3** DynamoDB rejects `Many` permanently | dynamodb #15, #16, #17, #20 | dynamodb #17, #18, #20, #22 |
| **§7.4** Error attribution for failing `Many` model | docdb #3, postgres #11 | docdb #4, #7, postgres #10, dynamodb #23 |
| **§7.5** All matching documents updated | docdb #2 | docdb #1 |
| **§7.6** Engine propagation (`Many` field flows through) | *(existing engine tests + field copy)* | *(implicit in all sink tests)* |
| **§7.7** Zero regression (default `Many:false`) | docdb #5, dynamodb #19 | postgres #16, dynamodb #21 |
| **§3.4** Counts may exceed model count | docdb #6 | docdb #5 |
| **§3.6** Postgres raw SQL path | postgres #7–#14 | postgres #8–#16 |
| **§5 Option A** Idempotent / convergent reapplication | *(sink-level)* | docdb #6, postgres #14 |

---

## 8. Summary Counts

| Category | Sink/Package | Test Count |
|----------|--------------|------------|
| **Unit** | `sink/docdb` | 6 |
| **Unit** | `sink/postgres` | 8 |
| **Unit** | `sink/dynamodb` | 6 |
| **Unit** | **Subtotal** | **20** |
| **Integration** | `tests/integration/documentdb` | 7 |
| **Integration** | `tests/integration/postgres` | 9 |
| **Integration** | `tests/integration/dynamodb` | 7 |
| **Integration** | **Subtotal** | **23** |
| **Total** | — | **43** |

---

## 9. Verification Checklist

- [x] `go build ./...` and `go vet ./...` pass cleanly.
- [x] Unit tests for `classifyBulkErr` pass with v2 error types.
- [x] Postgres `BulkWrite` correctly executes `Many` models and attributes errors to the correct `CorrelationKey`.
- [x] Zero regressions in existing single-document write behaviors (default `Many: false`).
- [x] Integration tests pass against real infrastructure via testcontainers.

---

## 10. Notes & Gotchas

| Topic | Detail |
|-------|--------|
| **Postgres table segregation** | Integration tests use two tables: `test_many` (no unique on `user_id`, for `Many` tests) and `test_upsert` (`user_id` PK, for upsert tests). `ON CONFLICT` requires a unique constraint, but `UpdateMany` needs multiple rows per key — hence the segregation. |
| **Postgres sink pool access** | Tests create a separate `pgxpool.Pool` from the connection string for seeding/verification, since the sink's internal pool is private. |
| **DynamoDB `PKAttribute`** | The `Config` requires `PKAttribute` (validated by `New()`); test helpers must set it. |
| **v2 `Disconnect` vs `Close`** | The RFP notes `Disconnect` → `Close`, but the v2 `*mongo.Client` retains `Disconnect(ctx)`; `Close` applies to streams/encryption. |