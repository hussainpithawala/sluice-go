# RFP #22 Upgrade MongoDB Driver to `go.mongodb.org/mongo-driver/v2`

**Status:** Proposed (2026-10-05)  
**Target Release:** v1.0.9  
**Dependencies:** None  
**Related:** RFP #21 (UpdateMany support)
---

## 1. Context & Problem Statement

sluice-go currently depends on `go.mongodb.org/mongo-driver` v1.15.0. The v2 driver
(`go.mongodb.org/mongo-driver/v2`) is the actively maintained line and is already the standard in
newer Go services.

### 1.1 Scope of the driver change

The v1→v2 migration affects three packages:

| Package | v1 imports | Key change in v2 |
|---|---|---|
| `sink/docdb` | `mongo.UpdateOneModel`, `options.BulkWrite()`, `mongo.BulkWriteException` | `mongo.ClientBulkWriteException` for client-level bulk writes; `options` split into sub-packages (`options/mongooptions`) |
| `source/docdb` | `mongo.Client`, `bson.M`, `options.Client()`, `options.Find()` | `bson` package moves to `go.mongodb.org/mongo-driver/v2/bson`; `FindOne`/`Find` option types change |
| `sink/docdb/classify_test.go` | `mongo.BulkWriteError`, `mongo.WriteError` | Error type hierarchy restructured |

No changes to `sink/dynamodb`, `sink/postgres`, `source/dynamodb`, or `source/postgres`.

### 1.2 What does NOT change

- The `sink.FlushSink` interface (`sink/types.go`) — no change to `WriteModel`, `BulkWriteResult`,
  `SinkError`, or `ErrorClass`. These are driver-agnostic.
- The `source.Source` interface (`source/source.go`) — no change to `ReadModel`, `BulkReadModel`,
  `BulkReadResult`.
- The engine, shield, DLQ, and all Redis-side code — they never touch the Mongo driver.
- The public `sluice.WriteModel`, `sluice.WriteContract`, `sluice.ReadContract` types.

### 1.3 Why not support both v1 and v2 simultaneously?

Go's type system prevents a single binary from sharing types across major-version module paths.
A `bson.D` from `go.mongodb.org/mongo-driver/bson` is a different type than one from
`go.mongodb.org/mongo-driver/v2/bson`, even if structurally identical. This means:

- A `WriteContract` that builds a `bson.D` filter must use the same bson package as the sink.
- Connection pool sharing (`NewSourceWithClient(sink.Client(), ...)`) requires both to use the same
  `*mongo.Client` type.

Build-tag-gated compilation (`//go:build mongov1` / `//go:build mongov2`) would allow the source
to compile against either, but it fragments testing and CI — every test matrix entry doubles. The
cleaner path is a hard cut to v2.

---

## 2. Goals

1. Replace `go.mongodb.org/mongo-driver` v1.15.0 with `go.mongodb.org/mongo-driver/v2` across
   `sink/docdb` and `source/docdb`.
2. Preserve the existing error classification semantics (RFP #18) — the `classifyBulkErr` logic
   and its test suite must pass with the v2 error types.
3. Preserve the `NewSourceWithClient` pool-sharing pattern.
4. Zero changes to `sink.FlushSink`, `source.Source`, or any public sluice types.

---

## 3. Migration Plan

### Phase 1: Driver swap in `go.mod`

```
go get go.mongodb.org/mongo-driver/v2@latest
go mod tidy  # removes v1 if nothing else imports it
```

### Phase 2: `sink/docdb` migration

#### 2.1 Import paths

```go
// Before
import (
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
    "go.mongodb.org/mongo-driver/mongo/readpref"
)

// After
import (
    "go.mongodb.org/mongo-driver/v2/mongo"
    "go.mongodb.org/mongo-driver/v2/mongo/options"
    "go.mongodb.org/mongo-driver/v2/mongo/readpref"
)
```

#### 2.2 `mongo.Connect` signature change

v2 removes the two-step `Connect` + `Ping` pattern. `mongo.Connect` now accepts options directly
and returns a connected client:

```go
// Before (v1)
opts := options.Client().ApplyURI(cfg.URI)...
client, err := mongo.Connect(ctx, opts)
client.Ping(ctx, readpref.Primary())

// After (v2)
client, err := mongo.Connect(options.Client().ApplyURI(cfg.URI)...)
client.Ping(ctx, readpref.Primary())
```

The context parameter to `Connect` is removed in v2 — the driver uses the context from subsequent
operations instead.

#### 2.3 `BulkWrite` — ordered option

```go
// Before (v1)
s.collection.BulkWrite(ctx, mongoModels, options.BulkWrite().SetOrdered(false))

// After (v2) — verify exact v2 API; may use functional options:
s.collection.BulkWrite(ctx, mongoModels, options.BulkWrite().SetOrdered(false))
```

The `options` package in v2 preserves `SetOrdered` on `BulkWriteOptions`. Verify at integration
time.

#### 2.4 `BulkWriteException` / `WriteError` types

v2 restructures the error hierarchy. The classification function must be updated:

```go
// Before (v1)
var bwe mongo.BulkWriteException
errors.As(err, &bwe)
bwe.WriteErrors[i].Code
bwe.WriteConcernError

// After (v2) — verify field names
var bwe mongo.BulkWriteException
errors.As(err, &bwe)
// WriteErrors and WriteConcernError remain — confirm struct field names
```

The existing `classifyBulkErr` function and `classify_test.go` must be ported and all 4 test cases
must pass.

#### 2.5 `Disconnect` → `Close`

v2 renames `client.Disconnect(ctx)` to `client.Close(ctx)`. Update the `Close` method in both
sink and source.

#### 2.6 `Upsert` field type

In v1, `UpdateOneModel.Upsert` is `*bool`. In v2, verify whether it remains a pointer or becomes
a value. The current code uses `&upsert` — adjust accordingly.

### Phase 3: `source/docdb` migration

Same import path changes. Additionally:

- `FindOne` and `Find` option types may change (v2 uses `options.FindOne()` / `options.Find()`
  — verify).
- `bson.M` moves to `go.mongodb.org/mongo-driver/v2/bson`.
- `mongo.ErrNoDocuments` — verify it exists at the same path in v2.
- Cursor handling in `ReadBulk` should work the same; verify `cursor.Close` signature.

### Phase 4: Test migration

- `classify_test.go`: update `mongo.BulkWriteError` and `mongo.WriteError` to their v2
  equivalents.
- `docdb_test.go`: update any test helpers that construct mongo types.
- Integration tests (testcontainers): verify they still spin up and connect correctly.

### Phase 5: Documentation

- Update `README.md` installation instructions (import path change).
- Update `CHANGELOG.md` with the breaking change.
- Note in the migration guide that consumers on v1 must either upgrade or write a custom adapter.

---

## 4. Breaking Changes

This is a **breaking change** for any consumer that:

1. Calls `sink/docdb.New()` or `source/docdb.NewSource()` — the returned types now wrap a v2
   `*mongo.Client`.
2. Uses `sink.Client()` to share the connection pool — the returned type changes from
   `*mongo.Client` (v1) to `*mongo.Client` (v2).
3. Builds `WriteContract` filters using `bson.D` / `bson.M` from v1 — must switch to v2 bson.

This warrants a minor version bump (v1.1.0), not a patch.

---

## 5. Risks & Mitigations

| Risk | Mitigation |
|---|---|
| v2 API surface may differ from assumptions in §3 | Pin to a specific v2 release; verify each API call against godoc before implementation |
| Error type hierarchy changes break `classifyBulkErr` | Port test suite first; ensure all 4 existing test cases pass before merging |
| Consumers on v1 cannot upgrade immediately | Document the migration path; v1.0.x remains available on the v1.0.x branch |
| `testcontainers-go` compatibility with v2 client | Test locally before CI; testcontainers only needs a connection string, not driver types |

---

## 6. Verification

1. All existing tests in `sink/docdb` and `source/docdb` pass with v2 driver
2. `classify_test.go` — all 4 error classification cases pass
3. Integration test: `sluice.Write` → flush → verify document in MongoDB via v2 `FindOne`
4. Pool sharing: `NewSourceWithClient(sink.Client(), ...)` compiles and works
5. `go vet ./...` and `golangci-lint run` clean
