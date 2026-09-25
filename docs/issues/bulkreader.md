# RFC: Set-Based Cache Pre-Warming via BulkReadContract and BulkIndexContract

**Status:** Accepted / Implemented  
**Target Release:** v1.0.8 (Consolidated with DynamoDB, PostgreSQL, and L1 Local Journal)  
**Dependencies:** DynamoDB Adapter, PostgreSQL Adapter, L1 Local Journal

---

## 1. Context & Problem Statement

The current `sluice` architecture excels at single-key point lookups. When an application requests a `correlationKey`, `sluice` checks the L1 local cache, falls back to the L2 Redis journal, and finally executes a single-key cold read against the Long-Term Store (LTS) via the `ReadContract`.

However, modern data access patterns—especially in relational databases and wide-breadth NoSQL stores—frequently require set-based operations. Examples include:
*   *"Load all active campaigns for this user this month."*
*   *"Fetch the latest 50 transactions for this account."*
*   *"Retrieve the complete inventory state for this specific ad-roll."*

If an application relies solely on the single-key `ReadContract` to fulfill these requirements, it faces severe architectural bottlenecks:
1.  **The N+1 Query Problem:** The application must loop through 50 IDs, triggering 50 separate `FindOne` / `GetItem` / `SELECT` calls to the LTS.
2.  **LTS Round-Trip Explosion:** Even if the LTS supports batching, executing 50 individual network round-trips defeats the purpose of the velocity shield.
3.  **Redis Pipeline Inefficiency:** Hydrating the L1/L2 journals one key at a time generates massive Redis network chatter, negating the sub-microsecond benefits of the L1 cache.

While database-level Materialized Views could theoretically solve this, they are bound by WAL generation, vacuuming, and eventual consistency delays, which completely breaks the causal consistency guarantees `sluice` provides.

---

## 2. The Goal

Introduce `ReadBulkContract` and `IndexBulkContract` to enable **set-based cache pre-warming**.

The objective is to allow a single lookup key (e.g., `user_id`) to trigger **one** highly optimized, set-based query against the LTS. `sluice` will then take the resulting multiple rows, atomically hydrate the L1/L2 journals for *all* returned items in a single Redis pipeline, and return the fully materialized working set to the application.

Crucially, this feature is built on the **Unified Coordination Model**. `sluice` is not a collection of isolated modes; it is a single platform where all operations (`Write`, `Read`, `ReadBulk`, `IndexContract`, `IndexBulkContract`) coordinate through the shared L1/L2 state plane and a strict version gate (`ts`). This enables flexible deployment topologies (from full write/read pods to stateless reader-only pods) while guaranteeing "Read your own writes" semantics across both single-key and bulk operations.

---

## 3. Proposed Design & Architecture

To maintain `sluice`'s strict datastore-agnostic core, the bulk contracts will follow the same "Execution Plan + Projector" pattern established in the PostgreSQL adapter.

### 3.1 The `BulkReadModel` and `ReadBulkContract`

The `ReadBulkContract` translates a parent/lookup key into a bulk read execution plan.

```go
package source

// BulkReadResult represents a single hydrated row/document from a bulk read operation.
type BulkReadResult struct {
    CorrelationKey string 
    Payload        []byte 
}

// BulkReadModel defines the execution plan for a bulk read.
type BulkReadModel struct {
    Query interface{} 
    Args  []any
    Projector func(scanner any) ([]BulkReadResult, error) 
}

// ReadBulkContract translates a parent/lookup key into a bulk read operation.
type ReadBulkContract func(lookupKey string) (*BulkReadModel, error)
```

### 3.2 The IndexBulkContract
If sluice fetches 50 campaigns in one go, calling the standard IndexContract 50 times in a loop is inefficient. The IndexBulkContract allows the operator to extract indexes for the entire batch at once.
go

```go
// IndexBulkContract extracts secondary index fields from multiple payloads simultaneously.
// Returns a map of correlationKey -> indexFields.
// This allows sluice to pipeline all SADD/ZADD operations into a single Redis transaction.
type IndexBulkContract func(results []source.BulkReadResult) (map[string]map[string]interface{}, error)
```
### 3.3 The Hydration Flow (`sluice.ReadBulk`)

When the application calls `sluice.ReadBulk(ctx, "user_123")`:

1. **Capture Pre-Query Timestamp:** `sluice` captures `bulkTs = time.Now().UnixMilli()` *before* invoking the LTS query.
2. **L3 Bulk Fallback:** `sluice` invokes the `ReadBulkContract("user_123")` and the adapter executes the `Query` with `Args` exactly once.
3. **Projection:** The `Projector` iterates over the rows, building a `[]source.BulkReadResult`.
4. **Atomic Journal Hydration (Shield Delegation):** `sluice` takes the returned slice and delegates all Redis operations to the `shield` package:
    * Calls `shield.BulkWriteJournal` to write all payloads to the L2 Redis journal via a single pipeline, using the **pre-query `bulkTs`** as the version timestamp.
    * Calls `shield.BulkUpdateIndexes` (if `IndexBulkContract` is configured) to pipeline all `SADD`/`ZADD` operations to Redis in one go.
5. **L1 Hydration:** Writes all payloads to the L1 local cache using the same `bulkTs`.
6. **Return:** `sluice` returns a `map[string][]byte` to the caller, containing the fully hydrated working set.

### 3.4 Unified Coordination & "Read Your Own Writes"

The core innovation of the Bulk Read feature is not just the LTS query reduction, but how it safely coordinates with concurrent single-key `Write()` operations through the shared state plane.

All operations use the **same version gate** (`ts` = `time.Now().UnixMilli()`) and the **same L1/L2 keys**.

**Scenario: Concurrent Write During Bulk Hydration**

```text
T=0ms:  ReadBulk("user_123") starts
        → bulkTs captured as 1000
        → LTS query begins (takes 50ms)

T=10ms: Write("campaign_42", payload_v2) from another pod
        → L1 Put("campaign_42", payload_v2, ts=1010)
        → L2 HSET("campaign_42", payload_v2, ts=1010)

T=50ms: ReadBulk LTS query completes, returns payload_v1 for campaign_42
        → L1 Put("campaign_42", payload_v1, ts=1000)
        → Version gate check: Is 1000 >= 1010? NO.
        → payload_v1 is REJECTED. payload_v2 is preserved.
```

By using the **pre-query timestamp** for bulk hydration, any `Write()` that occurs *after* the LTS query starts will inherently have a newer `ts` and win the version gate. This guarantees causal consistency without requiring distributed locks or complex sequencing.

### 3.5 Asynchronous Read-Through Hydration & Fire-and-Forget HotLoad

To ensure the shared state plane remains consistent without penalizing the critical read path, cold reads and `HotLoad` participate in the coordination model via **asynchronous read-through hydration**.

* **Cold Reads (`Read`, `ReadFresh`, `ReadOld`):** When a cold read successfully fetches data from the LTS, it returns the payload to the caller immediately, and spawns a background goroutine to hydrate the L2 Redis Journal, L1 Local Cache, and execute the `IndexContract`.
* **Fire-and-Forget `HotLoad`:** `HotLoad` has been refined into a **command** rather than a query. It now returns `error` instead of `([]byte, error)`. It synchronously sets the hot marker (the "command") and asynchronously hydrates the payload and indexes (the "side effect"), ensuring login handlers are never blocked by cold database reads. Callers needing the payload immediately should call `Read()` after `HotLoad()`.

### 3.6 Deployment Topologies

Because all operations coordinate through the same state plane, `sluice` can be deployed in multiple topologies without changing the core:

**Topology 1: Full Pod (Writer + Reader)**  
Configured with `Sink`, `WriteContract`, `Source`, `ReadContract`, `ReadBulkContract`, and `IndexBulkContract`. Handles both Kafka/SQS ingestion and HTTP read APIs.

**Topology 2: Reader-Only Pod (No Sink)**  
Configured *without* a `Sink` or `WriteContract`. Acts as a stateless API gateway that serves `Read()`, `ReadBulk()`, and `Query()`. It reads from the shared Redis journal (written by Full Pods) and falls back to the LTS on cold misses. It **never writes to the LTS**, but it **does write to the Redis journal** (via `ReadBulk` hydration and async read-through), participating fully in the coordination plane. Calling `Write()` on this topology returns `ErrWriteNotConfigured`.

**Topology 3: Bulk Pre-Warmer Pod**  
Configured with only `Source`, `ReadBulkContract`, and `IndexBulkContract`. Runs as a background job to periodically execute `ReadBulk` on active users, keeping the Redis journal hot for peak traffic.

---

## 4. Resolved Design Decisions (Strict Boundaries)

1. **Unified Platform, Not Modes:** `sluice` is a single platform. There is no "READ-only mode" toggle; there is simply a `sluice` build configured without a `Sink`/`WriteContract`. Calling `Write()` on such an instance returns `ErrWriteNotConfigured`.
2. **Pre-Query Timestamp Discipline:** `ReadBulk` must use the timestamp captured *before* the LTS query execution for L1/L2 hydration. This preserves causality with concurrent `Write()` operations.
3. **Fire-and-Forget `HotLoad`:** `HotLoad` returns `error` (not `([]byte, error)`). It is a command to promote a key to hot status, not a query. Callers needing the payload immediately should call `Read()` after `HotLoad()`.
4. **Asynchronous Read-Through Hydration:** Cold reads and `HotLoad` hydrate the L1/L2 journals and indexes asynchronously to prevent blocking the critical read path. Index maintenance remains best-effort and eventually consistent.
5. **Strict Shield Delegation:** The core `Sluice` struct acts purely as an orchestrator. All Redis key formatting, hashing, and pipeline execution for bulk operations are strictly delegated to the `shield` package (`BulkWriteJournal`, `BulkUpdateIndexes`, `BulkGetTTL`).
6. **No Unbounded Result Sets (Operator Responsibility):** `sluice` will not enforce hard limits on the `Projector` output. It is the operator's responsibility to bound their queries (e.g., `LIMIT 50`) and size their Redis `MaxEntries` accordingly.
7. **Datastore Agnosticism Preserved:** The core `sluice` engine will never parse SQL or NoSQL query languages. It only sees `[]byte` payloads and `correlationKey`s. The adapter-specific `Projector` handles all datastore shaping.

---

## 5. Implementation Plan

*All phases have been successfully implemented and merged for the v1.0.8 consolidated release.*

* **Phase 1: Core Interface Definition ✅**
    * Added `BulkReadModel`, `BulkReadResult`, `ReadBulkContract`, and `IndexBulkContract` to the `source` and root `sluice` packages.
    * Added `ReadBulk(ctx, lookupKey)` and `ReadBulkWithTTL(ctx, lookupKey)` to the `Sluice` struct.
    * Relaxed `Build()` validation to allow `Sink`/`WriteContract` to be omitted (Reader-Only topology).

* **Phase 2: Adapter Implementations ✅**
    * Implemented `pgx.Rows` projection in `source/postgres`.
    * Implemented `mongo.Cursor` projection in `source/docdb`.
    * Implemented `dynamodbattribute` unmarshaling in `source/dynamodb`.

* **Phase 3: Redis Pipeline Optimization & Shield Delegation ✅**
    * Refactored the internal `shield` layer to support bulk `MSET` (`BulkWriteJournal`), bulk `SADD`/`ZADD` (`BulkUpdateIndexes`), and bulk `PTTL` (`BulkGetTTL`) pipelines.
    * Ensured `sluice_bulk.go` strictly delegates all Redis key formatting and pipeline execution to the `shield` package.

* **Phase 4: Functional Examples ✅**
    * Created `examples/bulk_read_postgres/main.go`, `examples/bulk_read_dynamodb/main.go`, and `examples/bulk_read_documentdb/main.go` demonstrating the "Load all active campaigns for a user" pattern, proving the reduction in LTS round-trips and Redis network chatter.

## 6 Summary of the Unified Approach

`sluice` is designed as a **Unified Data Platform**, not a collection of mutually exclusive modes. There is no "READ-only mode" toggle or strict enforcement that `Sink` and `WriteContract` must always be present. Instead, the library provides a flexible foundation where any combination of operations can coexist within a single instance.

### 6.1. No Mutual Exclusion at Build Time
The `Build()` method does not enforce that all contracts must be provided. It simply initializes the components that *are* configured. If a `Sink` and `WriteContract` are provided, the flush engine starts. If they are omitted, the engine is skipped, and the instance operates purely as a read-through cache and query accelerator.

### 6.2. Call-Time Dependency Validation
Each operation independently verifies its own dependencies at execution time. This ensures that an instance is only used for the capabilities it was configured to support:
*   **`Write()` / `WriteIdempotent()` / `ProcessDLQ()`**: Requires `WriteContract` and `Sink`. Returns `ErrWriteNotConfigured` if missing.
*   **`Read()` / `ReadFresh()`**: Requires Redis (always present). Falls back to `Source` and `ReadContract` only on a journal miss.
*   **`ReadBulk()` / `ReadBulkWithTTL()`**: Requires `ReadBulkContract` and `Source`.
*   **`HotLoad()`**: Requires `Source` and `ReadContract`.

### 6.3. Flexible Deployment Topologies
Because the platform is unified, the same codebase supports multiple deployment patterns simply by changing the builder configuration:

| Topology | Configuration | Use Case |
| :--- | :--- | :--- |
| **Full Pod** | `Sink`, `WriteContract`, `Source`, `ReadContract`, `ReadBulkContract` | Monolithic service handling both Kafka/SQS ingestion and HTTP read APIs. |
| **Reader-Only Pod** | `Source`, `ReadContract`, `ReadBulkContract` *(No Sink)* | Stateless API gateway serving reads and bulk pre-warming. Reads from the shared Redis journal written by Full Pods. |
| **Bulk Pre-Warmer** | `Source`, `ReadBulkContract`, `IndexBulkContract` | Background job that periodically executes `ReadBulk` to keep the Redis journal hot for peak traffic. |
| **Write-Only** | `Sink`, `WriteContract` *(No Source)* | High-velocity ingestion worker that never serves reads. |

### 6.4. Coordination Through a Shared State Plane
The true sophistication of this approach lies in how all operations coordinate through the shared L1/L2 state plane:
*   **Unified Version Gate**: All operations use the same timestamp (`ts`) for version gating.
*   **Causal Consistency**: `ReadBulk` uses a **pre-query timestamp** for hydration. This guarantees that if a concurrent `Write()` occurs *after* the bulk LTS query starts, the `Write()` will have a newer `ts` and win the version gate, preventing stale bulk data from overwriting fresh single-key updates.
*   **Asynchronous Hydration**: Cold reads and `HotLoad` hydrate the L1/L2 journals and indexes asynchronously, ensuring the critical read path remains unblocked while still participating in the shared coordination plane.

## 7 Internal summary of operations
| Operation    | L1 (Local)                                   | L2 (Redis Journal)                      | Redis Indexes                                  | LTS (Source/Sink)                             |
|:-------------|:---------------------------------------------|:----------------------------------------|:-----------------------------------------------|:----------------------------------------------|
| `Write()`    | **Write-through** (Put with current `ts`)    | **Atomic HSET** (payload + `ts`)        | **Pipeline SADD/ZADD** via `IndexContract`     | Async flush via engine                        |
| `Read()`     | **Get** (sub-µs hit)                         | **HGET + PTTL** (lazy heal L1 on miss)  | —                                              | Fallback via `ReadContract`                   |
| `ReadBulk()` | **Bulk Put** (each result with current `ts`) | **Pipeline MSET** (all payloads + `ts`) | **Pipeline SADD/ZADD** via `IndexBulkContract` | Single set-based query via `ReadBulkContract` |
| `Query()`    | —                                            | **SINTER + ZSCORE** (reads indexes)     | —                                              | —                                             |
| `HotLoad()`  | **Put** (with current `ts`)                  | **HSET + SET hot marker**               | **Pipeline SADD/ZADD** via `IndexContract`     | Source read via `ReadContract`                |