# RFC: Set-Based Cache Pre-Warming via BulkReadContract and BulkIndexContract

**Status:** Draft / Future Roadmap  
**Target Release:** v1.1.0 (Following v1.0.9 PostgreSQL Adapter)  
**Dependencies:** RFC #8 (DynamoDB), RFC #9 (PostgreSQL)

---

## 1. Context & Problem Statement

The current `sluice` architecture excels at single-key point lookups. When an application requests a `correlationKey`, `sluice` checks the L1 local cache, falls back to the L2 Redis journal, and finally executes a single-key cold read against the Long-Term Store (LTS) via the `ReadContract`.

However, modern data access patterns—especially in relational databases and wide-breadth NoSQL stores—frequently require **set-based operations**. Examples include:
*   *"Load all active campaigns for this user this month."*
*   *"Fetch the latest 50 transactions for this account."*
*   *"Retrieve the complete inventory state for this specific ad-roll."*

If an application relies solely on the single-key `ReadContract` to fulfill these requirements, it faces severe architectural bottlenecks:
1.  **The N+1 Query Problem:** The application must loop through 50 IDs, triggering 50 separate `FindOne` / `GetItem` / `SELECT` calls to the LTS.
2.  **LTS Round-Trip Explosion:** Even if the LTS supports batching, executing 50 individual network round-trips defeats the purpose of the velocity shield.
3.  **Redis Pipeline Inefficiency:** Hydrating the L1/L2 journals one key at a time generates massive Redis network chatter, negating the sub-microsecond benefits of the L1 cache.

While database-level Materialized Views could theoretically solve this, they are bound by WAL generation, vacuuming, and eventual consistency delays, which completely breaks the causal consistency guarantees `sluice` provides.

## 2. The Goal

Introduce `ReadBulkContract` and `IndexBulkContract` to enable **set-based cache pre-warming**. 

The objective is to allow a single lookup key (e.g., `user_id`) to trigger **one** highly optimized, set-based query against the LTS. `sluice` will then take the resulting multiple rows, atomically hydrate the L1/L2 journals for *all* returned items in a single Redis pipeline, and return the fully materialized working set to the application.

This eliminates LTS round-trips, collapses N+1 queries into a single set-based execution, and pipelines all secondary index updates into one Redis network traversal.

## 3. Proposed Design & Architecture

To maintain `sluice`'s strict datastore-agnostic core, the bulk contracts will follow the same "Execution Plan + Projector" pattern established in the PostgreSQL adapter (RFC #9).

### 3.1 The `BulkReadModel` and `ReadBulkContract`

The `ReadBulkContract` translates a parent/lookup key into a bulk read execution plan.

```go
package source

// BulkReadResult represents a single hydrated row/document from a bulk read operation.
type BulkReadResult struct {
    // CorrelationKey is the unique key for this specific item (e.g., "campaign_123").
    CorrelationKey string 
    
    // Payload is the canonical JSON payload that will be stored in the Redis journal.
    Payload        []byte 
}

// BulkReadModel defines the execution plan for a bulk read.
type BulkReadModel struct {
    // Query is the datastore-specific statement. 
    // For SQL: "SELECT campaign_id, json_build_object(...) FROM campaigns WHERE user_id = $1"
    // For NoSQL: A specific find query with projection.
    Query interface{} 
    
    // Args are the parameters to bind to the query (e.g., []any{userID}).
    Args []any
    
    // Projector iterates over the returned rows and shapes them into BulkReadResults.
    // The exact type of the scanner depends on the adapter (e.g., pgx.Rows, mongo.Cursor).
    // We use 'any' here to keep the core source package datastore-agnostic.
    Projector func(scanner any) ([]BulkReadResult, error) 
}

// ReadBulkContract translates a parent/lookup key into a bulk read operation.
type ReadBulkContract func(lookupKey string) (*BulkReadModel, error)
```

#### PostgreSQL-Specific Implementation Example

```go
package postgres

import "github.com/jackc/pgx/v5"

// PostgresBulkReadModel is the PostgreSQL-specific execution plan.
type PostgresBulkReadModel struct {
    Query     string
    Args      []any
    Projector func(rows pgx.Rows) ([]source.BulkReadResult, error)
}

// Example usage in the application's ReadBulkContract:
func campaignBulkReadContract(userID string) (*source.BulkReadModel, error) {
    return &source.BulkReadModel{
        Query: PostgresBulkReadModel{
            Query: `
                SELECT campaign_id, json_build_object(
                    'id', campaign_id, 
                    'name', name, 
                    'status', status
                ) 
                FROM campaigns 
                WHERE user_id = $1 AND status = 'active'
            `,
            Args: []any{userID},
            Projector: func(rows pgx.Rows) ([]source.BulkReadResult, error) {
                var results []source.BulkReadResult
                for rows.Next() {
                    var crn string
                    var payload []byte
                    if err := rows.Scan(&crn, &payload); err != nil {
                        return nil, err
                    }
                    results = append(results, source.BulkReadResult{
                        CorrelationKey: crn,
                        Payload:        payload,
                    })
                }
                return results, rows.Err()
            },
        },
    }, nil
}
```

### 3.2 The `IndexBulkContract`

If `sluice` fetches 50 campaigns in one go, calling the standard `IndexContract` 50 times in a loop is inefficient. The `IndexBulkContract` allows the operator to extract indexes for the entire batch at once.

```go
// IndexBulkContract extracts secondary index fields from multiple payloads simultaneously.
// Returns a map of correlationKey -> indexFields.
// This allows sluice to pipeline all SADD/ZADD operations into a single Redis transaction.
type IndexBulkContract func(results []source.BulkReadResult) (map[string]map[string]interface{}, error)
```

### 3.3 The Hydration Flow (`sluice.ReadBulk`)

When the application calls `sluice.ReadBulk(ctx, "user_123")`:

1.  **L1/L2 Check:** `sluice` checks the local L1 cache and issues a single Redis `MGET` pipeline to the L2 journal for any known keys.
2.  **L3 Bulk Fallback (The Magic):** For keys still missing (or if the application explicitly requests a full refresh), `sluice` invokes the `ReadBulkContract("user_123")`.
3.  **Single LTS Execution:** The PostgreSQL/DocumentDB adapter executes the `Query` with `Args` *exactly once*.
4.  **Projection:** The `Projector` iterates over the rows, building a `[]source.BulkReadResult`.
5.  **Atomic Journal Hydration:** `sluice` takes the returned slice and:
    *   Writes all payloads to the L2 Redis journal via a single `MSET` pipeline.
    *   Writes all payloads to the L1 local cache.
    *   Invokes `IndexBulkContract` and pipelines all `SADD`/`ZADD` operations to Redis in one go.
6.  **Return:** `sluice` returns a `map[string][]byte` to the caller, containing the fully hydrated working set.

## 4. Resolved Design Decisions (Strict Boundaries)

1.  **No Unbounded Result Sets (Operator Responsibility):** `sluice` will not enforce hard limits on the `Projector` output. If an operator writes a query that returns 100,000 rows, `sluice` will attempt to hydrate them all. It is the operator's responsibility to bound their queries (e.g., `LIMIT 50`) and size their Redis `MaxEntries` accordingly.
2.  **Datastore Agnosticism Preserved:** The core `sluice` engine will never parse SQL or NoSQL query languages. It only sees `[]byte` payloads and `correlationKey`s. The adapter-specific `Projector` handles all datastore shaping.
3.  **Version Gating in Bulk Writes:** When bulk-hydrating the L1 cache, `sluice` will use the current wall-clock millisecond as the version timestamp for all items in the batch. This ensures that subsequent single-key `Write()` operations (which will have newer timestamps) will correctly overwrite the bulk-hydrated state.
4.  **Out of Scope for v1.1.0:** This RFC is strictly deferred to v1.2.0. The immediate priority is solidifying the single-key PostgreSQL adapter.

## 5. Implementation Plan

1.  **Phase 1: Core Interface Definition**
    *   Add `BulkReadModel`, `BulkReadResult`, `ReadBulkContract`, and `IndexBulkContract` to the `source` and root `sluice` packages.
    *   Add `ReadBulk(ctx, lookupKey)` and `ReadBulkWithTTL(ctx, lookupKey)` to the `Sluice` struct.
2.  **Phase 2: Adapter Implementations**
    *   Implement `pgx.Rows` projection in `source/postgres`.
    *   Implement `mongo.Cursor` projection in `source/docdb`.
    *   Implement `dynamodbattribute` unmarshaling in `source/dynamodb`.
3.  **Phase 3: Redis Pipeline Optimization**
    *   Refactor the internal `shield` layer to support bulk `MSET` and bulk `SADD`/`ZADD` pipelines triggered by the bulk hydration flow.
4.  **Phase 4: Functional Examples**
    *   Create `examples/bulk_read_postgres/main.go` demonstrating the "Load all active campaigns for a user" pattern, proving the reduction in LTS round-trips and Redis network chatter.

---

*This RFC serves as the definitive design document for the next evolution of sluice's read path. Implementation will commence immediately following the merge and stabilization of the v1.1.0 PostgreSQL adapter.*
