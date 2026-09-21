# RFC: DynamoDB Hybrid Sink/Source Adapter for `sluice-go`

**Status:** Proposed  
**Author:** Hussain Pithawala  
**Date:** September 11, 2026  
**Component:** `adapter/dynamodb`

## 1. Context & Problem Statement

`sluice` was built to shield document stores from high-velocity, uncoordinated write storms. While initial designs targeted generic document stores (like DocumentDB/MongoDB), AWS DynamoDB is a highly requested target. DynamoDB is a fully managed service accessed via standard HTTP APIs with SDK-level connection pooling — it can scale well beyond 40K TPS. The constraint is never connections; it is **cost and capacity management**.

At scale (10K–100K TPS), the following DynamoDB cost and capacity challenges emerge:

1. **DAX Does Not Reduce Write Costs:** AWS DAX (DynamoDB Accelerator) is an in-memory cache for reads only. It does not absorb, batch, or coalesce writes. Every individual write still consumes WCUs on the DynamoDB table. DAX adds infrastructure cost without addressing the primary expense driver: write throughput.

2. **WCU/RCU Cost Explosion with GSIs:** R/W access patterns in modern ad-tech and transaction systems are rarely fully ascertainable upfront. When access patterns are unknown, Global Secondary Indexes (GSIs) become mandatory. Each GSI independently consumes WCUs for every write to the base table, multiplying costs linearly with the number of indexes. A single 10K TPS write stream with 3 GSIs effectively becomes 40K WCU of sustained spend.

3. **GSI Replication Lag Under Sustained Load:** In write-heavy systems (>1,000 TPS sustained), querying a GSI immediately after a write often results in replication lag, pushing read latencies to 500ms+ and breaking real-time SLAs. This is an inherent limitation of DynamoDB's asynchronous GSI replication, not a capacity issue.

4. **Transaction Write Cost Penalty:** Using `TransactWriteItems` to enforce idempotency or multi-item atomicity costs 2x the WCUs of standard writes and is strictly limited to 100 items / 4MB per transaction, making it economically unviable at high throughput.

5. **Capacity Planning Dilemma:** Without write absorption, teams face a trilemma:
    - **Provisioned Capacity:** Must provision for peak WCU, leading to significant over-provisioning and wasted spend during off-peak hours.
    - **On-Demand Mode:** Eliminates provisioning but charges a premium per request, making it the most expensive option at sustained high throughput.
    - **Auto-Scaling:** Reacts to load changes with a lag (typically minutes), during which throttling occurs. It also scales based on sustained utilization, not burst absorption.

**The Goal:** Position `sluice` in front of DynamoDB to provide a hybrid capability: **ultra-fast, highly available writes (AP)** shielded by a Redis journal, combined with **strongly consistent historical reads (CP)** falling back to DynamoDB. The primary value proposition is **dramatic cost reduction** — `sluice` flattens the write burst curve, allowing DynamoDB to operate on a low, predictable provisioned WCU baseline without reserved capacity, auto-scaling, or on-demand pricing.

## 2. Decision

We will introduce a first-class `dynamodb` adapter package within `sluice-go`. This adapter will utilize DynamoDB strictly as a **cold sink** (via `BatchWriteItem`) and a **cold source** (via strongly consistent `GetItem`).

By leveraging `sluice`'s Redis journal, we will:
- **Eliminate the need for DAX** (Redis serves hot reads at <1ms).
- **Eliminate most GSIs** (Redis-side secondary indexing via `WithIndexContract`).
- **Avoid `TransactWriteItems`** (Redis `SETNX` handles idempotency).
- **Flatten the WCU curve** (100K individual writes/sec → ~100 `BatchWriteItem` calls/sec), enabling a low, flat provisioned capacity without auto-scaling or on-demand pricing.

## 3. Architecture & Implementation Details

### 3.1 The Write Path (Sink)
* **API:** `dynamodb.BatchWriteItem` (25 items per batch, 16MB limit).
* **Idempotency:** Handled entirely at the Redis layer via `sluice.WriteIdempotent` (`SETNX`). The DynamoDB flusher will use standard `PutRequest` without `ConditionExpression`, avoiding the 2x WCU penalty and 100-item limit of `TransactWriteItems`.
* **Throttling & Backoff:** The adapter must implement a robust retry loop specifically for DynamoDB's `UnprocessedItems` response, utilizing exponential backoff with jitter.
* **Payload Guardrails:** DynamoDB has a hard 400KB item limit. The adapter will validate payload size pre-flush. Oversized payloads will be routed to the `sluice` Dead Letter Queue (DLQ) with an `ErrPayloadTooLarge` marker to prevent batch poisoning.

### 3.2 The Read Path (Source)
* **API:** `dynamodb.GetItem` / `BatchGetItem`.
* **Consistency:** The adapter **must** enforce `ConsistentRead: true`. Because `sluice` acknowledges writes when they hit Redis, a "cold" read falling back to DynamoDB immediately after a flush must guarantee it sees the flushed state.
* **Hot/Cold Regime:** The adapter does not need to know if a key is hot or cold. `sluice`'s core engine handles the routing: hot keys are served from Redis (<1ms), cold keys trigger the `ReadContract` fallback to DynamoDB (single-digit ms).

### 3.3 Bypassing GSIs via Redis Indexing
Instead of creating expensive DynamoDB GSIs, the adapter will leverage `sluice`'s `WithIndexContract`.
* Projected index fields are written to Redis Sets or Sorted Sets concurrently with the main payload.
* Compound queries for *active/recent* data are executed entirely in Redis via `SMEMBERS` or `ZRANGE`, returning correlation keys that can be fetched via `HMGET`.
* This reduces GSI WCU costs to zero for the hot path and eliminates replication lag.

### 3.4 Capacity Smoothing & Cost Model
The core economic benefit of `sluice` is **write curve flattening**:

| Scenario | Without `sluice` | With `sluice` |
| :--- | :--- | :--- |
| Ingestion rate | 100K individual writes/sec | 100K writes to Redis (sub-ms) |
| DynamoDB write rate | 100K WCU/sec (peak) | ~4K WCU/sec (steady, batched) |
| Capacity model | On-Demand or high Provisioned + Auto-Scaling | Low, flat Provisioned Capacity |
| GSI WCU cost | 3x multiplier (300K WCU/sec) | 0 (Redis indexes for hot path) |
| Idempotency cost | 2x via `TransactWriteItems` | 0 (Redis `SETNX`) |

## 4. CAP Theorem Positioning

This architecture explicitly maps to the CAP theorem to provide the optimal trade-off for high-scale systems:
* **Write Path is AP (Available + Partition Tolerant):** Ingestion accepts writes into Redis, acknowledges immediately, and drains asynchronously. If DynamoDB experiences a partition or severe throttling, `sluice` holds the state in the Redis journal (up to memory/TTL limits) or routes to the DLQ, ensuring the ingestion pipeline never drops writes.
* **Cold Read Path is CP (Consistent + Partition Tolerant):** Once data is flushed and goes "cold", historical reads fall back to DynamoDB using `ConsistentRead: true`, guaranteeing strict consistency for reporting, auditing, and post-session lookups.

## 5. Alternatives Considered

| Alternative | Why it was rejected |
| :--- | :--- |
| **Native DynamoDB + DAX** | DAX is read-only and does not reduce write WCU consumption. At 100K TPS, every write still hits DynamoDB individually, requiring either expensive on-demand pricing or massive over-provisioned WCU capacity. DAX adds cost without addressing the primary expense driver. |
| **DynamoDB + `TransactWriteItems`** | Rejected due to 2x WCU cost, 100-item/4MB throughput limits, and inability to economically scale past ~2,000 TPS per table partition. |
| **DynamoDB On-Demand Mode** | Rejected for sustained high-throughput workloads. On-demand pricing is significantly more expensive per request than provisioned capacity. `sluice` eliminates the need for on-demand by flattening the burst curve to a predictable baseline. |
| **DynamoDB Auto-Scaling** | Rejected because auto-scaling reacts with a lag of minutes, during which throttling occurs at burst onset. It also scales based on sustained utilization, not burst absorption, leading to over-provisioning after the burst subsides. |
| **Eventual Consistency for Cold Reads** | Rejected. If a user reads a key immediately after it transitions from Hot to Cold, eventual consistency could return stale/missing data, breaking application logic. |

## 6. Consequences

### Positive
* **Massive Cost Reduction:** Eliminates DAX costs, removes GSI WCU multipliers, avoids `TransactWriteItems` 2x penalty, and enables low flat provisioned capacity instead of on-demand or auto-scaling.
* **Sub-Millisecond Hot Reads:** Completely bypasses DynamoDB GSI replication lag for active sessions/users.
* **Capacity Predictability:** Write bursts are absorbed by Redis and drained at a steady rate, eliminating the need for reserved capacity planning or dynamic scaling.
* **High Throughput:** Uncouples ingestion velocity from DynamoDB WCU limits.

### Negative / Trade-offs
* **Operational Complexity:** Introduces Redis as a critical stateful dependency alongside DynamoDB. (Mitigation: `sluice` treats Redis as a durable journal with TTLs, not just a transient cache).
* **Cold Read Latency:** Cold reads will experience DynamoDB `GetItem` latency (typically 5-15ms), which is higher than Redis (<1ms). This is an acceptable trade-off for historical data.

## 7. Action Items & Next Steps

1. [ ] Create the `adapter/dynamodb` package structure.
2. [ ] Implement `DynamoWriteContract` with `BatchWriteItem` and `UnprocessedItems` retry logic.
3. [ ] Implement `DynamoReadContract` with `ConsistentRead: true`.
4. [ ] Add 400KB payload size validation and DLQ routing.
5. [ ] Write integration tests using `localstack` or DynamoDB Local.
6. [ ] Publish a benchmark comparing Native DynamoDB (On-Demand) vs. `sluice` + DynamoDB (Provisioned) at 10K TPS, highlighting WCU cost savings.