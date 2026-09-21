# RFC: PostgreSQL Hybrid Sink/Source Adapter for `sluice-go`

**Status:** Proposed (Revision 5 — Open questions resolved into firm design boundaries)
**Author:** Hussain Pithawala
**Date:** September 19, 2026
**Component:** `adapter/postgres`
**Companion docs:** [hybrid-dynamo.md / Issue #8](https://github.com/hussainpithawala/sluice-go/issues/8) (DynamoDB adapter), `local-journal.md` (L1 read tier), [Issue #9](https://github.com/hussainpithawala/sluice-go/issues/9) (Future `ProjectionContract` unification)

---

## Revision 5 change log (resolving open questions into design boundaries)

1. **§3.3 / §8** — **Auto-DDL and schema management completely rejected.** `sluice` will never auto-create tables, emit DDL, or manage migrations. Schema evolution is strictly the operator's responsibility. `sluice` contracts define the *data shape*, not the physical database schema.
2. **§8** — **Partitioning explicitly delegated to the operator.** `sluice` remains agnostic to underlying table partitioning strategies.
3. **§8** — **`ProjectionContract` unification deferred to Issue #9.** The merging of Redis and Postgres projection interfaces is tracked as a dedicated follow-up RFC.
4. **§8** — **CDC integration permanently rejected.** `sluice` will not publish flush completions to logical replication slots; downstream CDC is out of scope for all future versions.
5. **§8** — **Monotonic ordering sequence deferred.** Wall-clock millisecond `ts` remains the standard until a production workload concretely demands sub-millisecond cross-window ordering.

---

## Revision 4 change log (retained for audit trail)

1. **§3.2** — Probe lifecycle moved from one-shot `Build`/`Ping` to `pgxpool.AfterConnect`.
2. **§3.1** — Timestamp contract promoted to a canonical, source-cited definition with a regression test.
3. **§3.1** — Single-axis AIMD split into two axes plus a pure-backoff class.
4. **§3.6** — New gauge added for effective in-flight flush concurrency.

---

## 1. Context & Problem Statement

`sluice` shields backing stores from high-velocity, uncoordinated write storms by interposing a Redis journal: writes land in Redis first (fast, available) and drain to the backing store on a controlled cadence. The DynamoDB adapter (#8) addressed a request-priced, horizontally-scaled NoSQL store. PostgreSQL is a strongly-consistent, ACID relational database with no per-request pricing — its constraints are structurally different, and this RFC does not assume the DynamoDB cost model transfers.

At 10K–100K TPS, PostgreSQL faces:

### 1.1 Connection Pool Saturation
Each client connection consumes ~5–10 MB of backend memory and a dedicated backend process (or thread under pgBouncer transaction mode). A typical RDS/Aurora instance caps concurrent connections at 100–5,000 depending on class. At 100K TPS with hundreds of writer workers, naive per-write connections exhaust the pool in seconds — `FATAL: sorry, too many clients already` — with cascading failures across any other service sharing the instance. Unlike DynamoDB's stateless HTTP access, this is a hard, immediate ceiling, not a soft cost.

### 1.2 Transaction Overhead
Every `BEGIN`/`COMMIT` round-trip adds latency and a WAL fsync. At 100K individual transactions/sec, that's 100K fsyncs, parser passes, and planner invocations per second, even with `synchronous_commit=off` — parser/planner cost still dominates.

### 1.3 Synchronous Index Write Amplification
Unlike a DynamoDB GSI (async, eventually consistent), a Postgres B-tree/GIN/GiST index is maintained inline, in the same transaction, on the same write path. A table with 5 indexes writing at 100K rows/sec generates 500K index-tuple writes/sec — no replication-lag excuse; it's direct contention on the critical path, pushing checkpoint/WAL writer CPU and churning the buffer cache.

### 1.4 MVCC Bloat from High-Frequency Updates
`UPDATE` doesn't modify in place — it writes a new tuple version and marks the old one dead, requiring `VACUUM` to reclaim space. A hot key updated 1,000 times/sec directly against Postgres produces 1,000 dead tuples/sec: real bloat and autovacuum pressure with no DynamoDB analog.

### 1.5 Bulk Write Primitives Are Underused
Postgres has two efficient bulk primitives most ORMs don't expose well:
- `COPY ... FROM` — the fastest insert path (100K–1M rows/sec), but no `ON CONFLICT` support — insert-only.
- Multi-row `INSERT ... ON CONFLICT DO UPDATE` — supports upserts with conflict resolution, but needs batching and prepared-statement caching to avoid re-parse overhead.

Without a dedicated adapter, applications default to single-row round-trips, leaving 10–100x throughput on the table.

### 1.6 Two Independent, Non-Interchangeable Savings Levers

- **Batching (statement-count reduction) is unconditional.** A single N-row `INSERT ... ON CONFLICT` is cheaper than N individual statements — fewer round trips, one parse/plan cycle, one fsync amortized across all rows, less connection churn — regardless of write-key cardinality.
- **Coalescing (row-count reduction) is workload-dependent.** If many writes in a flush window target the same key and only the latest value per key is flushed, the number of physical rows/index-tuples written — and therefore WAL volume and bloat — drops toward unique-key count, not raw write count.

Reducing statement count from 100K/sec to ~200/sec does not, by itself, reduce index-tuple writes or WAL volume if those 200 statements collectively still carry 100K distinct rows. Any cost/capacity table in this RFC must be read with this distinction attached, and any benchmark must report statement-count reduction and row-count reduction as two separate numbers, never one blended figure.

### 1.7 Cold-Read Consistency Is Structurally Simple, But Not Unconditionally "Free"
Postgres's default `READ COMMITTED` isolation against the primary always returns the latest committed row. But it is conditional on the read path staying pinned to the primary (or a synchronous replica): pointing cold reads at an asynchronous replica silently reintroduces staleness risk. This constraint must be enforced in code (§3.2), not left as an assumption.

### The Goal
Position `sluice` in front of PostgreSQL as a write-rate flattener and connection multiplexer: Redis absorbs the write burst, drains it as a small, steady stream of bulk upserts (or `COPY` batches for append-only data) over a small, stable connection pool, while cold reads stay strongly consistent against the primary.

---

## 2. Decision

We will introduce a `postgres` adapter package within `sluice-go`, reusing the `WriteContract`/`ReadContract`/`WithIndexContract` interfaces established by the DynamoDB adapter for structural consistency across adapters.

The adapter uses PostgreSQL strictly as a cold sink (multi-row `INSERT ... ON CONFLICT DO UPDATE`, or `COPY` for append-only contracts) and a cold source (indexed `SELECT` against the primary), leveraging the Redis journal to:

- **Collapse connection pressure:** hundreds of writer workers share a small (`~10–30`) fixed pool.
- **Eliminate per-write transactions:** one multi-row upsert replaces thousands of `BEGIN`/`COMMIT` pairs — unconditional gain.
- **Exploit `COPY` for append-only contracts:** an additional 5–10x over bulk upsert where no conflict resolution is needed.
- **Offload hot-path indexing to Redis:** compound live-state queries run against Redis (`SINTER`/`ZRANGE`), leaving Postgres indexes for cold/historical/reporting use.
- **Reduce WAL volume and MVCC bloat where the write pattern coalesces** — conditional, per §1.6, not automatic.
- **Guard against stale overwrites natively,** via a `ts`-ordered upsert clause (§3.1).

### 2.1 Measuring which levers actually apply to a given workload
Before committing to a specific savings claim for an adopting workload, measure — not assume:
- **Statement-count reduction (always present):** compare per-row overhead of individual statements vs. batched upserts.
- **Write-key cardinality ratio (workload-dependent):** unique keys per flush window ÷ total writes per flush window.
- **Bloat and WAL proxies:** `pg_stat_user_tables.n_dead_tup`, autovacuum activity, `pg_table_size` growth rate, and WAL generation via `pg_stat_wal`.

---

## 3. Architecture & Implementation Details

### 3.1 The Write Path (Sink)

Two modes, selectable per-namespace via the builder:

**Mode A — Bulk Upsert (default).** `INSERT INTO ... VALUES (...), (...), ... ON CONFLICT (pk) DO UPDATE SET ...` with prepared-statement caching. `MaxBatchSize` default 1000, typically tuned 200–500, bounded by lock hold time, statement memory, and WAL batch size (not the 65,535 parameter protocol ceiling).

```sql
INSERT INTO nudge_inventory (correlation_key, payload, channel, priority, ts, updated_at)
VALUES ($1, $2, $3, $4, $5, $6), ($7, $8, $9, $10, $11, $12), ...
ON CONFLICT (correlation_key) DO UPDATE SET
    payload    = EXCLUDED.payload,
    channel    = EXCLUDED.channel,
    priority   = EXCLUDED.priority,
    ts         = EXCLUDED.ts,
    updated_at = EXCLUDED.updated_at
WHERE EXCLUDED.ts > nudge_inventory.ts
```

> **Timestamp contract (canonical definition).** `ts` is **milliseconds since the Unix epoch**, assigned once at journal-write time in `internal/shield` (`float64(time.Now().UnixMilli())`). The same value serves four roles: the dirty-set `ZADD` score, the payload hash's `ts` field, this adapter's ordering-guard column, and the L1 version for version-checked puts. Tie-break semantics: the guard is strict `>`, so same-millisecond writes landing in different flush windows tie, and the resident row wins; sub-millisecond end-to-end ordering is undefined by design.

**Mode B — `COPY` Append (opt-in, insert-only contracts).** Uses `pgx.CopyFrom`, bypassing the planner/parser entirely. Restricted to contracts that guarantee key uniqueness upstream.

> **`COPY` retry safety:** The adapter must `COPY` into an **unlogged staging table**, then move rows into the target via `INSERT ... SELECT ... ON CONFLICT DO NOTHING`. The staging table **must be `TRUNCATE`d at the start of every attempt**. Keys remain in the Redis dirty set until `CommitKeys` runs after the confirmed move, making this crash-safe.

**Connection handling.** `pgxpool.Pool` with `MinConns=5`, `MaxConns=30`, `MaxConnLifetime=30m`, `HealthCheckPeriod=30s`.

**Throttling & Backoff — permanent vs. transient error routing:**

| Postgres error | Class | Routing |
|---|---|---|
| `23505` unique_violation | permanent | DLQ |
| `23502` not_null_violation | permanent | DLQ |
| `23514` check_violation | permanent | DLQ |
| `42P01` undefined_table / `42703` undefined_column | permanent | DLQ + alert (schema drift) |
| `22xxx` data_exception | permanent | DLQ |
| `57014` statement_timeout · `53200` out_of_memory · `53100` out_of_shared_memory | transient | **Batch axis**: halve effective batch (floor 50), additive +10% per success |
| `53300` too_many_connections | transient | **Concurrency axis**: halve in-flight-flush semaphore (floor 2), additive recovery; batch untouched |
| `57P03` cannot_connect_now · `55P03` lock_not_available · `40001` serialization_failure · `40P01` deadlock_detected | transient | **Pure backoff** with jitter; neither axis adjusted |

The adapter wraps these in `SinkError{Code: pgerrcode, Err: ...}` so the engine's existing DLQ routing works unchanged. A batch failing repeatedly on a permanent error is split via binary search to isolate the offending row(s) rather than blocking the whole batch indefinitely.

**Payload size.** Postgres's practical row limit (~1GB before TOAST dominates, TOAST threshold ~8KB) is far looser than DynamoDB's 400KB. The adapter logs a warning above 64KB and routes payloads above 1MB to the DLQ with `ErrPayloadTooLarge` — a pragmatic guardrail tuned to OLTP row-width norms, not a hard architectural constraint.

### 3.2 The Read Path (Source)

`SELECT payload FROM <table> WHERE correlation_key = $1` via a prepared statement on a pooled connection.

> **Runtime enforcement, not config convention.** The adapter probes `SELECT pg_is_in_recovery()` at two points: (a) once at `Build` as a fail-fast check, and (b) in pgxpool's `AfterConnect` hook, so **every physical connection is verified at creation** — including connections grown or recycled (`MaxConnLifetime=30m`) long after `Build`. A connection failing the hook is discarded before entering the pool unless the explicit `ReplicaMode: sync` override is set. Two caveats remain honest limits: a standby cannot self-report sync vs async, so the override is an operator attestation; and behind a PgBouncer **transaction-mode** pooler the probe attests nothing about later transactions, because server backends rotate per transaction — for read paths behind transaction-mode poolers, enforcement lives in the pooler's server DSN (point it at the primary) plus operator attestation, and session-mode pooling or a direct DSN is recommended wherever the probe is meant to be the guarantee. The probe works unchanged on Aurora readers, where `pg_is_in_recovery()` also returns true.

`pgx.ErrNoRows` maps to `source.ErrRecordNotFound`, re-mapped by `sluice` to the public `sluice.ErrRecordNotFound`.

**Hot/Cold regime:** unchanged — the adapter is regime-agnostic; `sluice`'s core engine routes hot keys to Redis (<1ms) and cold keys to the `ReadContract` fallback (typically 1–5ms warm cache, 10–50ms cold).

### 3.3 Schema Strategy & Provisioning Boundary

| Mode | Schema | Use when |
|---|---|---|
| Opaque JSONB | `correlation_key TEXT PK, payload JSONB, ts BIGINT, updated_at TIMESTAMPTZ` | Payloads are opaque, schema evolves frequently. |
| Projected columns | Native columns for indexed fields + `payload JSONB` for the rest | SQL-level reporting or joins on cold data are needed. |

Projected columns are populated by an optional `PostgresProjector` on the `WriteContract`:

```go
type PostgresProjector func(correlationKey string, payload []byte) (map[string]any, error)
```

This is distinct from `IndexContract` (writes to Redis, serves live-state hot-path queries) — `PostgresProjector` writes to Postgres columns for cold historical queries and SQL reporting. A contract can implement both. Note the two mechanisms currently must be kept aligned by convention (§6 flags this as a follow-up consolidation candidate, tracked in Issue #9).

> **Strict Schema Provisioning Boundary (Resolved in R5).** `sluice` **will never** auto-create tables, emit DDL, manage migrations, or govern technical primitives related to schema evolution. The library cannot and should not aim to solve fundamental database migration problems.
>
> The `WriteContract`, `ReadContract`, `IndexContract`, and `PostgresProjector` define the *contractual shape* of the data `sluice` expects to interact with. It is strictly the operator's responsibility to provision the target table, indexes, and partitions using standard migration tooling (e.g., `goose`, `golang-migrate`, Flyway) before `sluice` begins flushing. If the physical database schema drifts from the contracts (e.g., a missing column), `sluice` will fail fast with a permanent error (`42P01` / `42703`) and route the batch to the DLQ.

### 3.4 Bypassing Heavy Indexes via Redis
For live-state compound queries ("all active users in campaign X with priority ≥ 3"), `WithIndexContract` keeps indexes in Redis (`SINTER`/`ZRANGE`). This is a stronger argument than the DynamoDB GSI case, because Postgres index maintenance is synchronous and transactional — removing an index removes real write-path lock contention, not just an async-replication side effect. Postgres indexes are then reserved for: cold historical queries older than `ActivityWindow`, SQL reporting/ad-hoc analytics, foreign-key relationships, and time-based partition pruning (BRIN).

**Index durability guard,** identical caveat to the DynamoDB adapter (`hybrid-dynamo.md` §3.3): these Redis indexes are never persisted to Postgres. If Redis loses this state before a key goes cold, the base row is still reachable by primary key (if flushed or DLQ'd), but any workflow depending on the secondary index for correctness — not just performance — needs a reconciliation job, not an assumption.

### 3.5 Capacity Smoothing & Cost Model

| Effect | Driver | Applies when |
|---|---|---|
| Fewer connections held | Fixed flusher pool replacing N direct writers | Always |
| Fewer round trips / fsyncs / parser-planner passes | Batched multi-row upsert or `COPY` | Always |
| Lower WAL volume | Batching (statement overhead) — always; coalescing (fewer physical rows) — conditional | Batching: always. Coalescing: only when write pattern has low key cardinality |
| Reduced MVCC bloat / autovacuum pressure | Coalescing (fewer dead tuples per key over time) | Only when write pattern coalesces |
| Zero synchronous secondary-index write latency on hot path | Redis-side indexing replaces Postgres indexes | Always |
| Native stale-overwrite protection | `WHERE excluded.ts > t.ts` on upsert | Always (Mode A) |
| Instance downsizing headroom | Statement-count + connection-pressure relief, amplified further by coalescing | Connection/CPU relief: always. Full downsizing: depends on coalescing ratio + IOPS profile |

| Scenario | Without `sluice` | With `sluice` — statement/connection view (always true) | With `sluice` — row/WAL view (coalescing-dependent) |
|---|---|---|---|
| Ingestion rate | 100K writes/sec | 100K writes to Redis (sub-ms) | 100K writes to Redis (sub-ms) |
| Postgres statements/sec | 100K | ~100–200 | ~100–200 |
| Required connection pool | 200+ | 10–30 | 10–30 |
| Postgres rows written/sec | 100K | 100K (high-cardinality) or ≈ unique-key count (coalescing) | ≈ unique-key count only if the workload actually coalesces |
| Index-tuple writes/sec (5 indexes) | 500K | 500K unless coalescing | ≈ 5× unique-key count, coalescing-dependent |
| WAL generation | high, proportional to row count | reduced overhead component only, unless coalescing | reduced proportionally to unique-key count, coalescing-dependent |

The row/WAL column is a hypothesis until measured per §2.1 for the specific workload — this table intentionally splits what both prior drafts blended into one "Nx reduction" narrative.

### 3.6 Observability
Adapter metrics live on an **adapter-local recorder interface**, not the core `MetricsRecorder`, to avoid another breaking change to the shared interface (see the interface-freeze discussion in `local-journal.md` §9). Minimum set, with stable names for dashboards:

- **pgxpool acquire-wait p99** — earliest warning of connection-pressure return.
- **Flush batch-size histogram** and **per-band effective batch gauge** — makes the batch-axis shrink/recover cycle (§3.1) visible.
- **Effective in-flight flush concurrency gauge** — makes the concurrency-axis shrink/recover cycle (§3.1) visible.
- **Retry counts by pg error class** (permanent vs transient) — distinguishes poison from pressure.
- **DLQ inflow rate tagged by SQLSTATE** — e.g. a `23505` spike vs a `22xxx` spike mean different things.
- **Mode A vs Mode B flush counters** and, for Mode B, **staging rows-moved and move duration** per attempt.
- **Benchmark-only bloat proxies** (§2.1): `n_dead_tup`, dead/live ratio, `pg_table_size` growth rate, WAL rate via `pg_stat_wal` / LSN deltas.

---

## 4. CAP Theorem Positioning

| Path | CAP stance | Rationale |
|---|---|---|
| Write path | AP | Redis absorbs writes, acknowledges immediately; Postgres updates asynchronously. Partition/failover holds state in the journal or DLQ; ingestion never blocks on Postgres availability. |
| Cold read path | CP | Postgres `SELECT` against the primary (or sync replica only, per §3.2) is strongly consistent by default — no `ConsistentRead`-style flag needed, but the primary-only constraint is enforced at runtime via the `pg_is_in_recovery()` probe, not assumed. |

---

## 5. Alternatives Considered

| Alternative | Why it was rejected |
|---|---|
| Native Postgres + direct writes | Connection pool saturates at high TPS; WAL and synchronous index amplification push CPU saturation; requires oversizing the instance for peak. |
| Postgres + pgBouncer only | Multiplexes connections but doesn't coalesce or batch writes — parser/planner and WAL-record volume per row is unchanged. |
| Logical replication to a read replica | Addresses read fan-out, not write-path pressure; reintroduces the replication-lag consistency risk this RFC explicitly guards against for cold reads (§1.7, §3.2). |
| `COPY` for all writes | No `ON CONFLICT` support — unusable for upsert-majority workloads. Retained as an opt-in mode for genuinely append-only contracts, with the retry-duplication guard in §3.1. |
| `LISTEN / NOTIFY` for change propagation | Adds operational complexity without addressing write-rate pressure; downstream consumers should read the Redis journal or use CDC (Debezium), not `NOTIFY`. |
| Sharding the Postgres write path (Citus, manual sharding) | Solves horizontal write scaling but is a much larger operational lift, doesn't address MVCC bloat or index contention per shard, and doesn't reduce total write volume — orthogonal to, not a substitute for, what `sluice` provides. |
| Eventual consistency for cold reads | Rejected — Postgres is strongly consistent by default at negligible cost; no reason to trade it away, and reporting/audit consumers depend on it. |

---

## 6. Consequences

### Positive
- **Connection-pressure relief is unconditional** — the single hardest, most immediate Postgres ceiling, solved regardless of write pattern.
- **Statement-count and transaction-overhead relief is unconditional** — batching wins independent of coalescing ratio.
- **Synchronous index write-path relief is unconditional,** and a stronger case than the DynamoDB GSI argument.
- **Native, near-free ordering guard,** simpler than the DynamoDB adapter's bolted-on retry path — with its millisecond granularity now pinned to source rather than implied.
- **Runtime replica probe (R3/R4)** turns the most likely future consistency accident — cold reads drifting onto a standby — into a startup/connection failure instead of a silent staleness bug, and now covers connection recycling, not just the first connection.
- **`COPY` mode gives append-only contracts an additional 5–10x,** with retry-duplication handled explicitly and its crash-safety argument grounded in the two-phase commit.
- **Where the write pattern coalesces:** WAL volume, bloat, and index-tuple-write reduction all scale down toward unique-key count, potentially enabling real instance downsizing — but this must be measured, not assumed (§2.1).
- **Rich cold-query surface** — SQL, joins, window functions, BRIN partition pruning — available without hydrating JSONB in application code, if the projected-columns schema mode is used.
- **Strict schema boundary** keeps `sluice` focused on data transport, avoiding the trap of becoming a shadow ORM/migration tool.

### Negative / Trade-offs
- **Operational complexity:** Redis becomes a critical stateful dependency alongside Postgres.
- **Degraded-mode risk is real, not just a parenthetical:** if Redis is unavailable and `sluice` falls back to direct writes, that fallback can reproduce the exact connection-storm and bloat problems this adapter exists to prevent. The degraded-mode write path needs its own design and load-testing — batching/backpressure behavior under Redis-down conditions, not just "falls back to direct writes" as a one-line mitigation.
- **Cold-read latency higher than Redis** (1–50ms vs. <1ms) — acceptable for historical data, and only valid if reads stay pinned to the primary (§3.2).
- **Schema drift risk** between `IndexContract` (Redis) and `PostgresProjector` (Postgres columns) — both project fields from the same payload but must be kept aligned by convention until Issue #9 lands; operators must also keep the physical schema aligned with the contracts manually.
- **Index durability is Redis-only** unless a reconciliation job is built (§3.4).
- **`COPY` mode's retry-duplication risk** requires one of the two patterns in §3.1 to be shipped by default, not left as an integration detail for adopting teams to discover the hard way.
- **Shared Redis capacity risk:** this adapter's index writes add load to the same Redis journal the Local Journal RFC's broadcast stream writes to, and potentially the DynamoDB adapter's index writes too if a namespace runs more than one adapter. Combined-load validation applies here as much as there.
- **Per-band adaptive state (R3/R4)** adds a small amount of mutable adapter state (effective batch per band + in-flight semaphore); it is bounded, gauged (§3.6), and reset on restart — noted for completeness, not as a material risk.

---

## 7. Action Items & Next Steps

- [ ] **Gate:** measure statement/connection reduction, write-key cardinality ratio, **and the §2.1 bloat/WAL proxies** for the first candidate workload before writing adapter code. Publish all numbers separately.
- [ ] Create `adapter/postgres` with `sink/postgres` and `source/postgres` sub-packages, reusing `WriteContract`/`ReadContract`/`WithIndexContract` for structural consistency with `adapter/dynamodb`.
- [ ] Implement `PostgresSink` — Mode A (bulk upsert with the `ts`-ordering `WHERE` clause) and Mode B (`COPY`, with the staging-table-or-`ON CONFLICT DO NOTHING` retry guard, **staging `TRUNCATE`d per attempt**) — with prepared-statement caching via `pgx`.
- [ ] Implement `PostgresSource` with **both** defenses from §3.2: config-level rejection of async replicas **and** the `pg_is_in_recovery()` runtime probe wired into `pgxpool.AfterConnect` (not just `Build`), with the explicit `ReplicaMode: sync` override; plus `ErrRecordNotFound` mapping. Document the PgBouncer transaction-mode caveat.
- [ ] Implement the permanent/transient error-mapping table (§3.1), extend the engine's permanent-failure predicate, **and implement the two-axis adaptive backpressure** (batch axis, concurrency axis via internal semaphore, pure-backoff class).
- [ ] **Pin the Timestamp Contract (§3.1):** add the regression test asserting `DrainBand` scores equal `UnixMilli()` at write time, and extend `shield.ReadWithTTL` to return `ts` (shared with the L1 Phase 2 dependency).
- [ ] Implement `PostgresProjector` for the projected-column schema mode alongside opaque JSONB.
- [ ] Implement the adapter-local observability recorder per §3.6, with stable metric names.
- [ ] **Document the strict schema boundary:** provide example migration snippets (e.g., `goose` or `golang-migrate` SQL files) for both the Opaque JSONB and Projected Column modes, explicitly stating that `sluice` does not execute them.
- [ ] Design and load-test the degraded-mode (Redis-down) fallback path specifically for connection and bloat behavior — not a one-line mitigation.
- [ ] Add integration tests using `testcontainers-go` with Postgres 16, including: out-of-order-flush regression test, **same-millisecond tie test (resident row wins)**, poisoned-batch isolation test, `COPY`-mode retry-duplication test, **and a crash-between-`COPY`-and-move replay test proving journal recovery**.
- [ ] Publish a benchmark at 10K/50K/100K TPS reporting, as separate numbers: statement count, connection pool utilization, row count written, WAL generation rate, **`n_dead_tup` / bloat growth**, index-tuple writes, instance CPU, p99 write latency, p99 cold-read latency — against the measured cardinality ratio from item 1.
- [ ] Document the `COPY` append-only mode with a dedicated example (`examples/postgres_audit_log/main.go`).
- [ ] Add a "PostgreSQL vs. DynamoDB vs. DocumentDB adapter selection guide" to the README.
- [ ] Cross-check combined Redis load with the Local Journal broadcast stream and the DynamoDB adapter's index writes before enabling more than one adapter against the same namespace at production scale.

---

## 8. Resolved Design Decisions (formerly Open Questions)

The following architectural boundaries were debated during the RFC process and are now resolved as firm design constraints for this adapter:

1. **Schema Management & DDL (Rejected).** `sluice` will never auto-create tables, emit DDL, or manage migrations. Schema evolution is strictly the operator's responsibility using standard migration tooling. The `WriteContract`, `ReadContract`, and `PostgresProjector` define the *contractual shape* of the data, but if the database schema drifts, `sluice` will fail fast with a permanent error (`42P01` / `42703`) and route to the DLQ. The library cannot and should not aim to solve fundamental database migration problems.
2. **Partitioning (Delegated to Operator).** Time-range or declarative partitioning (e.g., by `updated_at`) is strictly left to the operator. `sluice` treats the target table as an opaque sink/source and is entirely agnostic to underlying partition routing or DDL.
3. **`ProjectionContract` Unification (Deferred to Issue #9).** The unification of `IndexContract` (Redis) and `PostgresProjector` (Postgres columns) into a single `ProjectionContract` that fans out to both stores is tracked as a dedicated follow-up RFC ([Issue #9](https://github.com/hussainpithawala/sluice-go/issues/9)). Until that RFC lands, they remain distinct interfaces that must be aligned by convention.
4. **CDC Integration (Permanently Rejected).** Publishing flush completions to a logical replication slot or integrating with CDC tools (like Debezium) is permanently out of scope for `sluice` in all future versions. Downstream consumers requiring change data capture should consume directly from the backing store's native CDC mechanisms or the upstream event stream.
5. **Monotonic Ordering Sequence (Deferred).** Replacing the wall-clock millisecond `ts` with a monotonic sequence (HLC or counter) remains out of scope. It will only be revisited if a production workload demonstrates a concrete need for sub-millisecond cross-window ordering that cannot be solved at the application layer.

---

## 9. References

- [sluice-go DynamoDB adapter RFC (#8)](https://github.com/hussainpithawala/sluice-go/issues/8)
- [ProjectionContract Unification RFC (#9)](https://github.com/hussainpithawala/sluice-go/issues/9)
- [local-journal.md — L1 read tier RFC](https://github.com/hussainpithawala/sluice-go)
- [pgx v5 — `CopyFrom` and prepared-statement caching](https://github.com/jackc/pgx)
- [PostgreSQL `INSERT ... ON CONFLICT`](https://www.postgresql.org/docs/current/sql-insert.html#SQL-ON-CONFLICT)
- [PostgreSQL error codes](https://www.postgresql.org/docs/current/errcodes-appendix.html)
- [pgBouncer transaction mode semantics](https://www.pgbouncer.org/features.html)
- [TOAST and large-row behavior](https://www.postgresql.org/docs/current/storage-toast.html)
- [PostgreSQL message formats — Parse/Bind parameter count limit (Int16, 65,535)](https://www.postgresql.org/docs/current/protocol-message-formats.html)
- [`pg_is_in_recovery()` — recovery control functions](https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-RECOVERY)
- [`pgxpool.Config.AfterConnect` hook](https://pkg.go.dev/github.com/jackc/pgx/v5/pgxpool#Config)
- [Monitoring statistics — `pg_stat_wal`, `pg_stat_user_tables`](https://www.postgresql.org/docs/current/monitoring-stats.html)

---

*Revision 2 (2026-09-19) merged two independently drafted proposals. Revision 3 applied seven peer-review edits. Revision 4 applied three further review edits (`AfterConnect` probe lifecycle, source-verified Timestamp Contract, two-axis adaptive backpressure). **Revision 5 (2026-09-19) resolves all open questions into firm design boundaries: auto-DDL/migration management rejected, CDC integration permanently out of scope, partitioning delegated to the operator, projection unification deferred to Issue #9, monotonic ordering deferred.** Ready for implementation gating.*