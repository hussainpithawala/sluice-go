# sluice

[![Release](https://github.com/hussainpithawala/sluice-go/actions/workflows/release.yml/badge.svg)](https://github.com/hussainpithawala/sluice-go/actions/workflows/release.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/hussainpithawala/sluice-go.svg)](https://pkg.go.dev/github.com/hussainpithawala/sluice-go)
[![Go Version](https://img.shields.io/badge/go-1.25-blue)](https://go.dev)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> **A volume scaling platform for your datastore — not just a write shield.**
>
> `sluice` sits in front of any backing store and absorbs bursty, uncoordinated writes in Redis, draining
> them as a handful of efficient batches so no single store primary ever feels 100K individual writes/sec.
> Active correlation keys live in that same journal as a **hot read/write path** — read and written back
> in sub-millisecond time without touching the store at all — and an internal broadcast pushes the
> hottest keys further still, into an in-process **L1 Local Journal** (v1.0.8), for sub-microsecond reads
> kept convergent across pods via Redis Streams. Set-based **bulk reads** (`ReadBulk`) collapse N+1 lookups
> into a single store query and hydrate the whole working set into the journal in one Redis pipeline.
>
> One mechanism, scaling write *and* read volume for whatever sits behind it. Every operation coordinates
> through the same L1/L2 state plane, so a single build can run as a full writer/reader pod, a reader-only
> API gateway, or a bulk pre-warmer — just by changing which contracts you configure. `sluice` is datastore-agnostic
> by design, with first-class adapters for **AWS DocumentDB / MongoDB**, **AWS DynamoDB**, and
> **PostgreSQL** — each exploiting the journal to dissolve that store's specific bottleneck: index I/O,
> WCU pricing, or connection/transaction overhead.

---

## The problem

Modern ad-roll platforms process inventory update events at 10K–100K TPS from SQS queues and Kafka
topics. Each event is a single incoherent write — one document, one customer, one update — arriving
unbatched and uncoordinated. Sending each directly to the DataStore means:

- Every write hits the single primary node individually
- Every index multiplies the I/O cost per write
- Connection pools saturate under spikes (many DataStores hard-cap connections per instance class)
- 100,000 events for 80,000 unique customers becomes 100,000 individual round-trips instead of ~100 BulkWrite calls

```mermaid
flowchart LR
    SQS([SQS Queue])
    KAF([Kafka Topics])
    DB[(DataStore)]

    SQS -->|individual write per event| DB
    KAF -->|individual write per event| DB

    style DB fill:#FAECE7,stroke:#993C1D,color:#712B13
```

---

## How sluice solves it

sluice introduces a **write journal in Redis** between your consumers and the DataStore.
Every event is written atomically to Redis — sub-millisecond — and acknowledged immediately.
Background goroutines drain the journal in configurable windows and assemble efficient `BulkWrite` calls.

**100,000 events/sec → ~100 BulkWrite calls/sec — a 1,000× reduction in DataStore I/O.**

```mermaid
flowchart TD
    SQS(["SQS Queue"])
    KAF(["Kafka Topics — partitioned by correlation_key hash"])
    CON["Consumer Workers — stateless, correlation_key-hash routed"]
    RED[("Redis Cluster — Velocity Shield")]
    ENG["Batch Flusher Engine — 16 bands, 250ms window"]
    DB[("DataStore — Long-Term Store")]

    SQS --> CON
    KAF --> CON
    CON -->|"sluice.Write — returns immediately"| RED
    RED -->|drain dirty bands async| ENG
    ENG -->|"BulkWrite — one call per band per window"| DB

    style RED fill:#FAEEDA,stroke:#BA7517,color:#633806
    style ENG fill:#E1F5EE,stroke:#0F6E56,color:#085041
    style DB  fill:#FAECE7,stroke:#993C1D,color:#712B13
```

---

## Redis as a state journal

The core innovation is treating Redis not as a cache but as a **durable write journal with
correlation-key deduplication** — and, for keys under active traffic, as the authoritative
read surface until the next flush lands:

| Traditional cache | sluice Redis journal |
|---|---|
| Read-optimised — avoids re-computation | Write-optimised — absorbs write velocity, then serves reads off the absorbed state |
| TTL = staleness budget for reads | TTL = crash-recovery safety net, extended to a session window for hot keys |
| Cache miss → go to DB | Journal miss → key already flushed, so the DataStore is authoritative |
| Eviction under pressure loses data | Deduplication under pressure is intentional |

For every event, sluice executes a single atomic Lua script — three Redis operations in one round-trip:

```mermaid
sequenceDiagram
    participant C as Consumer worker
    participant R as Redis Lua script
    participant DS as Dirty sorted set

    C->>R: EVALSHA atomicWrite(corrKey, payload, ts, ttl)
    activate R
    R->>R: HSET sl:ns:payload:{band}:corrKey  p=payload ts=ts
    R->>R: EXPIRE sl:ns:payload:{band}:corrKey ttl
    R->>DS: ZADD sl:ns:dirty:{band} score=ts member=corrKey
    R-->>C: 1 ACK
    deactivate R

    Note over C,DS: Single round-trip. Atomic. No partial state possible.
```

With `WithContentDedup(true)` the script also stores and compares an `h` content-hash field, and
returns `0` instead of `1` when the payload is unchanged — see
[Content deduplication](#content-deduplication).

The `ZADD` score is the event timestamp — the flush engine always processes the oldest keys first,
giving natural ordering and a staleness bound equal to `FlushWindow`.

---

## The coalescing mechanism

Because `ZADD` on an existing member only updates the score, multiple events for the same correlation_key
collapse to **one dirty-set entry**. The `HSET` keeps the latest payload.

```mermaid
flowchart LR
    subgraph IN ["Events arriving (100K/sec)"]
        E1["correlationKey_001 event 1"]
        E2["correlationKey_002 event 1"]
        E3["correlationKey_001 event 2"]
        E4["correlationKey_003 event 1"]
        E5["correlationKey_001 event 3"]
        E6["correlationKey_002 event 2"]
    end

    subgraph RDS ["Redis dirty set after 250ms"]
        D1["correlationKey_001 — score = latest ts"]
        D2["correlationKey_002 — score = latest ts"]
        D3["correlationKey_003 — score = ts"]
    end

    subgraph BW ["DataStore BulkWrite — 1 call"]
        B1[upsert correlationKey_001]
        B2[upsert correlationKey_002]
        B3[upsert correlationKey_003]
    end

    E1 --> D1
    E2 --> D2
    E3 --> D1
    E4 --> D3
    E5 --> D1
    E6 --> D2
    D1 --> B1
    D2 --> B2
    D3 --> B3

    style D1 fill:#FAEEDA,stroke:#BA7517,color:#633806
    style D2 fill:#FAEEDA,stroke:#BA7517,color:#633806
    style D3 fill:#FAEEDA,stroke:#BA7517,color:#633806
```

6 events → 3 dirty keys → **1 BulkWrite call** with 3 upserts.

---

## The flush engine — four triggers

One goroutine per band wakes on four independent triggers, whichever fires first:

```mermaid
stateDiagram-v2
    [*] --> Idle
    Idle --> Draining : time trigger fires — 250ms elapsed
    Idle --> Draining : volume trigger fires — dirty queue >= MaxBatchSize
    Idle --> Draining : pre-eviction trigger — oldest dirty key nears KeyTTL
    Idle --> Draining : hot trigger — Write() on a hot correlation_key
    Draining --> Reading : ZRANGEBYSCORE fetch dirty keys
    Reading --> Fetching : pipeline HMGET read all payloads in one round-trip
    Fetching --> Building : apply WriteContract per key
    Building --> Writing : BulkWrite to DataStore ordered=false
    Writing --> Cleanup : ZREM confirmed keys from dirty set
    Cleanup --> Refresh : HotAwareFlush — extend TTL on committed keys
    Refresh --> Idle : goroutine sleeps until next trigger
```

| Trigger | Fires when | Purpose |
|---|---|---|
| Time | `FlushWindow` elapses | Caps DataStore staleness |
| Volume | Dirty queue depth ≥ `MaxBatchSize` | Prevents Redis memory pressure during spikes |
| Pre-eviction | Oldest dirty key is within 5s of `KeyTTL` expiry | Prevents silent data loss if a payload would expire before being flushed |
| Hot | `Write()` lands on a correlation_key with an active hot marker (requires `HotAwareFlush`) | Keeps the store current for keys being read synchronously |

The pre-eviction flusher runs on its own goroutine, polling every `KeyTTL / 2` (minimum 1s) and
signalling any band whose oldest dirty key is approaching expiry. It is disabled if `KeyTTL <= 0`.

---

## Exactly-once logical application through idempotent correlation-key materialization

```mermaid
sequenceDiagram
    participant E as Flush engine
    participant R as Redis
    participant D as DataStore

    E->>R: ZRANGEBYSCORE dirty:band LIMIT 0 1000
    R-->>E: correlationKey_001, correlationKey_002, correlationKey_003 ...

    E->>R: pipeline HMGET payload for each key
    R-->>E: payload_001, payload_002, payload_003 ...

    E->>D: BulkWrite — upsert correlationKey_001, upsert correlationKey_002, upsert correlationKey_003
    D-->>E: upsertedCount 3

    E->>R: ZREM dirty:band correlationKey_001 correlationKey_002 correlationKey_003
    R-->>E: 3

    Note over E,R: ZREM happens AFTER confirmed BulkWrite.
    Note over E,R: Crash between BulkWrite and ZREM triggers re-flush.
    Note over E,D: Upsert semantics make re-flush safe — last write wins.
```

Keys are removed from the dirty set only after the DataStore confirms the write. If the flusher crashes
after `BulkWrite` but before `ZREM`, those keys are re-flushed on the next cycle. Because every sink
operation is an upsert, re-flushing is always safe.

---

## Hot/Cold regime — the read path

A wide-breadth stream is not uniformly wide. At any moment a small subset of correlation keys is
*active* — a user is logged in, a session is open, an HTTP handler is about to read the same key it
just wrote. sluice keeps those keys resident in the journal and serves them without touching the
DataStore.

| | Cold correlation_key | Hot correlation_key |
|---|---|---|
| Definition | No payload resident in the journal | Payload resident, with an active hot marker |
| `Read()` | Falls back to `Source` via `ReadContract` | Sub-millisecond Redis `HGET`, with lazy TTL refresh |
| `Write()` | Coalesces; flushed on time / volume / pre-eviction trigger | Also signals an immediate flush |
| Journal residency | `KeyTTL` (30s) until flushed | `ActivityWindow` (4h), refreshed by reads and by flush |
| Marker | none | `sl:{ns}:hot:{band}:{corrKey}`, set by `HotLoad` |

```mermaid
sequenceDiagram
    participant A as Application
    participant S as sluice
    participant R as Redis journal
    participant D as DataStore

    Note over A,D: User logs in — promote the key to hot (fire-and-forget)
    A->>S: HotLoad(ctx, correlationKey)
    S->>R: SET hot marker (TTL = ActivityWindow)
    S-->>A: nil — returns immediately
    par background hydration
        S->>D: ReadContract → Source.Read
        D-->>S: record bytes
        S->>R: HSET payload (+ L1 Put, + IndexContract)
    end

    Note over A,D: Session traffic — DataStore is never read
    A->>S: Write(ctx, correlationKey, payload)
    S->>R: atomic HSET + ZADD
    S->>S: key is hot → signal immediate flush
    A->>S: Read(ctx, correlationKey)
    S->>R: pipeline HGET + PTTL
    R-->>S: payload, remaining TTL
    S-->>A: payload (sub-ms)

    Note over A,D: Key not in the journal — cold fallback
    A->>S: Read(ctx, coldKey)
    S->>R: pipeline HGET + PTTL
    R-->>S: nil
    S->>D: ReadContract → Source.Read
    D-->>S: document bytes
    S-->>A: payload (or ErrRecordNotFound)
    S--)R: async read-through hydration (L2 + L1 + indexes)
```

### API

```go
// Promote a correlation_key to hot — typically on user login.
// Sets the hot marker synchronously and hydrates the payload in the background.
// HotLoad is a command, not a query: it returns only an error.
err := s.HotLoad(ctx, correlationKey)

// Read current state. Hot → Redis. Cold → Source. Neither → ErrRecordNotFound.
payload, err := s.Read(ctx, correlationKey)

// Reports whether the correlation_key currently has a payload resident in the journal.
hot, err := s.IsHot(ctx, correlationKey)
```

> **With the L1 cache enabled (v1.0.8),** `Read()` consults the in-process [L1 Local Journal](#l1-local-journal)
> before Redis and may return data up to `LocalTTL` stale. Use `ReadFresh()` for reads that must hit the
> authoritative journal.

> **Breaking change (v1.0.8):** `HotLoad` previously returned `([]byte, error)` and blocked on the
> cold read. It now returns `error` only and never blocks on the store — callers that need the payload
> immediately should call `Read()` after `HotLoad()`. If background hydration fails, the next `Read()`
> simply takes the cold path.

### Asynchronous read-through hydration

When `Read`, `ReadFresh`, or `ReadOld` misses the journal and falls back to the `Source`, the payload
is returned to the caller immediately and a background goroutine hydrates the Redis journal (L2), the
L1 cache (`Read`/`ReadOld` only — `ReadFresh` never touches L1), and the secondary indexes via
`IndexContract`. The next read for that key is served from the journal. Hydration is best-effort:
failures are reported through `RecordRedisOp` (`write_hydration`, `updateindexes_hydration`) and never
surface on the read path.

Hydration (here, in `HotLoad`, and in [`ReadBulk`](#bulk-reads--set-based-cache-pre-warming)) runs a
dedicated Lua script with two guarantees:

- **It only fills a journal miss.** Every write lands in the journal before the store, so an existing
  entry — pending flush, already flushed, or dead-lettered — is always at least as new as anything read
  from the store, including a `Write()` that landed while the store read was in flight. The existing
  entry wins, and only its TTL is extended (never shortened).
- **It never marks the key dirty.** Data read from the store is never flushed back to it, and
  reader-only instances (which run no flush engine) never accumulate a dirty backlog.

Hydrated entries live for `ActivityWindow`. Indexes are only updated when the store payload was
actually written; a winning journal entry was already indexed by the write path.

### TTL mechanics

- **Lazy refresh on read** — when `Read()` hits the journal and the remaining payload TTL has fallen
  below 20% of `ActivityWindow`, the hot marker and payload TTL are refreshed synchronously. Steady
  read traffic keeps a key hot indefinitely; silence lets it age out.
- **Hot-aware flush** (`WithHotAwareFlush`, **default `true`**) — after a band commits successfully,
  the payload TTL of every committed key is extended to `ActivityWindow`, and hot markers are
  re-armed for keys still marked hot. This is what stops a flush from evicting a live session.

> **Memory note:** hot-aware flush extends the TTL of *every* successfully committed key, not only
> the marked-hot ones. With the default configuration a flushed payload therefore lives for
> `ActivityWindow` (4h) rather than `KeyTTL` (30s) — recently written keys stay readable from the
> journal even without an explicit `HotLoad`, and Redis resident-set size is governed by your
> active-key count rather than by in-flight writes. For pure write-through ingest with no read path,
> set `WithHotAwareFlush(false)` to keep the 30s transit-buffer profile described in
> [Scale envelope](#scale-envelope).

`HotLoad` and the cold path of `Read` both require `WithSource()` **and** `WithReadContract()`.
Dependencies are validated at call time, not at `Build()`: `HotLoad` returns `ErrMissingSource` when
no `Source` is configured and `ErrMissingReadContract` when no `ReadContract` is configured, while a
journal miss on `Read` without either returns `ErrRecordNotFound`.

## L1 Local Journal

**Sub-microsecond reads and cross-pod convergence (v1.0.8)**

Sluice v1.0.8 introduces an optional in-process L1 cache that sits in front of the Redis journal. It
delivers sub-microsecond reads for hot correlation keys while maintaining eventual consistency across
distributed pods via a Redis Streams broadcast.

| Tier | Store | Role |
|---|---|---|
| **L1** | In-process, sharded LRU | Sub-microsecond reads; write-through on `Write()`; bounded by `LocalTTL` |
| **L2** | Redis journal | Authoritative hot state (see [Hot/Cold regime](#hotcold-regime--the-read-path)) |
| **L3** | DataStore (`Source`) | Cold reads when the key is in neither tier |

### Enabling the L1 cache

The L1 cache is **opt-in**. Enable it with the `WithLocalCache` builder option:

```go
sl, _ := sluice.New("nudge_inventory").
    WithRedis(sluice.RedisConfig{Addrs: redisAddrs, ClusterMode: true}).
    WithSink(sink).
    WithWriteContract(contract).
    // Enable L1 Local Journal
    WithLocalCache(localjournal.LocalCacheConfig{
        Mode:       localjournal.LocalCachePushPull,
        MaxEntries: 200_000,          // Global LRU cap (distributed across 256 shards)
        LocalTTL:   60 * time.Second, // Max staleness bound
        Broadcast:  shield.BroadcastPayload,
        Retention:  200_000,          // Stream MAXLEN ~ N
    }).
    Build(ctx)
```

See [Configuration → L1 Local Journal](#l1-local-journal-v108) for the full option reference.

### Architecture

The L1 Local Journal operates across three tiers. The following sequence diagrams illustrate each
critical path.

#### 1. The write path — zero-cost broadcasting

When a pod writes a payload, the L1 cache is updated immediately (write-through). Simultaneously, the
broadcast message is piggy-backed onto the **same Redis pipeline** as the journal write, so broadcasting
adds zero extra network round-trips.

```mermaid
sequenceDiagram
    participant App as Application
    participant S as Sluice (Pod A)
    participant L1 as L1 Cache
    participant R as Redis journal

    App->>S: Write(key, payload)
    S->>L1: Put(key, payload, ts) [Write-Through]

    rect rgb(240, 248, 255)
        Note over S,R: Single Redis Pipeline (1 Round-Trip)
        S->>R: EVALSHA (Journal Write)
        S->>R: XADD (Broadcast Stream)
    end

    R-->>S: OK
    S-->>App: nil (Success)
```

#### 2. Cross-pod convergence

Peer pods run a background `XREAD` subscriber. When Pod A writes, Pod B's subscriber wakes up, applies
the payload to its local L1 cache, and advances its cursor. The **version gate** ensures that
out-of-order network replays cannot overwrite newer state.

```mermaid
sequenceDiagram
    participant A as Pod A
    participant RS as Redis Stream
    participant SUB as Pod B Subscriber
    participant L1B as Pod B L1

    A->>RS: XADD (ts=100, payload)
    RS-->>SUB: XREAD (wakes up)

    SUB->>L1B: Put(key, payload, ts=100)
    Note over L1B: Version Gate:<br/>Is 100 >= existing_ts?

    alt Newer or Equal
        L1B-->>SUB: Applied (true)
    else Stale
        L1B-->>SUB: Rejected (false)
    end
```

#### 3. The read path — tiered fallback

`Read()` checks L1 first. On a hit it returns instantly. On a miss (or expiry) it falls through to the
Redis journal (L2), heals the L1 cache, and returns the payload. If L2 also misses, it falls through to
the configured `Source` (L3) for a cold read.

```mermaid
sequenceDiagram
    participant App as Application
    participant S as Sluice
    participant L1 as L1
    participant L2 as Redis journal (L2)
    participant L3 as Source (L3)

    App->>S: Read(key)
    S->>L1: Get(key)

    alt L1 Hit (Sub-µs)
        L1-->>S: payload
        S-->>App: payload
    else L1 Miss / Expired
        S->>L2: HGET (Journal L2)
        alt L2 Found
            L2-->>S: payload, ts
            S->>L1: Put(key, payload, ts) [Lazy Heal]
            S-->>App: payload
        else L2 Miss
            S->>L3: Read(key) [Cold Path]
            L3-->>S: payload
            S-->>App: payload
        end
    end
```

### Strong-consistency escape hatch

For reads that cannot tolerate bounded staleness (financial transactions, checkout flows, compliance
queries), use `ReadFresh()`. It bypasses the L1 cache entirely and reads directly from the authoritative
Redis journal, falling through to the `Source` on a journal miss — identical semantics to v1.0.7's
`Read()`.

```go
// Bypasses L1 entirely, reads from the Redis journal (L2)
fresh, err := sl.ReadFresh(ctx, correlationKey)
```

### Observability

L1 behaviour is surfaced through four additional `MetricsRecorder` methods — see
[Telemetry](#telemetry).

---

## Bulk reads — set-based cache pre-warming

**One store query, one Redis pipeline, the whole working set (v1.0.8)**

Single-key `Read()` is ideal for point lookups, but many access patterns are set-shaped — *"load all
active campaigns for this user"*, *"fetch the latest 50 transactions for this account"*. Serving those
through `ReadContract` means N separate `FindOne` / `GetItem` / `SELECT` round-trips to the store, then
N separate journal hydrations.

`ReadBulk` replaces that with a single set-based query driven by a lookup key (e.g. a `user_id`), then
hydrates the L2 journal, the secondary indexes, and the L1 cache for **every** returned item in one
pipelined pass:

```mermaid
sequenceDiagram
    participant A as Application
    participant S as sluice
    participant D as Source (L3)
    participant R as Redis journal (L2)
    participant L1 as L1 cache

    A->>S: ReadBulk(ctx, "user_123")
    S->>S: ReadBulkContract("user_123") → BulkReadModel
    S->>S: bulkTs = now (captured before the query)
    S->>D: Source.ReadBulk — one query
    D-->>S: rows → Projector → []BulkReadResult
    rect rgb(240, 248, 255)
        Note over S,R: shield.BulkHydrateJournal — one pipeline
        S->>R: hydrate script (× N): fill journal misses, keep existing entries
    end
    S->>R: shield.BulkUpdateIndexes — SADD / ZADD for hydrated items, one pipeline
    S->>L1: Put (× N, version-gated)
    S->>R: shield.BulkGetTTL — PTTL (× N), one pipeline (ReadBulkWithTTL)
    S-->>A: map[correlationKey]payload
```

### Contracts

```go
// ReadBulkContract translates a lookup key into an adapter-specific bulk execution plan.
type ReadBulkContract func(lookupKey string) (*source.BulkReadModel, error)

// IndexBulkContract extracts index fields for the whole batch at once
// (correlationKey → field → value), so every SADD/ZADD lands in one pipeline.
type IndexBulkContract func(results []source.BulkReadResult) (map[string]map[string]interface{}, error)
```

`source.BulkReadModel.Query` carries the adapter-specific plan; the adapter's `Projector` shapes the
raw rows into `[]source.BulkReadResult{CorrelationKey, Payload}`. The core never parses SQL or NoSQL —
it only sees correlation keys and `[]byte` payloads.

| Adapter | `BulkReadModel.Query` type | Native plan | Projector input |
|---|---|---|---|
| `source/postgres` | `postgres.PostgresBulkReadModel` | `Query string` + `Args []any` | `pgx.Rows` |
| `source/docdb` | `docdb.DocDBBulkReadModel` | `Filter` + `*options.FindOptions` (Sort, Limit, Projection) | `*mongo.Cursor` |
| `source/dynamodb` | `dynamodb.DynamoBulkReadModel` | `*dynamodb.QueryInput` (single page, `KeyConditionExpression` on a PK) | `[]map[string]types.AttributeValue` |

### Example (PostgreSQL)

```go
sl, _ := sluice.New("campaigns").
    WithRedis(sluice.RedisConfig{Addrs: redisAddrs}).
    WithSource(pgsource.NewSourceWithPool(pool)).
    WithReadBulkContract(func(userID string) (*source.BulkReadModel, error) {
        return &source.BulkReadModel{
            Query: pgsource.PostgresBulkReadModel{
                Query: `SELECT id, json_build_object('campaign_id', id, 'status', status, 'priority', priority)
                        FROM campaigns WHERE user_id = $1 LIMIT 50`,
                Args:  []any{userID},
                Projector: func(rows pgx.Rows) ([]source.BulkReadResult, error) {
                    var out []source.BulkReadResult
                    for rows.Next() {
                        var r source.BulkReadResult
                        if err := rows.Scan(&r.CorrelationKey, &r.Payload); err != nil {
                            return nil, err
                        }
                        out = append(out, r)
                    }
                    return out, rows.Err()
                },
            },
        }, nil
    }).
    WithIndexBulkContract(campaignIndexes). // optional — makes bulk-loaded items visible to Query()
    Build(ctx)

payloads, err := sl.ReadBulk(ctx, "user_123")              // map[string][]byte
payloads, ttls, err := sl.ReadBulkWithTTL(ctx, "user_123") // + remaining journal TTL (ms) per key

// Every item is now resident — individual reads and compound queries skip the store.
p, _ := sl.Read(ctx, "camp_3")
```

### Semantics

- **Read-your-writes** — items already in the journal (e.g. a pending `Write()`) are never overwritten
  with store data; `ReadBulk` returns the journal payload for them instead. Only journal misses are
  hydrated, and hydrated keys are never marked dirty — see
  [Asynchronous read-through hydration](#asynchronous-read-through-hydration).
- **Residency** — bulk-hydrated payloads are written with an `ActivityWindow` TTL, the same as hot keys.
- **Indexes are best-effort** — `IndexBulkContract` receives only the items that were hydrated from
  the store; a contract error is reported via `RecordContractError` and a
  pipeline failure via `RecordRedisOp("bulkupdateindexes")`; neither fails the `ReadBulk` call.
- **TTL fallback** — if the `BulkGetTTL` pipeline fails, `ReadBulkWithTTL` reports `ActivityWindow` for
  every key rather than returning an error.
- **Bound your queries** — `sluice` does not cap `Projector` output. Use `LIMIT` / `FindOptions.SetLimit`
  / `QueryInput.Limit`, and size `MaxEntries` for the L1 cache accordingly.
- An empty result set returns empty maps and touches Redis not at all.
- `ReadBulk` requires both `WithSource()` and `WithReadBulkContract()`; it returns an error if either
  is missing.

---

## Deployment topologies

`sluice` is one platform, not a set of modes. `Build()` only requires a namespace and Redis; every
other component is optional and each operation validates its own dependencies at call time. The flush
engine starts only when **both** `WithSink()` and `WithWriteContract()` are provided.

| Topology | Configuration | Use case |
|---|---|---|
| **Full pod** | `Sink`, `WriteContract`, `Source`, `ReadContract`, `ReadBulkContract` | Kafka/SQS ingestion and HTTP read APIs in one service |
| **Reader-only pod** | `Source`, `ReadContract`, `ReadBulkContract` *(no Sink)* | Stateless API gateway reading the shared journal written by full pods; falls back to the store on a miss and hydrates the journal, but never writes to the store |
| **Bulk pre-warmer** | `Source`, `ReadBulkContract`, `IndexBulkContract` | Background job that runs `ReadBulk` over active users to keep the journal hot ahead of peak traffic |
| **Write-only** | `Sink`, `WriteContract` *(no Source)* | High-velocity ingestion worker that never serves reads |

| Operation | Requires | Error when missing |
|---|---|---|
| `Write`, `WriteIdempotent`, `ProcessDLQ` | `WriteContract` + `Sink` | `ErrMissingWriteContract` / `ErrMissingSink` |
| `Read`, `ReadFresh` | Redis; `Source` + `ReadContract` only for the cold path | `ErrRecordNotFound` on a full miss |
| `ReadBulk`, `ReadBulkWithTTL` | `Source` + `ReadBulkContract` | non-sentinel error |
| `HotLoad` | `Source` + `ReadContract` | `ErrMissingSource` / `ErrMissingReadContract` |
| `Query` | `IndexContract` and/or `IndexBulkContract` to populate indexes | empty result |

`DrainAndClose` is safe on every topology — it skips the engine drain and sink close when no write
path was configured.

---

## Content deduplication

High-velocity streams frequently re-send an unchanged payload — a heartbeat, a periodic full-state
refresh, an at-least-once redelivery. `WithContentDedup(true)` fingerprints each payload with
**xxHash64** and short-circuits redundant work.

```go
s, _ := sluice.New("nudge_inventory").
    // ...
    WithContentDedup(true).
    Build(ctx)
```

The dedup path runs a single Lua script that compares the incoming hash against the `h` field stored
alongside the payload:

```mermaid
flowchart TD
    W["Write(correlationKey, payload)"]
    H["hash = xxhash64(payload)"]
    CMP{"hash == stored h?"}
    SKIP["EXPIRE payload key only<br/>return 0 — deduplicated"]
    WR["HSET p, ts, h<br/>EXPIRE<br/>ZADD dirty set<br/>return 1 — written"]
    POST["Index maintenance + volume signalling"]

    W --> H --> CMP
    CMP -->|yes| SKIP
    CMP -->|no| WR --> POST

    style SKIP fill:#E1F5EE,stroke:#0F6E56,color:#085041
    style WR fill:#FAEEDA,stroke:#BA7517,color:#633806
```

A deduplicated write refreshes the payload TTL and returns `nil` to the caller — but skips the
dirty-set insertion, index maintenance, and volume signalling entirely. The DataStore never
sees a write it already has. Disabled by default, since it costs one extra hash field per key.

---

## Secondary indexes and compound queries

`WithIndexContract` lets the caller project index fields out of each payload. sluice maintains them
in Redis as the write lands, and `Query()` answers compound lookups against the journal without
touching the DataStore.

```go
func nudgeIndexContract(correlationKey string, payload []byte) (map[string]interface{}, error) {
    var p NudgeInventoryPayload
    if err := json.Unmarshal(payload, &p); err != nil {
        return nil, err
    }
    return map[string]interface{}{
        "channel":  p.Channel,                        // string  → equality SET index
        "campaign": p.CampaignID,                     // string  → equality SET index
        "priority": float64(p.Priority),              // float64 → range ZSET index
        "expires":  float64(p.ExpiresAt.UnixMilli()), // float64 → range ZSET index
    }, nil
}
```

| Value type | Index built | Redis key |
|---|---|---|
| `string` | Equality `SET` | `sl:{ns}:idx:{band}:{field}:{value}` |
| `float64`, `int64` | Range `ZSET` (value = score) | `sl:{ns}:ridx:{band}:{field}` |
| anything else | *ignored* | — |

Indexes carry an `ActivityWindow` TTL and are written through a Go-side pipeline — no Lua, no
`cjson`, so the path is safe on Valkey as well as Redis. Index maintenance is **best-effort and
eventually consistent**: a failure is swallowed rather than failing the primary write.

```go
results, err := s.Query(ctx, sluice.Query{
    Equality: map[string]string{"channel": "push"},
    RangeMin: map[string]float64{"priority": 3},
    RangeMax: map[string]float64{"expires": float64(deadline.UnixMilli())},
})
// []QueryResult{ {CorrelationKey, Payload}, ... }
```

`Query` runs **per band** — every `SINTER` operand shares the same `{band}` hash tag, so the
intersection never crosses a cluster slot. Equality filters are intersected in Redis; range filters
are then applied with a `ZSCORE` check per candidate. At least one equality filter is required.

> `Query` sees only what is currently resident in the journal — hot keys plus in-flight writes. It is
> a live-state lookup, not a replacement for querying the DataStore.

---

## Idempotent writes

`WriteIdempotent` guards against duplicate delivery from an upstream queue using `SETNX` with a TTL:

```go
err := s.WriteIdempotent(ctx, correlationKey, payload, msg.MessageID)
if errors.Is(err, sluice.ErrDuplicateIdempotencyKey) {
    // already processed — ACK the message and move on
}
```

The idempotency key is stored as `sl:{ns}:idem:{band}:{idempotencyKey}`, sharing the correlation
key's `{band}` hash tag so it stays slot-local in cluster mode. It expires after `IdempotencyTTL`
(default 4h) — sized to your upstream's redelivery window, not to your retention needs.

---

## Degraded mode — Redis outage handling

When Redis is unavailable, sluice falls back to direct single-document writes rather than dropping data:

```mermaid
flowchart TD
    W["sluice.Write called"]
    RT{"Redis available?"}
    RS["HSET and ZADD — fast path"]
    DC{"DegradedModeDirect = true?"}
    DW["Apply WriteContract — call sink.Write directly"]
    ER["Return ErrRedisUnavailable"]
    ACK["Return nil — ACK to caller"]

    W --> RT
    RT -->|yes| RS --> ACK
    RT -->|no| DC
    DC -->|yes| DW --> ACK
    DC -->|no| ER

    style RS fill:#E1F5EE,stroke:#0F6E56,color:#085041
    style DW fill:#FAEEDA,stroke:#BA7517,color:#633806
    style ER fill:#FCEBEB,stroke:#A32D2D,color:#791F1F
```

---

## Scale envelope

| Metric | Value |
|---|---|
| Sustained ingest | 10K events/sec |
| Peak spike | 100K events/sec |
| Unique correlation_keys at peak (wide-breadth) | ~80–90K/sec |
| Redis resident keys (transit buffer) | ~25K at peak |
| DataStore BulkWrite calls/sec | ~100–130 |
| I/O reduction vs individual writes | **~1,000x** |
| Flush window (max DataStore lag) | 250ms (configurable) |
| Crash recovery | at-least-once via Redis journal |
| Hot-path `Read()` | sub-millisecond — one pipelined `HGET` + `PTTL` |
| L1-hit `Read()` (opt-in, v1.0.8) | sub-microsecond — in-process, no network |
| Cold-path `Read()` | one `Source.Read` against the DataStore; journal hydrated asynchronously |
| `ReadBulk()` (N items) | one `Source.ReadBulk` + three Redis pipelines, instead of N reads + N hydrations |
| Hot key residency | `ActivityWindow` (4h default), refreshed by read traffic |

Resident-key figures assume `WithHotAwareFlush(false)` — a pure transit buffer. With the hot regime
enabled, plan Redis memory against your concurrent active-session count instead.

---

## Architecture — full system view

```mermaid
flowchart TD
%% ============================================================
%% ASYNCHRONOUS / COLD WRITE
%% ============================================================

  subgraph ColdWrite["COLD WRITE — asynchronous ingestion"]
    direction LR

    SQS([SQS])
    KAF(["Kafka<br/>16 partitions / correlation_key"])

    CW["Consumer Workers"]

    SQS --> CW
    KAF --> CW
  end


%% ============================================================
%% SYNCHRONOUS / HOT WRITE
%% ============================================================

  subgraph HotWrite["HOT WRITE — synchronous API"]
    direction LR

    API["Application / HTTP API"]

    HW["Write()<br/>synchronous journal update"]

    API -->|"hot write"| HW
  end


%% ============================================================
%% SLUICE
%% ============================================================

  subgraph Sluice["SLUICE"]
    direction TB

    subgraph WriteBuffer["WRITE BUFFER"]
      direction LR

      W["Write()"]
      BC["In-memory Channel<br/>burst absorption"]
      BP["Pipeline Flush<br/>EVALSHA × N"]

      W --> BC
      BC --> BP
    end


    subgraph Journal["REDIS STATE JOURNAL"]
      direction TB

      PAYLOAD["Payload<br/>HSET payload / timestamp / hash"]

      DIRTY["Dirty Queue<br/>ZSET · 16 bands"]

      HOT["Hot Marker<br/>TTL = ActivityWindow"]

      INDEX["Secondary Indexes<br/>SET + ZSET"]

      DLQ["Dead Letter<br/>ZSET · TTL 7 days"]

      PAYLOAD --> DIRTY
      PAYLOAD --> INDEX
    end


    subgraph Flush["FLUSH ENGINE"]
      direction TB

      TRIG["Flush Triggers<br/>250ms / MaxBatchSize / Pre-eviction"]

      DRAIN["DrainBand<br/>ZRANGEBYSCORE + HMGET"]

      CONTRACT["WriteContract<br/>caller-supplied"]

      BULK["BulkWrite<br/>ordered=false"]

      HOTTTL["HotAwareFlush<br/>extend ActivityWindow"]

      TRIG --> DRAIN
      DRAIN --> CONTRACT
      CONTRACT --> BULK
      BULK --> HOTTTL
    end
  end


%% ============================================================
%% DATASTORE
%% ============================================================

  subgraph Store["DATASTORE"]
    DB[("DataStore<br/>nudge_inventory")]
  end


%% ============================================================
%% COLD WRITE → SLUICE
%% ============================================================

  CW -->|"Write() · cold"| W


%% ============================================================
%% HOT WRITE → SLUICE
%% ============================================================

  HW --> W

%% Hot synchronous writes can establish / refresh hot state
  HW -.->|"hotLoad / hot session"| HOT


%% ============================================================
%% SLUICE WRITE PIPELINE
%% ============================================================

  BP -->|"pipeline EVALSHA"| PAYLOAD

  DIRTY --> DRAIN

  BULK -->|"successful flush"| DB

  BULK -->|"non-retryable error"| DLQ

  HOTTTL -->|"refresh TTL"| HOT


%% ============================================================
%% READ PATH — ONE READ MODEL
%% ============================================================

  subgraph Read["UNIFIED READ PATH"]
    direction LR

    READ["Read()"]

    HOTREAD["Hot Read<br/>Redis Journal<br/><b>sub-ms</b>"]

    COLDREAD["Cold Miss<br/>ReadContract"]

    QUERY["Query()<br/>secondary indexes"]

    BULKREAD["ReadBulk()<br/>ReadBulkContract<br/>set-based pre-warm"]

    READ -->|"hot"| HOTREAD
    READ -->|"cold miss"| COLDREAD

    QUERY --> INDEX
  end


%% ============================================================
%% READ CONNECTIONS
%% ============================================================

  HOTREAD --> PAYLOAD
  HOTREAD --> HOT

  COLDREAD -->|"Source.Read"| DB
  COLDREAD -.->|"async hydrate"| PAYLOAD

  BULKREAD -->|"Source.ReadBulk · one query"| DB
  BULKREAD -->|"pipelined HSET"| PAYLOAD
  BULKREAD -->|"pipelined SADD / ZADD"| INDEX


%% ============================================================
%% STYLING
%% ============================================================

  classDef cold fill:#E8EAF6,stroke:#3949AB,color:#1A237E
  classDef hot fill:#E1F5EE,stroke:#0F6E56,color:#085041
  classDef journal fill:#FAEEDA,stroke:#BA7517,color:#633806
  classDef store fill:#FAECE7,stroke:#993C1D,color:#712B13
  classDef read fill:#EDE7F6,stroke:#5E35B1,color:#311B92
  classDef engine fill:#F3F4F6,stroke:#6B7280,color:#374151
  classDef dlq fill:#FAECE7,stroke:#993C1D,color:#712B13

  class SQS,KAF,CW cold
  class API,HW hot
  class W,BC,BP cold
  class PAYLOAD,DIRTY,HOT,INDEX journal
  class TRIG,DRAIN,CONTRACT,BULK,HOTTTL engine
  class READ,HOTREAD,COLDREAD,QUERY,BULKREAD read
  class DB store
  class DLQ dlq
```

---

## Redis key layout

Every key embeds a `{band}` hash tag so that all keys touched by a single Lua script, pipeline, or
`SINTER` co-locate on one Redis Cluster slot.

| Key | Type | TTL | Purpose |
|---|---|---|---|
| `sl:{ns}:payload:{band}:{corrKey}` | `HASH` — `p` payload, `ts` score, `h` content hash | `KeyTTL`, or `ActivityWindow` when hot | Journalled state |
| `sl:{ns}:dirty:{band}` | `ZSET` — score = event ms | none | Pending flush queue |
| `sl:{ns}:hot:{band}:{corrKey}` | `STRING` | `ActivityWindow` | Hot marker for the read path |
| `sl:{ns}:idx:{band}:{field}:{value}` | `SET` | `ActivityWindow` | Equality index |
| `sl:{ns}:ridx:{band}:{field}` | `ZSET` — score = value | `ActivityWindow` | Range index |
| `sl:{ns}:idem:{band}:{idemKey}` | `STRING` | `IdempotencyTTL` | `WriteIdempotent` guard |
| `sl:{ns}:dlq:{band}` | `ZSET` — score = failure ms | none | Dead-letter queue |

A namespace containing `{` or `}` is rejected by `Build()` — it would hijack cluster hash-tag parsing.

---

## Install

```bash
go get github.com/hussainpithawala/sluice-go@latest
```

---

## Quickstart

```go
import (
    sluice "github.com/hussainpithawala/sluice-go"
    "github.com/hussainpithawala/sluice-go/sink/docdb"
    "go.mongodb.org/mongo-driver/bson"
)

sk, _ := docdb.New(ctx, docdb.DefaultConfig(
    "mongodb://user:pass@cluster.docdb.amazonaws.com:27017/?tls=true&replicaSet=rs0",
    "adroll", "nudge_inventory",
))

contract := func(correlationKey string, payload []byte) (*sluice.WriteModel, error) {
    var doc map[string]any
    json.Unmarshal(payload, &doc)
    return &sluice.WriteModel{
        Filter: bson.D{{"_id", correlationKey}},
        Update: bson.D{{"$set", doc}},
        Upsert: true,
    }, nil
}

s, _ := sluice.New("nudge_inventory").
    WithRedis(sluice.RedisConfig{Addrs: []string{"redis:6379"}}).
    WithSink(sk).
    WithWriteContract(contract).
    WithFlushWindow(250 * time.Millisecond).
    WithMaxBatchSize(1000).
    WithBandCount(16).
    Build(ctx)

defer s.DrainAndClose(ctx)

// Write path — DocumentDB is never touched here
s.Write(ctx, correlationKey, payload)
```

### Adding the read path

To serve reads from the journal, wire a `Source` and a `ReadContract` alongside the sink. The sink's
`mongo.Client` can be shared so both paths use one connection pool:

```go
import (
    "github.com/hussainpithawala/sluice-go/source"
    sourcedocdb "github.com/hussainpithawala/sluice-go/source/docdb"
)

src := sourcedocdb.NewSourceWithClient(sk.Client(), "adroll", "nudge_inventory")

readContract := func(correlationKey string) (*source.ReadModel, error) {
    return &source.ReadModel{Filter: bson.M{"_id": correlationKey}}, nil
}

s, _ := sluice.New("nudge_inventory").
    WithRedis(sluice.RedisConfig{Addrs: []string{"redis:6379"}}).
    WithSink(sk).
    WithWriteContract(contract).
    WithSource(src).
    WithReadContract(readContract).
    WithIndexContract(indexContract).      // optional — enables Query()
    WithActivityWindow(4 * time.Hour).     // hot session TTL
    WithHotAwareFlush(true).               // refresh TTL after successful flush
    WithContentDedup(true).                // xxHash64 payload deduplication
    Build(ctx)

// On login: mark the key hot and warm the journal from DocumentDB in the background
_ = s.HotLoad(ctx, correlationKey)

// During the session: sub-millisecond reads, no DocumentDB round-trip
payload, _ := s.Read(ctx, correlationKey)
```

To pre-warm a whole working set in one query, add `WithReadBulkContract()` — see
[Bulk reads](#bulk-reads--set-based-cache-pre-warming). To run a read-only instance, omit `WithSink()`
and `WithWriteContract()` — see [Deployment topologies](#deployment-topologies).

---

## Configuration

### Write path

| Builder method | Default | Description |
|---|---|---|
| `WithRedis(cfg)` | — | **Required.** Redis/Valkey connectivity; set `ClusterMode` for CME |
| `WithSink(s)` | nil | Write-side DataStore (`sink.FlushSink`). Required, together with `WithWriteContract`, for the write path |
| `WithWriteContract(fn)` | nil | Translates a payload into a `WriteModel`. Without it (or without a sink) the flush engine is not started and `Write` returns `ErrMissingWriteContract` / `ErrMissingSink` |
| `WithFlushWindow(d)` | `250ms` | Maximum dirty key age before flush — caps DataStore staleness |
| `WithMaxBatchSize(n)` | `1000` | Keys per BulkWrite call; also the volume trigger threshold |
| `WithBandCount(n)` | `16` | Parallel flush goroutines — one per dirty-set partition |
| `WithKeyTTL(d)` | `30s` | In-flight payload TTL — crash recovery net and pre-eviction bound |
| `WithDegradedModeDirect(bool)` | `true` | Fall back to single-doc writes when Redis is unavailable |
| `WithBatchedWrites(size, window)` | disabled | Enable pipelined Redis writes for high-velocity streams |
| `WithContentDedup(bool)` | `false` | xxHash64 payload fingerprinting — skip redundant writes |
| `WithIdempotencyTTL(d)` | `4h` | Retention for `WriteIdempotent` guard keys |
| `WithMetrics(m)` | noop | Plug in Prometheus, Datadog, or CloudWatch |
| `OnFlush(cb)` | nil | Callback invoked after every BulkWrite attempt |

### Read path and hot/cold regime

| Builder method | Default | Description |
|---|---|---|
| `WithSource(s)` | nil | Read-side DataStore (`source.Source`) — required with `WithReadContract` / `WithReadBulkContract` |
| `WithReadContract(fn)` | nil | Translates a correlation key into a `source.ReadModel` for `HotLoad` / cold `Read` |
| `WithIndexContract(fn)` | nil | Projects secondary index fields from a payload — enables `Query()` |
| `WithReadBulkContract(fn)` | nil | Translates a lookup key into a `source.BulkReadModel` — enables `ReadBulk()` / `ReadBulkWithTTL()` |
| `WithIndexBulkContract(fn)` | nil | Projects index fields for a whole bulk result set in one pipeline |
| `WithActivityWindow(d)` | `4h` | Journal residency for hot correlation keys |
| `WithHotAwareFlush(bool)` | `true` | Extend payload TTL to `ActivityWindow` after a successful commit |

### L1 Local Journal (v1.0.8)

| Builder method | Default | Description |
|---|---|---|
| `WithLocalCache(cfg)` | disabled (`LocalCacheOff`) | Enable the in-process L1 cache in front of the Redis journal |

`localjournal.LocalCacheConfig` fields:

| Field | Type | Default | Description |
|---|---|---|---|
| `Mode` | `LocalCacheMode` | `LocalCacheOff` | Cache mode: `Off`, `Lazy`, `PushPull`, `Invalidation` |
| `MaxEntries` | `int` | `200_000` | Global LRU cap (distributed across 256 shards) |
| `LocalTTL` | `time.Duration` | `60s` | Per-entry staleness bound |
| `Broadcast` | `BroadcastMode` | `BroadcastPayload` | `Payload` (full broadcast, ~1KB/msg) or `Invalidation` (~40B/msg, peers refetch) |
| `Retention` | `int64` | `200_000` | Broadcast stream `MAXLEN ~ N` (approximate trimming) |

### DLQ

| Builder method | Default | Description |
|---|---|---|
| `WithDLQAutoProcess(interval, strategy)` | disabled | Background ticker that calls `ProcessDLQ`; stopped by `DrainAndClose` |

---

## High-velocity write batching

For streams exceeding ~10K writes/sec, the default mode makes one Redis round-trip per `Write()` call. Enable batched writes to pipeline all entries in a single network round-trip:

```go
s, _ := sluice.New("nudge_inventory").
    WithRedis(sluice.RedisConfig{Addrs: []string{"redis:6379"}}).
    WithSink(sk).
    WithWriteContract(contract).
    WithBatchedWrites(200, 5*time.Millisecond). // buffer up to 200 entries, flush every 5ms
    Build(ctx)
```

**How it works:**

- `Write()` sends the entry to an in-memory channel and returns immediately
- A background goroutine collects entries and flushes them in a single Redis pipeline
- The pipeline flush is triggered when the buffer reaches `size` entries **or** `window` elapses — whichever comes first
- Each key's HSET + EXPIRE + ZADD remains **atomically isolated** via EVALSHA (Lua script pre-loaded at startup)
- Volume signals (flush triggers) are derived from ZCARD results **batched into the same pipeline** — no extra round-trips
- `DrainAndClose` stops the batcher first, ensuring all buffered entries land in Redis before the engine drain begins

```mermaid
sequenceDiagram
    participant CW as Consumer Workers (N goroutines)
    participant CH as In-memory channel
    participant BG as Batcher goroutine
    participant R  as Redis Pipeline

    CW->>CH: Write(corrKey, payload) — returns immediately
    CW->>CH: Write(corrKey, payload) — returns immediately
    CW->>CH: Write ... (up to batchSize)
    Note over BG: timer fires OR buffer full
    BG->>R: pipeline [ EVALSHA×N + ZCARD×bands ]
    R-->>BG: ACK (one round-trip)
    BG->>BG: signal volume triggers for full bands
```

| Mode | Redis round-trips for N writes |
|---|---|
| Default (unbatched) | N |
| `WithBatchedWrites` | ⌈N / batchSize⌉ |

**When to use:** Kafka/SQS consumers ingesting >10K msg/sec where the per-write Redis latency becomes the throughput bottleneck. No behaviour change for existing callers — disabled by default.

---

## Dead-letter queue (DLQ)

When a flush cycle encounters a non-retryable error (e.g. a `WriteContract` violation, a duplicate-key collision on a unique index, or a schema enforcement failure), the offending keys are moved to a per-band dead-letter sorted set rather than dropped silently or retried indefinitely.

```
sl:{ns}:dlq:{band}                 — dead-letter sorted set (score = failure timestamp)
sl:{ns}:payload:{band}:{corrKey}   — payload hash, TTL extended to 7 days for inspection
```

### Processing DLQ records

Call `ProcessDLQ` with a recovery strategy. The call is context-aware — pass a timeout or a cancellable context derived from your application root:

```go
result, err := s.ProcessDLQ(ctx, sluice.DLQIgnore)    // drain and discard
result, err := s.ProcessDLQ(ctx, sluice.DLQUpsert)    // retry as upsert (force-overwrite)
result, err := s.ProcessDLQ(ctx, sluice.DLQReInsert)  // mutate key and re-enqueue to normal queue
```

### Strategies

| Strategy | Behaviour |
|---|---|
| `DLQIgnore` | Logs each record and removes it from the DLQ. Use when the failure is expected and the record can be safely discarded. |
| `DLQUpsert` | Re-runs the `WriteContract` and re-attempts the write with `Upsert: true`, overwriting any conflicting document. The write contract executes again — use this together with a **payload healing** pattern (see below) to correct bad records before they land. |
| `DLQReInsert` | Mutates the correlation key (appends a timestamp suffix by default) and re-enqueues to the normal dirty queue. Use to preserve both the original and the new document side-by-side. |

### Options

```go
result, err := s.ProcessDLQ(ctx, sluice.DLQReInsert,
    sluice.WithDLQBatchSize(500),
    sluice.WithKeyMutator(func(key string) string {
        return key + ":retry:" + time.Now().Format("20060102T150405")
    }),
    sluice.WithDLQLogger(slog.Default()),
)
```

| Option | Default | Description |
|---|---|---|
| `WithDLQBatchSize(n)` | `MaxBatchSize` | Per-band batch size when draining the DLQ |
| `WithKeyMutator(fn)` | append timestamp suffix | Key transformation for `DLQReInsert` strategy |
| `WithDLQLogger(l)` | `slog.Default()` | Structured logger for per-record DLQ activity |

### Result

```go
type DLQResult struct {
    Processed int // total records drained across all bands
    Succeeded int // records handled without error
    Failed    int // records that failed during processing
}
```

### Payload healing pattern

Because `DLQUpsert` re-executes the `WriteContract`, you can correct quarantined payloads at recovery time without touching the DLQ data itself. Toggle a healing flag in your contract before calling `ProcessDLQ`:

```go
var healBadRecords atomic.Bool

// WriteContract: applied on every write AND on DLQ re-processing
contract := func(correlationKey string, raw []byte) (*sluice.WriteModel, error) {
    var p NudgePayload
    json.Unmarshal(raw, &p)

    if p.Channel == "REJECT" {
        if healBadRecords.Load() {
            p.Channel = "email" // correct the offending field at recovery time
        } else {
            return nil, fmt.Errorf("contract violation: invalid channel on %s", correlationKey)
        }
    }

    return &sluice.WriteModel{
        Filter: bson.D{{Key: "_id", Value: correlationKey}},
        Update: bson.D{{Key: "$set", Value: p}},
        Upsert: true,
    }, nil
}

// ... normal ingest runs here; bad records land in DLQ ...

// Recovery phase: flip the flag, then drain
healBadRecords.Store(true)
result, err := s.ProcessDLQ(ctx, sluice.DLQUpsert,
    sluice.WithDLQBatchSize(100),
    sluice.WithDLQLogger(slog.Default()),
)
// result.Succeeded == number of healed records written to DocumentDB
```

```mermaid
flowchart TD
    DLQ[("sl:ns:dlq:band — dead-letter set")]
    ST{"Strategy"}
    IGN["Log + CommitDLQKeys<br/>(remove from DLQ)"]
    UPS["Re-run WriteContract<br/>BulkWrite Upsert=true<br/>then CommitDLQKeys"]
    REI["Mutate key → Write to dirty queue<br/>then CommitDLQKeys"]

    DLQ --> ST
    ST -->|DLQIgnore| IGN
    ST -->|DLQUpsert| UPS
    ST -->|DLQReInsert| REI

    style DLQ fill:#FAECE7,stroke:#993C1D,color:#712B13
    style IGN fill:#E1F5EE,stroke:#0F6E56,color:#085041
    style UPS fill:#FAEEDA,stroke:#BA7517,color:#633806
    style REI fill:#E8EAF6,stroke:#3949AB,color:#1A237E
```

### Scheduling DLQ processing

The simplest option is to let sluice run the ticker itself:

```go
s, _ := sluice.New("nudge_inventory").
    // ...
    WithDLQAutoProcess(2*time.Minute, sluice.DLQUpsert).
    Build(ctx)
// Background goroutine calls ProcessDLQ every 2 minutes; DrainAndClose stops it.
```

Two reference examples show how to drive it externally instead:

**`examples/ticker_dlq/documentdb/main.go`** — minimal inline scheduler using `time.Ticker`. No extra dependencies. Suitable for single-instance services or CLI tooling where the DLQ run should happen at a fixed cadence inside the same process.

**`examples/asynq_dlq/documentdb/main.go`** — production-grade distributed scheduler using [hibiken/asynq](https://github.com/hibiken/asynq). The DLQ task is registered as a cron entry (default: every 2 minutes) and executed by an Asynq worker. Supports multiple replicas, task deduplication via Redis, and structured logging through the `asynqLogger` bridge. The `healBadRecords` atomic flag is toggled before immediate task enqueue to demonstrate end-to-end payload correction without cron lag.

---

## Pluggable sinks

```go
type FlushSink interface {
    BulkWrite(ctx context.Context, models []WriteModel) (*sluice.BulkWriteResult, error)
    Write(ctx context.Context, model WriteModel) error
    Ping(ctx context.Context) error
    Close(ctx context.Context) error
}
```

| Package | Target |
|---|---|
| `sink/docdb` | AWS DocumentDB · MongoDB |
| `sink/dynamodb` | AWS DynamoDB — `BatchWriteItem`, 25-item chunking, `UnprocessedItems` backoff |
| `sink/postgres` | PostgreSQL — multi-row `INSERT ... ON CONFLICT DO UPDATE`, or `COPY` for append-only contracts |

See [DynamoDB adapter](#dynamodb-adapter) and [PostgreSQL adapter](#postgresql-adapter) below for the
cost model and configuration each one targets.

---

## Pluggable sources

The read path is symmetric to the write path: `ReadContract` produces a datastore-agnostic
`ReadModel`, and the `Source` executes it.

```go
type Source interface {
    // Read returns the raw document bytes, or source.ErrRecordNotFound.
    Read(ctx context.Context, model ReadModel) ([]byte, error)
    // ReadBulk executes a set-based query and returns multiple hydrated results.
    ReadBulk(ctx context.Context, model BulkReadModel) ([]BulkReadResult, error)
    Ping(ctx context.Context) error
    Close(ctx context.Context) error
}

type ReadModel struct {
    Filter interface{}
}

type BulkReadModel struct {
    Query     interface{} // adapter-specific plan, e.g. postgres.PostgresBulkReadModel
    Args      []any
    Projector func(scanner any) ([]BulkReadResult, error)
}

type BulkReadResult struct {
    CorrelationKey string
    Payload        []byte
}
```

> **Breaking change (v1.0.8):** custom `Source` implementations must now implement `ReadBulk`.

| Package | Single-key `Read` | `ReadBulk` |
|---|---|---|
| `source/docdb` | AWS DocumentDB · MongoDB (`FindOne`, result marshalled to JSON bytes) | `Find` with `DocDBBulkReadModel` → `*mongo.Cursor` |
| `source/dynamodb` | AWS DynamoDB — `GetItem` with `ConsistentRead: true` | `Query` with `DynamoBulkReadModel` → item maps |
| `source/postgres` | PostgreSQL — `QueryParams` + `Projector`, prepared `SELECT` against the primary | `PostgresBulkReadModel` → `pgx.Rows` |

Each adapter type-asserts `BulkReadModel.Query` to its own model and returns an error on a mismatch,
a missing query / filter / `QueryInput`, or a nil `Projector`.

`source/docdb` offers two constructors: `NewSource(ctx, Config)` opens its own connection pool, and
`NewSourceWithClient(client, db, collection)` shares an existing `*mongo.Client` — pass `sink.Client()`
to run both paths over one pool.

`source.ErrRecordNotFound` is translated to the public `sluice.ErrRecordNotFound` by `Read` and
`HotLoad`, so callers never need to import the `source` package for error handling.

---

## DynamoDB adapter

`adapter/dynamodb` (RFC: `hybrid-dynamo.md`) positions `sluice` in front of DynamoDB as a **cost and
capacity flattener**. DynamoDB is never connection-bound — the problem is WCU spend: DAX doesn't cache
writes, GSIs multiply WCU cost per index, and `TransactWriteItems` carries a 2x WCU penalty. `sluice`
removes all three:

| DynamoDB cost driver | Without `sluice` | With `sluice` |
|---|---|---|
| Ingestion rate | 100K individual writes/sec | 100K writes to Redis (sub-ms) |
| DynamoDB write rate | 100K WCU/sec (peak) | ~100 `BatchWriteItem` calls/sec, ~4K WCU/sec (steady) |
| GSI WCU cost | multiplies linearly per index | 0 — secondary indexes live in Redis (`WithIndexContract`) |
| Idempotency cost | 2x via `TransactWriteItems` | 0 — Redis `SETNX` (`WriteIdempotent`) |
| Capacity model | On-Demand or Provisioned + Auto-Scaling | Low, flat Provisioned Capacity |

**Write path:** `dynamodb.BatchWriteItem` (25 items/batch, 16MB limit), exponential backoff with jitter
on `UnprocessedItems`, and pre-flush validation against DynamoDB's 400KB item limit — oversized payloads
route to the DLQ with `ErrPayloadTooLarge`.

**Read path:** `GetItem` / `BatchGetItem` with `ConsistentRead: true` enforced — a cold read immediately
after a flush must see the flushed state, since `sluice` already acknowledged the write at the Redis layer.

**CAP positioning:** write path is AP (Redis absorbs and acks; DynamoDB partitions/throttling hold in the
journal or DLQ); cold read path is CP (`ConsistentRead: true` guarantees strict consistency).

---

## PostgreSQL adapter

`adapter/postgres` (RFC: `postgres-hybrid-adapter.md`) reuses the same `WriteContract` /
`ReadContract` / `WithIndexContract` interfaces for structural consistency with the DynamoDB adapter,
but targets a structurally different bottleneck: PostgreSQL has no per-request pricing — its limits are
**connection pool saturation, per-transaction overhead, synchronous index write amplification, and
MVCC bloat**, not WCU cost.

Two independent, non-interchangeable savings levers apply:
- **Batching** (statement-count reduction) — always on: one multi-row upsert replaces thousands of
  `BEGIN`/`COMMIT` round-trips.
- **Coalescing** (row-count reduction) — workload-dependent: only reduces WAL volume and MVCC bloat
  when many writes in a flush window target the same key.

**Write path — two modes, selectable per namespace:**

| Mode | Mechanism | Use when |
|---|---|---|
| Mode A — Bulk Upsert (default) | `INSERT ... VALUES (...), (...) ON CONFLICT (pk) DO UPDATE ... WHERE EXCLUDED.ts > t.ts` | General upsert workloads; default `MaxBatchSize` 1000, typically tuned 200–500 |
| Mode B — `COPY` Append (opt-in) | `pgx.CopyFrom` into an unlogged staging table, then `INSERT ... SELECT ... ON CONFLICT DO NOTHING` | Insert-only contracts with upstream-guaranteed key uniqueness — 5–10x over bulk upsert |

Connection handling uses a fixed `pgxpool.Pool` (`MinConns=5`, `MaxConns=30`), replacing hundreds of
direct writer connections. Postgres errors are routed by class — permanent (`23505`, `23502`, `42P01`,
etc.) go straight to the DLQ; transient errors shrink either the batch-size axis (`57014`, `53200`) or
the concurrency axis (`53300`), or fall back to pure backoff with jitter (`40001`, `40P01`).

**Read path:** prepared `SELECT ... WHERE correlation_key = $1` against the primary, with
`pg_is_in_recovery()` probed both at `Build` and in `pgxpool.AfterConnect` so every physical connection
is verified — including ones grown or recycled after startup.

**Schema boundary:** `sluice` never auto-creates tables, emits DDL, or manages migrations. The
`WriteContract` / `ReadContract` / `IndexContract` / optional `PostgresProjector` define the data shape
`sluice` expects; provisioning the physical table, indexes, and partitions is strictly the operator's
responsibility via standard migration tooling. A schema mismatch fails fast (`42P01` / `42703`) and
routes to the DLQ rather than attempting to self-heal the schema.

**CAP positioning:** write path is AP (Redis absorbs writes, acks immediately; Postgres updates
asynchronously); cold read path is CP (`READ COMMITTED` against the primary, enforced at runtime rather
than assumed).

---

## Telemetry

`MetricsRecorder` gained three methods for the hot/cold regime. Implementations of the interface must
provide all of them:

```go
// Write path
RecordWrite(namespace string)
RecordDegradedWrite(namespace string, reason error)
RecordRedisOp(namespace, op string, duration time.Duration, err error)
RecordFlush(namespace, band string, batchSize int, duration time.Duration, err error)
RecordDirtyQueueDepth(namespace, band string, depth int)
RecordContractError(namespace, correlationKey string, err error)
RecordDeadLetter(namespace, band string, count int)
RecordDLQProcess(namespace, strategy string, processed, succeeded, failed int)

// Hot/cold regime
RecordWarmUp(namespace string, duration time.Duration, err error)         // HotLoad latency
RecordRead(namespace string, duration time.Duration, isHot bool, err error) // Read latency + hit/miss
RecordHotSetSize(namespace string, size int)
```

`isHot` on `RecordRead` is the signal to watch: a falling hot-hit ratio means sessions are ageing out
of the journal before their traffic finishes, and `ActivityWindow` is too short.

### L1 Local Journal metrics (v1.0.8)

Implement these additional methods in your `MetricsRecorder` to observe L1 behaviour in Prometheus,
Datadog, CloudWatch, or any other monitoring stack:

```go
type MetricsRecorder interface {
    // ... existing v1.0.7 methods ...

    // L1 Local Journal
    RecordLocalCacheHit(namespace string)
    RecordLocalCacheMiss(namespace string, reason MissReason)
    RecordLocalSetSize(namespace string, size int)

    // Cross-pod convergence
    RecordBroadcastLag(namespace string, lag time.Duration)
}
```

Watch the L1 hit ratio (`RecordLocalCacheHit` vs `RecordLocalCacheMiss`, broken down by `reason`) to
size `MaxEntries` and `LocalTTL`, and `RecordBroadcastLag` to see how far peer pods trail a write.

---

## Errors

All sentinel errors are comparable with `errors.Is`.

| Error | Returned by | Meaning |
|---|---|---|
| `ErrRecordNotFound` | `Read`, `ReadFresh` | Absent from both the journal and the source |
| `ErrDuplicateIdempotencyKey` | `WriteIdempotent` | The idempotency key was already consumed |
| `ErrMissingSource` | `HotLoad` | No `Source` configured |
| `ErrMissingReadContract` | `HotLoad` | No `ReadContract` configured |
| `ErrMissingSink` · `ErrMissingWriteContract` | `Write`, `WriteIdempotent`, `ProcessDLQ` | Write path not configured (reader-only instance) |
| `ErrRedisUnavailable` | `Write` | Redis failed and `DegradedModeDirect` is off |
| `ErrContractViolation` | degraded `Write` | `WriteContract` rejected the payload |
| `ErrEmptyCorrelationKey` | `Write`, `WriteIdempotent` | Empty correlation or idempotency key |
| `ErrLibraryClosed` | every method | Called after `DrainAndClose` |
| `ErrMissingNamespace` · `ErrMissingRedis` | `Build` | Required builder option not set |

> **Breaking change (v1.0.8):** `ErrMissingContract` was renamed to `ErrMissingWriteContract`, and
> `Build()` no longer fails when `WithSink()` / `WithWriteContract()` are omitted — those errors are
> now returned at call time by the write-path methods.

---

## Running tests

```bash
make test-unit          # unit tests — Redis + MongoDB + DynamoDB Local + PostgreSQL auto-started via Docker
make test-integration   # full stack: Redis + MongoDB + DynamoDB Local + PostgreSQL + Kafka + LocalStack
make test-all           # unit + integration, then tear down
make coverage           # HTML coverage report
make check              # pre-commit: tidy + vet + lint + unit tests
```

Hot/cold regime coverage lives in `tests/unit/documentdb/hot_features_test.go` and
`tests/integration/documentdb/hot_features_test.go` — `HotLoad`/`Read`, `WriteIdempotent`, content dedup, and
compound `Query`. `tests/unit/dynamodb` covers chunking/batching, degraded mode, and strong consistency
against DynamoDB Local; the PostgreSQL adapter is tested alongside its code in `sink/postgres` and
`source/postgres` (including complex projections) against a real PostgreSQL container. `ReadBulk` is
covered per adapter in `source/docdb`, `source/dynamodb`, and `source/postgres`.

---

## Local development

Examples live under `examples/<scenario>/<adapter>/`, where `<adapter>` is `documentdb`, `dynamodb`, or
`postgres` (`nudge_secured` is DocumentDB-only). The Makefile discovers and runs them with local
defaults for `MONGO_URI`, `DYNAMODB_ENDPOINT`, `POSTGRES_URI`, `REDIS_ADDR`, `REDIS_ADDRS`, and
`REDIS_CLUSTER_MODE` — override any of them in the environment.

```bash
make docker-up                                          # start all services
make list-examples                                      # list every runnable example
make run-example DIR=examples/bulk_read/postgres        # run one example
make run-examples                                       # run them all sequentially (Ctrl+C skips long-running ones)
make docker-down
```

| Scenario | What it demonstrates |
|---|---|
| `nudge` | Minimal quickstart — basic write + flush |
| `nudge_hot_reload` | Hot/cold regime — fire-and-forget `HotLoad`, `Read`, `IsHot`, `Query`, dedup, secondary indexes |
| `nudge_write_dual_read_hot` | Full lifecycle — cold stream writes, hot API writes, unified `Read`, and compound `Query` |
| `bulk_read` | `ReadBulk` set-based pre-warming, `IndexBulkContract`, and `Query` over bulk-loaded items |
| `localjournal_pushpull` | L1 Local Journal cross-pod convergence (plus split `reader/` and `writer/` processes) |
| `ticker_dlq` | DLQ processing with an inline `time.Ticker` scheduler |
| `asynq_dlq` | DLQ processing with a distributed Asynq cron scheduler |
| `nudge_secured` | TLS-secured Redis (`REDIS_ADDR=localhost:7380`) |

Running an example directly works too:

```bash
POSTGRES_URI=postgres://sluice:sluice@localhost:5432/sluice_test?sslmode=disable \
REDIS_ADDRS=localhost:6379 \
go run ./examples/bulk_read/postgres
```

`docker-compose.yml` runs `dynamodb-local` and `postgres` services alongside Redis/Valkey and MongoDB,
so the examples need no external AWS account or managed database.

`examples/nudge_hot_reload/documentdb/main.go` runs both regimes side by side: eight cold-path bulk consumers
driving the flush engine, plus a hot-path simulator that logs in a synthetic user every two seconds
and reports observed `Read()` latency in microseconds. It defaults to the 4-shard Valkey cluster
(`localhost:7001-7004`) from `docker-compose.yml`.

---

## Releasing

```bash
git push -u origin main
git tag v0.1.0 && git push origin v0.1.0
```

---

## License

MIT — see [LICENSE](LICENSE).