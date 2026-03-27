# sluice

[![CI](https://github.com/hussainpithawala/sluice-go/actions/workflows/ci.yml/badge.svg)](https://github.com/hussainpithawala/sluice-go/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/hussainpithawala/sluice-go.svg)](https://pkg.go.dev/github.com/hussainpithawala/sluice-go)
[![Go Version](https://img.shields.io/badge/go-1.25-blue)](https://go.dev)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> **Wide-breadth Redis-shielded write batcher for document stores.**
> Built for ad-tech platforms where millions of customers receive nudges, bids, and inventory updates at rates that no single document store primary can absorb directly.

---

## The problem

Modern ad-roll platforms process inventory update events at 10K–100K TPS from SQS queues and Kafka topics. Each event is a single incoherent write — one document, one customer, one update — arriving unbatched and uncoordinated. Sending each of these directly to a document store like AWS DocumentDB means:

- Every write hits the single primary node individually
- Every index on the collection multiplies the I/O cost per write
- Connection pools saturate under spikes (DocumentDB hard-caps connections per instance class)
- High-frequency small writes never coalesce — 100,000 events for 80,000 unique customers becomes 100,000 individual round-trips instead of ~100 BulkWrite calls

The naive architecture breaks at scale. The write path becomes the system's weakest link precisely when traffic is highest.

```mermaid
flowchart LR
    SQS([SQS Queue])
    KAF([Kafka Topics])
    DB[(AWS DocumentDB)]

    SQS -->|individual write per event| DB
    KAF -->|individual write per event| DB

    style DB fill:#FAECE7,stroke:#993C1D,color:#712B13
```

---

## How sluice solves it

sluice introduces a **write journal in Redis** that sits between your event consumers and the document store. Every incoming event is written atomically to Redis — a sub-millisecond in-memory operation — and acknowledged immediately. The document store never sees individual events. Instead, a fleet of background goroutines (one per partition band) drains the Redis journal in configurable time windows and assembles efficient `BulkWrite` calls against DocumentDB.

The result: **100,000 events per second become ~100 BulkWrite calls per second** — a 1,000× reduction in document store I/O without any change to the consuming application beyond replacing a single-document write with `sluice.Write()`.

```mermaid
flowchart TD
    SQS([SQS Queue\n~80K unique CRNs/sec])
    KAF([Kafka Topics\npartitioned by CRN hash])
    CON[Consumer Workers\nstateless · CRN-hash routed]
    RED[(Redis Cluster\nVelocity Shield\nHSET + ZADD atomic)]
    ENG[Batch Flusher Engine\n16 band goroutines\n250ms window · 1000 CRNs/batch]
    DB[(AWS DocumentDB\nLong-Term Store\nordered=false BulkWrite)]

    SQS --> CON
    KAF --> CON
    CON -->|sluice.Write·returns immediately| RED
    RED -->|drain dirty bands async| ENG
    ENG -->|single BulkWrite call per band per window| DB

    style RED fill:#FAEEDA,stroke:#BA7517,color:#633806
    style ENG fill:#E1F5EE,stroke:#0F6E56,color:#085041
    style DB  fill:#FAECE7,stroke:#993C1D,color:#712B13
```

---

## Redis as a write journal

The core innovation in sluice is treating Redis not as a cache, but as a **durable write journal with correlation-key deduplication**. This is a fundamentally different mental model from traditional caching:

| Traditional cache | sluice Redis journal |
|---|---|
| Read-optimised: stores computed results to avoid re-computation | Write-optimised: absorbs write velocity to protect the primary store |
| TTL = staleness budget for reads | TTL = safety net for crash recovery |
| Cache miss = go to DB | Journal miss = key already flushed (correct behaviour) |
| Eviction under pressure loses data | Deduplication under pressure is intentional |

For every incoming event, sluice executes a single atomic Lua script on Redis that performs three operations in one round-trip:

```mermaid
sequenceDiagram
    participant C  as Consumer worker
    participant R  as Redis (Lua script)
    participant DS as Dirty sorted set

    C  ->> R  : EVALSHA atomicWrite(corrKey, payload, ts, ttl)
    activate R
    R  ->> R  : HSET sl:{ns}:payload:{corrKey} p=payload ts=timestamp
    R  ->> R  : EXPIRE sl:{ns}:payload:{corrKey} ttl
    R  ->> DS : ZADD sl:{ns}:dirty:{band} score=timestamp member=corrKey
    R  -->> C : 1 (ACK)
    deactivate R

    Note over C,DS: Single round-trip · atomic · no partial state possible
```

The `ZADD` into the dirty sorted set is the journal entry. The score is the event timestamp — which means the flush engine always processes the oldest dirty keys first, providing natural ordering and a bounded staleness guarantee equal to the flush window.

---

## The coalescing mechanism

The dirty sorted set is the heart of sluice's efficiency. Because `ZADD` on an existing member simply updates its score, multiple events for the same correlation key collapse to a **single entry**. The `HSET` stores the latest payload, overwriting any prior value. This means:

- 500 events for `crn_9876543` in one second → **1 entry** in the dirty set, **1 HGETALL** at flush time, **1 upsert** in BulkWrite
- The coalescing ratio is highest for re-targeting workloads where the same customer is hit repeatedly by campaign evaluations
- For wide-breadth workloads (each event targets a unique CRN), the gain is in **batching** — N unique writes become 1 BulkWrite network call

```mermaid
flowchart LR
    subgraph "Events arriving (100K/sec)"
        E1[crn_001 · event 1]
        E2[crn_002 · event 1]
        E3[crn_001 · event 2]
        E4[crn_003 · event 1]
        E5[crn_001 · event 3]
        E6[crn_002 · event 2]
    end

    subgraph "Redis dirty set after 250ms"
        D1[crn_001 · score=latest_ts]
        D2[crn_002 · score=latest_ts]
        D3[crn_003 · score=ts]
    end

    subgraph "DocumentDB BulkWrite"
        B1[upsert crn_001]
        B2[upsert crn_002]
        B3[upsert crn_003]
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

## The flush engine — dual trigger

The flush engine runs one goroutine per band. Each goroutine wakes on two independent triggers, whichever fires first:

```mermaid
stateDiagram-v2
    [*]      --> Idle
    Idle     --> Draining : time trigger fires (250ms elapsed)
    Idle     --> Draining : volume trigger fires (dirty queue ≥ MaxBatchSize)
    Draining --> Reading  : ZRANGEBYSCORE — fetch dirty keys
    Reading  --> Fetching : pipeline HMGET — read all payloads in one round-trip
    Fetching --> Building : apply WriteContract per key
    Building --> Writing  : BulkWrite to DocumentDB (ordered=false)
    Writing  --> Cleanup  : ZREM confirmed keys from dirty set
    Cleanup  --> Idle     : band goroutine sleeps until next trigger

    note right of Writing
        ordered=false: DocumentDB
        parallelises upserts
        within the batch
    end note
```

The time trigger caps maximum DocumentDB staleness at the `FlushWindow` value. The volume trigger fires immediately when the dirty queue reaches `MaxBatchSize`, preventing Redis memory growth during traffic spikes without waiting for the timer.

---

## At-least-once delivery and crash safety

sluice provides **at-least-once delivery** with **idempotent upsert semantics** on the sink:

```mermaid
sequenceDiagram
    participant E  as Flush engine
    participant R  as Redis
    participant D  as DocumentDB

    E  ->> R  : ZRANGEBYSCORE dirty:{band} LIMIT 0 1000
    R  -->> E : [crn_001, crn_002, crn_003, ...]

    E  ->> R  : pipeline HMGET payload for each key
    R  -->> E : [payload_001, payload_002, payload_003, ...]

    E  ->> D  : BulkWrite [upsert crn_001, upsert crn_002, ...]
    D  -->> E : {upsertedCount: 3}

    E  ->> R  : ZREM dirty:{band} crn_001 crn_002 crn_003
    R  -->> E : 3

    Note over E,R: ZREM happens AFTER confirmed BulkWrite.
    Note over E,R: Crash between BulkWrite and ZREM causes re-flush.
    Note over E,D: Upsert semantics make re-flush safe — idempotent.
```

Keys are only removed from the dirty set after DocumentDB confirms the write. If the flusher crashes between a successful `BulkWrite` and the `ZREM`, those keys survive in Redis and are re-flushed on the next cycle. Because every sink operation is an `upsert` (not an `insert`), re-flushing the same key is always safe — the last write wins.

---

## Degraded mode — Redis outage handling

When Redis is unavailable, sluice falls back to a direct single-document write path rather than dropping data silently:

```mermaid
flowchart TD
    W[sluice.Write called]
    RT{Redis\navailable?}
    RS[HSET + ZADD\nfast path]
    DC{DegradedMode\nDirect = true?}
    DW[Apply WriteContract\nimmediately\ncall sink.Write directly]
    ER[Return\nErrRedisUnavailable]
    ACK[Return nil\nACK to caller]

    W  --> RT
    RT -->|yes| RS --> ACK
    RT -->|no|  DC
    DC -->|yes| DW --> ACK
    DC -->|no|  ER

    style RS fill:#E1F5EE,stroke:#0F6E56,color:#085041
    style DW fill:#FAEEDA,stroke:#BA7517,color:#633806
    style ER fill:#FCEBEB,stroke:#A32D2D,color:#791F1F
```

In degraded mode, writes bypass Redis and batching entirely — they are slower (no BulkWrite) but data is never silently dropped. The `MetricsRecorder.RecordDegradedWrite` hook fires on every fallback write, making Redis outages immediately visible in your monitoring system.

---

## Scale envelope

| Metric | Value |
|---|---|
| Sustained ingest | 10K events/sec |
| Peak spike | 100K events/sec |
| Unique CRNs at peak (wide-breadth) | ~80–90K/sec |
| Redis resident keys (transit buffer) | ~25K at peak |
| DocumentDB BulkWrite calls/sec | ~100–130 |
| I/O reduction vs individual writes | **~1,000×** |
| Flush window (max DocumentDB lag) | 250ms (configurable) |
| Crash recovery | at-least-once via Redis journal |

---

## Architecture — full system view

```mermaid
flowchart TD
    subgraph "Ingest layer"
        SQS([SQS Queue])
        KAF([Kafka Topics\n16 partitions by CRN])
    end

    subgraph "Consumer layer"
        CW1[Consumer Worker 1]
        CW2[Consumer Worker 2]
        CWN[Consumer Worker N]
    end

    subgraph "sluice library"
        direction TB
        subgraph "Redis cluster — write journal"
            PH["sl:{ns}:payload:{crn}\nHSET — opaque bytes\nTTL: 30s"]
            DS["sl:{ns}:dirty:{0..15}\nZSET — score = event timestamp\n16 band partitions"]
        end

        subgraph "Flush engine — 16 band goroutines"
            TT[Time trigger\n250ms window]
            VT[Volume trigger\nMaxBatchSize threshold]
            DR[DrainBand\nZRANGEBYSCORE + pipeline HMGET]
            WC[WriteContract\ncaller-supplied · domain logic here]
            BW[BulkWrite assembly\nordered=false]
        end
    end

    subgraph "Document store"
        DB[(AWS DocumentDB\nnudge_inventory collection\n_id = CRN)]
    end

    SQS --> CW1 & CW2 & CWN
    KAF --> CW1 & CW2 & CWN

    CW1 & CW2 & CWN -->|sluice.Write\nreturns immediately| PH
    PH --> DS

    TT & VT --> DR
    DS --> DR
    DR --> WC
    WC --> BW
    BW --> DB

    style PH fill:#FAEEDA,stroke:#BA7517,color:#633806
    style DS fill:#FAEEDA,stroke:#BA7517,color:#633806
    style DB fill:#FAECE7,stroke:#993C1D,color:#712B13
    style TT fill:#E1F5EE,stroke:#0F6E56,color:#085041
    style VT fill:#E1F5EE,stroke:#0F6E56,color:#085041
```

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

// 1. Connect to DocumentDB (or MongoDB)
sk, _ := docdb.New(ctx, docdb.DefaultConfig(
    "mongodb://user:pass@cluster.docdb.amazonaws.com:27017/?tls=true&replicaSet=rs0",
    "adroll",
    "nudge_inventory",
))

// 2. Define your WriteContract — the only domain logic sluice needs.
//    Called once per unique CRN per flush cycle, never on Write().
contract := func(crn string, payload []byte) (*sluice.WriteModel, error) {
    var doc map[string]any
    json.Unmarshal(payload, &doc)
    return &sluice.WriteModel{
        Filter: bson.D{{"_id", crn}},
        Update: bson.D{{"$set", doc}},
        Upsert: true,
    }, nil
}

// 3. Build
s, _ := sluice.New("nudge_inventory").
    WithRedis(sluice.RedisConfig{Addrs: []string{"redis:6379"}}).
    WithSink(sk).
    WithWriteContract(contract).
    WithFlushWindow(250 * time.Millisecond).
    WithMaxBatchSize(1000).
    WithBandCount(16).
    Build(ctx)

defer s.DrainAndClose(ctx)

// 4. Hot path — one call from any SQS/Kafka consumer goroutine.
//    DocumentDB is never touched here.
s.Write(ctx, crn, payload)
```

---

## Configuration

| Builder method | Default | Description |
|---|---|---|
| `WithFlushWindow(d)` | `250ms` | Maximum age of a dirty key before flush — caps DocumentDB staleness |
| `WithMaxBatchSize(n)` | `1000` | Keys per BulkWrite call; also the volume trigger threshold |
| `WithBandCount(n)` | `16` | Parallel flush goroutines — one per dirty-set partition |
| `WithKeyTTL(d)` | `30s` | Redis key safety TTL — self-cleaning crash recovery net |
| `WithDegradedModeDirect(bool)` | `true` | Fall back to single-doc writes when Redis is unavailable |
| `WithMetrics(m)` | noop | Plug in Prometheus, Datadog, or CloudWatch |
| `OnFlush(cb)` | nil | Callback invoked after every BulkWrite attempt |

---

## Pluggable sinks

Implement `sink.FlushSink` to target any document store:

```go
type FlushSink interface {
    BulkWrite(ctx context.Context, models []WriteModel) (*sluice.BulkWriteResult, error)
    Write(ctx context.Context, model WriteModel) error
    Ping(ctx context.Context) error
    Close(ctx context.Context) error
}
```

Included implementations:

| Package | Target |
|---|---|
| `sink/docdb` | AWS DocumentDB · MongoDB |
| `sink/mock` | In-memory sink for unit and integration tests |

---

## Running tests

```bash
# Unit tests — Redis started automatically via Docker
make test-unit

# Full integration stack: Redis + MongoDB + Kafka + LocalStack (SQS)
make test-integration

# Targeted suites
make test-integration-sqs
make test-integration-kafka

# Everything, then tear down
make test-all

# Pre-commit gate: tidy + vet + lint + unit tests
make check
```

## License

MIT — see [LICENSE](LICENSE).