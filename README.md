# sluice

[![Release](https://github.com/hussainpithawala/sluice-go/actions/workflows/release.yml/badge.svg)](https://github.com/hussainpithawala/sluice-go/actions/workflows/release.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/hussainpithawala/sluice-go.svg)](https://pkg.go.dev/github.com/hussainpithawala/sluice-go)
[![Go Version](https://img.shields.io/badge/go-1.25-blue)](https://go.dev)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> **Wide-breadth Redis-shielded write batcher for document stores.**
> Built for ad-tech platforms where millions of customers receive nudges, bids, and inventory updates
> at rates that no single document store primary can absorb directly.

---

## The problem

Modern ad-roll platforms process inventory update events at 10K–100K TPS from SQS queues and Kafka
topics. Each event is a single incoherent write — one document, one customer, one update — arriving
unbatched and uncoordinated. Sending each directly to AWS DocumentDB means:

- Every write hits the single primary node individually
- Every index multiplies the I/O cost per write
- Connection pools saturate under spikes (DocumentDB hard-caps connections per instance class)
- 100,000 events for 80,000 unique customers becomes 100,000 individual round-trips instead of ~100 BulkWrite calls

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

sluice introduces a **write journal in Redis** between your consumers and the document store.
Every event is written atomically to Redis — sub-millisecond — and acknowledged immediately.
Background goroutines drain the journal in configurable windows and assemble efficient `BulkWrite` calls.

**100,000 events/sec → ~100 BulkWrite calls/sec — a 1,000× reduction in document store I/O.**

```mermaid
flowchart TD
    SQS(["SQS Queue"])
    KAF(["Kafka Topics — partitioned by CRN hash"])
    CON["Consumer Workers — stateless, CRN-hash routed"]
    RED[("Redis Cluster — Velocity Shield")]
    ENG["Batch Flusher Engine — 16 bands, 250ms window"]
    DB[("AWS DocumentDB — Long-Term Store")]

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

## Redis as a write journal

The core innovation is treating Redis not as a cache but as a **durable write journal with
correlation-key deduplication**:

| Traditional cache | sluice Redis journal |
|---|---|
| Read-optimised — avoids re-computation | Write-optimised — absorbs write velocity |
| TTL = staleness budget for reads | TTL = crash-recovery safety net |
| Cache miss → go to DB | Journal miss → key already flushed (correct) |
| Eviction under pressure loses data | Deduplication under pressure is intentional |

For every event, sluice executes a single atomic Lua script — three Redis operations in one round-trip:

```mermaid
sequenceDiagram
    participant C as Consumer worker
    participant R as Redis Lua script
    participant DS as Dirty sorted set

    C->>R: EVALSHA atomicWrite(corrKey, payload, ts, ttl)
    activate R
    R->>R: HSET sl:ns:payload:corrKey  p=payload ts=ts
    R->>R: EXPIRE sl:ns:payload:corrKey ttl
    R->>DS: ZADD sl:ns:dirty:band score=ts member=corrKey
    R-->>C: 1 ACK
    deactivate R

    Note over C,DS: Single round-trip. Atomic. No partial state possible.
```

The `ZADD` score is the event timestamp — the flush engine always processes the oldest keys first,
giving natural ordering and a staleness bound equal to `FlushWindow`.

---

## The coalescing mechanism

Because `ZADD` on an existing member only updates the score, multiple events for the same CRN
collapse to **one dirty-set entry**. The `HSET` keeps the latest payload.

```mermaid
flowchart LR
    subgraph IN ["Events arriving (100K/sec)"]
        E1["crn_001 event 1"]
        E2["crn_002 event 1"]
        E3["crn_001 event 2"]
        E4["crn_003 event 1"]
        E5["crn_001 event 3"]
        E6["crn_002 event 2"]
    end

    subgraph RDS ["Redis dirty set after 250ms"]
        D1["crn_001 — score = latest ts"]
        D2["crn_002 — score = latest ts"]
        D3["crn_003 — score = ts"]
    end

    subgraph BW ["DocumentDB BulkWrite — 1 call"]
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

One goroutine per band wakes on two independent triggers, whichever fires first:

```mermaid
stateDiagram-v2
    [*] --> Idle
    Idle --> Draining : time trigger fires — 250ms elapsed
    Idle --> Draining : volume trigger fires — dirty queue >= MaxBatchSize
    Draining --> Reading : ZRANGEBYSCORE fetch dirty keys
    Reading --> Fetching : pipeline HMGET read all payloads in one round-trip
    Fetching --> Building : apply WriteContract per key
    Building --> Writing : BulkWrite to DocumentDB ordered=false
    Writing --> Cleanup : ZREM confirmed keys from dirty set
    Cleanup --> Idle : goroutine sleeps until next trigger
```

The time trigger caps DocumentDB staleness at `FlushWindow`. The volume trigger fires immediately
when the dirty queue reaches `MaxBatchSize`, preventing Redis memory pressure during spikes.

---

## At-least-once delivery and crash safety

```mermaid
sequenceDiagram
    participant E as Flush engine
    participant R as Redis
    participant D as DocumentDB

    E->>R: ZRANGEBYSCORE dirty:band LIMIT 0 1000
    R-->>E: crn_001, crn_002, crn_003 ...

    E->>R: pipeline HMGET payload for each key
    R-->>E: payload_001, payload_002, payload_003 ...

    E->>D: BulkWrite — upsert crn_001, upsert crn_002, upsert crn_003
    D-->>E: upsertedCount 3

    E->>R: ZREM dirty:band crn_001 crn_002 crn_003
    R-->>E: 3

    Note over E,R: ZREM happens AFTER confirmed BulkWrite.
    Note over E,R: Crash between BulkWrite and ZREM triggers re-flush.
    Note over E,D: Upsert semantics make re-flush safe — last write wins.
```

Keys are removed from the dirty set only after DocumentDB confirms the write. If the flusher crashes
after `BulkWrite` but before `ZREM`, those keys are re-flushed on the next cycle. Because every sink
operation is an upsert, re-flushing is always safe.

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
| Unique CRNs at peak (wide-breadth) | ~80–90K/sec |
| Redis resident keys (transit buffer) | ~25K at peak |
| DocumentDB BulkWrite calls/sec | ~100–130 |
| I/O reduction vs individual writes | **~1,000x** |
| Flush window (max DocumentDB lag) | 250ms (configurable) |
| Crash recovery | at-least-once via Redis journal |

---

## Architecture — full system view

```mermaid
flowchart TD
    subgraph Ingest ["Ingest layer"]
        SQS([SQS Queue])
        KAF(["Kafka Topics — 16 partitions by CRN"])
    end

    subgraph Consumers ["Consumer layer"]
        CW1[Consumer Worker 1]
        CW2[Consumer Worker 2]
        CWN[Consumer Worker N]
    end

    subgraph Library ["sluice library"]
        subgraph Journal ["Redis cluster — write journal"]
            PH["sl:ns:payload:crn — HSET opaque bytes — TTL 30s"]
            DS["sl:ns:dirty:0..15 — ZSET score=timestamp — 16 band partitions"]
        end
        subgraph Engine ["Flush engine — 16 band goroutines"]
            TT["Time trigger — 250ms window"]
            VT["Volume trigger — MaxBatchSize threshold"]
            DR["DrainBand — ZRANGEBYSCORE + pipeline HMGET"]
            WC["WriteContract — caller-supplied domain logic"]
            BW["BulkWrite assembly — ordered=false"]
        end
    end

    subgraph Store ["Document store"]
        DB[("AWS DocumentDB — nudge_inventory — _id = CRN")]
    end

    SQS --> CW1
    SQS --> CW2
    SQS --> CWN
    KAF --> CW1
    KAF --> CW2
    KAF --> CWN
    CW1 -->|"sluice.Write"| PH
    CW2 -->|"sluice.Write"| PH
    CWN -->|"sluice.Write"| PH
    PH --> DS
    TT --> DR
    VT --> DR
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

sk, _ := docdb.New(ctx, docdb.DefaultConfig(
    "mongodb://user:pass@cluster.docdb.amazonaws.com:27017/?tls=true&replicaSet=rs0",
    "adroll", "nudge_inventory",
))

contract := func(crn string, payload []byte) (*sluice.WriteModel, error) {
    var doc map[string]any
    json.Unmarshal(payload, &doc)
    return &sluice.WriteModel{
        Filter: bson.D{{"_id", crn}},
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

// Hot path — DocumentDB is never touched here
s.Write(ctx, crn, payload)
```

---

## Configuration

| Builder method | Default | Description |
|---|---|---|
| `WithFlushWindow(d)` | `250ms` | Maximum dirty key age before flush — caps DocumentDB staleness |
| `WithMaxBatchSize(n)` | `1000` | Keys per BulkWrite call; also the volume trigger threshold |
| `WithBandCount(n)` | `16` | Parallel flush goroutines — one per dirty-set partition |
| `WithKeyTTL(d)` | `30s` | Redis key safety TTL — self-cleaning crash recovery net |
| `WithDegradedModeDirect(bool)` | `true` | Fall back to single-doc writes when Redis is unavailable |
| `WithMetrics(m)` | noop | Plug in Prometheus, Datadog, or CloudWatch |
| `OnFlush(cb)` | nil | Callback invoked after every BulkWrite attempt |

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
| `sink/mock` | In-memory sink for unit and integration tests |

---

## Running tests

```bash
make test-unit              # unit tests — Redis auto-started via Docker
make test-integration       # full stack: Redis + MongoDB + Kafka + LocalStack
make test-integration-sqs   # SQS tests only
make test-integration-kafka # Kafka tests only
make test-all               # everything, then tear down
make check                  # pre-commit: tidy + vet + lint + unit tests
```

---

## Local development

```bash
make docker-up              # start all services

MONGO_URI=mongodb://localhost:27017 \
REDIS_ADDR=localhost:6379 \
go run ./examples/nudge/main.go

make docker-down
```

---

## Releasing

```bash
git push -u origin main
git tag v0.1.0 && git push origin v0.1.0
```

---

## License

MIT — see [LICENSE](LICENSE).
