# L1 Local Journal — Phase 1 Integration Tests

This suite validates the **Phase 1 (Lazy Mode)** implementation of the L1 Local Journal ([RFC-001](../../docs/local-journal.md)).

It proves the core L1 → L2 → L3 read cascade, the synchronous write-through contract, and the strict version-gating invariants using a dual-signal assertion strategy: **positive metric signals** (telemetry fires) and **negative network signals** (Redis I/O does *not* fire).

## Prerequisites

- A running Redis instance (local or Docker).
- Tests automatically isolate themselves by targeting **Redis DB 15** and executing `FLUSHDB` to guarantee a clean slate without polluting your default development database.

## Running the Tests

```bash
# Run all L1 integration tests with the race detector enabled
go test -race -v -run TestL1_ ./tests/integration/...

# Run against a custom Redis address
REDIS_ADDR=redis:6379 go test -race -run TestL1_ ./tests/integration/...
```

## Test Inventory & Contracts Proved

| Test Function | Contract Proved | Positive Signal | Negative Signal |
| :--- | :--- | :--- | :--- |
| `TestL1_WriteThenRead_HitsL1` | **Write-Through:** `Write()` synchronously seeds L1. Same-pod `Read()` hits L1 instantly. | `RecordLocalCacheHit` == 1 | `ReadJournal` (Redis) == 0 calls |
| `TestL1_SecondRead_AlsoHitsL1` | **Cache Stickiness:** Repeated reads stay in L1 memory. | `RecordLocalCacheHit` == N | `ReadJournal` (Redis) == 0 calls |
| `TestL1_OffMode_BehavesLikeV107` | **Zero Behavior Change:** `Mode=Off` bypasses L1 entirely, matching v1.0.7 exactness. | `ReadJournal` (Redis) == 1 | `RecordLocalCacheHit` == 0 |
| `TestL1_TTLExpiry_FallsThrough...` | **Lazy Heal:** Expired L1 entries fall through to L2, heal L1, and subsequent reads hit L1 again. | Miss reason == `"expired"`, post-heal Hit == 1 | Exactly 1 `ReadJournal` call (for the heal) |
| `TestL1_StaleVersion_Rejected...` | **Principle C3 (Version Gating):** `Put()` rejects older or equal versions to prevent state regression. | Newer version applies | Older/Equal versions return `false` |

## Architecture of the Test Spy

To prove that L1 is actually intercepting reads (and not just silently falling through to Redis), the tests use a `countingRecorder` that implements the embedded `localjournal.MetricsRecorder` interface.

It includes a `redisOpHook` that intercepts `RecordRedisOp` calls. If the `Sluice.Read()` method incorrectly calls `shield.ReadJournal()` when an L1 hit should have occurred, the hook increments the `redisReads` atomic counter, causing the negative signal assertion (`assert.Zero(t, redisReads)`) to fail immediately.

## Troubleshooting

- **Context Deadline Exceeded:** If tests fail with timeout errors, your local Redis instance may be hung or unreachable. Verify with `redis-cli -p 6379 PING`.
- **Cross-Slot Errors:** These tests do not use cluster-mode hash tags because they test the in-memory L1 cache and single-node shield mechanics. They are safe to run against standalone Redis or DB 15 of a cluster node.