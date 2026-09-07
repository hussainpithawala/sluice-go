# Changelog

All notable changes to **sluice** will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


- [1.0.7] - 2026-09-07
### Added
- Hot/Cold CRN Regimes: Introduced HotLoad(), Read(), and IsHot() for activity-driven TTL management. Active CRNs are pinned in the Redis journal with an extended ActivityWindow TTL (default 4h) for sub-millisecond reads, while inactive CRNs gracefully fall back to the backing Source.
- Source Abstraction: New source.Source interface and source/docdb implementation. The read path is now fully decoupled from the write path, mirroring the sink.FlushSink pattern. NewSourceWithClient() enables connection pool sharing between sink and source.
- Queryable Journal: Added IndexContract and Query() API for compound lookups against the Redis journal. Maintains secondary equality (SET) and range (ZSET) indexes, resolved safely in Cluster Mode via band-scoped SINTER and in-process range filtering.
- Exactly-Once Delivery: Added WriteIdempotent() backed by band-scoped Redis SETNX EX (key: sl:{ns}:idem:{band}:{key}) to safely handle Kafka/SQS message replays without polluting the dirty queue.
- Content Deduplication: Added WithContentDedup(true) builder option to enable xxHash64 payload fingerprinting via a dedicated Valkey-safe Lua script (atomicDedupWriteLua). Identical payloads only refresh the Redis TTL and skip redundant dirty-set queuing and sink writes.
- DLQ Auto-Processor: Added WithDLQAutoProcess(interval, strategy) builder option to run a background ticker that periodically drains and processes the Dead Letter Queue using configurable strategies (Ignore, Upsert, ReInsert).
- Pre-Eviction Flusher: Added a background engine goroutine that monitors OldestDirtyScore() per band and force-flushes before Redis KeyTTL expires the payload, preventing silent data loss during extreme ingest spikes.
- Hot Marker Keys: Introduced sl:{ns}:hot:{band}:{crn} Redis keys with SET NX EX ActivityWindow semantics. IsHot() is a single EXISTS call; HotLoad() sets this marker alongside the payload write.
- Metrics Expansion: Added RecordWarmUp, RecordRead, and RecordHotSetSize hooks to the MetricsRecorder interface for comprehensive hot/cold regime observability.
- ReadWithTTL Pipeline: Added ReadWithTTL() to the shield layer, fetching both payload and remaining TTL in a single Redis pipeline round-trip for lazy TTL refresh decisions.

### Changed
- ReadContract Signature: Updated ReadContract from func(correlationKey string) ([]byte, error) to func(correlationKey string) (*source.ReadModel, error). The caller now returns a datastore-agnostic filter; the Source executes the query. This eliminates direct mongo.Collection references from domain contracts.
- Lazy TTL Refresh: Replaced fire-and-forget goroutines in the read path with a synchronous, lazy PTTL threshold check. If remaining TTL falls below 20% of ActivityWindow, the TTL is refreshed inline. This prevents goroutine storms at 100K+ TPS reads.
- Index Maintenance Pipeline: Shifted secondary index updates out of the atomic Lua script and into a Go-side Redis Pipeline (UpdateIndexes()). This eliminates the cjson dependency and guarantees compatibility with Valkey and OSS Redis forks where cjson is unavailable.
- Domain Leakage Removal: Removed WriteOptions (containing ForceHot, ContentHash, DedupEnabled, IndexesJSON) from the shield layer. The shield is now strictly infrastructure — hot/cold regime decisions, dedup hashing, and index extraction are orchestrated by the Sluice struct.
- HotLoad TTL Extension: HotLoad() now extends the payload hash TTL to ActivityWindow and sets the hot marker, rather than relying on the default KeyTTL (30s).
- Type Consolidation: Centralized all public types, interfaces, and contracts (WriteContract, ReadContract, IndexContract, WriteModel, BulkWriteResult, SinkError, Query, QueryResult, DLQStrategy, DLQResult) into types.go for a cleaner, more discoverable API surface.
- Error Mapping: Read() and HotLoad() now map an internal source.ErrRecordNotFound to the public sluice.ErrRecordNotFound sentinel, preventing internal package error leakage.

### Fixed
- Cluster Mode Safety: Secondary index keys (sl:{ns}:idx:{band}:field:value), range index keys (sl:{ns}:ridx:{band}:field), idempotency keys (sl:{ns}:idem:{band}:{key}), and hot marker keys (sl:{ns}:hot:{band}:{crn}) now correctly embed {band} hash tags, completely eliminating CROSSSLOT errors in Redis Cluster and Valkey Cluster environments.
- Lua Type Mismatches: Formatted millisecond timestamps as strings (fmt.Sprintf("%.0f", ts)) before passing to Lua scripts in both Write() and flushBatch(), resolving ERR Lua redis lib command arguments must be strings or integers errors that caused silent write failures in Redis/Valkey.
- WriteDedup Script Binding: Fixed WriteDedup() which was incorrectly using atomicWriteLua (4-arg script) instead of the dedicated atomicDedupWriteLua (5-arg script with hash comparison). Deduplication was previously non-functional.
- Test Isolation (Redis): Fixed the cleanRedisKeys glob pattern from fmt.Sprintf("sl:%s:", ns) to fmt.Sprintf("sl:%s:*", ns), enabling proper key cleanup between test runs and eliminating ErrDuplicateIdempotencyKey false positives.
- Test Isolation (Kafka): Implemented unique Kafka topic and group ID generation per test run (fmt.Sprintf("topic-%d", time.Now().UnixNano())) to eliminate offset pollution and OffsetOutOfRange errors in CI/CD pipelines.
- Goroutine Leaks in Tests: Added case <-ctx.Done(): return to SQS and Kafka consumer loops alongside stop channels, preventing orphaned goroutines from triggering 5-minute global test timeouts.
- Unexported Struct Fields: Fixed sqsEvent.CorrelationKey being lowercase (correlation_key), which caused json.Marshal to silently omit the field and all SQS messages to arrive with empty correlation keys.
- Return Type Mismatch: Fixed buildIntegrationSluice returning docdb.Sink (value) instead of *docdb.Sink (pointer), resolving compile errors in integration tests.

## [1.0.2] - 2026-08-21
### Added
- **Explicit Cluster Mode Support**: Introduced `ClusterMode` boolean field to `RedisConfig` across root and `shield` packages to explicitly select between standalone (`redis.Client`) and cluster-aware (`redis.ClusterClient`) go-redis clients.
- **Valkey Cluster Testbed**: Added a 4-shard Valkey 9 cluster environment (`valkey-node-0..3` on ports 7001–7004) and initialization service (`valkey-cluster-init`) to `docker-compose.yml` for testing band/shard distribution.
- **Verification Script**: Added `scripts/verify_bands.sh` to check and validate whether sluice band hash-tags map to distinct master shards in Redis/Valkey clusters.
- **Exported Key Utilities**: Exported `BandForKey`, `PayloadKey`, `DirtyKey`, and `DLQKey` in `internal/shield` as single sources of truth for on-wire key formatting and testing assertions.

### Changed
- **Redis Key Naming Strategy**: Updated payload key formatting to include `{band}` hash tags (`sl:<namespace>:payload:{<band>}:<correlationKey>`). This ensures payload hashes and dirty/DLQ sorted sets co-locate on the same Redis cluster slot, preventing cross-slot errors in cluster mode.
- **Config Address & Credentials Plumbing**:
  - Updated `RedisConfig` address field (`Addrs`) to accept multiple endpoints without forcing cluster mode inference.
  - Fixed `toInternal()` in `config.go` to properly forward `Network`, `ClusterMode`, and `Username` parameters to `shield.RedisConfig`.
- **Example Runner Refactoring**: Updated `examples/nudge/main.go` to support cluster configuration via environment variables (`REDIS_ADDRS`, `REDIS_CLUSTER_MODE`), improved shutdown lifecycle error handling, and robust argument validation.

### Fixed
- **ACL Authentication Silent Fallback**: Fixed an issue in `shield.New()` where `Username` was previously dropped during config conversion, causing connections to fallback silently to the `default` user.
- **Cross-Slot Execution in Lua Scripts**: Resolved potential `CROSSSLOT` script execution errors by strictly enforcing band hash tag co-location on all related keys in `atomicWriteLua`.
- **Flaky Integration Tests**: Fixed race conditions in DLQ auto-processor integration tests (`tests/integration/dlq_auto_processor_test.go`) by adding `dlqProcessCounter` metrics tracking instead of polling transient Redis sorted-set depths.
- **Namespace Validation**: Added upfront validation in `shield.New()` to reject namespaces containing `{` or `}` characters that would conflict with Redis Cluster hash tag parsing.

---
## [1.0.1] - 2026-06-05

### Added
- **Opt-in High-Velocity Redis Write Batching**: Introduced an in-memory buffered pipeline layer for `Sluice.Write()` to combine multiple writes into a single Redis round-trip. This drops network traversal overhead from $N$ to $1$, making the library highly optimized for streams exceeding 10K writes/sec. Enable it via the new `.WithBatchedWrites(size, window)` builder option.
- **Pre-loaded Lua Script Optimization**: Shifted pipeline storage to use `EVALSHA` (`pipe.EvalSha`) by caching script hashes at client startup. This reduces massive TCP payload block overhead down to a 40-byte identifier per pipeline item.
- **Pipelined Volume Check Piggybacking**: Unified the band queue depth evaluation (`ZCARD`) directly behind the same pipeline payload as the batch writes. The engine retains automated volume draining triggers without adding sequential network penalties.
- **Lifecycle Integration**: Integrated background batch worker processes directly with the application's root context topology (`context.Context`). A SIGTERM or explicit service teardown safely intercepts the runtime loop to flush any remaining in-flight memory elements before close.
- **Data-Race Safety**: Added explicit inner slice memory deep-copy allocations during worker handoffs to guarantee full pointer separation from incoming stream appends.
- **Observability Enhancement**: Replaced silent error drops during pipeline executions with structural diagnostics leveraging structured `log/slog` reporting.
---
## [1.0.0] - 2026-06-05

### Added
- **DLQ Management Engine**: Introduced a high-level orchestration API (`Sluice.ProcessDLQ`) to inspect, drain, and clear records from the Dead Letter Queue.
- **Transactional DLQ Operations**: Added batch-oriented storage methods (`DrainDLQ`, `CommitDLQKeys`) inside the `shield` layer utilizing Redis pipelines (`HMGet`, `ZRem`, `Del`) for atomic state management.
- **Recovery Strategies**: Implemented three foundational recovery behaviors via `DLQStrategy`:
  - `DLQIgnore`: Logs and safely drops dead-letter records.
  - `DLQUpsert`: Re-runs the core execution contract with `Upsert=true` for data reconciliation.
  - `DLQReInsert`: Mutates correlation keys using a safety timestamp suffix (`DefaultKeyMutator`) and moves them back to the active execution queue.
- **Telemetry & Monitoring**: Added the `RecordDLQProcess` telemetry hook to the `MetricsRecorder` interface, supported by structured logger updates and placeholder `noop` implementations.
- **Kafka Readiness Probes**: Added a dedicated `_wait-kafka` recipe inside the `Makefile` utilizing `kafka-broker-api-versions` with a 60-second execution safety cutoff.

### Fixed
- **Kafka Topic Race Conditions**: Resolved intermittent `Unknown Topic Or Partition` test breaks by engineering an explicit `require.Eventually` barrier that polls broker partition metadata via `ReadPartitions` before proceeding.
- **Example Crash Path**: Remedied a critical fallback flaw in `examples/nudge/main.go` where initialization connectivity errors failed to halt the runtime environment, ensuring it now correctly exits with `os.Exit(1)`.

### Changed
- **Linter Engine Update**: Replaced `golangci/golangci-lint-action@v6` inside the CI matrix with an explicit `curl`-based binary installation targeting `v2.4.0` directly to patch local/remote caching drift.
- **Test Infrastructure Stability**: Appended `--health-start-period 15s` to the MongoDB service setup in `.github/workflows/ci.yml` and a `start_period: 30s` to Kafka inside `docker-compose.yml` to gracefully account for intensive startup initialization delays.
- **Linter Simplification**: Migrated `.golangci.yml` to configuration `version: "2"`, purged the deprecated `disable-all` flag, and streamlined overall custom rule definitions in favor of standard system defaults.
- **Makefile Color Modernization**: Swapped rigid ANSI hardcoded color escapes across the build layer with platform-agnostic, portable `tput` terminal sequences.

### Removed
- **Dependency Hygiene**: Cleaned up duplicate, stale `github.com/redis/go-redis/v9 v9.5.1` entries from `go.sum`.

## [v1.0.0-alpha.1] - 2026-03-27

### 🚀 New Features

- **Initial release of sluice** — A wide-breadth Redis-shielded write batcher for document stores
  - Redis as velocity shield for high-velocity write absorption
  - Band partitioning with FNV-32a hashing (default 16 bands)
  - Dual-trigger flush: time-based (250ms) or volume-based (batch size threshold)
  - Degraded mode for direct-to-sink writes when Redis unavailable
  - Pluggable metrics recorder for Prometheus/Datadog/CloudWatch
  - Graceful shutdown with `DrainAndClose()`

### 🔧 Improvements

- **Cyclic import resolution** — Refactored internal package structure to eliminate import cycles
  - Moved shared types to `sink` package (lowest dependency)
  - Created internal types in `internal/shield` and `internal/engine`
  - Added type conversion wrappers in main package

- **Makefile enhancements**
  - Added `install-releaser` target for goreleaser auto-installation
  - Added `release` target for full GitHub release creation
  - Added `release-check` target for `.goreleaser.yml` validation
  - Simplified integration tests (Redis + MongoDB only)
  - Removed Kafka/LocalStack dependencies

- **Documentation updates**
  - Enhanced README with Mermaid diagrams showing:
    - MongoDB Atlas vs AWS DocumentDB write path comparison
    - Write spike handling differences (single primary bottleneck)
    - Sluice architecture and solution pattern
  - Added detailed configuration examples
  - Updated testing instructions

- **CI/CD improvements**
  - Updated GitHub Actions workflows (ci.yml, release.yml)
  - Added MongoDB service to unit test jobs
  - Updated golangci-lint to v1.64.8 (compatible with v1 config format)
  - Fixed `.goreleaser.yml` deprecation warnings (`format` → `formats`)

### 🧹 Cleanup

- **Removed mock sink package** — Tests now use real `docdb.Sink` against actual MongoDB
  - Simplified test infrastructure
  - More realistic test coverage
  - Reduced maintenance burden

- **Removed unused services**
  - Kafka integration tests (no Kafka sink implementation)
  - SQS integration tests (no SQS sink implementation)
  - LocalStack dependencies

### 📦 Sinks

- **DocumentDB/MongoDB** (`sink/docdb`)
  - Bulk upserts to AWS DocumentDB or MongoDB
  - Connection pooling with configurable min/max pool sizes
  - Automatic retry on transient failures
  - Comprehensive error handling for bulk write exceptions

### 🔒 Security

- Go module dependencies pinned to specific versions
- CGO disabled for cross-platform builds
- No sensitive data logged or exposed

---

## Legend

- **Added** — New features or functionality
- **Changed** — Changes in existing functionality
- **Deprecated** — Soon-to-be removed features
- **Removed** — Removed features
- **Fixed** — Bug fixes
- **Security** — Security improvements
- **Improvements** — Non-breaking enhancements
- **Cleanup** — Code cleanup and refactoring

---

**Links:**
- [v1.0.0-alpha.1](https://github.com/hussainpithawala/sluice-go/releases/tag/v1.0.0-alpha.1)
