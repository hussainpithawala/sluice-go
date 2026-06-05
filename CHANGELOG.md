# Changelog

All notable changes to **sluice** will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
