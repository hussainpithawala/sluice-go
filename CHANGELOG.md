# Changelog

All notable changes to **sluice** will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

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
