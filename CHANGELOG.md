# Changelog

All notable changes are documented here. The project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Prometheus execution-duration histogram grouped by final state.
- Maximum-concurrency gauge and lifecycle-log duration metadata for operational dashboards.

## [0.3.0] - 2026-07-18

### Added

- Operator-wide `list_active_execs`, `get_exec_status`, and idempotent `cancel_exec` MCP tools.
- MCP cancellation notification and HTTP disconnect propagation.
- Bounded recent execution history with command fingerprints and sanitized metadata.
- Emergency execution circuit breaker for unconfirmed transport cleanup.
- Structured MCP output schemas for command and file tools.
- Binary-safe base64 file upload and download with realpath and size validation.
- Lifecycle, cancellation, history, and circuit-breaker metrics.
- Security policy, threat model, contribution guide, CodeQL, Dependabot, and branch protection.

### Changed

- Unified all public project identifiers on Semantic Version `0.3.0`; removed internal architecture labels from the public interface and documentation.
- Replaced deployment-specific hostnames, paths, defaults, and SSH host keys with neutral examples.
- Expanded documentation for security boundaries, cancellation limits, release operations, and failure recovery.
- Updated CI to current supported GitHub Action major versions.

### Security

- Documented the mandatory external authentication/TLS boundary and trusted single-tenant model.
- Defaulted SSH host-key checking to strict mode and neutral secret paths.
- Removed a tracked deployment-specific `known_hosts` file.

[Unreleased]: https://github.com/3011/exec-mcp/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/3011/exec-mcp/releases/tag/v0.3.0
