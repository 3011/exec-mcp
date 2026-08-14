# Changelog

All notable changes are documented here. The project follows [Semantic Versioning](https://semver.org/).

## [0.6.2] - 2026-08-14

### Changed

- Clarified MCP tool-selection guidance so agents use synchronous `exec` for short, result-dependent commands and asynchronous `start_exec` for long, uncertain, or parallelizable work.
- `start_exec` now explicitly guides agents to continue independent work and defer `get_exec_status` polling until a synchronization point.
- Documented the same `exec` versus `start_exec` selection rules in the public README and clarified immutable versus moving container tags.
- Added tag-driven GitHub Release automation with changelog-derived notes plus an idempotent historical-release reconciliation path.

## [0.6.1] - 2026-08-14

### Fixed

- Fixed running cancellation so it terminates the remote command process group instead of relying on local SSH transport teardown.
- Replaced shell-builtin negative-PID process-group signalling with explicit `python3 os.killpg()` calls to avoid `/bin/sh` portability ambiguity.
- Added a per-job remote `/tmp/exec-mcp-runtime/<exec_id>` cancellation handshake and `remote_exit_confirmed` reporting; unconfirmed remote termination now finalizes as `failed` with `failure_reason=remote_termination_unconfirmed`.
- Updated the fake SSH harness to isolate the simulated remote shell from the local transport process group, preventing false-positive cancellation tests.
- Added a regression test matching the live failure mode where a cancelled background `(sleep; touch marker)` process previously survived.

## [0.6.0] - 2026-08-13

### Added

- Added `start_exec` for asynchronous background execution through a unified in-process Exec Job Manager.
- Added queued admission with independent sync, async, and global running-slot limits plus a bounded queue.
- Added incremental `get_exec_status` stdout/stderr reads with independent cursors, bounded long-polling, pagination indicators, and permanent-log-truncation indicators.
- Added bounded per-job retained logs, job retention/GC controls, queue/admission metrics, and graceful shutdown of active background jobs.

### Changed

- Refactored `exec` to submit through the same `ExecutionSpec` and runner lifecycle as `start_exec`, then wait internally for terminal completion while preserving the synchronous public result.
- Runtime timeout now starts when a queued job enters execution rather than while it waits for admission.
- `list_active_execs` now reports queued and running jobs together with sync/async/global capacity and queue position.
- `cancel_exec` terminalizes queued jobs immediately and preserves immutable terminal states; running cancellation continues to terminate the full process group.
- Job metadata, history, list output, and lifecycle logs never retain raw environment values; retained Job Manager output additionally redacts submitted environment values.

### Security

- Preserved bounded process-group termination and added explicit regression coverage for manual cancellation of asynchronous commands with background children.
- Added hard limits for queue length, retained job logs, status output pages, and `wait_seconds` (maximum 30 seconds).


## [0.5.1] - 2026-08-05

### Changed

- Hard-capped embedded remote exports at 1.45 MB (1,450,000 bytes); `ARTIFACT_EMBED_MAX_BYTES` may lower but cannot raise the ceiling.
- Updated the MCP tool description and project documentation to expose the 1.45 MB limit and `file_too_large` behavior explicitly.
- Set the operational embedded-resource ceiling to 1.45 MB to keep a conservative margin below the platform-sensitive range observed during ChatGPT ingestion/materialization.
- Updated boundary tests so exactly 1,450,000 bytes succeeds and 1,450,001 bytes is rejected, even when configuration attempts to request a higher value.

## [0.5.0] - 2026-08-05

### Removed

- Removed the legacy `download_file` and `upload_file` MCP tools and their JSON `data_base64` transport path.
- Removed remote-export `resource_link` delivery, public artifact GET/HEAD endpoints, capability tokens, fixed tool-container bridge URLs, and their TTL/download-count configuration.

### Changed

- Refined MCP titles and selection-oriented descriptions while preserving all six public tool names and the configured test-environment wording.
- Added strict active/history status schemas, cancellation outcome descriptions, and `delivery_mode` for exported files while retaining `embedded` for compatibility.
- Changed `export_remote_file` to return exactly one embedded MCP resource; files above the configured ceiling now fail instead of falling back to an external URL.
- Raised the default embedded-resource ceiling to 16 MiB and added exact-boundary backend tests for 16 MiB success and 16 MiB plus one byte rejection.
- Tightened titles, descriptions, annotations, and output schemas for all remaining MCP tools.
- Clarified import-host suffix matching and the security scope of broad rules such as `.blob.core.windows.net`.

## [0.4.0] - 2026-08-04

### Added

- `import_chatgpt_file` with ChatGPT `openai/fileParams` binding, bounded HTTPS download, SHA-256 verification, binary SSH streaming, and atomic remote commit.
- `export_remote_file` with raw SSH streaming, verified local spool, ChatGPT tool-result file references, and MCP HTTPS resource links.
- Short-lived 256-bit artifact capability URLs with download-count limits, HEAD, byte ranges, digest headers, and immutable file metadata.
- Artifact size, concurrency, timeout, host-allowlist, spool, and public-origin configuration.
- Random-binary end-to-end tests for both directions, idempotent retries, conflicting content, checksum rejection, and HTTP download behavior.
- Prometheus execution-duration histogram grouped by final state.
- Maximum-concurrency gauge and lifecycle-log duration metadata for operational dashboards.

### Changed

- Retained base64 `upload_file` and `download_file` as small-file compatibility tools rather than the primary artifact path.
- Generalized SSH process spawning so binary stdin/stdout transfers preserve backpressure.

### Security

- Remote artifact writes now use same-directory temporary files, fsync, SHA-256 validation, and atomic commit.
- Identical retries are idempotent; existing files with different content require explicit overwrite.
- Documented the separate public `/artifacts/` data-plane boundary required when Secure MCP Tunnel keeps MCP JSON-RPC private.

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

[Unreleased]: https://github.com/3011/exec-mcp/compare/v0.6.2...HEAD
[0.6.2]: https://github.com/3011/exec-mcp/compare/v0.6.1...v0.6.2
[0.6.1]: https://github.com/3011/exec-mcp/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/3011/exec-mcp/compare/v0.5.1...v0.6.0
[0.5.1]: https://github.com/3011/exec-mcp/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/3011/exec-mcp/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/3011/exec-mcp/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/3011/exec-mcp/releases/tag/v0.3.0
