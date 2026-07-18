# Contributing

Contributions are welcome through GitHub issues and pull requests.

## Development requirements

- Node.js 20 or newer.
- npm.
- Docker for container validation.
- No live SSH host is required for the test suite; tests use the included fake SSH transport.

## Setup

```bash
git clone https://github.com/3011/exec-mcp.git
cd exec-mcp
npm ci
npm run validate
```

## Pull requests

1. Keep changes focused and explain the operational or security boundary affected.
2. Add or update tests for behavior changes.
3. Update README, design, runbook, threat model, and changelog entries when relevant.
4. Preserve bounded memory, timeout, path-validation, and finalization guarantees.
5. Do not add internal hostnames, IP addresses, credentials, private paths, or deployment-specific secrets.
6. Ensure `npm run validate` passes before opening the pull request.

## Design principles

- Keep the gateway transport-focused and domain-neutral.
- Treat arbitrary command execution as an explicit trusted-caller capability.
- Reject invalid inputs before spawning work.
- Never buffer unbounded command or file output.
- Never release execution capacity before runner finalization.
- Prefer fail-closed behavior when execution lifecycle is uncertain.
- Do not expose raw commands, environments, or secrets in metrics and history.
- Maintain compatibility through Semantic Versioning rather than internal architecture labels.

## Commit style

Use concise imperative commit subjects, for example:

```text
Harden file path validation
Document remote process boundary
Add cancellation regression test
```

## Security reports

Do not report vulnerabilities in a public issue. Follow [SECURITY.md](SECURITY.md).
