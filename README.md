# exec-mcp

A dependency-free Node.js prototype for a bounded streaming exec gateway.

It is intentionally small: one exec capability, SSE output, hard limits, tail summaries, process-group cleanup, and Prometheus-style metrics.

See `DESIGN.md` for the validation plan.

## Commands

```bash
npm test
npm run build
npm run validate
npm start
```

## Endpoints

- `GET /healthz`
- `GET /metrics`
- `POST /exec` with `Accept: text/event-stream`

## Validation status

Current local validation:

```text
node --test: 27 tests, 27 pass
scripts/validate.sh: pass
scripts/memory-smoke.sh: pass
```

Memory smoke writes 5 MB from a child process while forwarding only 1 KiB plus summary/tail metadata. On this host, RSS increased from about 59 MiB to about 63 MiB.
