import http from 'node:http';
import { once } from 'node:events';
import { randomUUID } from 'node:crypto';
import { parseConfig } from './config.js';
import { ExecRunner, ExecRejectedError } from './exec-runner.js';

export function createServer(config = parseConfig()) {
  const runner = new ExecRunner(config);

  const server = http.createServer(async (req, res) => {
    try {
      if (req.method === 'GET' && req.url === '/healthz') {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ ok: true, active_execs: runner.active }));
        return;
      }

      if (req.method === 'GET' && req.url === '/metrics') {
        res.writeHead(200, { 'content-type': 'text/plain; version=0.0.4' });
        res.end(renderMetrics(runner));
        return;
      }

      if (req.method === 'POST' && req.url === '/exec') {
        await handleSseExec(req, res, runner);
        return;
      }

      if (req.method === 'POST' && req.url === '/mcp') {
        await handleMcp(req, res, runner);
        return;
      }

      res.writeHead(404, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: 'not_found' }));
    } catch (err) {
      const status = err.statusCode || 500;
      res.writeHead(status, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: err.code || 'internal_error', message: err.message }));
    }
  });

  return { server, runner, config };
}

async function handleSseExec(req, res, runner) {
  const body = await readJson(req, 1024 * 1024);
  res.writeHead(200, {
    'content-type': 'text/event-stream; charset=utf-8',
    'cache-control': 'no-cache, no-transform',
    connection: 'keep-alive',
    'x-accel-buffering': 'no'
  });
  res.write(': connected\n\n');

  const abortController = new AbortController();
  let finished = false;
  const abortIfOpen = () => {
    if (!finished) abortController.abort();
  };
  req.on('close', abortIfOpen);
  res.on('close', abortIfOpen);

  const emit = (event) => writeSse(res, event.type || 'message', event);
  try {
    await runner.run(body, emit, { abortSignal: abortController.signal });
  } catch (err) {
    const code = err instanceof ExecRejectedError ? err.code : 'internal_error';
    writeSse(res, 'error', { type: 'error', code, message: err.message });
  } finally {
    finished = true;
    res.end();
  }
}

async function handleMcp(req, res, runner) {
  const body = await readJson(req, 1024 * 1024);
  const sessionId = req.headers['mcp-session-id'] || randomUUID();
  res.setHeader('mcp-session-id', sessionId);
  res.setHeader('content-type', 'application/json');

  if (Array.isArray(body)) {
    const out = [];
    for (const item of body) {
      const r = await handleMcpMessage(item, runner);
      if (r) out.push(r);
    }
    res.end(JSON.stringify(out));
    return;
  }

  const response = await handleMcpMessage(body, runner);
  if (!response) {
    res.statusCode = 202;
    res.end('{}');
    return;
  }
  res.end(JSON.stringify(response));
}

async function handleMcpMessage(msg, runner) {
  if (!msg || typeof msg !== 'object') return jsonError(null, -32600, 'Invalid Request');
  const id = msg.id ?? null;
  const method = msg.method;

  if (method === 'initialize') {
    return {
      jsonrpc: '2.0',
      id,
      result: {
        protocolVersion: msg.params?.protocolVersion || '2025-11-25',
        capabilities: { tools: { listChanged: true } },
        serverInfo: { name: 'exec-mcp', version: '0.2.1' }
      }
    };
  }

  if (method === 'notifications/initialized' || method === 'notifications/cancelled') {
    return null;
  }

  if (method === 'tools/list') {
    return { jsonrpc: '2.0', id, result: { tools: [execToolSchema()] } };
  }

  if (method === 'tools/call') {
    const name = msg.params?.name;
    if (name !== 'exec') return jsonError(id, -32602, `Unknown tool: ${name}`);
    const args = msg.params?.arguments || {};
    try {
      const events = [];
      const summary = await runner.run(args, (event) => events.push(event));
      const text = renderToolText(summary);
      return {
        jsonrpc: '2.0',
        id,
        result: {
          content: [{ type: 'text', text }],
          isError: summary.code !== 0 || summary.timed_out === true
        }
      };
    } catch (err) {
      const code = err instanceof ExecRejectedError ? err.code : 'internal_error';
      return {
        jsonrpc: '2.0',
        id,
        result: {
          content: [{ type: 'text', text: `${code}: ${err.message}` }],
          isError: true
        }
      };
    }
  }

  return jsonError(id, -32601, `Method not found: ${method}`);
}

function execToolSchema() {
  return {
    name: 'exec',
    description: 'Execute one shell command on the configured remote execution host and return a bounded final text result with stdout/stderr tails plus an exec summary. The command runs through /bin/sh -c after cwd allowlist validation. This tool is for general remote shell operations only; it is not a GitOps, Kubernetes, or file-management API. Output may be truncated. The final [exec summary] is authoritative for exit code, signal, timeout, duration, byte counts, and truncation. stderr output alone does not mean failure; non-zero exit code, signal, or timed_out=true means failure. Concurrency is bounded; too_many_active_execs means the active exec limit is currently reached and includes active/max/oldest_age_seconds/states for diagnosis.',
    inputSchema: {
      type: 'object',
      properties: {
        command: { type: 'string', description: 'Command string to run via /bin/sh -c on the configured remote execution host. Use explicit quoting for pipelines, redirection, &&, and environment expansion. Avoid interactive or unbounded long-running commands.' },
        cwd: { type: 'string', description: 'Working directory on the remote execution host. It must be under the configured allowlist. If omitted, the server uses DEFAULT_CWD.' },
        timeout_seconds: { type: 'integer', minimum: 1, description: 'Maximum runtime in seconds before the command is aborted. Values above MAX_TIMEOUT_SECONDS are rejected. On timeout the server sends SIGTERM, then SIGKILL after the configured kill grace period.' },
        max_output_bytes: { type: 'integer', minimum: 1, description: 'Maximum combined stdout/stderr bytes forwarded before truncation. The process is still drained until exit. The final summary includes byte counts, truncation status, and bounded stdout/stderr tails.' },
        env: { type: 'object', additionalProperties: { type: 'string' }, description: 'Additional environment variables for the command. Invalid variable names are ignored. ENV and BASH_ENV are removed before spawning.' }
      },
      required: ['command'],
      additionalProperties: false
    }
  };
}

function renderToolText(summary) {
  let text = '';
  if (summary.stdout_tail) text += summary.stdout_tail;
  if (summary.stderr_tail) {
    if (text && !text.endsWith('\n')) text += '\n';
    text += summary.stderr_tail;
  }
  const meta = `\n[exec summary] exit=${summary.code} signal=${summary.signal || 'null'} duration_ms=${summary.duration_ms} stdout_bytes=${summary.stdout_bytes} stderr_bytes=${summary.stderr_bytes} truncated=${summary.truncated} timed_out=${summary.timed_out}`;
  return text ? text + meta : meta.trimStart();
}

function jsonError(id, code, message) {
  return { jsonrpc: '2.0', id, error: { code, message } };
}

async function readJson(req, maxBytes) {
  let size = 0;
  const chunks = [];
  for await (const chunk of req) {
    size += chunk.length;
    if (size > maxBytes) {
      const err = new Error('request body too large');
      err.statusCode = 413;
      err.code = 'body_too_large';
      throw err;
    }
    chunks.push(chunk);
  }
  if (chunks.length === 0) return {};
  try {
    return JSON.parse(Buffer.concat(chunks).toString('utf8'));
  } catch {
    const err = new Error('invalid JSON body');
    err.statusCode = 400;
    err.code = 'invalid_json';
    throw err;
  }
}

function writeSse(res, event, payload) {
  res.write(`event: ${event}\n`);
  res.write(`data: ${JSON.stringify(payload)}\n\n`);
}

function renderMetrics(runner) {
  const lines = [];
  lines.push('# HELP exec_active Number of active exec calls');
  lines.push('# TYPE exec_active gauge');
  lines.push(`exec_active ${runner.active}`);
  lines.push('# HELP exec_requests_total Total exec calls');
  lines.push('# TYPE exec_requests_total counter');
  lines.push(`exec_requests_total ${runner.metrics.requestsTotal}`);
  lines.push(`exec_timeout_total ${runner.metrics.timeoutTotal}`);
  lines.push(`exec_truncated_total ${runner.metrics.truncatedTotal}`);
  lines.push(`exec_stream_disconnect_total ${runner.metrics.streamDisconnectTotal}`);
  lines.push(`exec_output_bytes_total{stream="stdout"} ${runner.metrics.outputBytesTotal.stdout}`);
  lines.push(`exec_output_bytes_total{stream="stderr"} ${runner.metrics.outputBytesTotal.stderr}`);
  for (const [reason, count] of runner.metrics.rejectedTotal.entries()) {
    lines.push(`exec_rejected_total{reason="${escapeLabel(reason)}"} ${count}`);
  }
  for (const [signal, count] of runner.metrics.killedTotal.entries()) {
    lines.push(`exec_killed_total{signal="${escapeLabel(signal)}"} ${count}`);
  }
  for (const [code, count] of runner.metrics.exitCodeTotal.entries()) {
    lines.push(`exec_exit_code_total{code="${escapeLabel(code)}"} ${count}`);
  }
  if (process.memoryUsage) lines.push(`process_resident_memory_bytes ${process.memoryUsage().rss}`);
  return lines.join('\n') + '\n';
}

function escapeLabel(value) {
  return String(value).replaceAll('\\', '\\\\').replaceAll('"', '\\"');
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const { server, runner, config } = createServer();
  server.listen(config.port, config.host, async () => {
    const address = server.address();
    console.error(`exec-mcp listening on ${address.address}:${address.port}`);
  });


  const metricsPort = Number.parseInt(process.env.METRICS_PORT || "0", 10);
  let metricsServer;
  if (metricsPort > 0) {
    metricsServer = http.createServer((req, res) => {
      if (req.method === "GET" && req.url === "/metrics") {
        res.writeHead(200, { "content-type": "text/plain; version=0.0.4" });
        res.end(renderMetrics(runner));
        return;
      }
      if (req.method === "GET" && req.url === "/healthz") {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ ok: true, active_execs: runner.active }));
        return;
      }
      res.writeHead(404, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "not_found" }));
    });
    metricsServer.listen(metricsPort, config.host, () => {
      const address = metricsServer.address();
      console.error();
    });
  }

  const shutdown = async (signal) => {
    console.error(`received ${signal}, shutting down`);
    server.close();
    if (metricsServer) metricsServer.close();
    try { await once(server, 'close'); } catch {}
    process.exit(0);
  };
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}
