import http from 'node:http';
import { once } from 'node:events';
import { randomUUID } from 'node:crypto';
import { extname } from 'node:path';
import { readFileSync } from 'node:fs';
import { parseConfig } from './config.js';
import { ExecRunner, ExecRejectedError, spawnRemoteShell } from './exec-runner.js';

const PACKAGE_VERSION = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8')).version;
const DEFAULT_MCP_MAX_REQUEST_BYTES = 16 * 1024 * 1024;
const DEFAULT_MAX_FILE_DOWNLOAD_BYTES = 10 * 1024 * 1024;
const DEFAULT_MAX_FILE_UPLOAD_BYTES = 10 * 1024 * 1024;

class FileToolError extends Error {
  constructor(code, message) {
    super(message);
    this.name = 'FileToolError';
    this.code = code;
  }
}

class McpRequestRegistry {
  constructor() { this.sessions = new Map(); }
  register(sessionId, requestId) {
    let session = this.sessions.get(sessionId);
    if (!session) {
      session = new Map();
      this.sessions.set(sessionId, session);
    }
    const key = typedRequestKey(requestId);
    if (session.has(key)) {
      const err = new Error('duplicate in-flight MCP request id');
      err.code = 'duplicate_request_id';
      throw err;
    }
    const record = {
      requestId,
      typedRequestId: key,
      abortController: new AbortController(),
      execId: null,
      createdAt: Date.now(),
      cancelSource: null,
      completed: false
    };
    session.set(record.typedRequestId, record);
    return record;
  }
  get(sessionId, requestId) { return this.sessions.get(sessionId)?.get(typedRequestKey(requestId)) || null; }
  remove(sessionId, requestId) {
    const session = this.sessions.get(sessionId);
    if (!session) return false;
    const removed = session.delete(typedRequestKey(requestId));
    if (session.size === 0) this.sessions.delete(sessionId);
    return removed;
  }
  get size() {
    let size = 0;
    for (const session of this.sessions.values()) size += session.size;
    return size;
  }
}

function typedRequestKey(requestId) {
  if (typeof requestId === 'number') return `number:${requestId}`;
  if (typeof requestId === 'string') return `string:${requestId}`;
  return `${typeof requestId}:${JSON.stringify(requestId)}`;
}

export function createServer(config = parseConfig()) {
  const runner = new ExecRunner(config);
  const mcpRequests = new McpRequestRegistry();

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
        await handleMcp(req, res, runner, mcpRequests);
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

  return { server, runner, config, mcpRequests };
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
    if (!finished && !abortController.signal.aborted) abortController.abort(new Error('http_disconnect'));
  };
  const onResponseClose = () => {
    if (!res.writableEnded) abortIfOpen();
  };
  req.on('aborted', abortIfOpen);
  res.on('close', onResponseClose);

  const emit = (event) => writeSse(res, event.type || 'message', event);
  try {
    await runner.run(body, emit, { abortSignal: abortController.signal, abortReason: 'http_disconnect', abortSource: 'http' });
  } catch (err) {
    const code = err instanceof ExecRejectedError ? err.code : 'internal_error';
    writeSse(res, 'error', { type: 'error', code, message: err.message });
  } finally {
    finished = true;
    req.removeListener('aborted', abortIfOpen);
    res.removeListener('close', onResponseClose);
    res.end();
  }
}

async function handleMcp(req, res, runner, mcpRequests) {
  const disconnectController = new AbortController();
  let disconnectHandled = false;
  const abortForDisconnect = () => {
    if (disconnectHandled) return;
    disconnectHandled = true;
    disconnectController.abort(new Error('http_disconnect'));
  };
  const onAborted = () => abortForDisconnect();
  const onResponseClose = () => {
    if (!res.writableEnded) abortForDisconnect();
  };
  req.on('aborted', onAborted);
  res.on('close', onResponseClose);

  try {
    const body = await readJson(req, runner.config.mcpMaxRequestBytes || DEFAULT_MCP_MAX_REQUEST_BYTES);
    const sessionId = req.headers['mcp-session-id'] || randomUUID();
    res.setHeader('mcp-session-id', sessionId);
    res.setHeader('content-type', 'application/json');
    const context = { sessionId: String(sessionId), signal: disconnectController.signal, mcpRequests };
    if (Array.isArray(body)) {
      const out = [];
      for (const item of body) {
        const r = await handleMcpMessage(item, runner, { ...context, isBatch: true });
        if (r) out.push(r);
      }
      res.end(JSON.stringify(out));
      return;
    }

    const response = await handleMcpMessage(body, runner, { ...context, isBatch: false });
    if (!response) {
      res.statusCode = 202;
      res.end('{}');
      return;
    }
    res.end(JSON.stringify(response));
  } finally {
    req.removeListener('aborted', onAborted);
    res.removeListener('close', onResponseClose);
  }
}

async function handleMcpMessage(msg, runner, context) {
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
        serverInfo: { name: 'exec-mcp', version: PACKAGE_VERSION }
      }
    };
  }

  if (method === 'notifications/initialized') {
    return null;
  }

  if (method === 'notifications/cancelled') {
    const record = context.mcpRequests.get(context.sessionId, msg.params?.requestId);
    if (record) {
      record.cancelSource = 'mcp_notification';
      record.abortController.abort(new Error('mcp_notification_cancel'));
      if (record.execId) runner.registry.requestAbort(record.execId, 'mcp_notification_cancel', 'mcp_notification');
    }
    return null;
  }

  if (method === 'tools/list') {
    return { jsonrpc: '2.0', id, result: { tools: [execToolSchema(), listActiveExecsToolSchema(), getExecStatusToolSchema(), cancelExecToolSchema(), downloadFileToolSchema(), uploadFileToolSchema()] } };
  }

  if (method === 'tools/call') {
    const name = msg.params?.name;
    const args = msg.params?.arguments || {};
    try {
      if (name === 'exec') {
        if (context.isBatch) return toolResult(id, 'exec_not_supported_in_batch: Send exec as a standalone JSON-RPC request.', true);
        const events = [];
        const requestRecord = context.mcpRequests.register(context.sessionId, id);
        const abortFromHttp = () => requestRecord.abortController.abort(new Error('http_disconnect'));
        context.signal?.addEventListener('abort', abortFromHttp, { once: true });
        if (context.signal?.aborted) abortFromHttp();
        try {
          const summary = await runner.run(args, (event) => events.push(event), {
            abortSignal: requestRecord.abortController.signal,
            abortReason: requestRecord.cancelSource === 'mcp_notification' ? 'mcp_notification_cancel' : 'http_disconnect',
            abortSource: requestRecord.cancelSource || 'http',
            onAcquire: (rec) => { requestRecord.execId = rec.id; }
          });
          const text = renderToolText(summary);
          return toolResult(id, text, summary.code !== 0 || summary.timed_out === true, execStructuredContent(summary));
        } finally {
          requestRecord.completed = true;
          context.signal?.removeEventListener('abort', abortFromHttp);
          context.mcpRequests.remove(context.sessionId, id);
        }
      }
      if (name === 'list_active_execs') {
        const result = runner.listActive();
        return toolResult(id, JSON.stringify(result, null, 2), false, result);
      }
      if (name === 'get_exec_status') {
        const result = runner.getStatus(requireExecId(args));
        return toolResult(id, JSON.stringify(result, null, 2), !result.found, result);
      }
      if (name === 'cancel_exec') {
        const result = runner.cancel(requireExecId(args));
        return toolResult(id, JSON.stringify(result, null, 2), result.result === 'exec_not_found', result);
      }
      if (name === 'download_file') {
        const result = await downloadFileTool(args, runner.config);
        return toolResult(id, JSON.stringify(result, null, 2), false, result);
      }
      if (name === 'upload_file') {
        const result = await uploadFileTool(args, runner.config);
        return toolResult(id, JSON.stringify(result, null, 2), false, result);
      }
      return jsonError(id, -32602, `Unknown tool: ${name}`);
    } catch (err) {
      const code = err instanceof ExecRejectedError || err instanceof FileToolError || err.code === 'duplicate_request_id' ? err.code : 'internal_error';
      const details = { code, ...(err.details || {}) };
      return toolResult(id, `${code}: ${err.message}`, true, details);
    }
  }

  return jsonError(id, -32601, `Method not found: ${method}`);
}

function toolResult(id, text, isError, structuredContent) {
  const result = {
    jsonrpc: '2.0',
    id,
    result: {
      content: [{ type: 'text', text }],
      isError
    }
  };
  if (structuredContent !== undefined) {
    result.result.structuredContent = structuredContent;
  }
  return result;
}

function execToolSchema() {
  return {
    name: 'exec',
    description: 'Run one non-interactive command in the configured test execution environment and return a bounded final text result with stdout/stderr tails plus an exec summary. The request is accepted only after cwd allowlist validation, runtime/output limits, environment filtering, and concurrency checks. Commands are evaluated by the environment command processor (/bin/sh -c), so use this for controlled diagnostics, validation, and test-environment maintenance tasks rather than interactive sessions or long-running services. This tool can run high-risk commands when they are intentionally requested for an approved isolated test scenario. Output may be truncated. The final [exec summary] is authoritative for exit code, signal, timeout, duration, byte counts, and truncation. stderr output alone does not mean failure; non-zero exit code, signal, or timed_out=true means failure. If capacity is full, too_many_active_execs reports active/max/oldest_age_seconds/states for diagnosis.',
    inputSchema: {
      type: 'object',
      properties: {
        command: { type: 'string', description: 'Non-interactive command for the configured test execution environment. Use explicit quoting for pipelines, redirection, &&, and environment expansion. Avoid privileged, destructive, interactive, or unbounded long-running operations unless they are part of an approved isolated test scenario.' },
        cwd: { type: 'string', description: 'Absolute working directory in the test execution environment. It must be under the configured allowlist after remote realpath/symlink resolution. If omitted, the server uses DEFAULT_CWD.' },
        timeout_seconds: { type: 'integer', minimum: 1, description: 'Maximum runtime in seconds before the command is aborted. Values above MAX_TIMEOUT_SECONDS are rejected. On timeout the server sends SIGTERM, then SIGKILL after the configured kill grace period.' },
        max_output_bytes: { type: 'integer', minimum: 1, description: 'Maximum combined stdout/stderr bytes forwarded before truncation. The process is still drained until exit. The final summary includes byte counts, truncation status, and stdout_tail plus stderr_tail bounded by this value and the server tail limit.' },
        env: { type: 'object', additionalProperties: { type: 'string' }, description: 'Additional environment variables for the command. Invalid variable names are ignored. ENV and BASH_ENV are removed before spawning.' },
        label: { type: 'string', maxLength: 120, description: 'Optional sanitized operator label. Do not include credentials or secrets.' }
      },
      required: ['command'],
      additionalProperties: false
    },
    outputSchema: {
      type: 'object',
      properties: {
        exec_id: { type: 'string', description: 'Unique identifier for this exec call.' },
        type: { type: 'string', enum: ['exit'], description: 'Final event type for the command.' },
        code: { type: ['integer', 'null'], description: 'Process exit code. Null when the process ended by signal before reporting a code.' },
        signal: { type: ['string', 'null'], description: 'Signal associated with process termination, or null.' },
        duration_ms: { type: 'integer', minimum: 0, description: 'Command runtime in milliseconds.' },
        stdout_bytes: { type: 'integer', minimum: 0, description: 'Total stdout bytes observed before redaction.' },
        stderr_bytes: { type: 'integer', minimum: 0, description: 'Total stderr bytes observed before redaction.' },
        truncated: { type: 'boolean', description: 'True when forwarded output exceeded max_output_bytes.' },
        timed_out: { type: 'boolean', description: 'True when the command exceeded timeout_seconds.' },
        stdout_tail: { type: 'string', description: 'Redacted tail of stdout retained for final inspection. stdout_tail plus stderr_tail is bounded by max_output_bytes and the server tail limit.' },
        stderr_tail: { type: 'string', description: 'Redacted tail of stderr retained for final inspection. stdout_tail plus stderr_tail is bounded by max_output_bytes and the server tail limit.' }
      },
      required: ['exec_id', 'type', 'code', 'signal', 'duration_ms', 'stdout_bytes', 'stderr_bytes', 'truncated', 'timed_out', 'stdout_tail', 'stderr_tail'],
      additionalProperties: false
    }
  };
}

function listActiveExecsToolSchema() {
  return {
    name: 'list_active_execs',
    description: 'List active executions without consuming an execution slot. This is an operator-wide control-plane tool for a trusted single-tenant connection.',
    inputSchema: { type: 'object', properties: {}, additionalProperties: false },
    outputSchema: { type: 'object', properties: { active: { type: 'integer' }, max_concurrent: { type: 'integer' }, circuit_open: { type: 'boolean' }, tasks: { type: 'array', items: { type: 'object' } } }, required: ['active', 'max_concurrent', 'circuit_open', 'tasks'], additionalProperties: false }
  };
}

function getExecStatusToolSchema() {
  return {
    name: 'get_exec_status',
    description: 'Get one execution from the active registry or bounded recent history.',
    inputSchema: { type: 'object', properties: { exec_id: { type: 'string' } }, required: ['exec_id'], additionalProperties: false },
    outputSchema: { type: 'object', additionalProperties: true }
  };
}

function cancelExecToolSchema() {
  return {
    name: 'cancel_exec',
    description: 'Idempotently request cancellation of an active execution. Cancellation never releases capacity before runner finalization.',
    inputSchema: { type: 'object', properties: { exec_id: { type: 'string' } }, required: ['exec_id'], additionalProperties: false },
    outputSchema: { type: 'object', properties: { exec_id: { type: 'string' }, result: { type: 'string' }, accepted: { type: 'boolean' } }, required: ['exec_id', 'result', 'accepted'], additionalProperties: true }
  };
}

function requireExecId(args) {
  const execId = typeof args?.exec_id === 'string' ? args.exec_id.trim() : '';
  if (!execId) throw new ExecRejectedError('invalid_exec_id', 'exec_id must be a non-empty string');
  return execId;
}

function downloadFileToolSchema() {
  return {
    name: 'download_file',
    description: 'Download one file from the configured test execution environment after path allowlist validation. The response is JSON text containing file metadata and data_base64 so binary files such as images, PDFs, documents, spreadsheets, and archives can be transferred through MCP. Relative paths are resolved from DEFAULT_CWD. Files larger than max_bytes or FILE_MAX_DOWNLOAD_BYTES are rejected instead of truncated.',
    inputSchema: {
      type: 'object',
      properties: {
        path: { type: 'string', description: 'File path to download. Absolute paths are validated directly; relative paths are resolved from DEFAULT_CWD. The final path must be under ALLOWED_CWDS.' },
        max_bytes: { type: 'integer', minimum: 1, description: 'Maximum file size allowed for this download. If omitted, the server uses FILE_MAX_DOWNLOAD_BYTES or the built-in default.' }
      },
      required: ['path'],
      additionalProperties: false
    },
    outputSchema: {
      type: 'object',
      properties: {
        path: { type: 'string', description: 'Resolved real path of the downloaded file in the test execution environment.' },
        bytes: { type: 'integer', minimum: 0, description: 'Number of decoded file bytes.' },
        mime_type: { type: 'string', description: 'Best-effort MIME type derived from the file extension.' },
        data_base64: { type: 'string', description: 'Base64-encoded raw file bytes.' }
      },
      required: ['path', 'bytes', 'mime_type', 'data_base64'],
      additionalProperties: false
    }
  };
}

function uploadFileToolSchema() {
  return {
    name: 'upload_file',
    description: 'Upload one file to the configured test execution environment after path allowlist validation and decoded-size checks. The request carries raw file bytes as data_base64 so binary files such as images, PDFs, documents, spreadsheets, and archives can be transferred through MCP. Relative paths are resolved from DEFAULT_CWD. Parent directories must already exist.',
    inputSchema: {
      type: 'object',
      properties: {
        path: { type: 'string', description: 'File path to upload. Absolute paths are validated directly; relative paths are resolved from DEFAULT_CWD. The final path must be under ALLOWED_CWDS.' },
        data_base64: { type: 'string', description: 'Base64-encoded raw file bytes. The decoded byte length must not exceed FILE_MAX_UPLOAD_BYTES or the built-in default.' },
        mime_type: { type: 'string', description: 'Optional advisory MIME type for client bookkeeping. The server does not enforce file extensions from this value.' },
        append: { type: 'boolean', description: 'Append decoded bytes to the file when true. If false or omitted, replace the file content.' }
      },
      required: ['path', 'data_base64'],
      additionalProperties: false
    },
    outputSchema: {
      type: 'object',
      properties: {
        path: { type: 'string', description: 'Resolved real path of the uploaded file in the test execution environment.' },
        bytes: { type: 'integer', minimum: 0, description: 'Number of decoded bytes written.' },
        action: { type: 'string', enum: ['write', 'append'], description: 'Whether the operation replaced or appended file content.' },
        mime_type: { type: 'string', description: 'Advisory MIME type supplied by the caller or inferred from the file extension.' }
      },
      required: ['path', 'bytes', 'action', 'mime_type'],
      additionalProperties: false
    }
  };
}

async function downloadFileTool(args, config) {
  const inputPath = requireInputPath(args?.path);
  const maxBytes = clampFileLimit(args?.max_bytes, config.fileMaxDownloadBytes || DEFAULT_MAX_FILE_DOWNLOAD_BYTES, 'invalid_max_bytes');
  const maxStdoutBytes = Math.ceil(maxBytes * 4 / 3) + 8192;
  const body = await runRemoteFileScript(config, buildRemoteDownloadScript(inputPath, maxBytes, config), maxStdoutBytes);
  return {
    path: body.path,
    bytes: body.bytes,
    mime_type: detectMimeType(body.path),
    data_base64: body.data_base64
  };
}

async function uploadFileTool(args, config) {
  const inputPath = requireInputPath(args?.path);
  const data = decodeBase64(args?.data_base64, config.fileMaxUploadBytes || DEFAULT_MAX_FILE_UPLOAD_BYTES);
  const body = await runRemoteFileScript(
    config,
    buildRemoteUploadScript(inputPath, data.toString('base64'), args.append === true, config),
    8192
  );
  return {
    path: body.path,
    bytes: body.bytes,
    action: args.append === true ? 'append' : 'write',
    mime_type: typeof args?.mime_type === 'string' && args.mime_type.trim() ? args.mime_type.trim() : detectMimeType(body.path)
  };
}

function execStructuredContent(summary) {
  return {
    exec_id: summary.exec_id,
    type: summary.type,
    code: summary.code,
    signal: summary.signal || null,
    duration_ms: summary.duration_ms,
    stdout_bytes: summary.stdout_bytes,
    stderr_bytes: summary.stderr_bytes,
    truncated: summary.truncated,
    timed_out: summary.timed_out,
    stdout_tail: summary.stdout_tail,
    stderr_tail: summary.stderr_tail
  };
}

function requireInputPath(inputPath) {
  if (typeof inputPath !== 'string' || !inputPath.trim()) {
    throw new FileToolError('invalid_path', 'path must be a non-empty string');
  }
  return inputPath;
}

function clampFileLimit(value, max, errorCode) {
  if (value === undefined || value === null) return max;
  const n = Number.parseInt(String(value), 10);
  if (!Number.isFinite(n) || n <= 0) throw new FileToolError(errorCode, `${errorCode}: ${value}`);
  if (n > max) throw new FileToolError('file_limit_too_large', `file_limit_too_large: ${n} > ${max}`);
  return n;
}

function decodeBase64(value, maxBytes) {
  if (typeof value !== 'string') {
    throw new FileToolError('invalid_base64', 'data_base64 must be a string');
  }
  const compact = value.replace(/\s+/g, '');
  if (compact.length % 4 === 1 || !/^[A-Za-z0-9+/]*={0,2}$/.test(compact)) {
    throw new FileToolError('invalid_base64', 'data_base64 is not valid base64');
  }
  const padding = compact.endsWith('==') ? 2 : compact.endsWith('=') ? 1 : 0;
  const decodedBytes = Math.floor((compact.length * 3) / 4) - padding;
  if (decodedBytes > maxBytes) throw new FileToolError('file_too_large', `file_too_large: ${decodedBytes} > ${maxBytes}`);

  const data = Buffer.from(compact, 'base64');
  if (data.length !== decodedBytes) throw new FileToolError('invalid_base64', 'data_base64 is not valid base64');
  return data;
}

async function runRemoteFileScript(config, script, maxStdoutBytes) {
  let spawned;
  try {
    spawned = spawnRemoteShell(config, script);
  } catch (err) {
    throw new FileToolError('remote_config_error', err.message);
  }

  const { child, stdin } = spawned;
  let stdoutBytes = 0;
  let stderrBytes = 0;
  const stdout = [];
  const stderr = [];
  let outputTooLarge = false;
  let timedOut = false;

  const killRemote = (signal) => {
    try {
      if (child.pid) process.kill(-child.pid, signal);
    } catch {
      try { child.kill(signal); } catch {}
    }
  };

  const timer = setTimeout(() => {
    timedOut = true;
    killRemote('SIGTERM');
  }, config.defaultTimeoutSeconds * 1000);
  timer.unref?.();

  child.stdout.on('data', (chunk) => {
    stdoutBytes += chunk.length;
    if (stdoutBytes > maxStdoutBytes) {
      outputTooLarge = true;
      killRemote('SIGTERM');
      return;
    }
    stdout.push(chunk);
  });
  child.stderr.on('data', (chunk) => {
    stderrBytes += chunk.length;
    if (stderrBytes <= 65536) stderr.push(chunk);
  });

  const close = new Promise((resolve, reject) => {
    child.on('error', reject);
    child.on('close', (code, signal) => resolve({ code, signal }));
  });
  child.stdin.end(stdin);

  const { code, signal } = await close;
  clearTimeout(timer);

  if (timedOut) throw new FileToolError('remote_timeout', 'remote file operation timed out');
  if (outputTooLarge) throw new FileToolError('remote_output_too_large', `remote stdout exceeded ${maxStdoutBytes} bytes`);
  if (code !== 0) {
    const errText = Buffer.concat(stderr).toString('utf8').trim();
    throw new FileToolError('remote_failed', errText || `remote file operation failed: exit=${code} signal=${signal || 'null'}`);
  }

  let body;
  try {
    body = JSON.parse(Buffer.concat(stdout).toString('utf8'));
  } catch {
    throw new FileToolError('remote_protocol_error', 'remote file operation returned invalid JSON');
  }
  if (!body || body.ok !== true) {
    throw new FileToolError(body?.code || 'remote_failed', body?.message || 'remote file operation failed');
  }
  return body;
}

function buildRemoteDownloadScript(inputPath, maxBytes, config) {
  return `python3 - <<'PY'
import base64, json, os, stat, sys

INPUT_PATH = ${JSON.stringify(inputPath)}
DEFAULT_CWD = ${JSON.stringify(config.defaultCwd)}
ALLOWED_CWDS = ${JSON.stringify(config.allowedCwds)}
MAX_BYTES = ${maxBytes}

def emit(obj):
    print(json.dumps(obj, separators=(',', ':')))

def fail(code, message):
    emit({'ok': False, 'code': code, 'message': message})
    sys.exit(0)

def target_path(path):
    return path if os.path.isabs(path) else os.path.join(DEFAULT_CWD, path)

def allowed(path):
    for base in ALLOWED_CWDS:
        try:
            real_base = os.path.realpath(base)
        except OSError:
            continue
        prefix = real_base if real_base == os.sep else real_base + os.sep
        if path == real_base or path.startswith(prefix):
            return True
    return False

target = target_path(INPUT_PATH)
try:
    real = os.path.realpath(target)
    info = os.stat(real)
except FileNotFoundError:
    fail('not_found', 'file not found: ' + target)
except OSError as exc:
    fail('remote_error', str(exc))

if not allowed(real):
    fail('invalid_path', 'real path is not allowed: ' + real)
if not stat.S_ISREG(info.st_mode):
    fail('not_file', 'path is not a file: ' + real)
if info.st_size > MAX_BYTES:
    fail('file_too_large', 'file_too_large: %d > %d' % (info.st_size, MAX_BYTES))

with open(real, 'rb') as fh:
    data = fh.read(MAX_BYTES + 1)
if len(data) > MAX_BYTES:
    fail('file_too_large', 'file_too_large: more than %d' % MAX_BYTES)

emit({'ok': True, 'path': real, 'bytes': len(data), 'data_base64': base64.b64encode(data).decode('ascii')})
PY
`;
}

function buildRemoteUploadScript(inputPath, dataBase64, append, config) {
  return `python3 - <<'PY'
import base64, binascii, errno, json, os, stat, sys

INPUT_PATH = ${JSON.stringify(inputPath)}
DEFAULT_CWD = ${JSON.stringify(config.defaultCwd)}
ALLOWED_CWDS = ${JSON.stringify(config.allowedCwds)}
DATA_BASE64 = ${JSON.stringify(dataBase64)}
APPEND = ${append ? 'True' : 'False'}

def emit(obj):
    print(json.dumps(obj, separators=(',', ':')))

def fail(code, message):
    emit({'ok': False, 'code': code, 'message': message})
    sys.exit(0)

def target_path(path):
    return path if os.path.isabs(path) else os.path.join(DEFAULT_CWD, path)

def allowed(path):
    for base in ALLOWED_CWDS:
        try:
            real_base = os.path.realpath(base)
        except OSError:
            continue
        prefix = real_base if real_base == os.sep else real_base + os.sep
        if path == real_base or path.startswith(prefix):
            return True
    return False

try:
    data = base64.b64decode(DATA_BASE64.encode('ascii'), validate=True)
except (binascii.Error, ValueError) as exc:
    fail('invalid_base64', str(exc))

target = target_path(INPUT_PATH)
parent = os.path.dirname(target) or '.'
name = os.path.basename(target.rstrip(os.sep))
if not name:
    fail('invalid_path', 'path must name a file: ' + target)

try:
    real_parent = os.path.realpath(parent)
    parent_info = os.stat(real_parent)
except FileNotFoundError:
    fail('parent_not_found', 'parent directory does not exist for: ' + target)
except OSError as exc:
    fail('remote_error', str(exc))

if not stat.S_ISDIR(parent_info.st_mode):
    fail('parent_not_found', 'parent path is not a directory: ' + parent)
if not allowed(real_parent):
    fail('invalid_path', 'real parent path is not allowed: ' + real_parent)

real_target = os.path.join(real_parent, name)
if os.path.islink(real_target):
    fail('symlink_not_allowed', 'symlink path is not allowed: ' + real_target)

flags = os.O_WRONLY | os.O_CREAT | (os.O_APPEND if APPEND else os.O_TRUNC)
if hasattr(os, 'O_NOFOLLOW'):
    flags |= os.O_NOFOLLOW
try:
    fd = os.open(real_target, flags, 0o666)
except IsADirectoryError:
    fail('not_file', 'path is not a file: ' + real_target)
except OSError as exc:
    if exc.errno == errno.ELOOP:
        fail('symlink_not_allowed', 'symlink path is not allowed: ' + real_target)
    fail('remote_error', str(exc))

with os.fdopen(fd, 'ab' if APPEND else 'wb') as fh:
    fh.write(data)

emit({'ok': True, 'path': real_target, 'bytes': len(data)})
PY
`;
}

function detectMimeType(filePath) {
  switch (extname(filePath).toLowerCase()) {
    case '.txt': return 'text/plain';
    case '.md': return 'text/markdown';
    case '.json': return 'application/json';
    case '.csv': return 'text/csv';
    case '.html': return 'text/html';
    case '.xml': return 'application/xml';
    case '.png': return 'image/png';
    case '.jpg':
    case '.jpeg': return 'image/jpeg';
    case '.gif': return 'image/gif';
    case '.webp': return 'image/webp';
    case '.svg': return 'image/svg+xml';
    case '.pdf': return 'application/pdf';
    case '.doc': return 'application/msword';
    case '.docx': return 'application/vnd.openxmlformats-officedocument.wordprocessingml.document';
    case '.xls': return 'application/vnd.ms-excel';
    case '.xlsx': return 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';
    case '.ppt': return 'application/vnd.ms-powerpoint';
    case '.pptx': return 'application/vnd.openxmlformats-officedocument.presentationml.presentation';
    case '.zip': return 'application/zip';
    case '.gz': return 'application/gzip';
    case '.tar': return 'application/x-tar';
    default: return 'application/octet-stream';
  }
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
  lines.push('# HELP exec_mcp_active_execs Number of active exec calls');
  lines.push('# TYPE exec_mcp_active_execs gauge');
  lines.push(`exec_mcp_active_execs ${runner.active}`);
  lines.push(`exec_mcp_exec_started_total ${runner.metrics.startedTotal}`);
  lines.push(`exec_mcp_execution_circuit_open ${runner.registry.circuitOpen ? 1 : 0}`);
  lines.push(`exec_mcp_unconfirmed_reaped_total ${runner.registry.metrics.unconfirmedReapedTotal}`);
  lines.push(`exec_mcp_reaped_total ${runner.registry.metrics.unconfirmedReapedTotal}`);
  lines.push(`exec_mcp_unconfirmed_reaped_current ${runner.registry.unconfirmed.size}`);
  lines.push(`exec_mcp_late_transport_close_total ${runner.registry.metrics.lateTransportCloseTotal}`);
  lines.push(`exec_mcp_registry_invariant_violation_total ${runner.registry.metrics.invariantViolations}`);
  lines.push(`exec_mcp_recent_history_size ${runner.registry.recent.length}`);
  lines.push(`exec_mcp_disconnect_abort_total ${runner.metrics.abortRequestedTotal.get('http_disconnect') || 0}`);
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
  for (const [state, count] of runner.metrics.finishedTotal.entries()) {
    lines.push(`exec_mcp_exec_finished_total{final_state="${escapeLabel(state)}"} ${count}`);
  }
  for (const [reason, count] of runner.metrics.abortRequestedTotal.entries()) {
    lines.push(`exec_mcp_abort_requested_total{reason="${escapeLabel(reason)}"} ${count}`);
  }
  for (const [result, count] of runner.metrics.cancelRequestsTotal.entries()) {
    lines.push(`exec_mcp_cancel_requests_total{result="${escapeLabel(result)}"} ${count}`);
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
