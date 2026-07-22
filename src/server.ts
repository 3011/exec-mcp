import http from 'node:http';
import type { IncomingMessage, Server as HttpServer, ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import { once } from 'node:events';
import { randomUUID } from 'node:crypto';
import { extname } from 'node:path';
import { readFileSync } from 'node:fs';
import { parseConfig } from './config.js';
import type { ExecMcpConfig } from './config.js';
import { ExecRunner, ExecRejectedError, spawnRemoteShell } from './exec-runner.js';
import type { ExecEvent, ExecSummary } from './exec-runner.js';
import type { ExecutionRecord } from './exec-registry.js';
import { TOOL_SCHEMAS } from './tool-schemas.js';

const PACKAGE_VERSION = (JSON.parse(readFileSync(new URL('../../package.json', import.meta.url), 'utf8')) as { version: string }).version;
const DEFAULT_MCP_MAX_REQUEST_BYTES = 16 * 1024 * 1024;
const DEFAULT_MAX_FILE_DOWNLOAD_BYTES = 10 * 1024 * 1024;
const DEFAULT_MAX_FILE_UPLOAD_BYTES = 10 * 1024 * 1024;


type UnknownRecord = Record<string, unknown>;
type McpCancelSource = 'mcp_notification';

interface McpRequestRecord {
  requestId: unknown;
  typedRequestId: string;
  abortController: AbortController;
  execId: string | null;
  createdAt: number;
  cancelSource: McpCancelSource | null;
  completed: boolean;
}

interface McpContext {
  sessionId: string;
  signal: AbortSignal;
  mcpRequests: McpRequestRegistry;
  isBatch: boolean;
}

interface ToolResultEnvelope {
  jsonrpc: '2.0';
  id: unknown;
  result: {
    content: Array<{ type: 'text'; text: string }>;
    isError: boolean;
    structuredContent?: unknown;
  };
}

interface RemoteFileSuccess {
  ok: true;
  path: string;
  bytes: number;
}

interface RemoteDownloadSuccess extends RemoteFileSuccess {
  data_base64: string;
}

interface ProcessCloseResult {
  code: number | null;
  signal: NodeJS.Signals | null;
}

class CodedError extends Error {
  readonly code: string;

  constructor(code: string, message: string) {
    super(message);
    this.name = 'CodedError';
    this.code = code;
  }
}

class HttpRequestError extends Error {
  readonly statusCode: number;
  readonly code: string;

  constructor(statusCode: number, code: string, message: string) {
    super(message);
    this.name = 'HttpRequestError';
    this.statusCode = statusCode;
    this.code = code;
  }
}

class FileToolError extends Error {
  readonly code: string;

  constructor(code: string, message: string) {
    super(message);
    this.name = 'FileToolError';
    this.code = code;
  }
}

class McpRequestRegistry {
  private readonly sessions = new Map<string, Map<string, McpRequestRecord>>();

  register(sessionId: string, requestId: unknown): McpRequestRecord {
    let session = this.sessions.get(sessionId);
    if (!session) {
      session = new Map<string, McpRequestRecord>();
      this.sessions.set(sessionId, session);
    }
    const key = typedRequestKey(requestId);
    if (session.has(key)) {
      throw new CodedError('duplicate_request_id', 'duplicate in-flight MCP request id');
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
  get(sessionId: string, requestId: unknown): McpRequestRecord | null { return this.sessions.get(sessionId)?.get(typedRequestKey(requestId)) || null; }
  remove(sessionId: string, requestId: unknown): boolean {
    const session = this.sessions.get(sessionId);
    if (!session) return false;
    const removed = session.delete(typedRequestKey(requestId));
    if (session.size === 0) this.sessions.delete(sessionId);
    return removed;
  }
  get size(): number {
    let size = 0;
    for (const session of this.sessions.values()) size += session.size;
    return size;
  }
}

function typedRequestKey(requestId: unknown): string {
  if (typeof requestId === 'number') return `number:${requestId}`;
  if (typeof requestId === 'string') return `string:${requestId}`;
  return `${typeof requestId}:${JSON.stringify(requestId)}`;
}

export function createServer(config: ExecMcpConfig = parseConfig()): { server: HttpServer; runner: ExecRunner; config: ExecMcpConfig; mcpRequests: McpRequestRegistry } {
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
      const status = errorStatus(err);
      res.writeHead(status, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: errorCode(err) || 'internal_error', message: errorMessage(err) }));
    }
  });

  return { server, runner, config, mcpRequests };
}

async function handleSseExec(req: IncomingMessage, res: ServerResponse, runner: ExecRunner): Promise<void> {
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
  const abortIfOpen = (): void => {
    if (!finished && !abortController.signal.aborted) abortController.abort(new Error('http_disconnect'));
  };
  const onResponseClose = (): void => {
    if (!res.writableEnded) abortIfOpen();
  };
  req.on('aborted', abortIfOpen);
  res.on('close', onResponseClose);

  const emit = (event: ExecEvent): void => writeSse(res, event.type || 'message', event);
  try {
    await runner.run(body, emit, { abortSignal: abortController.signal, abortReason: 'http_disconnect', abortSource: 'http' });
  } catch (err) {
    const code = err instanceof ExecRejectedError ? err.code : 'internal_error';
    writeSse(res, 'error', { type: 'error', code, message: errorMessage(err) });
  } finally {
    finished = true;
    req.removeListener('aborted', abortIfOpen);
    res.removeListener('close', onResponseClose);
    res.end();
  }
}

async function handleMcp(req: IncomingMessage, res: ServerResponse, runner: ExecRunner, mcpRequests: McpRequestRegistry): Promise<void> {
  const disconnectController = new AbortController();
  let disconnectHandled = false;
  const abortForDisconnect = (): void => {
    if (disconnectHandled) return;
    disconnectHandled = true;
    disconnectController.abort(new Error('http_disconnect'));
  };
  const onAborted = (): void => abortForDisconnect();
  const onResponseClose = (): void => {
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
      const out: unknown[] = [];
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

async function handleMcpMessage(msg: unknown, runner: ExecRunner, context: McpContext): Promise<unknown | null> {
  if (!isRecord(msg)) return jsonError(null, -32600, 'Invalid Request');
  const id = msg.id ?? null;
  const method = msg.method;
  const params = isRecord(msg.params) ? msg.params : {};

  if (method === 'initialize') {
    return {
      jsonrpc: '2.0',
      id,
      result: {
        protocolVersion: params.protocolVersion || '2025-11-25',
        capabilities: { tools: { listChanged: true } },
        serverInfo: { name: 'exec-mcp', version: PACKAGE_VERSION }
      }
    };
  }

  if (method === 'notifications/initialized') {
    return null;
  }

  if (method === 'notifications/cancelled') {
    const record = context.mcpRequests.get(context.sessionId, params.requestId);
    if (record) {
      record.cancelSource = 'mcp_notification';
      record.abortController.abort(new Error('mcp_notification_cancel'));
      if (record.execId) runner.registry.requestAbort(record.execId, 'mcp_notification_cancel', 'mcp_notification');
    }
    return null;
  }

  if (method === 'tools/list') {
    return { jsonrpc: '2.0', id, result: { tools: TOOL_SCHEMAS } };
  }

  if (method === 'tools/call') {
    const name = params.name;
    const args: UnknownRecord = isRecord(params.arguments) ? params.arguments : {};
    try {
      if (name === 'exec') {
        if (context.isBatch) return toolResult(id, 'exec_not_supported_in_batch: Send exec as a standalone JSON-RPC request.', true);
        const events: ExecEvent[] = [];
        const requestRecord = context.mcpRequests.register(context.sessionId, id);
        const abortFromHttp = () => requestRecord.abortController.abort(new Error('http_disconnect'));
        context.signal?.addEventListener('abort', abortFromHttp, { once: true });
        if (context.signal?.aborted) abortFromHttp();
        try {
          const summary = await runner.run(args, (event) => events.push(event), {
            abortSignal: requestRecord.abortController.signal,
            abortReason: requestRecord.cancelSource === 'mcp_notification' ? 'mcp_notification_cancel' : 'http_disconnect',
            abortSource: requestRecord.cancelSource || 'http',
            onAcquire: (rec: ExecutionRecord) => { requestRecord.execId = rec.id; }
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
      const code = err instanceof ExecRejectedError || err instanceof FileToolError || errorCode(err) === 'duplicate_request_id'
        ? errorCode(err) || 'internal_error'
        : 'internal_error';
      const details = { code, ...errorDetails(err) };
      return toolResult(id, `${code}: ${errorMessage(err)}`, true, details);
    }
  }

  return jsonError(id, -32601, `Method not found: ${method}`);
}

function toolResult(id: unknown, text: string, isError: boolean, structuredContent?: unknown): ToolResultEnvelope {
  const result: ToolResultEnvelope = {
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

function requireExecId(args: UnknownRecord): string {
  const execId = typeof args?.exec_id === 'string' ? args.exec_id.trim() : '';
  if (!execId) throw new ExecRejectedError('invalid_exec_id', 'exec_id must be a non-empty string');
  return execId;
}

async function downloadFileTool(args: UnknownRecord, config: ExecMcpConfig): Promise<{ path: string; bytes: number; mime_type: string; data_base64: string }> {
  const inputPath = requireInputPath(args?.path);
  const maxBytes = clampFileLimit(args?.max_bytes, config.fileMaxDownloadBytes || DEFAULT_MAX_FILE_DOWNLOAD_BYTES, 'invalid_max_bytes');
  const maxStdoutBytes = Math.ceil(maxBytes * 4 / 3) + 8192;
  const body = await runRemoteFileScript<RemoteDownloadSuccess>(config, buildRemoteDownloadScript(inputPath, maxBytes, config), maxStdoutBytes);
  return {
    path: body.path,
    bytes: body.bytes,
    mime_type: detectMimeType(body.path),
    data_base64: body.data_base64
  };
}

async function uploadFileTool(args: UnknownRecord, config: ExecMcpConfig): Promise<{ path: string; bytes: number; action: 'write' | 'append'; mime_type: string }> {
  const inputPath = requireInputPath(args?.path);
  const data = decodeBase64(args?.data_base64, config.fileMaxUploadBytes || DEFAULT_MAX_FILE_UPLOAD_BYTES);
  const body = await runRemoteFileScript<RemoteFileSuccess>(
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

function execStructuredContent(summary: ExecSummary): ExecSummary {
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

function requireInputPath(inputPath: unknown): string {
  if (typeof inputPath !== 'string' || !inputPath.trim()) {
    throw new FileToolError('invalid_path', 'path must be a non-empty string');
  }
  return inputPath;
}

function clampFileLimit(value: unknown, max: number, errorCode: string): number {
  if (value === undefined || value === null) return max;
  const n = Number.parseInt(String(value), 10);
  if (!Number.isFinite(n) || n <= 0) throw new FileToolError(errorCode, `${errorCode}: ${value}`);
  if (n > max) throw new FileToolError('file_limit_too_large', `file_limit_too_large: ${n} > ${max}`);
  return n;
}

function decodeBase64(value: unknown, maxBytes: number): Buffer {
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

async function runRemoteFileScript<T extends RemoteFileSuccess>(config: ExecMcpConfig, script: string, maxStdoutBytes: number): Promise<T> {
  let spawned;
  try {
    spawned = spawnRemoteShell(config, script);
  } catch (err) {
    throw new FileToolError('remote_config_error', errorMessage(err));
  }

  const { child, stdin } = spawned;
  let stdoutBytes = 0;
  let stderrBytes = 0;
  const stdout: Buffer[] = [];
  const stderr: Buffer[] = [];
  let outputTooLarge = false;
  let timedOut = false;

  const killRemote = (signal: NodeJS.Signals): void => {
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

  const close = new Promise<ProcessCloseResult>((resolve, reject) => {
    child.on('error', reject);
    child.on('close', (code: number | null, signal: NodeJS.Signals | null) => resolve({ code, signal }));
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

  let body: unknown;
  try {
    body = JSON.parse(Buffer.concat(stdout).toString('utf8'));
  } catch {
    throw new FileToolError('remote_protocol_error', 'remote file operation returned invalid JSON');
  }
  if (!isRecord(body) || body.ok !== true) {
    throw new FileToolError(
      isRecord(body) && typeof body.code === 'string' ? body.code : 'remote_failed',
      isRecord(body) && typeof body.message === 'string' ? body.message : 'remote file operation failed'
    );
  }
  return body as T;
}

function buildRemoteDownloadScript(inputPath: string, maxBytes: number, config: ExecMcpConfig): string {
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

function buildRemoteUploadScript(inputPath: string, dataBase64: string, append: boolean, config: ExecMcpConfig): string {
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

function detectMimeType(filePath: string): string {
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

function renderToolText(summary: ExecSummary): string {
  let text = '';
  if (summary.stdout_tail) text += summary.stdout_tail;
  if (summary.stderr_tail) {
    if (text && !text.endsWith('\n')) text += '\n';
    text += summary.stderr_tail;
  }
  const meta = `\n[exec summary] exit=${summary.code} signal=${summary.signal || 'null'} duration_ms=${summary.duration_ms} stdout_bytes=${summary.stdout_bytes} stderr_bytes=${summary.stderr_bytes} truncated=${summary.truncated} timed_out=${summary.timed_out}`;
  return text ? text + meta : meta.trimStart();
}

function jsonError(id: unknown, code: number, message: string): { jsonrpc: '2.0'; id: unknown; error: { code: number; message: string } } {
  return { jsonrpc: '2.0', id, error: { code, message } };
}

async function readJson(req: IncomingMessage, maxBytes: number): Promise<unknown> {
  let size = 0;
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    size += buffer.length;
    if (size > maxBytes) {
      throw new HttpRequestError(413, 'body_too_large', 'request body too large');
    }
    chunks.push(buffer);
  }
  if (chunks.length === 0) return {};
  try {
    return JSON.parse(Buffer.concat(chunks).toString('utf8'));
  } catch {
    throw new HttpRequestError(400, 'invalid_json', 'invalid JSON body');
  }
}

function writeSse(res: ServerResponse, event: string, payload: unknown): void {
  res.write(`event: ${event}\n`);
  res.write(`data: ${JSON.stringify(payload)}\n\n`);
}

function renderMetrics(runner: ExecRunner): string {
  const lines: string[] = [];
  lines.push('# HELP exec_mcp_active_execs Number of active exec calls');
  lines.push('# TYPE exec_mcp_active_execs gauge');
  lines.push(`exec_mcp_active_execs ${runner.active}`);
  lines.push('# HELP exec_mcp_max_concurrent_execs Configured maximum concurrent executions');
  lines.push('# TYPE exec_mcp_max_concurrent_execs gauge');
  lines.push(`exec_mcp_max_concurrent_execs ${runner.registry.maxActive}`);
  lines.push('# HELP exec_mcp_requests_total Total validated and rejected exec requests');
  lines.push('# TYPE exec_mcp_requests_total counter');
  lines.push(`exec_mcp_requests_total ${runner.metrics.requestsTotal}`);
  lines.push('# HELP exec_mcp_exec_started_total Total executions that acquired capacity and started');
  lines.push('# TYPE exec_mcp_exec_started_total counter');
  lines.push(`exec_mcp_exec_started_total ${runner.metrics.startedTotal}`);
  lines.push('# HELP exec_mcp_timeout_total Total executions aborted by their request timeout');
  lines.push('# TYPE exec_mcp_timeout_total counter');
  lines.push(`exec_mcp_timeout_total ${runner.metrics.timeoutTotal}`);
  lines.push('# HELP exec_mcp_truncated_total Total executions whose forwarded output exceeded its configured limit');
  lines.push('# TYPE exec_mcp_truncated_total counter');
  lines.push(`exec_mcp_truncated_total ${runner.metrics.truncatedTotal}`);
  lines.push('# HELP exec_mcp_stream_disconnect_total Total active execution streams interrupted by a client or cancellation signal');
  lines.push('# TYPE exec_mcp_stream_disconnect_total counter');
  lines.push(`exec_mcp_stream_disconnect_total ${runner.metrics.streamDisconnectTotal}`);
  lines.push('# HELP exec_mcp_output_bytes_total Total bytes read from remote command output');
  lines.push('# TYPE exec_mcp_output_bytes_total counter');
  lines.push(`exec_mcp_output_bytes_total{stream="stdout"} ${runner.metrics.outputBytesTotal.stdout}`);
  lines.push(`exec_mcp_output_bytes_total{stream="stderr"} ${runner.metrics.outputBytesTotal.stderr}`);
  lines.push('# HELP exec_mcp_execution_circuit_open Whether the execution safety circuit is open');
  lines.push('# TYPE exec_mcp_execution_circuit_open gauge');
  lines.push(`exec_mcp_execution_circuit_open ${runner.registry.circuitOpen ? 1 : 0}`);
  lines.push('# HELP exec_mcp_unconfirmed_reaped_total Total unconfirmed executions force-reaped from capacity accounting');
  lines.push('# TYPE exec_mcp_unconfirmed_reaped_total counter');
  lines.push(`exec_mcp_unconfirmed_reaped_total ${runner.registry.metrics.unconfirmedReapedTotal}`);
  lines.push('# HELP exec_mcp_unconfirmed_reaped_current Current unconfirmed force-reaped executions');
  lines.push('# TYPE exec_mcp_unconfirmed_reaped_current gauge');
  lines.push(`exec_mcp_unconfirmed_reaped_current ${runner.registry.unconfirmed.size}`);
  lines.push('# HELP exec_mcp_late_transport_close_total Total transport closes observed after force reaping');
  lines.push('# TYPE exec_mcp_late_transport_close_total counter');
  lines.push(`exec_mcp_late_transport_close_total ${runner.registry.metrics.lateTransportCloseTotal}`);
  lines.push('# HELP exec_mcp_registry_invariant_violation_total Total execution registry invariant violations');
  lines.push('# TYPE exec_mcp_registry_invariant_violation_total counter');
  lines.push(`exec_mcp_registry_invariant_violation_total ${runner.registry.metrics.invariantViolations}`);
  lines.push('# HELP exec_mcp_recent_history_size Current bounded execution history size');
  lines.push('# TYPE exec_mcp_recent_history_size gauge');
  lines.push(`exec_mcp_recent_history_size ${runner.registry.recent.length}`);
  for (const [reason, count] of runner.metrics.rejectedTotal.entries()) {
    lines.push(`exec_mcp_rejected_total{reason="${escapeLabel(reason)}"} ${count}`);
  }
  for (const [signal, count] of runner.metrics.killedTotal.entries()) {
    lines.push(`exec_mcp_killed_total{signal="${escapeLabel(signal)}"} ${count}`);
  }
  for (const [code, count] of runner.metrics.exitCodeTotal.entries()) {
    lines.push(`exec_mcp_exit_code_total{code="${escapeLabel(code)}"} ${count}`);
  }
  for (const [state, count] of runner.metrics.finishedTotal.entries()) {
    lines.push(`exec_mcp_exec_finished_total{final_state="${escapeLabel(state)}"} ${count}`);
  }
  lines.push('# HELP exec_mcp_exec_duration_seconds Execution duration from acquisition to finalization');
  lines.push('# TYPE exec_mcp_exec_duration_seconds histogram');
  for (const [state, histogram] of runner.metrics.durationSecondsByState.entries()) {
    runner.metrics.durationSecondsBuckets.forEach((upperBound, index) => {
      lines.push(`exec_mcp_exec_duration_seconds_bucket{final_state="${escapeLabel(state)}",le="${upperBound}"} ${histogram.buckets[index] ?? 0}`);
    });
    lines.push(`exec_mcp_exec_duration_seconds_bucket{final_state="${escapeLabel(state)}",le="+Inf"} ${histogram.count}`);
    lines.push(`exec_mcp_exec_duration_seconds_sum{final_state="${escapeLabel(state)}"} ${histogram.sum}`);
    lines.push(`exec_mcp_exec_duration_seconds_count{final_state="${escapeLabel(state)}"} ${histogram.count}`);
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


function isRecord(value: unknown): value is UnknownRecord {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function errorCode(error: unknown): string | undefined {
  if (!isRecord(error)) return undefined;
  return typeof error.code === 'string' ? error.code : undefined;
}

function errorStatus(error: unknown): number {
  if (!isRecord(error)) return 500;
  return typeof error.statusCode === 'number' ? error.statusCode : 500;
}

function errorDetails(error: unknown): Record<string, unknown> {
  if (error instanceof ExecRejectedError && error.details) return error.details;
  return {};
}

function escapeLabel(value: unknown): string {
  return String(value).replaceAll('\\', '\\\\').replaceAll('"', '\\"');
}


function isAddressInfo(address: string | AddressInfo | null): address is AddressInfo {
  return address !== null && typeof address !== 'string';
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const { server, runner, config } = createServer();
  server.listen(config.port, config.host, async () => {
    const address = server.address();
    if (isAddressInfo(address)) console.error(`exec-mcp listening on ${address.address}:${address.port}`);
  });


  const metricsPort = Number.parseInt(process.env.METRICS_PORT || "0", 10);
  let metricsServer: HttpServer | null = null;
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
      const address = metricsServer?.address() ?? null;
      if (isAddressInfo(address)) console.error(`exec-mcp metrics listening on ${address.address}:${address.port}`);
    });
  }

  const shutdown = async (signal: NodeJS.Signals): Promise<void> => {
    console.error(`received ${signal}, shutting down`);
    server.close();
    if (metricsServer) metricsServer.close();
    try { await once(server, 'close'); } catch {}
    process.exit(0);
  };
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}
