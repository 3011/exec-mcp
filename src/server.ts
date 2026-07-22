import http from 'node:http';
import type { IncomingMessage, Server as HttpServer, ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import { once } from 'node:events';
import { randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { parseConfig } from './config.js';
import type { ExecMcpConfig } from './config.js';
import { ExecRunner, ExecRejectedError } from './exec-runner.js';
import type { ExecEvent, ExecSummary } from './exec-runner.js';
import type { ExecutionRecord } from './exec-registry.js';
import { TOOL_SCHEMAS } from './tool-schemas.js';
import { renderMetrics } from './metrics.js';
import { downloadFileTool, FileToolError, uploadFileTool } from './file-tools.js';

const PACKAGE_VERSION = (JSON.parse(readFileSync(new URL('../../package.json', import.meta.url), 'utf8')) as { version: string }).version;
const DEFAULT_MCP_MAX_REQUEST_BYTES = 16 * 1024 * 1024;


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
