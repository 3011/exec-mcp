import http from 'node:http';
import type { IncomingMessage, Server as HttpServer, ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import { once } from 'node:events';
import { randomUUID } from 'node:crypto';
import { parseConfig } from './config.js';
import type { ExecMcpConfig } from './config.js';
import { ExecRunner, ExecRejectedError } from './exec-runner.js';
import type { ExecEvent } from './exec-runner.js';
import { renderMetrics } from './metrics.js';
import { handleMcpMessage, McpRequestRegistry } from './mcp-handler.js';

const DEFAULT_MCP_MAX_REQUEST_BYTES = 16 * 1024 * 1024;

type UnknownRecord = Record<string, unknown>;



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
