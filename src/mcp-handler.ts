import { readFileSync } from 'node:fs';
import { ExecRejectedError, ExecRunner } from './exec-runner.js';
import type { ExecEvent, ExecSummary } from './exec-runner.js';
import type { ExecutionRecord } from './exec-registry.js';
import { TOOL_SCHEMAS } from './tool-schemas.js';
import { downloadFileTool, FileToolError, uploadFileTool } from './file-tools.js';

const PACKAGE_VERSION = (JSON.parse(readFileSync(new URL('../../package.json', import.meta.url), 'utf8')) as { version: string }).version;

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

export class McpRequestRegistry {
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

export async function handleMcpMessage(msg: unknown, runner: ExecRunner, context: McpContext): Promise<unknown | null> {
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

function errorDetails(error: unknown): Record<string, unknown> {
  if (error instanceof ExecRejectedError && error.details) return error.details;
  return {};
}
