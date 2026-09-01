import type { AbortReason, FinalExecutionState } from './exec-registry.js';
import type { RuntimeOrigin } from './runtime-observer.js';
import type { ExecutionRecord } from './exec-registry.js';
import type { TaskContext } from './task-context.js';

type EventPayload = { type: string; [key: string]: unknown };
export type ExecEvent = EventPayload & { exec_id: string };

export type ExecutionShell = 'sh' | 'bash';

export interface ExecutionSpec {
  command: string;
  shell: ExecutionShell;
  cwd: string;
  timeoutSeconds: number;
  maxOutputBytes: number;
  env: Record<string, string>;
  label: string | null;
  commandSha256: string;
  commandLength: number;
  commandPreview: string | null;
  allowedCwds: string[];
  killGraceSeconds: number;
}

export type ValidatedExecRequest = ExecutionSpec;

export interface ExecSummary {
  exec_id: string;
  type: 'exit';
  code: number | null;
  signal: NodeJS.Signals | null;
  duration_ms: number;
  stdout_bytes: number;
  stderr_bytes: number;
  truncated: boolean;
  timed_out: boolean;
  stdout_tail: string;
  stderr_tail: string;
  task_handle: string | null;
}

export interface RunOptions {
  abortSignal?: AbortSignal;
  abortReason?: AbortReason;
  abortSource?: string;
  onAcquire?: (record: ExecutionRecord) => void;
  traceId?: string;
  origin?: Partial<RuntimeOrigin>;
  requestReceivedAt?: number;
  taskContext?: TaskContext;
}

export interface GetStatusOptions {
  stdoutCursor?: number;
  stderrCursor?: number;
  maxOutputBytes?: number;
  waitSeconds?: number;
}

interface Histogram {
  count: number;
  sum: number;
  buckets: number[];
}

export interface ExecMetrics {
  requestsTotal: number;
  rejectedTotal: Map<string, number>;
  timeoutTotal: number;
  killedTotal: Map<string, number>;
  truncatedTotal: number;
  streamDisconnectTotal: number;
  exitCodeTotal: Map<string, number>;
  outputBytesTotal: { stdout: number; stderr: number };
  durationMsTotal: number;
  durationSecondsBuckets: number[];
  durationSecondsByState: Map<FinalExecutionState, Histogram>;
  startedTotal: number;
  abortRequestedTotal: Map<string, number>;
  cancelRequestsTotal: Map<string, number>;
  finishedTotal: Map<FinalExecutionState, number>;
}

export class ExecRejectedError extends Error {
  readonly code: string;
  readonly details: Record<string, unknown> | undefined;

  constructor(code: string, message: string, details: Record<string, unknown> | undefined = undefined) {
    super(message);
    this.name = 'ExecRejectedError';
    this.code = code;
    this.details = details;
  }
}
