import { randomUUID } from 'node:crypto';
import type { ExecutionClass, ExecutionHistoryRecord, ExecutionState, FinalExecutionState, PublicActiveExecution } from './exec-registry.js';

export type RuntimeOriginKind = 'mcp' | 'http_sse' | 'internal';
export type RuntimeTraceLevel = 'info' | 'warning' | 'error';

export interface RuntimeOrigin {
  kind: RuntimeOriginKind;
  tool: string | null;
  transport_session_id: string | null;
  request_id: string | null;
  task_handle: string | null;
}

export interface RuntimeTraceEvent {
  id: number;
  at: string;
  event: string;
  level: RuntimeTraceLevel;
  detail: string | null;
}

export type RuntimeDiagnosticPhase = 'queued' | 'starting' | 'running' | 'terminating' | 'finished';
export type RuntimeDiagnosticActivity = 'active' | 'quiet' | 'long_quiet' | 'unknown';
export type RuntimeFailurePhase = 'transport' | 'execution' | 'termination';

export interface RuntimeExecutionTimings {
  queue_ms: number | null;
  transport_startup_ms: number | null;
  time_to_first_output_ms: number | null;
  runtime_ms: number | null;
  termination_ms: number | null;
  total_ms: number;
}

export interface RuntimeExecutionDiagnostics {
  phase: RuntimeDiagnosticPhase;
  activity: RuntimeDiagnosticActivity;
  failure_phase: RuntimeFailurePhase | null;
  last_activity_age_ms: number | null;
  last_output_age_ms: number | null;
}

export interface RuntimeDerivedDiagnostics {
  timings: RuntimeExecutionTimings;
  diagnostics: RuntimeExecutionDiagnostics;
}

export interface RuntimeExecutionObservation {
  exec_id: string;
  trace_id: string;
  origin: RuntimeOrigin;
  created_at: string;
  last_activity_at: string;
  last_output_at: string | null;
  first_output_at: string | null;
  stdout_bytes: number;
  stderr_bytes: number;
  execution_class: ExecutionClass | null;
  label: string | null;
  cwd: string | null;
  command_preview: string | null;
  command_sha256: string | null;
  command_length: number;
  timeout_seconds: number | null;
  trace: RuntimeTraceEvent[];
}

interface MutableObservation {
  execId: string;
  traceId: string;
  origin: RuntimeOrigin;
  createdAt: number;
  lastActivityAt: number;
  lastOutputAt: number | null;
  firstOutputAt: number | null;
  stdoutBytes: number;
  stderrBytes: number;
  executionClass: ExecutionClass | null;
  label: string | null;
  cwd: string | null;
  commandPreview: string | null;
  commandSha256: string | null;
  commandLength: number;
  timeoutSeconds: number | null;
  trace: RuntimeTraceEvent[];
  nextEventId: number;
  finished: boolean;
}

export interface RuntimeObserverOptions {
  maxExecutions?: number;
  maxTraceEventsPerExecution?: number;
}

export class RuntimeObserver {
  private readonly maxExecutions: number;
  private readonly maxTraceEventsPerExecution: number;
  private readonly observations = new Map<string, MutableObservation>();

  constructor({ maxExecutions = 200, maxTraceEventsPerExecution = 64 }: RuntimeObserverOptions = {}) {
    this.maxExecutions = Math.max(10, maxExecutions);
    this.maxTraceEventsPerExecution = Math.max(16, maxTraceEventsPerExecution);
  }

  newTraceId(): string {
    return `trace-${randomUUID()}`;
  }

  registerExecution(input: {
    execId: string;
    traceId?: string;
    origin?: Partial<RuntimeOrigin>;
    createdAt?: number;
    executionClass?: ExecutionClass;
    label?: string | null;
    cwd?: string | null;
    commandPreview?: string | null;
    commandSha256?: string | null;
    commandLength?: number;
    timeoutSeconds?: number;
    requestReceivedAt?: number;
    validatedAt?: number;
  }): string {
    const existing = this.observations.get(input.execId);
    if (existing) return existing.traceId;
    const now = input.createdAt ?? Date.now();
    const traceId = input.traceId || this.newTraceId();
    const origin: RuntimeOrigin = {
      kind: input.origin?.kind ?? 'internal',
      tool: input.origin?.tool ?? null,
      transport_session_id: input.origin?.transport_session_id ?? null,
      request_id: input.origin?.request_id ?? null,
      task_handle: input.origin?.task_handle ?? null
    };
    const observation: MutableObservation = {
      execId: input.execId,
      traceId,
      origin,
      createdAt: now,
      lastActivityAt: now,
      lastOutputAt: null,
      firstOutputAt: null,
      stdoutBytes: 0,
      stderrBytes: 0,
      executionClass: input.executionClass ?? null,
      label: input.label ?? null,
      cwd: input.cwd ?? null,
      commandPreview: input.commandPreview ?? null,
      commandSha256: input.commandSha256 ?? null,
      commandLength: input.commandLength ?? 0,
      timeoutSeconds: input.timeoutSeconds ?? null,
      trace: [],
      nextEventId: 1,
      finished: false
    };
    this.observations.set(input.execId, observation);
    if (input.requestReceivedAt !== undefined) {
      this.pushEvent(observation, 'tool_request_received', 'info', originDetail(origin), input.requestReceivedAt);
    }
    if (input.validatedAt !== undefined) {
      this.pushEvent(observation, 'request_validated', 'info', null, input.validatedAt);
    }
    this.pushEvent(observation, 'job_registered', 'info', formatRegistrationDetail(input.executionClass, input.label), now);
    this.prune();
    return traceId;
  }

  event(execId: string, event: string, options: { at?: number; level?: RuntimeTraceLevel; detail?: string | null } = {}): void {
    const observation = this.observations.get(execId);
    if (!observation) return;
    const at = options.at ?? Date.now();
    observation.lastActivityAt = Math.max(observation.lastActivityAt, at);
    this.pushEvent(observation, event, options.level ?? 'info', options.detail ?? null, at);
  }

  touch(execId: string, at = Date.now()): void {
    const observation = this.observations.get(execId);
    if (!observation) return;
    observation.lastActivityAt = Math.max(observation.lastActivityAt, at);
  }

  output(execId: string, stream: 'stdout' | 'stderr', byteLength: number, at = Date.now()): void {
    const observation = this.observations.get(execId);
    if (!observation) return;
    observation.lastActivityAt = at;
    observation.lastOutputAt = at;
    if (observation.firstOutputAt === null) {
      observation.firstOutputAt = at;
      this.pushEvent(observation, 'first_output', 'info', stream, at);
    }
    if (stream === 'stdout') observation.stdoutBytes += Math.max(0, byteLength);
    else observation.stderrBytes += Math.max(0, byteLength);
  }

  finish(execId: string, record: ExecutionHistoryRecord | null, at = Date.now()): void {
    const observation = this.observations.get(execId);
    if (!observation || observation.finished) return;
    observation.finished = true;
    observation.lastActivityAt = at;
    const finalState = record?.final_state ?? 'failed';
    const level: RuntimeTraceLevel = finalState === 'completed' ? 'info' : finalState === 'cancelled' ? 'warning' : 'error';
    const details: string[] = [];
    if (record?.exit_code !== null && record?.exit_code !== undefined) details.push(`exit ${record.exit_code}`);
    if (record?.signal) details.push(record.signal);
    if (record?.failure_reason) details.push(record.failure_reason);
    this.pushEvent(observation, finalState, level, details.join(' · ') || null, at);
    this.prune();
  }

  get(execId: string): RuntimeExecutionObservation | null {
    const observation = this.observations.get(execId);
    return observation ? publicObservation(observation) : null;
  }

  list(): RuntimeExecutionObservation[] {
    return [...this.observations.values()]
      .sort((a, b) => b.createdAt - a.createdAt)
      .map(publicObservation);
  }

  decorateActive(task: PublicActiveExecution): PublicActiveExecution & RuntimeObservationFields {
    return { ...task, ...this.fields(task.exec_id) };
  }

  decorateHistory(task: ExecutionHistoryRecord): ExecutionHistoryRecord & RuntimeObservationFields {
    return { ...task, ...this.fields(task.exec_id) };
  }

  fields(execId: string): RuntimeObservationFields {
    const observation = this.observations.get(execId);
    if (!observation) {
      return {
        trace_id: null,
        origin: null,
        last_activity_at: null,
        last_output_at: null,
        first_output_at: null,
        stdout_bytes_observed: 0,
        stderr_bytes_observed: 0
      };
    }
    return {
      trace_id: observation.traceId,
      origin: { ...observation.origin },
      last_activity_at: new Date(observation.lastActivityAt).toISOString(),
      last_output_at: observation.lastOutputAt === null ? null : new Date(observation.lastOutputAt).toISOString(),
      first_output_at: observation.firstOutputAt === null ? null : new Date(observation.firstOutputAt).toISOString(),
      stdout_bytes_observed: observation.stdoutBytes,
      stderr_bytes_observed: observation.stderrBytes
    };
  }

  private pushEvent(observation: MutableObservation, event: string, level: RuntimeTraceLevel, detail: string | null, at: number): void {
    observation.trace.push({
      id: observation.nextEventId++,
      at: new Date(at).toISOString(),
      event,
      level,
      detail
    });
    while (observation.trace.length > this.maxTraceEventsPerExecution) observation.trace.shift();
  }

  private prune(): void {
    if (this.observations.size <= this.maxExecutions) return;
    const oldest = [...this.observations.values()].sort((a, b) => a.lastActivityAt - b.lastActivityAt);
    while (this.observations.size > this.maxExecutions) {
      const item = oldest.shift();
      if (!item) break;
      this.observations.delete(item.execId);
    }
  }
}

export interface RuntimeObservationFields {
  trace_id: string | null;
  origin: RuntimeOrigin | null;
  last_activity_at: string | null;
  last_output_at: string | null;
  first_output_at: string | null;
  stdout_bytes_observed: number;
  stderr_bytes_observed: number;
}

function publicObservation(observation: MutableObservation): RuntimeExecutionObservation {
  return {
    exec_id: observation.execId,
    trace_id: observation.traceId,
    origin: { ...observation.origin },
    created_at: new Date(observation.createdAt).toISOString(),
    last_activity_at: new Date(observation.lastActivityAt).toISOString(),
    last_output_at: observation.lastOutputAt === null ? null : new Date(observation.lastOutputAt).toISOString(),
    first_output_at: observation.firstOutputAt === null ? null : new Date(observation.firstOutputAt).toISOString(),
    stdout_bytes: observation.stdoutBytes,
    stderr_bytes: observation.stderrBytes,
    execution_class: observation.executionClass,
    label: observation.label,
    cwd: observation.cwd,
    command_preview: observation.commandPreview,
    command_sha256: observation.commandSha256,
    command_length: observation.commandLength,
    timeout_seconds: observation.timeoutSeconds,
    trace: observation.trace.map((event) => ({ ...event }))
  };
}

function originDetail(origin: RuntimeOrigin): string | null {
  const parts: string[] = [origin.kind];
  if (origin.tool) parts.push(origin.tool);
  return parts.join(' · ') || null;
}

function formatRegistrationDetail(executionClass: ExecutionClass | undefined, label: string | null | undefined): string | null {
  const parts: string[] = [];
  if (executionClass) parts.push(executionClass);
  if (label) parts.push(label);
  return parts.join(' · ') || null;
}

export function runtimeStateLevel(state: ExecutionState | FinalExecutionState): RuntimeTraceLevel {
  if (state === 'completed' || state === 'running' || state === 'starting' || state === 'queued') return 'info';
  if (state === 'cancelled' || state === 'cancel_aborting' || state === 'client_closed_aborting') return 'warning';
  return 'error';
}

export function deriveRuntimeDiagnostics(
  task: PublicActiveExecution | ExecutionHistoryRecord,
  observation: RuntimeExecutionObservation | null,
  now = Date.now()
): RuntimeDerivedDiagnostics {
  const createdAt = parseTime(observation?.created_at) ?? parseTime(task.created_at) ?? now;
  const queuedAt = traceTime(observation, 'queued') ?? createdAt;
  const startingAt = traceTime(observation, 'starting');
  const runningAt = traceTime(observation, 'execution_running') ?? parseTime(task.running_at);
  const firstOutputAt = parseTime(observation?.first_output_at) ?? traceTime(observation, 'first_output');
  const abortAt = traceTime(observation, 'abort_requested');
  const finishedAt = 'finished_at' in task ? parseTime(task.finished_at) : null;
  const referenceAt = finishedAt ?? now;
  const runtimeEndAt = abortAt ?? finishedAt ?? (runningAt !== null ? now : null);

  const timings: RuntimeExecutionTimings = {
    queue_ms: deltaMs(queuedAt, startingAt),
    transport_startup_ms: deltaMs(startingAt, runningAt),
    time_to_first_output_ms: deltaMs(runningAt, firstOutputAt),
    runtime_ms: deltaMs(runningAt, runtimeEndAt),
    termination_ms: deltaMs(abortAt, finishedAt),
    total_ms: Math.max(0, referenceAt - createdAt)
  };

  const lastActivityAt = parseTime(observation?.last_activity_at);
  const lastOutputAt = parseTime(observation?.last_output_at);
  const diagnostics: RuntimeExecutionDiagnostics = {
    phase: diagnosticPhase(task),
    activity: diagnosticActivity(task, lastActivityAt, lastOutputAt, referenceAt),
    failure_phase: diagnosticFailurePhase(task, observation),
    last_activity_age_ms: ageMs(lastActivityAt, referenceAt),
    last_output_age_ms: ageMs(lastOutputAt, referenceAt)
  };

  return { timings, diagnostics };
}

function diagnosticPhase(task: PublicActiveExecution | ExecutionHistoryRecord): RuntimeDiagnosticPhase {
  if ('final_state' in task) return 'finished';
  if (task.state === 'queued') return 'queued';
  if (task.state === 'starting') return 'starting';
  if (task.state === 'running') return 'running';
  return 'terminating';
}

function diagnosticActivity(
  task: PublicActiveExecution | ExecutionHistoryRecord,
  lastActivityAt: number | null,
  lastOutputAt: number | null,
  referenceAt: number
): RuntimeDiagnosticActivity {
  if ('final_state' in task || task.state === 'queued' || task.state === 'starting') return 'unknown';
  if (lastOutputAt !== null) {
    const age = Math.max(0, referenceAt - lastOutputAt);
    if (age <= 15_000) return 'active';
    if (age <= 300_000) return 'quiet';
    return 'long_quiet';
  }
  if (lastActivityAt === null) return 'unknown';
  return Math.max(0, referenceAt - lastActivityAt) <= 300_000 ? 'quiet' : 'long_quiet';
}

function diagnosticFailurePhase(
  task: PublicActiveExecution | ExecutionHistoryRecord,
  observation: RuntimeExecutionObservation | null
): RuntimeFailurePhase | null {
  if (!('final_state' in task)) return null;
  if (task.final_state === 'completed' || task.final_state === 'cancelled' || task.final_state === 'client_closed') return null;
  if (task.final_state === 'unconfirmed_reaped' || task.failure_reason === 'executor_restarted') return 'termination';
  if (task.final_state === 'spawn_failed') return 'transport';
  if (task.final_state === 'timed_out') return 'execution';
  const reachedTransport = traceTime(observation, 'transport_started') !== null;
  const reachedRunning = traceTime(observation, 'execution_running') !== null || parseTime(task.running_at) !== null;
  if (!reachedTransport || !reachedRunning) return 'transport';
  return 'execution';
}

function traceTime(observation: RuntimeExecutionObservation | null, event: string): number | null {
  if (!observation) return null;
  const match = observation.trace.find((item) => item.event === event);
  return match ? parseTime(match.at) : null;
}

function parseTime(value: string | null | undefined): number | null {
  if (!value) return null;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function deltaMs(start: number | null, end: number | null): number | null {
  if (start === null || end === null || end < start) return null;
  return end - start;
}

function ageMs(at: number | null, referenceAt: number): number | null {
  return at === null ? null : Math.max(0, referenceAt - at);
}
