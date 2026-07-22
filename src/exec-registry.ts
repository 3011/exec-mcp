import { randomUUID } from 'node:crypto';

export type ExecutionState =
  | 'starting'
  | 'running'
  | 'timeout_aborting'
  | 'cancel_aborting'
  | 'client_closed_aborting'
  | 'killing';

export type FinalExecutionState =
  | 'completed'
  | 'failed'
  | 'cancelled'
  | 'timed_out'
  | 'client_closed'
  | 'spawn_failed'
  | 'unconfirmed_reaped';

export type AbortReason =
  | 'request_timeout'
  | 'manual_cancel'
  | 'mcp_notification_cancel'
  | 'http_disconnect'
  | 'reaper_grace_exceeded';

export class TooManyActiveExecsError extends Error {
  readonly code = 'too_many_active_execs';

  constructor(message: string) {
    super(message);
    this.name = 'TooManyActiveExecsError';
  }
}

export class ExecutionCircuitOpenError extends Error {
  readonly code = 'execution_circuit_open';
  readonly reason = 'unconfirmed_reaped_transport';
  readonly unconfirmedCount: number;

  constructor(unconfirmedCount: number) {
    super(`execution_circuit_open: reason=unconfirmed_reaped_transport unconfirmed_count=${unconfirmedCount}`);
    this.name = 'ExecutionCircuitOpenError';
    this.unconfirmedCount = unconfirmedCount;
  }
}

const ABORT_STATES = {
  request_timeout: 'timeout_aborting',
  manual_cancel: 'cancel_aborting',
  mcp_notification_cancel: 'cancel_aborting',
  http_disconnect: 'client_closed_aborting',
  reaper_grace_exceeded: 'killing'
} as const satisfies Record<AbortReason, ExecutionState>;

export interface ExecutionMetadata {
  label?: string | null;
  commandPreview?: string | null;
  commandSha256?: string | null;
  commandLength?: number;
  cwd?: string | null;
}

export interface ExecutionRecord {
  id: string;
  state: ExecutionState;
  abortReason: AbortReason | null;
  abortSource: string | null;
  createdAt: number;
  deadlineAt: number;
  transportStartedAt: number | null;
  runningAt: number | null;
  abortRequestedAt: number | null;
  timeoutMs: number;
  timeoutSeconds: number;
  label: string | null;
  commandPreview: string | null;
  commandSha256: string | null;
  commandLength: number;
  cwd: string | null;
  transportPid: number | null;
  remotePid: number | null;
  remotePgid: number | null;
  controller: AbortController;
  transportExitConfirmed: boolean;
  remoteExitConfirmed: boolean | null;
  finalized: boolean;
  timer: NodeJS.Timeout | null;
  emergencyTimer: NodeJS.Timeout | null;
}

export interface ExecutionHistoryRecord {
  exec_id: string;
  label: string | null;
  command_sha256: string | null;
  command_length: number;
  final_state: FinalExecutionState;
  abort_reason: AbortReason | null;
  abort_source: string | null;
  started_at: string;
  finished_at: string;
  duration_ms: number;
  exit_code: number | null;
  signal: NodeJS.Signals | null;
  timed_out: boolean;
  transport_exit_confirmed: boolean;
  remote_exit_confirmed: boolean | null;
  diagnostic?: string;
  late_exit_observed_at?: string;
}

export interface PublicActiveExecution {
  exec_id: string;
  state: ExecutionState;
  label: string | null;
  command_preview: string | null;
  command_sha256: string | null;
  command_length: number;
  cwd: string | null;
  timeout_seconds: number;
  elapsed_seconds: number;
  created_at: string;
  transport_started_at: string | null;
  running_at: string | null;
  transport_pid: number | null;
  remote_pid: number | null;
  remote_pgid: number | null;
  abort_reason: AbortReason | null;
  transport_exit_confirmed: boolean;
  remote_exit_confirmed: boolean | null;
}

export interface FinalizeInput {
  exitCode?: number | null;
  signal?: NodeJS.Signals | null;
  transportExitConfirmed?: boolean;
  finalState?: FinalExecutionState;
  spawnFailed?: boolean;
  diagnostic?: string;
}

interface RegistryOptions {
  maxActive?: number;
  historyLimit?: number;
  reapIntervalMs?: number;
  reapGraceMs?: number;
  emergencyReapMs?: number;
}

interface RegistryMetrics {
  invariantViolations: number;
  unconfirmedReapedTotal: number;
  lateTransportCloseTotal: number;
}

export type CancelResult =
  | { exec_id: string; result: 'idempotent'; accepted: true; idempotent: true; state: ExecutionState }
  | { exec_id: string; result: 'conflicting_abort_reason'; accepted: false; state: ExecutionState; abort_reason: AbortReason }
  | { exec_id: string; result: 'accepted'; accepted: true; idempotent: false; state: 'cancel_aborting' }
  | { exec_id: string; result: 'already_finished'; accepted: false; final_state: FinalExecutionState }
  | { exec_id: string; result: 'exec_not_found'; accepted: false };

export class ExecRegistry {
  readonly maxActive: number;
  readonly historyLimit: number;
  readonly reapGraceMs: number;
  readonly emergencyReapMs: number;
  readonly active = new Map<string, ExecutionRecord>();
  readonly recent: ExecutionHistoryRecord[] = [];
  readonly unconfirmed = new Map<string, ExecutionHistoryRecord>();
  readonly metrics: RegistryMetrics = {
    invariantViolations: 0,
    unconfirmedReapedTotal: 0,
    lateTransportCloseTotal: 0
  };
  onEmergencyReap: ((record: ExecutionHistoryRecord | null) => void) | null = null;
  private readonly reaper: NodeJS.Timeout;

  constructor({ maxActive, historyLimit = 100, reapIntervalMs = 10000, reapGraceMs = 30000, emergencyReapMs = 30000 }: RegistryOptions = {}) {
    if (typeof maxActive !== 'number' || !Number.isInteger(maxActive) || maxActive <= 0) throw new Error('maxActive must be a positive integer');
    this.maxActive = maxActive;
    this.historyLimit = historyLimit;
    this.reapGraceMs = reapGraceMs;
    this.emergencyReapMs = emergencyReapMs;
    this.reaper = setInterval(() => this.reap(), reapIntervalMs);
    this.reaper.unref?.();
  }

  get activeCount(): number { return this.active.size; }
  get circuitOpen(): boolean { return this.unconfirmed.size > 0; }

  acquire({ timeoutMs, metadata = {} }: { timeoutMs: number; metadata?: ExecutionMetadata }): ExecutionRecord {
    if (!Number.isInteger(timeoutMs) || timeoutMs <= 0) throw new Error('timeoutMs must be a positive integer');
    if (this.circuitOpen) throw new ExecutionCircuitOpenError(this.unconfirmed.size);
    if (this.active.size >= this.maxActive) throw new TooManyActiveExecsError(this.tooManyActiveMessage());

    const now = Date.now();
    const id = `exec-${randomUUID()}`;
    const controller = new AbortController();
    const rec: ExecutionRecord = {
      id,
      state: 'starting',
      abortReason: null,
      abortSource: null,
      createdAt: now,
      deadlineAt: now + timeoutMs,
      transportStartedAt: null,
      runningAt: null,
      abortRequestedAt: null,
      timeoutMs,
      timeoutSeconds: Math.ceil(timeoutMs / 1000),
      label: metadata.label ?? null,
      commandPreview: metadata.commandPreview ?? null,
      commandSha256: metadata.commandSha256 ?? null,
      commandLength: metadata.commandLength ?? 0,
      cwd: metadata.cwd ?? null,
      transportPid: null,
      remotePid: null,
      remotePgid: null,
      controller,
      transportExitConfirmed: false,
      remoteExitConfirmed: null,
      finalized: false,
      timer: null,
      emergencyTimer: null
    };
    rec.timer = setTimeout(() => this.requestAbort(id, 'request_timeout', 'timeout'), timeoutMs);
    rec.timer.unref?.();
    this.active.set(id, rec);
    return rec;
  }

  markTransportStarted(id: string, pid: number | undefined): boolean {
    const rec = this.active.get(id);
    if (!rec || rec.finalized) return false;
    rec.transportPid = pid ?? null;
    rec.transportStartedAt = Date.now();
    return true;
  }

  markRunning(id: string): boolean {
    return this.transition(id, ['starting'], 'running', { runningAt: Date.now() });
  }

  transition(id: string, expectedStates: readonly ExecutionState[], nextState: ExecutionState, fields: Partial<ExecutionRecord> = {}): boolean {
    const rec = this.active.get(id);
    if (!rec || rec.finalized || !expectedStates.includes(rec.state)) return false;
    rec.state = nextState;
    Object.assign(rec, fields);
    return true;
  }

  requestAbort(id: string, reason: AbortReason, source: string = reason): { found: boolean; accepted?: boolean; idempotent?: boolean; record?: ExecutionRecord } {
    const rec = this.active.get(id);
    if (!rec || rec.finalized) return { found: false };
    if (rec.abortReason) {
      return { found: true, accepted: false, idempotent: rec.abortReason === reason, record: rec };
    }
    rec.abortReason = reason;
    rec.abortSource = source;
    rec.abortRequestedAt = Date.now();
    rec.state = ABORT_STATES[reason] || 'cancel_aborting';
    if (!rec.controller.signal.aborted) rec.controller.abort(new Error(reason));
    return { found: true, accepted: true, idempotent: false, record: rec };
  }

  requestCancel(id: string): CancelResult {
    const rec = this.active.get(id);
    if (rec) {
      if (rec.abortReason === 'manual_cancel') {
        return { exec_id: id, result: 'idempotent', accepted: true, idempotent: true, state: rec.state };
      }
      if (rec.abortReason) {
        return { exec_id: id, result: 'conflicting_abort_reason', accepted: false, state: rec.state, abort_reason: rec.abortReason };
      }
      this.requestAbort(id, 'manual_cancel', 'manual_tool');
      return { exec_id: id, result: 'accepted', accepted: true, idempotent: false, state: 'cancel_aborting' };
    }
    const history = this.findRecent(id);
    if (history) return { exec_id: id, result: 'already_finished', accepted: false, final_state: history.final_state };
    return { exec_id: id, result: 'exec_not_found', accepted: false };
  }

  markKilling(id: string): boolean {
    const rec = this.active.get(id);
    if (!rec || rec.finalized) return false;
    rec.state = 'killing';
    return true;
  }

  finalize(id: string, result: FinalizeInput = {}): { finalized: boolean; record: ExecutionHistoryRecord | null } {
    const rec = this.active.get(id);
    if (!rec || rec.finalized) {
      if (result.transportExitConfirmed) this.observeLateTransportClose(id, result);
      return { finalized: false, record: this.findRecent(id) };
    }
    rec.finalized = true;
    if (rec.timer) clearTimeout(rec.timer);
    if (rec.emergencyTimer) clearTimeout(rec.emergencyTimer);
    rec.transportExitConfirmed = result.transportExitConfirmed === true;
    const finalState = result.finalState || inferFinalState(rec, result);
    const history = historyRecord(rec, finalState, result);
    this.active.delete(id);
    this.pushRecent(history);
    return { finalized: true, record: history };
  }

  forceReap(id: string): boolean {
    const rec = this.active.get(id);
    if (!rec || rec.finalized) return false;
    this.metrics.unconfirmedReapedTotal++;
    const finalized = this.finalize(id, {
      finalState: 'unconfirmed_reaped',
      transportExitConfirmed: false,
      diagnostic: 'registry capacity forcibly released before transport close confirmation'
    });
    if (finalized.record) this.unconfirmed.set(id, finalized.record);
    this.onEmergencyReap?.(finalized.record);
    return true;
  }

  observeLateTransportClose(id: string, result: FinalizeInput = {}): boolean {
    const rec = this.unconfirmed.get(id) || this.findRecent(id);
    if (!rec || rec.transport_exit_confirmed) return false;
    rec.transport_exit_confirmed = true;
    rec.late_exit_observed_at = new Date().toISOString();
    if (result.exitCode !== undefined) rec.exit_code = result.exitCode;
    if (result.signal !== undefined) rec.signal = result.signal;
    this.unconfirmed.delete(id);
    this.metrics.lateTransportCloseTotal++;
    return true;
  }

  reap(now = Date.now()): void {
    for (const rec of this.active.values()) {
      if (rec.finalized || now <= rec.deadlineAt + this.reapGraceMs) continue;
      if (rec.state !== 'killing') {
        this.requestAbort(rec.id, rec.abortReason || 'reaper_grace_exceeded', 'reaper');
        rec.state = 'killing';
        rec.emergencyTimer = setTimeout(() => this.forceReap(rec.id), this.emergencyReapMs);
        rec.emergencyTimer.unref?.();
      }
    }
  }

  listActive(now = Date.now()): PublicActiveExecution[] {
    return [...this.active.values()]
      .sort((a, b) => a.createdAt - b.createdAt)
      .map((rec) => publicActive(rec, now));
  }

  status(id: string, now = Date.now()):
    | { found: true; source: 'active'; task: PublicActiveExecution }
    | { found: true; source: 'recent' | 'unconfirmed'; task: ExecutionHistoryRecord }
    | { found: false; result: 'exec_not_found'; exec_id: string } {
    const active = this.active.get(id);
    if (active) return { found: true, source: 'active', task: publicActive(active, now) };
    const recent = this.findRecent(id);
    if (recent) return { found: true, source: 'recent', task: { ...recent } };
    const unconfirmed = this.unconfirmed.get(id);
    if (unconfirmed) return { found: true, source: 'unconfirmed', task: { ...unconfirmed } };
    return { found: false, result: 'exec_not_found', exec_id: id };
  }

  findRecent(id: string): ExecutionHistoryRecord | null {
    return this.recent.find((item) => item.exec_id === id) || null;
  }

  pushRecent(record: ExecutionHistoryRecord): void {
    this.recent.push(record);
    while (this.recent.length > this.historyLimit) {
      this.recent.shift();
    }
  }

  tooManyActiveMessage(): string {
    const records = this.listActive();
    const oldest = records.reduce((max, rec) => Math.max(max, rec.elapsed_seconds), 0);
    const states = new Map<ExecutionState, number>();
    for (const rec of records) states.set(rec.state, (states.get(rec.state) || 0) + 1);
    const statesText = [...states.entries()]
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([state, count]) => `${state}:${count}`)
      .join(',');
    return `too_many_active_execs: active=${this.active.size} max=${this.maxActive} oldest_age_seconds=${oldest}${statesText ? ` states=${statesText}` : ''}`;
  }

  close(): void {
    clearInterval(this.reaper);
    for (const rec of this.active.values()) {
      if (rec.timer) clearTimeout(rec.timer);
      if (rec.emergencyTimer) clearTimeout(rec.emergencyTimer);
    }
  }
}

function inferFinalState(rec: ExecutionRecord, result: FinalizeInput): FinalExecutionState {
  if (rec.abortReason === 'request_timeout' || rec.abortReason === 'reaper_grace_exceeded') return 'timed_out';
  if (rec.abortReason === 'manual_cancel' || rec.abortReason === 'mcp_notification_cancel') return 'cancelled';
  if (rec.abortReason === 'http_disconnect') return 'client_closed';
  if (result.spawnFailed) return 'spawn_failed';
  return result.exitCode === 0 ? 'completed' : 'failed';
}

function publicActive(rec: ExecutionRecord, now: number): PublicActiveExecution {
  return {
    exec_id: rec.id,
    state: rec.state,
    label: rec.label,
    command_preview: rec.commandPreview,
    command_sha256: rec.commandSha256,
    command_length: rec.commandLength,
    cwd: rec.cwd,
    timeout_seconds: rec.timeoutSeconds,
    elapsed_seconds: Math.max(0, Math.floor((now - rec.createdAt) / 1000)),
    created_at: new Date(rec.createdAt).toISOString(),
    transport_started_at: rec.transportStartedAt ? new Date(rec.transportStartedAt).toISOString() : null,
    running_at: rec.runningAt ? new Date(rec.runningAt).toISOString() : null,
    transport_pid: rec.transportPid,
    remote_pid: rec.remotePid,
    remote_pgid: rec.remotePgid,
    abort_reason: rec.abortReason,
    transport_exit_confirmed: rec.transportExitConfirmed,
    remote_exit_confirmed: rec.remoteExitConfirmed
  };
}

function historyRecord(rec: ExecutionRecord, finalState: FinalExecutionState, result: FinalizeInput): ExecutionHistoryRecord {
  const finishedAt = Date.now();
  const history: ExecutionHistoryRecord = {
    exec_id: rec.id,
    label: rec.label,
    command_sha256: rec.commandSha256,
    command_length: rec.commandLength,
    final_state: finalState,
    abort_reason: rec.abortReason,
    abort_source: rec.abortSource,
    started_at: new Date(rec.createdAt).toISOString(),
    finished_at: new Date(finishedAt).toISOString(),
    duration_ms: Math.max(0, finishedAt - rec.createdAt),
    exit_code: result.exitCode ?? null,
    signal: result.signal ?? null,
    timed_out: finalState === 'timed_out',
    transport_exit_confirmed: result.transportExitConfirmed === true,
    remote_exit_confirmed: null
  };
  if (result.diagnostic) history.diagnostic = result.diagnostic;
  return history;
}
