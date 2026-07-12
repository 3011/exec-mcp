import { randomUUID } from 'node:crypto';

export class TooManyActiveExecsError extends Error {
  constructor(message) {
    super(message);
    this.name = 'TooManyActiveExecsError';
    this.code = 'too_many_active_execs';
  }
}

export class ExecutionCircuitOpenError extends Error {
  constructor(unconfirmedCount) {
    super(`execution_circuit_open: reason=unconfirmed_reaped_transport unconfirmed_count=${unconfirmedCount}`);
    this.name = 'ExecutionCircuitOpenError';
    this.code = 'execution_circuit_open';
    this.reason = 'unconfirmed_reaped_transport';
    this.unconfirmedCount = unconfirmedCount;
  }
}

const ABORT_STATES = {
  request_timeout: 'timeout_aborting',
  manual_cancel: 'cancel_aborting',
  mcp_notification_cancel: 'cancel_aborting',
  http_disconnect: 'client_closed_aborting',
  reaper_grace_exceeded: 'killing'
};

export class ExecRegistry {
  constructor({ maxActive, historyLimit = 100, reapIntervalMs = 10000, reapGraceMs = 30000, emergencyReapMs = 30000 } = {}) {
    if (!Number.isInteger(maxActive) || maxActive <= 0) throw new Error('maxActive must be a positive integer');
    this.maxActive = maxActive;
    this.historyLimit = historyLimit;
    this.reapGraceMs = reapGraceMs;
    this.emergencyReapMs = emergencyReapMs;
    this.active = new Map();
    this.recent = [];
    this.unconfirmed = new Map();
    this.onEmergencyReap = null;
    this.metrics = { invariantViolations: 0, unconfirmedReapedTotal: 0, lateTransportCloseTotal: 0 };
    this.reaper = setInterval(() => this.reap(), reapIntervalMs);
    this.reaper.unref?.();
  }

  get activeCount() { return this.active.size; }
  get circuitOpen() { return this.unconfirmed.size > 0; }

  acquire({ timeoutMs, metadata = {} }) {
    if (!Number.isInteger(timeoutMs) || timeoutMs <= 0) throw new Error('timeoutMs must be a positive integer');
    if (this.circuitOpen) throw new ExecutionCircuitOpenError(this.unconfirmed.size);
    if (this.active.size >= this.maxActive) throw new TooManyActiveExecsError(this.tooManyActiveMessage());

    const now = Date.now();
    const id = `exec-${randomUUID()}`;
    const controller = new AbortController();
    const rec = {
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

  markTransportStarted(id, pid) {
    const rec = this.active.get(id);
    if (!rec || rec.finalized) return false;
    rec.transportPid = pid ?? null;
    rec.transportStartedAt = Date.now();
    return true;
  }

  markRunning(id) {
    return this.transition(id, ['starting'], 'running', { runningAt: Date.now() });
  }

  transition(id, expectedStates, nextState, fields = {}) {
    const rec = this.active.get(id);
    if (!rec || rec.finalized || !expectedStates.includes(rec.state)) return false;
    rec.state = nextState;
    Object.assign(rec, fields);
    return true;
  }

  requestAbort(id, reason, source = reason) {
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

  requestCancel(id) {
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

  markKilling(id) {
    const rec = this.active.get(id);
    if (!rec || rec.finalized) return false;
    rec.state = 'killing';
    return true;
  }

  finalize(id, result = {}) {
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

  forceReap(id) {
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

  observeLateTransportClose(id, result = {}) {
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

  reap(now = Date.now()) {
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

  listActive(now = Date.now()) {
    return [...this.active.values()]
      .sort((a, b) => a.createdAt - b.createdAt)
      .map((rec) => publicActive(rec, now));
  }

  status(id, now = Date.now()) {
    const active = this.active.get(id);
    if (active) return { found: true, source: 'active', task: publicActive(active, now) };
    const recent = this.findRecent(id);
    if (recent) return { found: true, source: 'recent', task: { ...recent } };
    const unconfirmed = this.unconfirmed.get(id);
    if (unconfirmed) return { found: true, source: 'unconfirmed', task: { ...unconfirmed } };
    return { found: false, result: 'exec_not_found', exec_id: id };
  }

  findRecent(id) { return this.recent.find((item) => item.exec_id === id) || null; }
  pushRecent(record) {
    this.recent.push(record);
    while (this.recent.length > this.historyLimit) {
      this.recent.shift();
    }
  }

  tooManyActiveMessage() {
    const records = this.listActive();
    const oldest = records.reduce((max, rec) => Math.max(max, rec.elapsed_seconds), 0);
    const states = new Map();
    for (const rec of records) states.set(rec.state, (states.get(rec.state) || 0) + 1);
    const statesText = [...states.entries()].sort(([a], [b]) => a.localeCompare(b)).map(([state, count]) => `${state}:${count}`).join(',');
    return `too_many_active_execs: active=${this.active.size} max=${this.maxActive} oldest_age_seconds=${oldest}${statesText ? ` states=${statesText}` : ''}`;
  }

  close() {
    clearInterval(this.reaper);
    for (const rec of this.active.values()) {
      if (rec.timer) clearTimeout(rec.timer);
      if (rec.emergencyTimer) clearTimeout(rec.emergencyTimer);
    }
  }
}

function inferFinalState(rec, result) {
  if (rec.abortReason === 'request_timeout' || rec.abortReason === 'reaper_grace_exceeded') return 'timed_out';
  if (rec.abortReason === 'manual_cancel' || rec.abortReason === 'mcp_notification_cancel') return 'cancelled';
  if (rec.abortReason === 'http_disconnect') return 'client_closed';
  if (result.spawnFailed) return 'spawn_failed';
  return result.exitCode === 0 ? 'completed' : 'failed';
}

function publicActive(rec, now) {
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

function historyRecord(rec, finalState, result) {
  const finishedAt = Date.now();
  return {
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
    remote_exit_confirmed: null,
    ...(result.diagnostic ? { diagnostic: result.diagnostic } : {})
  };
}
