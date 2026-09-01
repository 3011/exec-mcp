import type { ChildProcessWithoutNullStreams } from 'node:child_process';
import type { ExecMcpConfig } from './config.js';
import { ExecRejectedError } from './execution-types.js';
import type { ExecEvent, ExecMetrics, ExecSummary, ExecutionSpec } from './execution-types.js';
import type { AbortReason, ExecutionHistoryRecord, ExecutionRecord, ExecutionState, FinalExecutionState } from './exec-registry.js';
import type { ExecRegistry } from './exec-registry.js';
import type { JobLogBuffer } from './job-log.js';
import type { JobOutputRedactor } from './job-output-redactor.js';
import { redact } from './redact.js';
import { RingBuffer } from './ring-buffer.js';
import { runtimeStateLevel } from './runtime-observer.js';
import type { RuntimeObserver } from './runtime-observer.js';
import { spawnRemoteProcess, terminateLocalProcessGroup } from './ssh-transport.js';
import {
  REMOTE_SUPERVISOR_PROTOCOL_VERSION,
  REMOTE_SUPERVISOR_PY,
  REMOTE_SUPERVISOR_RECONCILE_PY,
  RemoteSupervisorFrameDecoder,
  encodeSupervisorAbort,
  encodeSupervisorAck,
  encodeSupervisorConfig,
  parseSupervisorJson
} from './remote-supervisor.js';
import type { RemoteSupervisorDecision, RemoteSupervisorError, RemoteSupervisorOutcomeReason, RemoteSupervisorResult, RemoteSupervisorStarted } from './remote-supervisor.js';

type EventPayload = { type: string; [key: string]: unknown };

export interface RemoteExecutionSessionJob {
  record: ExecutionRecord;
  spec: ExecutionSpec;
  emit: (event: ExecEvent) => void;
  stdoutLog: JobLogBuffer;
  stderrLog: JobLogBuffer;
  stdoutRedactor: JobOutputRedactor;
  stderrRedactor: JobOutputRedactor;
}

export interface RemoteExecutionSessionOptions {
  config: ExecMcpConfig;
  registry: ExecRegistry;
  metrics: ExecMetrics;
  runtimeObserver: RuntimeObserver;
  job: RemoteExecutionSessionJob;
  logLifecycle: (state: ExecutionState | FinalExecutionState, execId: string | undefined, fields?: Record<string, unknown>) => void;
  onFinalized: (record: ExecutionHistoryRecord | null) => void;
}

export class RemoteExecutionSession {
  constructor(private readonly options: RemoteExecutionSessionOptions) {}

  async run(): Promise<ExecSummary> {
    const { config, registry, metrics, runtimeObserver, job, logLifecycle, onFinalized } = this.options;

    const { record: rec, spec: req } = job;
    const execId = rec.id;
    let seq = 0;
    let stdoutBytes = 0;
    let stderrBytes = 0;
    let forwardedBytes = 0;
    let truncated = false;
    let timeoutCounted = false;
    let disconnectCounted = false;
    let killedSignal: NodeJS.Signals | null = null;
    let childExited = false;
    let heartbeat: NodeJS.Timeout | null = null;
    let sigkillTimer: NodeJS.Timeout | null = null;
    let abortFallbackTimer: NodeJS.Timeout | null = null;
    const supervisorState: { result: RemoteSupervisorResult | null; error: RemoteSupervisorError | null; protocolError: string | null } = {
      result: null, error: null, protocolError: null
    };
    const transportStderr = new RingBuffer(8192);
    const decoder = new RemoteSupervisorFrameDecoder();
    const acceptedAt = new Date(rec.createdAt);
    const tailBufferBytes = Math.min(config.ringBufferBytes, req.maxOutputBytes);
    const stdoutTail = new RingBuffer(tailBufferBytes);
    const stderrTail = new RingBuffer(tailBufferBytes);
    const send = (event: EventPayload): void => job.emit({ exec_id: execId, ...event });

    let child: ChildProcessWithoutNullStreams | undefined;
    let finalSummary: ExecSummary | null = null;
    let spawnFailed = false;

    const killGroup = (signal: NodeJS.Signals): void => {
      if (childExited || !child?.pid) return;
      killedSignal = signal;
      terminateLocalProcessGroup(child, signal);
      bumpMap(metrics.killedTotal, signal);
    };

    const scheduleSigkill = (delaySeconds = config.killGraceSeconds, action = 'sigkill'): void => {
      if (sigkillTimer) return;
      sigkillTimer = setTimeout(() => {
        if (!childExited) {
          registry.markKilling(rec.id);
          send({ type: 'error', code: 'transport_termination_fallback', message: action });
          killGroup('SIGKILL');
        }
      }, delaySeconds * 1000);
      sigkillTimer.unref?.();
    };

    const requestSupervisorAbort = (reasonCode: string): void => {
      if (!child || childExited) return;
      try {
        if (child.stdin.writable) child.stdin.write(encodeSupervisorAbort(reasonCode));
      } catch {}
      if (!abortFallbackTimer) {
        abortFallbackTimer = setTimeout(() => {
          if (childExited) return;
          registry.markKilling(rec.id);
          killGroup('SIGTERM');
          scheduleSigkill(config.killGraceSeconds, 'local_transport_sigkill_fallback');
        }, (config.killGraceSeconds + 6) * 1000);
        abortFallbackTimer.unref?.();
      }
    };

    const noteTimeout = (reasonCode: string, action: string): void => {
      if (!timeoutCounted) {
        timeoutCounted = true;
        metrics.timeoutTotal++;
      }
      send({ type: 'timeout', timeout_seconds: req.timeoutSeconds, action, reason: reasonCode });
    };

    const onRegistryAbort = (): void => {
      const reasonCode = abortReasonCode(rec.controller.signal.reason);
      bumpMap(metrics.abortRequestedTotal, reasonCode);
      runtimeObserver.event(rec.id, 'abort_requested', { level: runtimeStateLevel(rec.state), detail: `${reasonCode} · ${rec.abortSource || 'unknown source'}` });
      logLifecycle(rec.state, rec.id, { abort_source: rec.abortSource, transport_pid: rec.transportPid });
      if (reasonCode === 'request_timeout' || reasonCode === 'reaper_grace_exceeded') {
        noteTimeout(reasonCode, 'remote_supervisor_request');
      } else if (reasonCode !== 'executor_shutdown' && !disconnectCounted) {
        disconnectCounted = true;
        metrics.streamDisconnectTotal++;
      }
      requestSupervisorAbort(reasonCode);
    };

    const maybeForward = (stream: 'stdout' | 'stderr', chunk: Buffer): void => {
      const len = chunk.length;
      runtimeObserver.output(rec.id, stream, len);
      const streamText = redact(chunk.toString('utf8'));
      const jobLogText = stream === 'stdout' ? job.stdoutRedactor.push(chunk) : job.stderrRedactor.push(chunk);
      if (stream === 'stdout') {
        stdoutBytes += len;
        metrics.outputBytesTotal.stdout += len;
        stdoutTail.append(chunk);
        job.stdoutLog.append(jobLogText);
      } else {
        stderrBytes += len;
        metrics.outputBytesTotal.stderr += len;
        stderrTail.append(chunk);
        job.stderrLog.append(jobLogText);
      }

      if (forwardedBytes < req.maxOutputBytes) {
        const remain = req.maxOutputBytes - forwardedBytes;
        const redactedBuffer = Buffer.from(streamText, 'utf8');
        const toSend = redactedBuffer.length > remain ? redactedBuffer.subarray(0, remain) : redactedBuffer;
        forwardedBytes += toSend.length;
        send({ type: stream, data: toSend.toString('utf8'), seq: ++seq });
      }

      if (!truncated && stdoutBytes + stderrBytes > req.maxOutputBytes) {
        truncated = true;
        metrics.truncatedTotal++;
        send({ type: 'truncated', stream: 'combined', max_output_bytes: req.maxOutputBytes });
      }
    };

    const handleSupervisorFrame = (type: string, payload: Buffer): void => {
      if (type === 'O') {
        maybeForward('stdout', payload);
        return;
      }
      if (type === 'E') {
        maybeForward('stderr', payload);
        return;
      }
      if (type === 'S') {
        const started = parseSupervisorJson<RemoteSupervisorStarted>(payload);
        if (started.protocol !== REMOTE_SUPERVISOR_PROTOCOL_VERSION || !Number.isInteger(started.pid) || !Number.isInteger(started.pgid)) {
          throw new Error('invalid remote supervisor started frame');
        }
        rec.remotePid = started.pid;
        rec.remotePgid = started.pgid;
        return;
      }
      if (type === 'D') {
        const decision = parseSupervisorJson<RemoteSupervisorDecision>(payload);
        if (decision.protocol !== REMOTE_SUPERVISOR_PROTOCOL_VERSION || decision.exec_id !== execId || !isRemoteOutcomeReason(decision.reason)) {
          throw new Error('invalid remote supervisor decision frame');
        }
        const remoteAbortReason = remoteOutcomeAbortReason(decision.reason);
        if (remoteAbortReason) registry.observeRemoteAbortDecision(execId, remoteAbortReason);
        if (isRemoteTimeout(decision.reason) && !timeoutCounted) noteTimeout(decision.reason, 'remote_supervisor_decision');
        runtimeObserver.event(execId, 'remote_termination_decided', {
          level: runtimeStateLevel(rec.state),
          detail: `${decision.reason} · supervisor`
        });
        logLifecycle(rec.state, rec.id, { abort_source: rec.abortSource, transport_pid: rec.transportPid, remote_reason: decision.reason });
        return;
      }
      if (type === 'R') {
        const result = parseSupervisorJson<RemoteSupervisorResult>(payload);
        if (result.protocol !== REMOTE_SUPERVISOR_PROTOCOL_VERSION || result.exec_id !== execId || !isRemoteOutcomeReason(result.reason)) {
          throw new Error('invalid remote supervisor result frame');
        }
        supervisorState.result = result;
        try {
          if (child?.stdin.writable) child.stdin.write(encodeSupervisorAck(execId));
        } catch {}
        if (result.reason === 'request_timeout' || result.reason === 'reaper_grace_exceeded') {
          if (!timeoutCounted) noteTimeout(result.reason, 'remote_supervisor_confirmed');
        }
        return;
      }
      if (type === 'X') {
        const error = parseSupervisorJson<RemoteSupervisorError>(payload);
        supervisorState.error = error;
      }
    };

    try {
      rec.controller.signal.addEventListener('abort', onRegistryAbort, { once: true });
      if (rec.controller.signal.aborted) {
        onRegistryAbort();
        throw new ExecRejectedError('request_cancelled', abortReasonCode(rec.controller.signal.reason));
      }

      const spawned = spawnCommand(config, req, execId);
      child = spawned.child;
      registry.markTransportStarted(rec.id, child.pid);
      runtimeObserver.event(rec.id, 'transport_started', { detail: child.pid ? `pid ${child.pid}` : null });
      child.stdin.on('error', () => {});
      child.stdin.write(spawned.bootstrap);
      req.env = {};

      if (!registry.markRunning(rec.id)) {
        if (rec.controller.signal.aborted) onRegistryAbort();
      } else {
        metrics.startedTotal++;
        runtimeObserver.event(rec.id, 'execution_running');
        logLifecycle('running', rec.id, { label: rec.label, execution_class: rec.executionClass, transport_pid: child.pid });
      }
      send({ type: 'start', transport_pid: child.pid, started_at: new Date(rec.runningAt || rec.createdAt).toISOString(), cwd: req.cwd });

      child.stdout.on('data', (chunk: Buffer) => {
        if (supervisorState.protocolError) return;
        try {
          for (const frame of decoder.push(chunk)) handleSupervisorFrame(frame.type, frame.payload);
        } catch (err) {
          supervisorState.protocolError = errorMessage(err);
          killGroup('SIGTERM');
          scheduleSigkill(1, 'protocol_error');
        }
      });
      child.stderr.on('data', (chunk: Buffer) => transportStderr.append(chunk));

      heartbeat = setInterval(() => {
        runtimeObserver.touch(rec.id);
        send({
          type: 'heartbeat',
          elapsed_ms: Date.now() - acceptedAt.getTime(),
          stdout_bytes: stdoutBytes,
          stderr_bytes: stderrBytes
        });
      }, config.heartbeatSeconds * 1000);
      heartbeat.unref?.();

      const runningChild = child;
      const transport = await new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolveTransport) => {
        let finished = false;
        const finish = (code: number | null, signal: NodeJS.Signals | null): void => {
          if (finished) return;
          finished = true;
          childExited = true;
          try { decoder.finish(); } catch (err) { supervisorState.protocolError ||= errorMessage(err); }
          runtimeObserver.event(rec.id, 'transport_closed', { detail: code !== null ? `exit ${code}` : signal });
          resolveTransport({ code, signal });
        };
        runningChild.on('error', (err) => {
          spawnFailed = true;
          send({ type: 'error', code: 'spawn_failed', message: err.message });
        });
        runningChild.on('exit', () => { childExited = true; });
        runningChild.on('close', finish);
      });

      if (!supervisorState.result && !supervisorState.error && !spawnFailed) {
        // Primary transport closed without an authoritative result. Quarantine execution
        // admission immediately while a bounded journal-recovery window runs.
        registry.markRemoteUnconfirmed(execId);
        const recovered = await reconcileRemoteSupervisorResultUntil(
          config,
          execId,
          Math.max(5, Math.min(30, config.emergencyReapSeconds))
        );
        if (recovered) {
          registry.markRemoteConfirmed(execId);
          supervisorState.result = recovered;
          if (isRemoteTimeout(recovered.reason) && !timeoutCounted) {
            noteTimeout(recovered.reason, 'remote_supervisor_reconciled');
          }
        }
      }

      if (!supervisorState.result) {
        const internal = transportStderr.toString();
        if (internal) maybeForward('stderr', Buffer.from(internal, 'utf8'));
        if (supervisorState.error) {
          const message = `${supervisorState.error.code}: ${supervisorState.error.message}`;
          maybeForward('stderr', Buffer.from((internal ? '\n' : '') + message + '\n', 'utf8'));
        } else if (supervisorState.protocolError) {
          maybeForward('stderr', Buffer.from((internal ? '\n' : '') + `remote_supervisor_protocol_error: ${supervisorState.protocolError}\n`, 'utf8'));
        }
      }

      const stdoutRemainder = job.stdoutRedactor.flush();
      const stderrRemainder = job.stderrRedactor.flush();
      if (stdoutRemainder) job.stdoutLog.append(stdoutRemainder);
      if (stderrRemainder) job.stderrLog.append(stderrRemainder);
      const durationMs = Date.now() - acceptedAt.getTime();
      metrics.durationMsTotal += durationMs;
      const code = supervisorState.result?.exit_code ?? supervisorState.error?.exit_code ?? null;
      const signal = supervisorState.result ? toNodeSignal(supervisorState.result.signal) : (transport.signal || killedSignal);
      bumpMap(metrics.exitCodeTotal, String(code ?? signal ?? 'null'));
      const tails = boundedRedactedTails(stdoutTail.toString(), stderrTail.toString(), req.maxOutputBytes);
      const summary: ExecSummary = {
        exec_id: execId,
        type: 'exit',
        code,
        signal,
        duration_ms: durationMs,
        stdout_bytes: stdoutBytes,
        stderr_bytes: stderrBytes,
        truncated,
        timed_out: supervisorState.result ? isRemoteTimeout(supervisorState.result.reason) : false,
        stdout_tail: tails.stdout_tail,
        stderr_tail: tails.stderr_tail,
        task_handle: rec.taskHandle
      };
      finalSummary = summary;
      send({ ...summary });
      return summary;
    } catch (err) {
      if (!child && !rec.abortReason) {
        spawnFailed = true;
        bumpMap(metrics.rejectedTotal, 'spawn_failed');
      }
      if (err instanceof ExecRejectedError) throw err;
      throw new ExecRejectedError('spawn_failed', errorMessage(err));
    } finally {
      if (heartbeat) clearInterval(heartbeat);
      if (sigkillTimer) clearTimeout(sigkillTimer);
      if (abortFallbackTimer) clearTimeout(abortFallbackTimer);
      rec.controller.signal.removeEventListener('abort', onRegistryAbort);

      let remoteExitConfirmed: boolean | null = null;
      let finalState: FinalExecutionState | undefined;
      let failureReason: string | undefined;
      if (supervisorState.result) {
        const result = supervisorState.result;
        remoteExitConfirmed = true;
        finalState = remoteOutcomeFinalState(result.reason, result.exit_code);
        const remoteAbortReason = remoteOutcomeAbortReason(result.reason);
        if (remoteAbortReason) {
          if (rec.abortReason !== remoteAbortReason) rec.abortSource = 'remote_supervisor';
          rec.abortReason = remoteAbortReason;
        } else {
          rec.abortReason = null;
          rec.abortSource = null;
        }
        if (result.reason === 'executor_shutdown') failureReason = 'executor_restarted';
        if (result.reason === 'transport_closed') failureReason = 'remote_transport_closed';
        if (result.reason === 'supervisor_signal') failureReason = 'remote_supervisor_signal';
      } else if (child && supervisorState.error) {
        remoteExitConfirmed = true;
        finalState = supervisorState.error.code === 'spawn_failed' ? 'spawn_failed' : 'failed';
        failureReason = supervisorState.error.code;
        rec.abortReason = null;
        rec.abortSource = null;
      } else if (child) {
        remoteExitConfirmed = false;
        finalState = 'failed';
        failureReason = rec.abortReason ? 'remote_termination_unconfirmed' : (supervisorState.protocolError ? 'remote_supervisor_protocol_error' : 'remote_result_missing');
      } else {
        remoteExitConfirmed = true;
      }

      const finalizeInput = {
        exitCode: finalSummary?.code ?? null,
        signal: finalSummary?.signal ?? null,
        transportExitConfirmed: child ? childExited : true,
        remoteExitConfirmed,
        spawnFailed
      } as { exitCode: number | null; signal: NodeJS.Signals | null; transportExitConfirmed: boolean; remoteExitConfirmed: boolean | null; spawnFailed: boolean; finalState?: FinalExecutionState; failureReason?: string };
      if (finalState) finalizeInput.finalState = finalState;
      if (failureReason) finalizeInput.failureReason = failureReason;
      const finalized = registry.finalize(rec.id, finalizeInput);
      onFinalized(finalized.record);
    }
  }
}

function delay(ms: number): Promise<void> {
  return new Promise((resolveDelay) => {
    const timer = setTimeout(resolveDelay, ms);
    timer.unref?.();
  });
}

function abortReasonCode(reason: unknown): string {
  if (reason instanceof Error && reason.message) return reason.message;
  return String(reason || 'aborted');
}

function spawnCommand(config: ExecMcpConfig, req: ExecutionSpec, execId: string): { child: ChildProcessWithoutNullStreams; bootstrap: Buffer } {
  const child = spawnRemoteProcess(config, ['python3', '-c', REMOTE_SUPERVISOR_PY]);
  const bootstrap = encodeSupervisorConfig({
    protocol: REMOTE_SUPERVISOR_PROTOCOL_VERSION,
    exec_id: execId,
    command: req.command,
    shell: req.shell,
    cwd: req.cwd,
    timeout_seconds: req.timeoutSeconds,
    kill_grace_seconds: req.killGraceSeconds,
    allowed_cwds: req.allowedCwds,
    env: req.env
  });
  return { child, bootstrap };
}

async function reconcileRemoteSupervisorResultUntil(
  config: ExecMcpConfig,
  execId: string,
  totalWaitSeconds: number
): Promise<RemoteSupervisorResult | null> {
  const deadline = Date.now() + Math.max(0, totalWaitSeconds) * 1000;
  do {
    const remainingMs = Math.max(0, deadline - Date.now());
    const perAttemptSeconds = Math.min(2, Math.max(0.25, remainingMs / 1000));
    const result = await reconcileRemoteSupervisorResult(config, execId, perAttemptSeconds);
    if (result) return result;
    if (Date.now() >= deadline) break;
    await delay(Math.min(250, Math.max(25, deadline - Date.now())));
  } while (Date.now() < deadline);
  return null;
}

async function reconcileRemoteSupervisorResult(
  config: ExecMcpConfig,
  execId: string,
  waitSeconds: number
): Promise<RemoteSupervisorResult | null> {
  const child = spawnRemoteProcess(config, [
    'python3', '-c', REMOTE_SUPERVISOR_RECONCILE_PY,
    execId, String(Math.max(0, waitSeconds))
  ]);
  child.stdin.end();
  const stdout = new RingBuffer(1024 * 1024);
  const stderr = new RingBuffer(8192);
  child.stdout.on('data', (chunk: Buffer) => stdout.append(chunk));
  child.stderr.on('data', (chunk: Buffer) => stderr.append(chunk));
  const timeoutMs = (config.remote.connectTimeoutSeconds + Math.max(0, waitSeconds) + 3) * 1000;
  return await new Promise<RemoteSupervisorResult | null>((resolveResult) => {
    let settled = false;
    const finish = (value: RemoteSupervisorResult | null) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolveResult(value);
    };
    const timer = setTimeout(() => {
      terminateLocalProcessGroup(child, 'SIGKILL')
      finish(null);
    }, timeoutMs);
    timer.unref?.();
    child.on('error', () => finish(null));
    child.on('close', (code) => {
      if (code !== 0) return finish(null);
      try {
        const result = JSON.parse(stdout.toString()) as RemoteSupervisorResult;
        if (result.protocol !== REMOTE_SUPERVISOR_PROTOCOL_VERSION
          || result.exec_id !== execId
          || !isRemoteOutcomeReason(result.reason)) return finish(null);
        finish(result);
      } catch {
        void stderr;
        finish(null);
      }
    });
  });
}

function isRemoteOutcomeReason(value: unknown): value is RemoteSupervisorOutcomeReason {
  return value === 'exit'
    || value === 'request_timeout'
    || value === 'manual_cancel'
    || value === 'mcp_notification_cancel'
    || value === 'http_disconnect'
    || value === 'reaper_grace_exceeded'
    || value === 'executor_shutdown'
    || value === 'transport_closed'
    || value === 'supervisor_signal';
}

function isRemoteTimeout(reason: RemoteSupervisorOutcomeReason): boolean {
  return reason === 'request_timeout' || reason === 'reaper_grace_exceeded';
}

function remoteOutcomeAbortReason(reason: RemoteSupervisorOutcomeReason): AbortReason | null {
  if (reason === 'request_timeout'
    || reason === 'manual_cancel'
    || reason === 'mcp_notification_cancel'
    || reason === 'http_disconnect'
    || reason === 'reaper_grace_exceeded'
    || reason === 'executor_shutdown') return reason;
  return null;
}

function remoteOutcomeFinalState(reason: RemoteSupervisorOutcomeReason, exitCode: number | null): FinalExecutionState {
  if (reason === 'request_timeout' || reason === 'reaper_grace_exceeded') return 'timed_out';
  if (reason === 'manual_cancel' || reason === 'mcp_notification_cancel') return 'cancelled';
  if (reason === 'http_disconnect') return 'client_closed';
  if (reason === 'exit') return exitCode === 0 ? 'completed' : 'failed';
  return 'failed';
}

function toNodeSignal(value: string | null): NodeJS.Signals | null {
  if (!value) return null;
  const allowed = new Set<NodeJS.Signals>([
    'SIGABRT', 'SIGALRM', 'SIGBUS', 'SIGCHLD', 'SIGCONT', 'SIGFPE', 'SIGHUP', 'SIGILL', 'SIGINT',
    'SIGIO', 'SIGIOT', 'SIGKILL', 'SIGPIPE', 'SIGPOLL', 'SIGPROF', 'SIGPWR', 'SIGQUIT', 'SIGSEGV',
    'SIGSTKFLT', 'SIGSTOP', 'SIGSYS', 'SIGTERM', 'SIGTRAP', 'SIGTSTP', 'SIGTTIN', 'SIGTTOU', 'SIGURG',
    'SIGUSR1', 'SIGUSR2', 'SIGVTALRM', 'SIGWINCH', 'SIGXCPU', 'SIGXFSZ'
  ]);
  return allowed.has(value as NodeJS.Signals) ? value as NodeJS.Signals : null;
}




function boundedRedactedTails(stdoutRaw: string, stderrRaw: string, maxBytes: number): { stdout_tail: string; stderr_tail: string } {
  return boundTailPair(redact(stdoutRaw), redact(stderrRaw), maxBytes);
}

function boundTailPair(stdout: string, stderr: string, maxBytes: number): { stdout_tail: string; stderr_tail: string } {
  const limit = Math.max(0, Number.parseInt(String(maxBytes), 10) || 0);
  if (limit === 0) return { stdout_tail: '', stderr_tail: '' };

  const stdoutBytes = Buffer.byteLength(stdout, 'utf8');
  const stderrBytes = Buffer.byteLength(stderr, 'utf8');
  if (stdoutBytes + stderrBytes <= limit) {
    return { stdout_tail: stdout, stderr_tail: stderr };
  }
  if (stdoutBytes === 0) {
    return { stdout_tail: '', stderr_tail: trimUtf8Tail(stderr, limit) };
  }
  if (stderrBytes === 0) {
    return { stdout_tail: trimUtf8Tail(stdout, limit), stderr_tail: '' };
  }

  let stdoutBudget = Math.min(stdoutBytes, Math.ceil(limit / 2));
  let stderrBudget = Math.min(stderrBytes, limit - stdoutBudget);
  let remaining = limit - stdoutBudget - stderrBudget;
  if (remaining > 0 && stdoutBudget < stdoutBytes) {
    const add = Math.min(stdoutBytes - stdoutBudget, remaining);
    stdoutBudget += add;
    remaining -= add;
  }
  if (remaining > 0 && stderrBudget < stderrBytes) {
    stderrBudget += Math.min(stderrBytes - stderrBudget, remaining);
  }

  return {
    stdout_tail: trimUtf8Tail(stdout, stdoutBudget),
    stderr_tail: trimUtf8Tail(stderr, stderrBudget)
  };
}

function trimUtf8Tail(value: string, maxBytes: number): string {
  if (maxBytes <= 0) return '';
  const buf = Buffer.from(value, 'utf8');
  if (buf.length <= maxBytes) return value;
  let text = buf.subarray(buf.length - maxBytes).toString('utf8');
  while (Buffer.byteLength(text, 'utf8') > maxBytes) {
    text = text.slice(1);
  }
  return text;
}

function bumpMap<K extends string>(map: Map<K, number>, key: K): void {
  map.set(key, (map.get(key) || 0) + 1);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
