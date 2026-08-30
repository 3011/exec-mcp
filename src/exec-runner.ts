import { spawn } from 'node:child_process';
import type { ChildProcessWithoutNullStreams } from 'node:child_process';
import { createHash } from 'node:crypto';
import { isAbsolute, resolve } from 'node:path';
import type { ExecMcpConfig } from './config.js';
import { RingBuffer } from './ring-buffer.js';
import { JobLogBuffer } from './job-log.js';
import { JobOutputRedactor } from './job-output-redactor.js';
import { redact } from './redact.js';
import { ExecRegistry, ExecutionCircuitOpenError } from './exec-registry.js';
import type { AbortReason, ExecutionClass, ExecutionHistoryRecord, ExecutionRecord, ExecutionState, FinalExecutionState } from './exec-registry.js';
import { RuntimeObserver, runtimeStateLevel } from './runtime-observer.js';
import type { RuntimeOrigin } from './runtime-observer.js';
import { TaskContextStore, TASK_HANDLE_PATTERN } from './task-context.js';
import type { TaskContext } from './task-context.js';

type UnknownRecord = Record<string, unknown>;
type EventPayload = { type: string; [key: string]: unknown };
export type ExecEvent = EventPayload & { exec_id: string };

export interface ExecutionSpec {
  command: string;
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

interface ManagedJob {
  record: ExecutionRecord;
  spec: ExecutionSpec;
  executionClass: ExecutionClass;
  emit: (event: ExecEvent) => void;
  stdoutLog: JobLogBuffer;
  stderrLog: JobLogBuffer;
  stdoutRedactor: JobOutputRedactor;
  stderrRedactor: JobOutputRedactor;
  completion: Promise<ExecSummary>;
  resolveCompletion: (summary: ExecSummary) => void;
  rejectCompletion: (error: unknown) => void;
  started: boolean;
  settled: boolean;
  finishedAt: number | null;
  abortSignal?: AbortSignal;
  abortListener?: () => void;
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

export class ExecRunner {
  readonly config: ExecMcpConfig;
  readonly registry: ExecRegistry;
  readonly metrics: ExecMetrics;
  readonly runtimeObserver: RuntimeObserver;
  readonly taskContexts: TaskContextStore;
  private readonly jobs = new Map<string, ManagedJob>();
  private readonly queue: string[] = [];
  private scheduling = false;
  private readonly jobGc: NodeJS.Timeout;

  constructor(config: ExecMcpConfig) {
    this.config = config;
    this.registry = new ExecRegistry({
      maxActive: config.globalMaxConcurrentExecs,
      historyLimit: config.recentHistoryLimit,
      reapGraceMs: config.registryReapGraceSeconds * 1000,
      emergencyReapMs: config.emergencyReapSeconds * 1000
    });
    this.runtimeObserver = new RuntimeObserver({ maxExecutions: Math.max(100, config.recentHistoryLimit * 2) });
    this.taskContexts = new TaskContextStore(Math.max(200, config.recentHistoryLimit * 4));
    this.registry.onEmergencyReap = (record) => {
      if (record?.exec_id) this.runtimeObserver.event(record.exec_id, 'unconfirmed_reaped', { level: 'error', detail: 'transport exit was not confirmed' });
      this.logLifecycle('unconfirmed_reaped', record?.exec_id, {
      abort_source: record?.abort_source,
      transport_exit_confirmed: false,
      remote_exit_confirmed: null
      });
    };
    this.metrics = {
      requestsTotal: 0,
      rejectedTotal: new Map<string, number>(),
      timeoutTotal: 0,
      killedTotal: new Map<string, number>(),
      truncatedTotal: 0,
      streamDisconnectTotal: 0,
      exitCodeTotal: new Map<string, number>(),
      outputBytesTotal: { stdout: 0, stderr: 0 },
      durationMsTotal: 0,
      durationSecondsBuckets: [0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600, 1800],
      durationSecondsByState: new Map<FinalExecutionState, Histogram>(),
      startedTotal: 0,
      abortRequestedTotal: new Map<string, number>(),
      cancelRequestsTotal: new Map<string, number>(),
      finishedTotal: new Map<FinalExecutionState, number>()
    };
    this.jobGc = setInterval(() => this.pruneJobs(), Math.min(60000, Math.max(5000, config.jobRetentionSeconds * 500)));
    this.jobGc.unref?.();
  }


  beginTask(input: unknown): TaskContext {
    const req: UnknownRecord = isRecord(input) ? input : {};
    return this.taskContexts.create(sanitizeLabel(req.label));
  }

  requireTaskContext(value: unknown): TaskContext {
    if (typeof value !== 'string' || !TASK_HANDLE_PATTERN.test(value)) {
      throw new ExecRejectedError('invalid_task_handle', 'task_handle must be a server-issued task handle from begin_task');
    }
    const context = this.taskContexts.get(value);
    if (!context) {
      throw new ExecRejectedError('unknown_task_handle', 'unknown_task_handle: call begin_task in this conversation and use the returned task_handle');
    }
    return context;
  }

  get active(): number { return this.registry.slotCount; }
  get activeJobs(): number { return this.registry.activeCount; }
  get queued(): number { return this.registry.queuedCount; }

  validate(input: unknown): ExecutionSpec {
    const req: UnknownRecord = isRecord(input) ? input : {};
    const command = typeof req.command === 'string' ? req.command.trim() : '';
    if (!command) throw new ExecRejectedError('invalid_command', 'command must be a non-empty string');

    const cwdInput = String(req.cwd || this.config.defaultCwd);
    if (!isAbsolute(cwdInput)) throw new ExecRejectedError('invalid_cwd', `cwd must be an absolute path: ${cwdInput}`);
    const cwd = resolve(cwdInput);
    if (!isAllowedCwd(cwd, this.config.allowedCwds)) throw new ExecRejectedError('invalid_cwd', `cwd is not allowed: ${cwd}`);

    const timeoutSeconds = clampInt(
      req.timeout_seconds,
      this.config.defaultTimeoutSeconds,
      1,
      this.config.maxTimeoutSeconds,
      'invalid_timeout',
      'timeout_too_large'
    );
    const maxOutputBytes = clampInt(
      req.max_output_bytes,
      this.config.defaultMaxOutputBytes,
      1,
      this.config.hardMaxOutputBytes,
      'invalid_output_limit',
      'output_limit_too_large'
    );

    const env: Record<string, string> = {};
    if (req.env && typeof req.env === 'object' && !Array.isArray(req.env)) {
      for (const [key, value] of Object.entries(req.env)) {
        if (/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) env[key] = String(value);
      }
    }
    delete env.ENV;
    delete env.BASH_ENV;

    const label = sanitizeLabel(req.label);
    const commandSha256 = createHash('sha256').update(command, 'utf8').digest('hex');
    const commandPreview = this.config.exposeRedactedCommandPreview
      ? sanitizePreview(redact(command), this.config.commandPreviewMaxChars)
      : null;

    return {
      command, cwd, timeoutSeconds, maxOutputBytes, env, label, commandSha256,
      commandLength: Buffer.byteLength(command, 'utf8'), commandPreview,
      allowedCwds: this.config.allowedCwds, killGraceSeconds: this.config.killGraceSeconds
    };
  }

  async run(input: unknown, emit: (event: ExecEvent) => void, options: RunOptions = {}): Promise<ExecSummary> {
    const job = this.submit(input, 'sync', emit, options);
    return await job.completion;
  }

  start(input: unknown, options: RunOptions = {}): ReturnType<ExecRunner['startResult']> {
    const job = this.submit(input, 'async', () => {}, options);
    return this.startResult(job);
  }

  private submit(input: unknown, executionClass: ExecutionClass, emit: (event: ExecEvent) => void, options: RunOptions = {}): ManagedJob {
    this.metrics.requestsTotal++;
    throwIfAborted(options.abortSignal);

    let spec: ExecutionSpec;
    let validatedAt: number;
    try {
      spec = this.validate(input);
      validatedAt = Date.now();
    } catch (err) {
      if (err instanceof ExecRejectedError) this.bumpRejected(err.code);
      throw err;
    }

    if (this.registry.circuitOpen) {
      const err = new ExecutionCircuitOpenError(this.registry.unconfirmed.size);
      this.bumpRejected(err.code);
      throw new ExecRejectedError(err.code, err.message, { reason: err.reason, unconfirmed_count: err.unconfirmedCount });
    }

    const canStartNow = this.hasCapacity(executionClass);
    if (!canStartNow && this.registry.queuedCount >= this.config.maxQueuedExecs) {
      this.bumpRejected('exec_queue_full');
      throw new ExecRejectedError(
        'exec_queue_full',
        `exec_queue_full: queued=${this.registry.queuedCount} max_queue=${this.config.maxQueuedExecs}`,
        { queued: this.registry.queuedCount, max_queue: this.config.maxQueuedExecs }
      );
    }

    let rec: ExecutionRecord;
    try {
      rec = this.registry.register({
        timeoutMs: spec.timeoutSeconds * 1000,
        metadata: {
          label: spec.label,
          commandPreview: spec.commandPreview,
          commandSha256: spec.commandSha256,
          commandLength: spec.commandLength,
          cwd: spec.cwd,
          executionClass,
          taskHandle: options.taskContext?.task_handle ?? null
        }
      });
    } catch (err) {
      if (err instanceof ExecutionCircuitOpenError) {
        this.bumpRejected(err.code);
        throw new ExecRejectedError(err.code, err.message, { reason: err.reason, unconfirmed_count: err.unconfirmedCount });
      }
      throw err;
    }

    const runtimeRegistration: Parameters<RuntimeObserver['registerExecution']>[0] = {
      execId: rec.id,
      createdAt: rec.createdAt,
      executionClass,
      label: spec.label,
      cwd: spec.cwd,
      commandPreview: spec.commandPreview,
      commandSha256: spec.commandSha256,
      commandLength: spec.commandLength,
      timeoutSeconds: spec.timeoutSeconds,
      validatedAt
    };
    if (options.taskContext) {
      runtimeRegistration.origin = { ...(options.origin || {}), task_handle: options.taskContext.task_handle };
      this.taskContexts.attach(options.taskContext.task_handle, rec.id, rec.createdAt);
    }
    if (options.traceId !== undefined) runtimeRegistration.traceId = options.traceId;
    if (options.origin !== undefined && !options.taskContext) runtimeRegistration.origin = options.origin;
    if (options.requestReceivedAt !== undefined) runtimeRegistration.requestReceivedAt = options.requestReceivedAt;
    this.runtimeObserver.registerExecution(runtimeRegistration);

    let resolveCompletion!: (summary: ExecSummary) => void;
    let rejectCompletion!: (error: unknown) => void;
    const completion = new Promise<ExecSummary>((resolveJob, rejectJob) => {
      resolveCompletion = resolveJob;
      rejectCompletion = rejectJob;
    });
    void completion.catch(() => {});

    const job: ManagedJob = {
      record: rec,
      spec,
      executionClass,
      emit,
      stdoutLog: new JobLogBuffer(this.config.jobLogBytes),
      stderrLog: new JobLogBuffer(this.config.jobLogBytes),
      stdoutRedactor: new JobOutputRedactor(Object.values(spec.env)),
      stderrRedactor: new JobOutputRedactor(Object.values(spec.env)),
      completion,
      resolveCompletion,
      rejectCompletion,
      started: false,
      settled: false,
      finishedAt: null
    };
    if (options.abortSignal) job.abortSignal = options.abortSignal;
    this.jobs.set(rec.id, job);
    this.queue.push(rec.id);
    this.runtimeObserver.event(rec.id, 'queued', { detail: canStartNow ? 'admission available' : 'waiting for execution capacity' });
    options.onAcquire?.(rec);

    if (options.abortSignal) {
      const onAbort = () => {
        const signalReason = abortReasonCode(options.abortSignal?.reason);
        const reason: AbortReason = signalReason === 'mcp_notification_cancel'
          ? 'mcp_notification_cancel'
          : (options.abortReason || 'http_disconnect');
        const source = reason === 'mcp_notification_cancel' ? 'mcp_notification' : (options.abortSource || 'http');
        this.abortJob(rec.id, reason, source);
      };
      job.abortListener = onAbort;
      options.abortSignal.addEventListener('abort', onAbort, { once: true });
      if (options.abortSignal.aborted) onAbort();
    }

    this.schedule();
    return job;
  }

  private schedule(): void {
    if (this.scheduling) return;
    this.scheduling = true;
    try {
      while (true) {
        const index = this.queue.findIndex((id) => {
          const job = this.jobs.get(id);
          return Boolean(job && !job.started && job.record.state === 'queued' && this.hasCapacity(job.executionClass));
        });
        if (index < 0) break;
        const [id] = this.queue.splice(index, 1);
        if (!id) break;
        const job = this.jobs.get(id);
        if (!job || job.started || job.record.state !== 'queued') continue;
        if (!this.registry.markStarting(id)) continue;
        this.runtimeObserver.event(id, 'starting');
        job.started = true;
        void this.executeJob(job).then(
          (summary) => this.settleJob(job, summary),
          (error) => this.settleJobError(job, error)
        ).finally(() => this.schedule());
      }
      this.compactQueue();
    } finally {
      this.scheduling = false;
    }
  }

  private hasCapacity(executionClass: ExecutionClass): boolean {
    const activeRecords = [...this.registry.active.values()].filter((rec) => rec.state !== 'queued');
    if (activeRecords.length >= this.config.globalMaxConcurrentExecs) return false;
    const classCount = activeRecords.filter((rec) => rec.executionClass === executionClass).length;
    const classMax = executionClass === 'sync' ? this.config.syncMaxConcurrentExecs : this.config.asyncMaxConcurrentExecs;
    return classCount < classMax;
  }

  private async executeJob(job: ManagedJob): Promise<ExecSummary> {
    const { record: rec, spec: req } = job;
    const execId = rec.id;
    let seq = 0;
    let stdoutBytes = 0;
    let stderrBytes = 0;
    let forwardedBytes = 0;
    let truncated = false;
    let timedOut = false;
    let killedSignal: NodeJS.Signals | null = null;
    let childExited = false;
    let timeoutCounted = false;
    let disconnectCounted = false;
    let heartbeat: NodeJS.Timeout | null = null;
    let sigkillTimer: NodeJS.Timeout | null = null;
    let abortFallbackTimer: NodeJS.Timeout | null = null;
    let remoteAbortPromise: Promise<boolean> | null = null;
    const acceptedAt = new Date(rec.createdAt);
    const tailBufferBytes = Math.min(this.config.ringBufferBytes, req.maxOutputBytes);
    const stdoutTail = new RingBuffer(tailBufferBytes);
    const stderrTail = new RingBuffer(tailBufferBytes);
    const send = (event: EventPayload): void => job.emit({ exec_id: execId, ...event });

    let child: ChildProcessWithoutNullStreams | undefined;
    let finalSummary: ExecSummary | null = null;
    let spawnFailed = false;

    const killGroup = (signal: NodeJS.Signals): void => {
      if (childExited || !child?.pid) return;
      killedSignal = signal;
      try {
        process.kill(-child.pid, signal);
        this.bumpMap(this.metrics.killedTotal, signal);
      } catch {
        try { child.kill(signal); } catch {}
      }
    };

    const scheduleSigkill = (delaySeconds = this.config.killGraceSeconds, action = 'sigkill'): void => {
      if (sigkillTimer) return;
      sigkillTimer = setTimeout(() => {
        if (!childExited) {
          this.registry.markKilling(rec.id);
          if (timedOut) send({ type: 'timeout', timeout_seconds: req.timeoutSeconds, action });
          killGroup('SIGKILL');
        }
      }, delaySeconds * 1000);
      sigkillTimer.unref?.();
    };

    const beginRemoteAbort = (): void => {
      if (!child || childExited) return;
      if (!remoteAbortPromise) remoteAbortPromise = requestRemoteCancellation(this.config, execId, this.config.killGraceSeconds);
      if (!abortFallbackTimer) {
        abortFallbackTimer = setTimeout(() => {
          if (childExited) return;
          this.registry.markKilling(rec.id);
          killGroup('SIGTERM');
          scheduleSigkill(this.config.killGraceSeconds, timedOut ? 'local_sigkill_fallback' : 'sigkill');
        }, (this.config.killGraceSeconds + 6) * 1000);
        abortFallbackTimer.unref?.();
      }
    };

    const onRegistryAbort = (): void => {
      const reasonCode = abortReasonCode(rec.controller.signal.reason);
      this.bumpMap(this.metrics.abortRequestedTotal, reasonCode);
      this.runtimeObserver.event(rec.id, 'abort_requested', { level: runtimeStateLevel(rec.state), detail: `${reasonCode} · ${rec.abortSource || 'unknown source'}` });
      this.logLifecycle(rec.state, rec.id, { abort_source: rec.abortSource, transport_pid: rec.transportPid });
      if (reasonCode === 'request_timeout' || reasonCode === 'reaper_grace_exceeded') {
        timedOut = true;
        if (!timeoutCounted) {
          timeoutCounted = true;
          this.metrics.timeoutTotal++;
        }
        send({ type: 'timeout', timeout_seconds: req.timeoutSeconds, action: 'remote_watchdog', reason: reasonCode });
        beginRemoteAbort();
        return;
      }
      if (reasonCode !== 'executor_shutdown' && !disconnectCounted) {
        disconnectCounted = true;
        this.metrics.streamDisconnectTotal++;
      }
      beginRemoteAbort();
    };

    try {
      rec.controller.signal.addEventListener('abort', onRegistryAbort, { once: true });
      if (rec.controller.signal.aborted) {
        onRegistryAbort();
        throw new ExecRejectedError('request_cancelled', abortReasonCode(rec.controller.signal.reason));
      }

      const spawned = spawnCommand(this.config, req, execId);
      child = spawned.child;
      req.env = {};
      this.registry.markTransportStarted(rec.id, child.pid);
      this.runtimeObserver.event(rec.id, 'transport_started', { detail: child.pid ? `pid ${child.pid}` : null });
      if (spawned.stdin) child.stdin.end(spawned.stdin);

      if (!this.registry.markRunning(rec.id)) {
        if (rec.controller.signal.aborted) onRegistryAbort();
      } else {
        this.metrics.startedTotal++;
        this.runtimeObserver.event(rec.id, 'execution_running');
        this.logLifecycle('running', rec.id, { label: rec.label, execution_class: rec.executionClass, transport_pid: child.pid });
      }
      send({ type: 'start', transport_pid: child.pid, started_at: new Date(rec.runningAt || rec.createdAt).toISOString(), cwd: req.cwd });

      const maybeForward = (stream: 'stdout' | 'stderr', chunk: Buffer): void => {
        const len = chunk.length;
        this.runtimeObserver.output(rec.id, stream, len);
        const streamText = redact(chunk.toString('utf8'));
        const jobLogText = stream === 'stdout' ? job.stdoutRedactor.push(chunk) : job.stderrRedactor.push(chunk);
        if (stream === 'stdout') {
          stdoutBytes += len;
          this.metrics.outputBytesTotal.stdout += len;
          stdoutTail.append(chunk);
          job.stdoutLog.append(jobLogText);
        } else {
          stderrBytes += len;
          this.metrics.outputBytesTotal.stderr += len;
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
          this.metrics.truncatedTotal++;
          send({ type: 'truncated', stream: 'combined', max_output_bytes: req.maxOutputBytes });
        }
      };

      child.stdout.on('data', (chunk: Buffer) => maybeForward('stdout', chunk));
      child.stderr.on('data', (chunk: Buffer) => maybeForward('stderr', chunk));

      heartbeat = setInterval(() => {
        this.runtimeObserver.touch(rec.id);
        send({
          type: 'heartbeat',
          elapsed_ms: Date.now() - acceptedAt.getTime(),
          stdout_bytes: stdoutBytes,
          stderr_bytes: stderrBytes
        });
      }, this.config.heartbeatSeconds * 1000);
      heartbeat.unref?.();

      const runningChild = child;
      finalSummary = await new Promise<ExecSummary>((resolveRun) => {
        let finished = false;
        const finish = (code: number | null, signal: NodeJS.Signals | null): void => {
          if (finished) return;
          finished = true;
          childExited = true;
          this.runtimeObserver.event(rec.id, 'transport_closed', { detail: code !== null ? `exit ${code}` : signal });
          const stdoutRemainder = job.stdoutRedactor.flush();
          const stderrRemainder = job.stderrRedactor.flush();
          if (stdoutRemainder) job.stdoutLog.append(stdoutRemainder);
          if (stderrRemainder) job.stderrLog.append(stderrRemainder);
          const durationMs = Date.now() - acceptedAt.getTime();
          this.metrics.durationMsTotal += durationMs;
          this.bumpMap(this.metrics.exitCodeTotal, String(code ?? signal ?? 'null'));
          const tails = boundedRedactedTails(stdoutTail.toString(), stderrTail.toString(), req.maxOutputBytes);
          const summary: Omit<ExecSummary, 'exec_id'> = {
            type: 'exit',
            code,
            signal: signal || killedSignal,
            duration_ms: durationMs,
            stdout_bytes: stdoutBytes,
            stderr_bytes: stderrBytes,
            truncated,
            timed_out: timedOut,
            stdout_tail: tails.stdout_tail,
            stderr_tail: tails.stderr_tail,
            task_handle: rec.taskHandle
          };
          send(summary);
          resolveRun({ exec_id: execId, ...summary });
        };

        runningChild.on('error', (err) => {
          spawnFailed = true;
          send({ type: 'error', code: 'spawn_failed', message: err.message });
        });
        runningChild.on('exit', () => { childExited = true; });
        runningChild.on('close', finish);
      });
      return finalSummary;
    } catch (err) {
      if (!child && !rec.abortReason) {
        spawnFailed = true;
        this.bumpRejected('spawn_failed');
      }
      if (err instanceof ExecRejectedError) throw err;
      throw new ExecRejectedError('spawn_failed', errorMessage(err));
    } finally {
      if (heartbeat) clearInterval(heartbeat);
      if (sigkillTimer) clearTimeout(sigkillTimer);
      if (abortFallbackTimer) clearTimeout(abortFallbackTimer);
      rec.controller.signal.removeEventListener('abort', onRegistryAbort);
      let remoteExitConfirmed: boolean | null = null;
      if (rec.abortReason) {
        remoteExitConfirmed = child ? (remoteAbortPromise ? await remoteAbortPromise : false) : true;
      } else if (child && childExited && !spawnFailed) {
        remoteExitConfirmed = true;
      }
      const finalizeInput = {
        exitCode: finalSummary?.code ?? null,
        signal: finalSummary?.signal ?? null,
        transportExitConfirmed: child ? childExited : true,
        remoteExitConfirmed,
        spawnFailed
      } as { exitCode: number | null; signal: NodeJS.Signals | null; transportExitConfirmed: boolean; remoteExitConfirmed: boolean | null; spawnFailed: boolean; finalState?: FinalExecutionState; failureReason?: string };
      if (rec.abortReason === 'executor_shutdown') finalizeInput.failureReason = 'executor_restarted';
      if (rec.abortReason && remoteExitConfirmed === false) {
        finalizeInput.finalState = 'failed';
        finalizeInput.failureReason = 'remote_termination_unconfirmed';
      }
      const finalized = this.registry.finalize(rec.id, finalizeInput);
      this.recordFinalization(job, finalized.record);
    }
  }

  private startResult(job: ManagedJob) {
    const status = this.registry.status(job.record.id);
    const task = status.found ? status.task : null;
    return {
      exec_id: job.record.id,
      status: task?.status ?? 'failed',
      label: job.record.label,
      created_at: new Date(job.record.createdAt).toISOString(),
      queue_position: this.queuePosition(job.record.id),
      task_handle: job.record.taskHandle
    };
  }

  listActive() {
    const tasks = this.registry.listActive().map((task) => ({ ...task, queue_position: this.queuePosition(task.exec_id) }));
    const activeRecords = [...this.registry.active.values()].filter((rec) => rec.state !== 'queued');
    const syncRunning = activeRecords.filter((rec) => rec.executionClass === 'sync').length;
    const asyncRunning = activeRecords.filter((rec) => rec.executionClass === 'async').length;
    return {
      active: activeRecords.length,
      queued: this.registry.queuedCount,
      total_active: this.registry.activeCount,
      max_concurrent: this.config.globalMaxConcurrentExecs,
      sync_running: syncRunning,
      async_running: asyncRunning,
      sync_max_concurrent: this.config.syncMaxConcurrentExecs,
      async_max_concurrent: this.config.asyncMaxConcurrentExecs,
      global_max_concurrent: this.config.globalMaxConcurrentExecs,
      max_queue: this.config.maxQueuedExecs,
      circuit_open: this.registry.circuitOpen,
      tasks
    };
  }

  async getStatus(execId: string, options: GetStatusOptions = {}) {
    const stdoutCursor = statusCursor(options.stdoutCursor, 'stdout_cursor');
    const stderrCursor = statusCursor(options.stderrCursor, 'stderr_cursor');
    const maxOutputBytes = statusBoundedInt(
      options.maxOutputBytes,
      this.config.statusDefaultMaxOutputBytes,
      1,
      this.config.statusHardMaxOutputBytes,
      'invalid_status_output_limit',
      'status_output_limit_too_large'
    );
    const waitSeconds = statusBoundedInt(options.waitSeconds, 0, 0, this.config.statusMaxWaitSeconds, 'invalid_wait_seconds', 'wait_seconds_too_large');

    let state = this.registry.status(execId);
    const job = this.jobs.get(execId);
    if (waitSeconds > 0 && state.found && state.source === 'active' && job) {
      await Promise.race([
        job.completion.then(() => undefined, () => undefined),
        delay(waitSeconds * 1000)
      ]);
      state = this.registry.status(execId);
    }

    if (!state.found) return state;
    const task = state.source === 'active'
      ? { ...state.task, queue_position: this.queuePosition(execId) }
      : state.task;
    const output = this.readJobOutput(job, stdoutCursor, stderrCursor, maxOutputBytes);
    return { ...state, task, ...output };
  }

  runtimeOverview(now = Date.now()) {
    const active = this.listActive();
    const recent = [...this.registry.recent].slice(-20);
    return {
      generated_at: new Date(now).toISOString(),
      health: this.registry.circuitOpen ? 'degraded' : 'healthy',
      circuit_open: this.registry.circuitOpen,
      counts: {
        running: active.active,
        queued: active.queued,
        recent_completed: recent.filter((item) => item.status === 'completed').length,
        recent_failed: recent.filter((item) => item.status === 'failed' || item.status === 'timed_out').length
      },
      capacity: {
        running: active.active,
        global_max: active.global_max_concurrent,
        sync_running: active.sync_running,
        sync_max: active.sync_max_concurrent,
        async_running: active.async_running,
        async_max: active.async_max_concurrent,
        queued: active.queued,
        queue_max: active.max_queue
      },
      task_contexts: { total: this.taskContexts.list().length },
      totals: {
        requests: this.metrics.requestsTotal,
        started: this.metrics.startedTotal,
        timed_out: this.metrics.timeoutTotal,
        truncated: this.metrics.truncatedTotal
      }
    };
  }

  runtimeListExecutions(limit = this.config.recentHistoryLimit) {
    const boundedLimit = Math.max(1, Math.min(500, Math.floor(limit)));
    const active = this.registry.listActive().map((task) => ({
      ...task,
      queue_position: this.queuePosition(task.exec_id),
      lifecycle: 'active' as const,
      finished_at: null,
      duration_ms: null,
      final_state: null,
      ...this.runtimeObserver.fields(task.exec_id),
      task_context: this.taskContexts.get(task.task_handle)
    }));
    const seen = new Set(active.map((task) => task.exec_id));
    const finished: Array<Record<string, unknown> & { exec_id: string; created_at: string }> = [];
    const candidates = [
      ...[...this.registry.unconfirmed.values()].reverse(),
      ...[...this.registry.recent].reverse()
    ];
    for (const task of candidates) {
      if (seen.has(task.exec_id)) continue;
      seen.add(task.exec_id);
      const observation = this.runtimeObserver.get(task.exec_id);
      finished.push({
        ...task,
        lifecycle: 'finished',
        state: task.final_state,
        cwd: observation?.cwd ?? null,
        command_preview: observation?.command_preview ?? null,
        ...this.runtimeObserver.fields(task.exec_id),
        task_context: this.taskContexts.get(task.task_handle)
      });
    }
    return [...active, ...finished]
      .sort((a, b) => Date.parse(String(b.created_at)) - Date.parse(String(a.created_at)))
      .slice(0, boundedLimit);
  }

  runtimeDetail(execId: string) {
    const state = this.registry.status(execId);
    if (!state.found) return state;
    const job = this.jobs.get(execId);
    const task = state.source === 'active'
      ? { ...state.task, queue_position: this.queuePosition(execId), ...this.runtimeObserver.fields(execId) }
      : { ...state.task, ...this.runtimeObserver.fields(execId) };
    return {
      found: true as const,
      source: state.source,
      task,
      task_context: this.taskContexts.get(task.task_handle),
      observation: this.runtimeObserver.get(execId),
      logs: job ? {
        available: true,
        stdout_start_cursor: job.stdoutLog.startCursor,
        stdout_end_cursor: job.stdoutLog.endCursor,
        stderr_start_cursor: job.stderrLog.startCursor,
        stderr_end_cursor: job.stderrLog.endCursor,
        stdout_truncated: job.stdoutLog.truncated,
        stderr_truncated: job.stderrLog.truncated
      } : {
        available: false,
        stdout_start_cursor: 0,
        stdout_end_cursor: 0,
        stderr_start_cursor: 0,
        stderr_end_cursor: 0,
        stdout_truncated: false,
        stderr_truncated: false
      }
    };
  }

  runtimeLogs(execId: string, options: { stdoutCursor?: number; stderrCursor?: number; maxOutputBytes?: number } = {}) {
    const state = this.registry.status(execId);
    if (!state.found) return state;
    const stdoutCursor = statusCursor(options.stdoutCursor, 'stdout_cursor');
    const stderrCursor = statusCursor(options.stderrCursor, 'stderr_cursor');
    const maxOutputBytes = statusBoundedInt(
      options.maxOutputBytes,
      Math.min(65536, this.config.statusHardMaxOutputBytes),
      1,
      this.config.statusHardMaxOutputBytes,
      'invalid_status_output_limit',
      'status_output_limit_too_large'
    );
    return {
      found: true as const,
      exec_id: execId,
      ...this.readJobOutput(this.jobs.get(execId), stdoutCursor, stderrCursor, maxOutputBytes)
    };
  }

  cancel(execId: string) {
    const job = this.jobs.get(execId);
    const result = this.registry.requestCancel(execId);
    this.bumpMap(this.metrics.cancelRequestsTotal, result.result);
    if (result.result === 'accepted' && job && !job.started) {
      this.runtimeObserver.event(execId, 'abort_requested', { level: 'warning', detail: 'manual_cancel · manual_tool' });
      this.finishUnstartedJob(job);
    }
    return result;
  }

  abortForMcp(execId: string, reason: AbortReason, source: string): void {
    this.abortJob(execId, reason, source);
  }

  close(): void {
    clearInterval(this.jobGc);
    for (const job of this.jobs.values()) {
      if (job.settled) continue;
      this.abortJob(job.record.id, 'executor_shutdown', 'executor_shutdown');
    }
    this.registry.close();
  }

  async shutdown(waitSeconds = Math.max(2, this.config.killGraceSeconds + 2)): Promise<void> {
    clearInterval(this.jobGc);
    const pending = [...this.jobs.values()].filter((job) => !job.settled);
    for (const job of pending) this.abortJob(job.record.id, 'executor_shutdown', 'executor_shutdown');
    if (pending.length > 0) {
      await Promise.race([
        Promise.allSettled(pending.map((job) => job.completion)).then(() => undefined),
        delay(Math.max(0, waitSeconds) * 1000)
      ]);
    }
    this.registry.close();
  }

  private abortJob(execId: string, reason: AbortReason, source: string): void {
    const job = this.jobs.get(execId);
    const result = this.registry.requestAbort(execId, reason, source);
    if (result.accepted && job && !job.started) {
      this.runtimeObserver.event(execId, 'abort_requested', { level: runtimeStateLevel(job.record.state), detail: `${reason} · ${source}` });
      this.finishUnstartedJob(job);
    }
  }

  private finishUnstartedJob(job: ManagedJob): void {
    if (job.settled) return;
    this.removeFromQueue(job.record.id);
    const timedOut = job.record.abortReason === 'request_timeout' || job.record.abortReason === 'reaper_grace_exceeded';
    const summary: ExecSummary = {
      exec_id: job.record.id,
      type: 'exit',
      code: null,
      signal: null,
      duration_ms: Math.max(0, Date.now() - job.record.createdAt),
      stdout_bytes: 0,
      stderr_bytes: 0,
      truncated: false,
      timed_out: timedOut,
      stdout_tail: '',
      stderr_tail: '',
      task_handle: job.record.taskHandle
    };
    job.emit({ ...summary });
    const finalizeInput = { exitCode: null, signal: null, transportExitConfirmed: true } as { exitCode: null; signal: null; transportExitConfirmed: true; failureReason?: string };
    if (job.record.abortReason === 'executor_shutdown') finalizeInput.failureReason = 'executor_restarted';
    const finalized = this.registry.finalize(job.record.id, finalizeInput);
    this.recordFinalization(job, finalized.record);
    this.settleJob(job, summary);
    this.schedule();
  }

  private recordFinalization(job: ManagedJob, record: ExecutionHistoryRecord | null): void {
    job.finishedAt = Date.now();
    job.spec.env = {};
    job.stdoutRedactor.clearSecrets();
    job.stderrRedactor.clearSecrets();
    if (job.abortSignal && job.abortListener) job.abortSignal.removeEventListener('abort', job.abortListener);
    if (!record?.final_state) return;
    this.bumpMap(this.metrics.finishedTotal, record.final_state);
    this.observeDuration(record.final_state, record.duration_ms);
    this.runtimeObserver.finish(record.exec_id, record);
    this.logLifecycle(record.final_state, record.exec_id, {
      label: record.label,
      execution_class: record.execution_class,
      exit_code: record.exit_code,
      signal: record.signal,
      abort_source: record.abort_source,
      failure_reason: record.failure_reason,
      duration_ms: record.duration_ms,
      transport_exit_confirmed: record.transport_exit_confirmed,
      remote_exit_confirmed: record.remote_exit_confirmed
    });
    this.pruneJobs();
  }

  private settleJob(job: ManagedJob, summary: ExecSummary): void {
    if (job.settled) return;
    job.settled = true;
    job.resolveCompletion(summary);
  }

  private settleJobError(job: ManagedJob, error: unknown): void {
    if (job.settled) return;
    job.settled = true;
    job.rejectCompletion(error);
  }

  private readJobOutput(job: ManagedJob | undefined, stdoutCursor: number, stderrCursor: number, maxOutputBytes: number) {
    if (!job) {
      return {
        logs_available: false,
        stdout: '', stderr: '',
        stdout_cursor: stdoutCursor, stderr_cursor: stderrCursor,
        has_more_stdout: false, has_more_stderr: false,
        stdout_log_truncated: false, stderr_log_truncated: false
      };
    }
    const stdoutAvailable = job.stdoutLog.availableFrom(stdoutCursor);
    const stderrAvailable = job.stderrLog.availableFrom(stderrCursor);
    let stdoutBudget = Math.min(stdoutAvailable, Math.ceil(maxOutputBytes / 2));
    let stderrBudget = Math.min(stderrAvailable, maxOutputBytes - stdoutBudget);
    let remaining = maxOutputBytes - stdoutBudget - stderrBudget;
    if (remaining > 0 && stdoutBudget < stdoutAvailable) {
      const add = Math.min(remaining, stdoutAvailable - stdoutBudget);
      stdoutBudget += add;
      remaining -= add;
    }
    if (remaining > 0 && stderrBudget < stderrAvailable) stderrBudget += Math.min(remaining, stderrAvailable - stderrBudget);
    const out = job.stdoutLog.read(stdoutCursor, stdoutBudget);
    const err = job.stderrLog.read(stderrCursor, stderrBudget);
    return {
      logs_available: true,
      stdout: out.data,
      stderr: err.data,
      stdout_cursor: out.nextCursor,
      stderr_cursor: err.nextCursor,
      has_more_stdout: out.hasMore,
      has_more_stderr: err.hasMore,
      stdout_log_truncated: out.logTruncated,
      stderr_log_truncated: err.logTruncated
    };
  }

  private queuePosition(execId: string): number | null {
    let position = 0;
    for (const id of this.queue) {
      const job = this.jobs.get(id);
      if (!job || job.started || job.record.state !== 'queued') continue;
      position++;
      if (id === execId) return position;
    }
    return null;
  }

  private removeFromQueue(execId: string): void {
    const index = this.queue.indexOf(execId);
    if (index >= 0) this.queue.splice(index, 1);
  }

  private compactQueue(): void {
    for (let i = this.queue.length - 1; i >= 0; i--) {
      const id = this.queue[i];
      const job = id ? this.jobs.get(id) : undefined;
      if (!job || job.started || job.record.state !== 'queued') this.queue.splice(i, 1);
    }
  }

  private pruneJobs(): void {
    const now = Date.now();
    const retentionMs = this.config.jobRetentionSeconds * 1000;
    const finished = [...this.jobs.values()]
      .filter((job) => job.finishedAt !== null)
      .sort((a, b) => (a.finishedAt || 0) - (b.finishedAt || 0));
    for (const job of finished) {
      if (job.finishedAt !== null && now - job.finishedAt > retentionMs) this.jobs.delete(job.record.id);
    }
    const remainingFinished = [...this.jobs.values()]
      .filter((job) => job.finishedAt !== null)
      .sort((a, b) => (a.finishedAt || 0) - (b.finishedAt || 0));
    while (remainingFinished.length > this.config.recentHistoryLimit) {
      const job = remainingFinished.shift();
      if (job) this.jobs.delete(job.record.id);
    }
  }

  logLifecycle(state: ExecutionState | FinalExecutionState, execId: string | undefined, fields: Record<string, unknown> = {}): void {
    if (this.config.lifecycleLogs === false) return;
    console.error(`exec_state_change ${JSON.stringify({ exec_id: execId, state, ...fields })}`);
  }

  observeDuration(finalState: FinalExecutionState, durationMs: number): void {
    if (!Number.isFinite(durationMs) || durationMs < 0) return;
    const seconds = durationMs / 1000;
    let histogram = this.metrics.durationSecondsByState.get(finalState);
    if (!histogram) {
      histogram = { count: 0, sum: 0, buckets: this.metrics.durationSecondsBuckets.map(() => 0) };
      this.metrics.durationSecondsByState.set(finalState, histogram);
    }
    histogram.count++;
    histogram.sum += seconds;
    this.metrics.durationSecondsBuckets.forEach((upperBound, index) => {
      if (seconds <= upperBound) histogram.buckets[index] = (histogram.buckets[index] ?? 0) + 1;
    });
  }

  bumpRejected(reason: string): void { this.bumpMap(this.metrics.rejectedTotal, reason); }

  bumpMap<K extends string>(map: Map<K, number>, key: K): void {
    map.set(key, (map.get(key) || 0) + 1);
  }
}

function statusCursor(value: number | undefined, name: string): number {
  if (value === undefined) return 0;
  if (!Number.isInteger(value) || value < 0) throw new ExecRejectedError(`invalid_${name}`, `${name} must be a non-negative integer`);
  return value;
}

function statusBoundedInt(value: number | undefined, fallback: number, min: number, max: number, lowCode: string, highCode: string): number {
  if (value === undefined) return fallback;
  if (!Number.isInteger(value) || value < min) throw new ExecRejectedError(lowCode, `${lowCode}: ${value}`);
  if (value > max) throw new ExecRejectedError(highCode, `${highCode}: ${value} > ${max}`);
  return value;
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

function spawnCommand(config: ExecMcpConfig, req: ValidatedExecRequest, execId: string): { child: ChildProcessWithoutNullStreams; stdin: string } {
  return spawnRemoteShell(config, buildRemoteScript(req, execId));
}

export function spawnRemoteProcess(config: ExecMcpConfig, remoteCommand: readonly string[]): ChildProcessWithoutNullStreams {
  if (!config.remote.host || !config.remote.keyPath) {
    throw new Error('remote execution requires REMOTE_HOST and REMOTE_KEY_PATH');
  }
  const destination = `${config.remote.user}@${config.remote.host}`;
  const args = [
    ...(config.remote.binArgs || []),
    '-i', config.remote.keyPath,
    '-p', String(config.remote.port),
    '-o', 'BatchMode=yes',
    '-o', `ConnectTimeout=${config.remote.connectTimeoutSeconds}`,
    '-o', `StrictHostKeyChecking=${config.remote.strictHostKeyChecking}`,
    '-o', 'UserKnownHostsFile=' + config.remote.knownHostsPath,
    '-o', 'LogLevel=ERROR',
    destination,
    remoteCommand.map(shellQuote).join(' ')
  ];
  return spawn(config.remote.bin || 'ssh', args, {
    cwd: '/tmp',
    env: sanitizedEnv({}),
    detached: true,
    stdio: ['pipe', 'pipe', 'pipe']
  });
}

export function spawnRemoteShell(config: ExecMcpConfig, stdin: string): { child: ChildProcessWithoutNullStreams; stdin: string } {
  return { child: spawnRemoteProcess(config, ['/bin/sh', '-s']), stdin };
}

function buildRemoteScript(req: ValidatedExecRequest, execId: string): string {
  const lines: string[] = [];
  lines.push('set -eu');
  lines.push(`CWD_B64='${b64(req.cwd)}'`);
  lines.push(`CMD_B64='${b64(req.command)}'`);
  lines.push(`TIMEOUT_SECONDS='${Number.parseInt(String(req.timeoutSeconds), 10)}'`);
  lines.push(`KILL_GRACE_SECONDS='${Math.max(1, Number.parseInt(String(req.killGraceSeconds), 10) || 1)}'`);
  lines.push(`CONTROL_DIR_B64='${b64(remoteControlDir(execId))}'`);
  lines.push('CWD=$(printf %s "$CWD_B64" | base64 -d)');
  lines.push('CONTROL_DIR=$(printf %s "$CONTROL_DIR_B64" | base64 -d)');
  lines.push('CANCEL_FILE="$CONTROL_DIR/cancel"');
  lines.push('DONE_FILE="$CONTROL_DIR/done"');
  lines.push('PGID_FILE="$CONTROL_DIR/pgid"');
  lines.push('umask 077');
  lines.push('mkdir -p "$CONTROL_DIR"');
  lines.push('rm -f "$DONE_FILE" "$PGID_FILE"');
  lines.push('CMD=$(printf %s "$CMD_B64" | base64 -d)');
  lines.push('if ! command -v setsid >/dev/null 2>&1; then echo "remote_environment_error: setsid is required" >&2; exit 127; fi');
  lines.push('REAL_CWD=$(cd "$CWD" 2>/dev/null && pwd -P) || { echo "invalid_cwd: cwd does not exist or is not accessible: $CWD" >&2; exit 126; }');
  lines.push('is_under_path() {');
  lines.push('  candidate="$1"');
  lines.push('  base="$2"');
  lines.push('  if [ "$base" = "/" ]; then return 0; fi');
  lines.push('  case "$candidate" in "$base"|"$base"/*) return 0 ;; *) return 1 ;; esac');
  lines.push('}');
  lines.push('CWD_ALLOWED=0');
  for (const base of req.allowedCwds || []) {
    lines.push(`BASE_B64='${b64(base)}'`);
    lines.push('BASE=$(printf %s "$BASE_B64" | base64 -d)');
    lines.push('if REAL_BASE=$(cd "$BASE" 2>/dev/null && pwd -P); then');
    lines.push('  if is_under_path "$REAL_CWD" "$REAL_BASE"; then CWD_ALLOWED=1; fi');
    lines.push('fi');
  }
  lines.push('if [ "$CWD_ALLOWED" != 1 ]; then echo "invalid_cwd: real cwd is not allowed: $REAL_CWD" >&2; exit 126; fi');
  for (const [key, value] of Object.entries(req.env || {})) {
    lines.push(`${key}_B64='${b64(value)}'`);
    lines.push(`export ${key}=$(printf %s "$${key}_B64" | base64 -d)`);
  }
  lines.push('cd "$REAL_CWD"');
  lines.push('CHILD_PID=');
  lines.push('WATCHDOG_PID=');
  lines.push('CANCEL_WATCH_PID=');
  lines.push('kill_child_group() {');
  lines.push('  sig="$1"');
  lines.push('  if [ -n "${CHILD_PID:-}" ]; then');
  lines.push("    python3 -c 'import os,signal,sys; pgid=int(sys.argv[1]); sig=getattr(signal,\"SIG\"+sys.argv[2])\ntry: os.killpg(pgid,sig)\nexcept ProcessLookupError: pass' \"$CHILD_PID\" \"$sig\" 2>/dev/null || true");
  lines.push('  fi');
  lines.push('}');
  lines.push('child_group_alive() {');
  lines.push('  [ -n "${CHILD_PID:-}" ] || return 1');
  lines.push("  python3 -c 'import os,sys; pgid=int(sys.argv[1])\ntry: os.killpg(pgid,0)\nexcept ProcessLookupError: raise SystemExit(1)\nexcept PermissionError: pass' \"$CHILD_PID\" 2>/dev/null");
  lines.push('}');
  lines.push('stop_watchdogs() {');
  lines.push('  if [ -n "${WATCHDOG_PID:-}" ]; then');
  lines.push('    kill "$WATCHDOG_PID" 2>/dev/null || true');
  lines.push('    wait "$WATCHDOG_PID" 2>/dev/null || true');
  lines.push('  fi');
  lines.push('  if [ -n "${CANCEL_WATCH_PID:-}" ]; then');
  lines.push('    kill "$CANCEL_WATCH_PID" 2>/dev/null || true');
  lines.push('    wait "$CANCEL_WATCH_PID" 2>/dev/null || true');
  lines.push('  fi');
  lines.push('}');
  lines.push('mark_done() {');
  lines.push('  if [ -e "$CANCEL_FILE" ]; then : > "$DONE_FILE"; else rm -rf "$CONTROL_DIR"; fi');
  lines.push('}');
  lines.push('terminate_child_group() {');
  lines.push('  trap - TERM HUP INT EXIT');
  lines.push('  kill_child_group TERM');
  lines.push('  sleep "$KILL_GRACE_SECONDS"');
  lines.push('  kill_child_group KILL');
  lines.push('  stop_watchdogs');
  lines.push('  mark_done');
  lines.push('  exit 143');
  lines.push('}');
  lines.push('trap terminate_child_group TERM HUP INT');
  lines.push('setsid /bin/sh -c "$CMD" &');
  lines.push('CHILD_PID=$!');
  lines.push("printf '%s\\n' \"$CHILD_PID\" > \"$PGID_FILE\"");
  lines.push("python3 -c 'import os,signal,sys,time\npgid=int(sys.argv[1]); cancel=sys.argv[2]; grace=float(sys.argv[3])\ndef alive():\n    try: os.killpg(pgid,0); return True\n    except ProcessLookupError: return False\nwhile alive():\n    if os.path.exists(cancel):\n        try: os.killpg(pgid,signal.SIGTERM)\n        except ProcessLookupError: break\n        end=time.monotonic()+grace\n        while time.monotonic() < end and alive(): time.sleep(0.1)\n        if alive():\n            try: os.killpg(pgid,signal.SIGKILL)\n            except ProcessLookupError: pass\n        break\n    time.sleep(0.1)' \"$CHILD_PID\" \"$CANCEL_FILE\" \"$KILL_GRACE_SECONDS\" &");
  lines.push('CANCEL_WATCH_PID=$!');
  lines.push('(');
  lines.push('  SLEEP_PID=');
  lines.push('  trap \'if [ -n "${SLEEP_PID:-}" ]; then kill "$SLEEP_PID" 2>/dev/null || true; fi; exit 0\' TERM');
  lines.push('  sleep "$TIMEOUT_SECONDS" &');
  lines.push('  SLEEP_PID=$!');
  lines.push('  wait "$SLEEP_PID" || exit 0');
  lines.push('  kill_child_group TERM');
  lines.push('  sleep "$KILL_GRACE_SECONDS"');
  lines.push('  kill_child_group KILL');
  lines.push(') &');
  lines.push('WATCHDOG_PID=$!');
  lines.push('set +e');
  lines.push('wait "$CHILD_PID"');
  lines.push('STATUS=$?');
  lines.push('set -e');
  lines.push('stop_watchdogs');
  lines.push('trap - TERM HUP INT');
  lines.push('if child_group_alive; then');
  lines.push('  kill_child_group TERM');
  lines.push('  sleep 1');
  lines.push('  kill_child_group KILL');
  lines.push('fi');
  lines.push('mark_done');
  lines.push('exit "$STATUS"');
  return lines.join('\n') + '\n';
}


function remoteControlDir(execId: string): string {
  if (!/^exec-[0-9a-f-]+$/i.test(execId)) throw new Error(`invalid exec id for remote control path: ${execId}`);
  return `/tmp/exec-mcp-runtime/${execId}`;
}

async function requestRemoteCancellation(config: ExecMcpConfig, execId: string, killGraceSeconds: number): Promise<boolean> {
  const controlDir = remoteControlDir(execId);
  const maxWaitSeconds = Math.max(10, killGraceSeconds + 10);
  const script = [
    'set -eu',
    `CONTROL_DIR_B64='${b64(controlDir)}'`,
    `MAX_WAIT_SECONDS='${maxWaitSeconds}'`,
    'CONTROL_DIR=$(printf %s "$CONTROL_DIR_B64" | base64 -d)',
    'CANCEL_FILE="$CONTROL_DIR/cancel"',
    'DONE_FILE="$CONTROL_DIR/done"',
    'umask 077',
    'mkdir -p "$CONTROL_DIR"',
    ': > "$CANCEL_FILE"',
    'LEFT="$MAX_WAIT_SECONDS"',
    'while [ "$LEFT" -gt 0 ]; do',
    '  if [ -e "$DONE_FILE" ]; then rm -rf "$CONTROL_DIR"; exit 0; fi',
    '  sleep 1',
    '  LEFT=$((LEFT - 1))',
    'done',
    'exit 75'
  ].join('\n') + '\n';
  const { child, stdin } = spawnRemoteShell(config, script);
  child.stdin.end(stdin);
  child.stdout.resume();
  child.stderr.resume();
  const timeoutMs = (config.remote.connectTimeoutSeconds + maxWaitSeconds + 3) * 1000;
  return await new Promise<boolean>((resolveControl) => {
    let settled = false;
    const finish = (ok: boolean) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolveControl(ok);
    };
    const timer = setTimeout(() => {
      try { if (child.pid) process.kill(-child.pid, 'SIGKILL'); } catch {}
      finish(false);
    }, timeoutMs);
    timer.unref?.();
    child.on('error', () => finish(false));
    child.on('close', (code) => finish(code === 0));
  });
}

function shellQuote(value: string): string {
  return `'${value.replace(/'/g, `'\"'\"'`)}'`;
}

function b64(value: unknown): string {
  return Buffer.from(String(value), 'utf8').toString('base64');
}

function sanitizedEnv(extraEnv: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  const env = { ...process.env, ...extraEnv };
  delete env.ENV;
  delete env.BASH_ENV;
  return env;
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

function clampInt(value: unknown, fallback: number, min: number, max: number, lowErrorCode: string, highErrorCode: string): number {
  if (value === undefined || value === null) return fallback;
  const n = Number(value);
  if (!Number.isInteger(n) || n < min) throw new ExecRejectedError(lowErrorCode, `${lowErrorCode}: ${value}`);
  if (n > max) throw new ExecRejectedError(highErrorCode, `${highErrorCode}: ${n} > ${max}`);
  return n;
}

function throwIfAborted(signal: AbortSignal | undefined): void {
  if (!signal?.aborted) return;
  const reason = signal.reason instanceof Error ? signal.reason.message : String(signal.reason || 'request_cancelled');
  throw new ExecRejectedError('request_cancelled', reason);
}

function sanitizeLabel(value: unknown): string | null {
  if (value === undefined || value === null || value === '') return null;
  if (typeof value !== 'string') throw new ExecRejectedError('invalid_label', 'label must be a string');
  const clean = redact(value.replace(/[\x00-\x1f\x7f]/g, ' ').replace(/\s+/g, ' ').trim());
  if (clean.length > 120) throw new ExecRejectedError('invalid_label', 'label must be at most 120 characters');
  return clean || null;
}

function sanitizePreview(value: unknown, maxChars: number): string {
  const clean = String(value).replace(/[\x00-\x1f\x7f]/g, ' ').replace(/\s+/g, ' ').trim();
  return clean.slice(0, maxChars);
}

function isRecord(value: unknown): value is UnknownRecord {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function isAllowedCwd(cwd: string, allowedCwds: readonly string[]): boolean {
  const normalized = resolve(cwd);
  return allowedCwds.some((base) => {
    const resolvedBase = resolve(base);
    return normalized === resolvedBase || normalized.startsWith(resolvedBase + '/');
  });
}
