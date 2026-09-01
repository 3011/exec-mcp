import { createHash } from 'node:crypto';
import { isAbsolute, resolve } from 'node:path';
import type { ExecMcpConfig } from './config.js';
import { RemoteExecutionSession } from './remote-execution-session.js';
import { JobLogBuffer } from './job-log.js';
import { JobOutputRedactor } from './job-output-redactor.js';
import { redact } from './redact.js';
import { ExecRegistry, ExecutionCircuitOpenError } from './exec-registry.js';
import type { AbortReason, ExecutionClass, ExecutionHistoryRecord, ExecutionRecord, ExecutionState, FinalExecutionState } from './exec-registry.js';
import { RuntimeObserver, deriveRuntimeDiagnostics, runtimeStateLevel } from './runtime-observer.js';
import { TaskContextStore, TASK_HANDLE_PATTERN } from './task-context.js';
import type { TaskContext } from './task-context.js';

type UnknownRecord = Record<string, unknown>;
interface Histogram { count: number; sum: number; buckets: number[]; }

export { ExecRejectedError } from './execution-types.js';
export type { ExecEvent, ExecutionShell, ExecutionSpec, ValidatedExecRequest, ExecSummary, RunOptions, GetStatusOptions, ExecMetrics } from './execution-types.js';
import { ExecRejectedError } from './execution-types.js';
import type { ExecEvent, ExecutionShell, ExecutionSpec, ValidatedExecRequest, ExecSummary, RunOptions, GetStatusOptions, ExecMetrics } from './execution-types.js';

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

    if (req.shell !== 'sh' && req.shell !== 'bash') {
      throw new ExecRejectedError('invalid_shell', 'shell is required and must be one of: sh, bash');
    }
    const shell: ExecutionShell = req.shell;

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
      command, shell, cwd, timeoutSeconds, maxOutputBytes, env, label, commandSha256,
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
      const err = new ExecutionCircuitOpenError(this.registry.unconfirmedCount);
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
      shell: spec.shell,
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
    const session = new RemoteExecutionSession({
      config: this.config,
      registry: this.registry,
      metrics: this.metrics,
      runtimeObserver: this.runtimeObserver,
      job,
      logLifecycle: (state, execId, fields = {}) => this.logLifecycle(state, execId, fields),
      onFinalized: (record) => this.recordFinalization(job, record)
    });
    return await session.run();
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
    const observation = this.runtimeObserver.get(execId);
    return { ...state, task, ...output, ...deriveRuntimeDiagnostics(task, observation) };
  }

  runtimeOverview(now = Date.now()) {
    const active = this.listActive();
    const recent = [...this.registry.recent].slice(-20);
    return {
      generated_at: new Date(now).toISOString(),
      health: this.registry.circuitOpen ? 'degraded' : 'healthy',
      circuit_open: this.registry.circuitOpen,
      counts: {
        active: active.active,
        running: active.active,
        queued: active.queued,
        completed: this.metrics.finishedTotal.get('completed') || 0,
        issues: ['failed', 'timed_out', 'spawn_failed', 'unconfirmed_reaped']
          .reduce((sum, state) => sum + (this.metrics.finishedTotal.get(state as FinalExecutionState) || 0), 0),
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
    const active = this.registry.listActive().map((rawTask) => {
      const task = { ...rawTask, queue_position: this.queuePosition(rawTask.exec_id) };
      const observation = this.runtimeObserver.get(task.exec_id);
      return {
        ...task,
        lifecycle: 'active' as const,
        finished_at: null,
        duration_ms: null,
        final_state: null,
        ...this.runtimeObserver.fields(task.exec_id),
        ...deriveRuntimeDiagnostics(task, observation),
        task_context: this.taskContexts.get(task.task_handle)
      };
    });
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
        ...deriveRuntimeDiagnostics(task, observation),
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
    const observation = this.runtimeObserver.get(execId);
    return {
      found: true as const,
      source: state.source,
      task,
      task_context: this.taskContexts.get(task.task_handle),
      observation,
      ...deriveRuntimeDiagnostics(state.task, observation),
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
