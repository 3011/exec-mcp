import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { isAbsolute, resolve } from 'node:path';
import { RingBuffer } from './ring-buffer.js';
import { redact } from './redact.js';
import { ExecRegistry, ExecutionCircuitOpenError, TooManyActiveExecsError } from './exec-registry.js';

export class ExecRejectedError extends Error {
  constructor(code, message, details = undefined) {
    super(message);
    this.name = 'ExecRejectedError';
    this.code = code;
    this.details = details;
  }
}

export class ExecRunner {
  constructor(config) {
    this.config = config;
    this.registry = new ExecRegistry({
      maxActive: config.maxConcurrentExecs,
      historyLimit: config.recentHistoryLimit,
      reapGraceMs: config.registryReapGraceSeconds * 1000,
      emergencyReapMs: config.emergencyReapSeconds * 1000
    });
    this.registry.onEmergencyReap = (record) => this.logLifecycle('unconfirmed_reaped', record?.exec_id, {
      abort_source: record?.abort_source,
      transport_exit_confirmed: false,
      remote_exit_confirmed: null
    });
    this.metrics = {
      requestsTotal: 0,
      rejectedTotal: new Map(),
      timeoutTotal: 0,
      killedTotal: new Map(),
      truncatedTotal: 0,
      streamDisconnectTotal: 0,
      exitCodeTotal: new Map(),
      outputBytesTotal: { stdout: 0, stderr: 0 },
      durationMsTotal: 0,
      durationSecondsBuckets: [0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600, 1800],
      durationSecondsByState: new Map(),
      startedTotal: 0,
      abortRequestedTotal: new Map(),
      cancelRequestsTotal: new Map(),
      finishedTotal: new Map()
    };
  }

  get active() {
    return this.registry.activeCount;
  }

  validate(input) {
    const req = input && typeof input === 'object' ? input : {};
    const command = typeof req.command === 'string' ? req.command.trim() : '';
    if (!command) throw new ExecRejectedError('invalid_command', 'command must be a non-empty string');

    const cwdInput = String(req.cwd || this.config.defaultCwd);
    if (!isAbsolute(cwdInput)) {
      throw new ExecRejectedError('invalid_cwd', `cwd must be an absolute path: ${cwdInput}`);
    }
    const cwd = resolve(cwdInput);
    if (!isAllowedCwd(cwd, this.config.allowedCwds)) {
      throw new ExecRejectedError('invalid_cwd', `cwd is not allowed: ${cwd}`);
    }

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

    const env = {};
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

  async run(input, emit, options = {}) {
    this.metrics.requestsTotal++;

    throwIfAborted(options.abortSignal);

    let req;
    try {
      req = this.validate(input);
    } catch (err) {
      if (err instanceof ExecRejectedError) this.bumpRejected(err.code);
      throw err;
    }

    let rec;
    try {
      rec = this.registry.acquire({
        timeoutMs: req.timeoutSeconds * 1000,
        metadata: {
          label: req.label,
          commandPreview: req.commandPreview,
          commandSha256: req.commandSha256,
          commandLength: req.commandLength,
          cwd: req.cwd
        }
      });
    } catch (err) {
      if (err instanceof TooManyActiveExecsError || err instanceof ExecutionCircuitOpenError) {
        this.bumpRejected(err.code);
        throw new ExecRejectedError(err.code, err.message, err instanceof ExecutionCircuitOpenError
          ? { reason: err.reason, unconfirmed_count: err.unconfirmedCount }
          : { active: this.active, max: this.registry.maxActive });
      }
      throw err;
    }

    const execId = rec.id;
    let seq = 0;
    let stdoutBytes = 0;
    let stderrBytes = 0;
    let forwardedBytes = 0;
    let truncated = false;
    let timedOut = false;
    let killedSignal = null;
    let childExited = false;
    let timeoutCounted = false;
    let disconnectCounted = false;
    let heartbeat = null;
    let sigkillTimer = null;
    const startedAt = new Date(rec.createdAt);
    const tailBufferBytes = Math.min(this.config.ringBufferBytes, req.maxOutputBytes);
    const stdoutTail = new RingBuffer(tailBufferBytes);
    const stderrTail = new RingBuffer(tailBufferBytes);

    const send = (event) => emit({ exec_id: execId, ...event });

    let child;
    let finalSummary = null;
    let spawnFailed = false;
    const killGroup = (signal) => {
      if (childExited || !child?.pid) return;
      killedSignal = signal;
      try {
        process.kill(-child.pid, signal);
        this.bumpMap(this.metrics.killedTotal, signal);
      } catch {
        try { child.kill(signal); } catch {}
      }
    };

    const scheduleSigkill = (delaySeconds = this.config.killGraceSeconds, action = 'sigkill') => {
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

    const onRegistryAbort = () => {
      const reasonCode = abortReasonCode(rec.controller.signal.reason);
      this.bumpMap(this.metrics.abortRequestedTotal, reasonCode);
      this.logLifecycle(rec.state, rec.id, { abort_source: rec.abortSource, transport_pid: rec.transportPid });
      if (reasonCode === 'request_timeout' || reasonCode === 'reaper_grace_exceeded') {
        timedOut = true;
        if (!timeoutCounted) {
          timeoutCounted = true;
          this.metrics.timeoutTotal++;
        }
        send({ type: 'timeout', timeout_seconds: req.timeoutSeconds, action: 'remote_watchdog', reason: reasonCode });
        if (reasonCode === 'reaper_grace_exceeded') killGroup('SIGTERM');
        scheduleSigkill(this.config.killGraceSeconds + 2, 'local_sigkill_fallback');
        return;
      } else if (!disconnectCounted) {
        disconnectCounted = true;
        this.metrics.streamDisconnectTotal++;
      }
      killGroup('SIGTERM');
      scheduleSigkill();
    };

    const clientAbortSignal = options.abortSignal;
    const onClientAbort = () => {
      const signalReason = abortReasonCode(clientAbortSignal?.reason);
      const reason = signalReason === 'mcp_notification_cancel' ? signalReason : (options.abortReason || 'http_disconnect');
      const source = reason === 'mcp_notification_cancel' ? 'mcp_notification' : (options.abortSource || 'http');
      this.registry.requestAbort(rec.id, reason, source);
    };

    try {
      options.onAcquire?.(rec);
      if (clientAbortSignal?.aborted) onClientAbort();
      throwIfAborted(clientAbortSignal);

      const spawned = spawnCommand(this.config, req);
      child = spawned.child;
      this.registry.markTransportStarted(rec.id, child.pid);
      if (spawned.stdin) child.stdin.end(spawned.stdin);

      rec.controller.signal.addEventListener('abort', onRegistryAbort, { once: true });
      clientAbortSignal?.addEventListener('abort', onClientAbort, { once: true });
      if (clientAbortSignal?.aborted) onClientAbort();

      this.registry.markRunning(rec.id);
      this.metrics.startedTotal++;
      this.logLifecycle('running', rec.id, { label: rec.label, transport_pid: child.pid });
      send({ type: 'start', transport_pid: child.pid, started_at: startedAt.toISOString(), cwd: req.cwd });

      const maybeForward = (stream, chunk) => {
        const len = chunk.length;
        if (stream === 'stdout') {
          stdoutBytes += len;
          this.metrics.outputBytesTotal.stdout += len;
          stdoutTail.append(chunk);
        } else {
          stderrBytes += len;
          this.metrics.outputBytesTotal.stderr += len;
          stderrTail.append(chunk);
        }

        if (forwardedBytes < req.maxOutputBytes) {
          const remain = req.maxOutputBytes - forwardedBytes;
          const toSend = len > remain ? chunk.subarray(0, remain) : chunk;
          forwardedBytes += toSend.length;
          send({ type: stream, data: redact(toSend.toString('utf8')), seq: ++seq });
        }

        if (!truncated && stdoutBytes + stderrBytes > req.maxOutputBytes) {
          truncated = true;
          this.metrics.truncatedTotal++;
          send({ type: 'truncated', stream: 'combined', max_output_bytes: req.maxOutputBytes });
        }
      };

      child.stdout.on('data', (chunk) => maybeForward('stdout', chunk));
      child.stderr.on('data', (chunk) => maybeForward('stderr', chunk));

      heartbeat = setInterval(() => {
        send({
          type: 'heartbeat',
          elapsed_ms: Date.now() - startedAt.getTime(),
          stdout_bytes: stdoutBytes,
          stderr_bytes: stderrBytes
        });
      }, this.config.heartbeatSeconds * 1000);
      heartbeat.unref?.();

      finalSummary = await new Promise((resolveRun) => {
        let finished = false;

        const finish = (code, signal) => {
          if (finished) return;
          finished = true;
          childExited = true;

          const durationMs = Date.now() - startedAt.getTime();
          this.metrics.durationMsTotal += durationMs;
          this.bumpMap(this.metrics.exitCodeTotal, String(code ?? signal ?? 'null'));

          const tails = boundedRedactedTails(stdoutTail.toString(), stderrTail.toString(), req.maxOutputBytes);
          const summary = {
            type: 'exit',
            code,
            signal: signal || killedSignal,
            duration_ms: durationMs,
            stdout_bytes: stdoutBytes,
            stderr_bytes: stderrBytes,
            truncated,
            timed_out: timedOut,
            stdout_tail: tails.stdout_tail,
            stderr_tail: tails.stderr_tail
          };
          send(summary);
          resolveRun({ exec_id: execId, ...summary });
        };

        child.on('error', (err) => {
          spawnFailed = true;
          send({ type: 'error', code: 'spawn_failed', message: err.message });
        });

        child.on('exit', () => {
          childExited = true;
        });

        child.on('close', finish);
      });
      return finalSummary;
    } catch (err) {
      if (!child && !rec.abortReason) {
        spawnFailed = true;
        this.bumpRejected('spawn_failed');
      }
      if (err instanceof ExecRejectedError) throw err;
      throw new ExecRejectedError('spawn_failed', err.message);
    } finally {
      if (heartbeat) clearInterval(heartbeat);
      if (sigkillTimer) clearTimeout(sigkillTimer);
      rec.controller.signal.removeEventListener('abort', onRegistryAbort);
      clientAbortSignal?.removeEventListener('abort', onClientAbort);
      const finalized = this.registry.finalize(rec.id, {
        exitCode: finalSummary?.code,
        signal: finalSummary?.signal,
        transportExitConfirmed: childExited,
        spawnFailed
      });
      if (finalized.record?.final_state) {
        this.bumpMap(this.metrics.finishedTotal, finalized.record.final_state);
        this.observeDuration(finalized.record.final_state, finalized.record.duration_ms);
        this.logLifecycle(finalized.record.final_state, rec.id, {
          label: rec.label,
          exit_code: finalized.record.exit_code,
          signal: finalized.record.signal,
          abort_source: finalized.record.abort_source,
          duration_ms: finalized.record.duration_ms,
          transport_exit_confirmed: finalized.record.transport_exit_confirmed,
          remote_exit_confirmed: null
        });
      }
    }
  }

  listActive() {
    return { active: this.active, max_concurrent: this.registry.maxActive, circuit_open: this.registry.circuitOpen, tasks: this.registry.listActive() };
  }

  getStatus(execId) { return this.registry.status(execId); }

  cancel(execId) {
    const result = this.registry.requestCancel(execId);
    this.bumpMap(this.metrics.cancelRequestsTotal, result.result);
    return result;
  }

  logLifecycle(state, execId, fields = {}) {
    if (this.config.lifecycleLogs === false) return;
    console.error(`exec_state_change ${JSON.stringify({ exec_id: execId, state, ...fields })}`);
  }

  observeDuration(finalState, durationMs) {
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
      if (seconds <= upperBound) histogram.buckets[index]++;
    });
  }

  bumpRejected(reason) {
    this.bumpMap(this.metrics.rejectedTotal, reason);
  }

  bumpMap(map, key) {
    map.set(key, (map.get(key) || 0) + 1);
  }
}
function abortReasonCode(reason) {
  if (reason instanceof Error && reason.message) return reason.message;
  return String(reason || 'aborted');
}

function spawnCommand(config, req) {
  return spawnRemoteShell(config, buildRemoteScript(req));
}

export function spawnRemoteShell(config, stdin) {
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
    '/bin/sh', '-s'
  ];
  const child = spawn(config.remote.bin || 'ssh', args, {
    cwd: '/tmp',
    env: sanitizedEnv({}),
    detached: true,
    stdio: ['pipe', 'pipe', 'pipe']
  });
  return { child, stdin };
}

function buildRemoteScript(req) {
  const lines = [];
  lines.push('set -eu');
  lines.push(`CWD_B64='${b64(req.cwd)}'`);
  lines.push(`CMD_B64='${b64(req.command)}'`);
  lines.push(`TIMEOUT_SECONDS='${Number.parseInt(String(req.timeoutSeconds), 10)}'`);
  lines.push(`KILL_GRACE_SECONDS='${Math.max(1, Number.parseInt(String(req.killGraceSeconds), 10) || 1)}'`);
  lines.push('CWD=$(printf %s "$CWD_B64" | base64 -d)');
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
  lines.push('kill_child_group() {');
  lines.push('  sig="$1"');
  lines.push('  if [ -n "${CHILD_PID:-}" ]; then');
  lines.push('    kill "-$sig" "-$CHILD_PID" 2>/dev/null || kill "-$sig" "$CHILD_PID" 2>/dev/null || true');
  lines.push('  fi');
  lines.push('}');
  lines.push('stop_watchdog() {');
  lines.push('  if [ -n "${WATCHDOG_PID:-}" ]; then');
  lines.push('    kill "$WATCHDOG_PID" 2>/dev/null || true');
  lines.push('    wait "$WATCHDOG_PID" 2>/dev/null || true');
  lines.push('  fi');
  lines.push('}');
  lines.push('terminate_child_group() {');
  lines.push('  trap - TERM HUP INT EXIT');
  lines.push('  kill_child_group TERM');
  lines.push('  sleep "$KILL_GRACE_SECONDS"');
  lines.push('  kill_child_group KILL');
  lines.push('  stop_watchdog');
  lines.push('  exit 143');
  lines.push('}');
  lines.push('trap terminate_child_group TERM HUP INT');
  lines.push('setsid /bin/sh -c "$CMD" &');
  lines.push('CHILD_PID=$!');
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
  lines.push('stop_watchdog');
  lines.push('trap - TERM HUP INT');
  lines.push('if kill -0 "-$CHILD_PID" 2>/dev/null; then');
  lines.push('  kill_child_group TERM');
  lines.push('  sleep 1');
  lines.push('  kill_child_group KILL');
  lines.push('fi');
  lines.push('exit "$STATUS"');
  return lines.join('\n') + '\n';
}

function b64(value) {
  return Buffer.from(String(value), 'utf8').toString('base64');
}

function sanitizedEnv(extraEnv) {
  const env = { ...process.env, ...extraEnv };
  delete env.ENV;
  delete env.BASH_ENV;
  return env;
}

function boundedRedactedTails(stdoutRaw, stderrRaw, maxBytes) {
  return boundTailPair(redact(stdoutRaw), redact(stderrRaw), maxBytes);
}

function boundTailPair(stdout, stderr, maxBytes) {
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

function trimUtf8Tail(value, maxBytes) {
  if (maxBytes <= 0) return '';
  const buf = Buffer.from(value, 'utf8');
  if (buf.length <= maxBytes) return value;
  let text = buf.subarray(buf.length - maxBytes).toString('utf8');
  while (Buffer.byteLength(text, 'utf8') > maxBytes) {
    text = text.slice(1);
  }
  return text;
}

function clampInt(value, fallback, min, max, lowErrorCode, highErrorCode) {
  if (value === undefined || value === null) return fallback;
  const n = Number(value);
  if (!Number.isInteger(n) || n < min) throw new ExecRejectedError(lowErrorCode, `${lowErrorCode}: ${value}`);
  if (n > max) throw new ExecRejectedError(highErrorCode, `${highErrorCode}: ${n} > ${max}`);
  return n;
}

function throwIfAborted(signal) {
  if (!signal?.aborted) return;
  const reason = signal.reason instanceof Error ? signal.reason.message : String(signal.reason || 'request_cancelled');
  throw new ExecRejectedError('request_cancelled', reason);
}

function sanitizeLabel(value) {
  if (value === undefined || value === null || value === '') return null;
  if (typeof value !== 'string') throw new ExecRejectedError('invalid_label', 'label must be a string');
  const clean = redact(value.replace(/[\x00-\x1f\x7f]/g, ' ').replace(/\s+/g, ' ').trim());
  if (clean.length > 120) throw new ExecRejectedError('invalid_label', 'label must be at most 120 characters');
  return clean || null;
}

function sanitizePreview(value, maxChars) {
  const clean = String(value).replace(/[\x00-\x1f\x7f]/g, ' ').replace(/\s+/g, ' ').trim();
  return clean.slice(0, maxChars);
}

function isAllowedCwd(cwd, allowedCwds) {
  const normalized = resolve(cwd);
  return allowedCwds.some((base) => {
    const resolvedBase = resolve(base);
    return normalized === resolvedBase || normalized.startsWith(resolvedBase + '/');
  });
}
