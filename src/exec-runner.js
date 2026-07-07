import { spawn } from 'node:child_process';
import { isAbsolute, resolve } from 'node:path';
import { RingBuffer } from './ring-buffer.js';
import { redact } from './redact.js';
import { ExecRegistry, TooManyActiveExecsError } from './exec-registry.js';

export class ExecRejectedError extends Error {
  constructor(code, message) {
    super(message);
    this.name = 'ExecRejectedError';
    this.code = code;
  }
}

export class ExecRunner {
  constructor(config) {
    this.config = config;
    this.registry = new ExecRegistry({ maxActive: config.maxConcurrentExecs });
    this.metrics = {
      requestsTotal: 0,
      rejectedTotal: new Map(),
      timeoutTotal: 0,
      killedTotal: new Map(),
      truncatedTotal: 0,
      streamDisconnectTotal: 0,
      exitCodeTotal: new Map(),
      outputBytesTotal: { stdout: 0, stderr: 0 },
      durationMsTotal: 0
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
      'timeout_too_large'
    );
    const maxOutputBytes = clampInt(
      req.max_output_bytes,
      this.config.defaultMaxOutputBytes,
      1,
      this.config.hardMaxOutputBytes,
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

    return { command, cwd, timeoutSeconds, maxOutputBytes, env, allowedCwds: this.config.allowedCwds, killGraceSeconds: this.config.killGraceSeconds };
  }

  async run(input, emit, options = {}) {
    this.metrics.requestsTotal++;

    let req;
    try {
      req = this.validate(input);
    } catch (err) {
      if (err instanceof ExecRejectedError) this.bumpRejected(err.code);
      throw err;
    }

    let rec;
    try {
      rec = this.registry.acquire({ timeoutMs: req.timeoutSeconds * 1000 });
    } catch (err) {
      if (err instanceof TooManyActiveExecsError) {
        this.bumpRejected(err.code);
        throw new ExecRejectedError(err.code, err.message);
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
    const startedAt = new Date(rec.startedAt);
    const tailBufferBytes = Math.min(this.config.ringBufferBytes, req.maxOutputBytes);
    const stdoutTail = new RingBuffer(tailBufferBytes);
    const stderrTail = new RingBuffer(tailBufferBytes);

    const send = (event) => emit({ exec_id: execId, ...event });

    let child;
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
          if (timedOut) send({ type: 'timeout', timeout_seconds: req.timeoutSeconds, action });
          killGroup('SIGKILL');
        }
      }, delaySeconds * 1000);
      sigkillTimer.unref?.();
    };

    const onRegistryAbort = () => {
      const reasonCode = abortReasonCode(rec.controller.signal.reason);
      if (reasonCode === 'exec_timeout' || reasonCode === 'exec_reaper_abort') {
        timedOut = true;
        if (!timeoutCounted) {
          timeoutCounted = true;
          this.metrics.timeoutTotal++;
        }
        send({ type: 'timeout', timeout_seconds: req.timeoutSeconds, action: 'remote_watchdog', reason: reasonCode });
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
      this.registry.abort(rec.id, 'client_closed_aborting', new Error('client_closed'));
    };

    try {
      const spawned = spawnCommand(this.config, req);
      child = spawned.child;
      if (spawned.stdin) child.stdin.end(spawned.stdin);

      rec.controller.signal.addEventListener('abort', onRegistryAbort, { once: true });
      clientAbortSignal?.addEventListener('abort', onClientAbort, { once: true });

      send({ type: 'start', pid: child.pid, started_at: startedAt.toISOString(), cwd: req.cwd });

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

      return await new Promise((resolveRun) => {
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
          send({ type: 'error', code: 'spawn_failed', message: err.message });
        });

        child.on('exit', () => {
          childExited = true;
        });

        child.on('close', finish);
      });
    } catch (err) {
      if (!child) this.bumpRejected('spawn_failed');
      if (err instanceof ExecRejectedError) throw err;
      throw new ExecRejectedError('spawn_failed', err.message);
    } finally {
      if (heartbeat) clearInterval(heartbeat);
      if (sigkillTimer) clearTimeout(sigkillTimer);
      rec.controller.signal.removeEventListener('abort', onRegistryAbort);
      clientAbortSignal?.removeEventListener('abort', onClientAbort);
      this.registry.release(rec.id);
    }
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

function clampInt(value, fallback, min, max, errorCode) {
  if (value === undefined || value === null) return fallback;
  const n = Number.parseInt(String(value), 10);
  if (!Number.isFinite(n) || n < min) return fallback;
  if (n > max) throw new ExecRejectedError(errorCode, `${errorCode}: ${n} > ${max}`);
  return n;
}

function isAllowedCwd(cwd, allowedCwds) {
  const normalized = resolve(cwd);
  return allowedCwds.some((base) => {
    const resolvedBase = resolve(base);
    return normalized === resolvedBase || normalized.startsWith(resolvedBase + '/');
  });
}
