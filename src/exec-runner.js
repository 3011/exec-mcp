import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';
import { RingBuffer } from './ring-buffer.js';
import { redact } from './redact.js';

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
    this.active = 0;
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

  validate(input) {
    const req = input && typeof input === 'object' ? input : {};
    const command = typeof req.command === 'string' ? req.command.trim() : '';
    if (!command) throw new ExecRejectedError('invalid_command', 'command must be a non-empty string');

    const cwd = resolve(String(req.cwd || this.config.defaultCwd));
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

    return { command, cwd, timeoutSeconds, maxOutputBytes, env };
  }

  async run(input, emit, options = {}) {
    this.metrics.requestsTotal++;
    if (this.active >= this.config.maxConcurrentExecs) {
      this.bumpRejected('too_many_active_execs');
      throw new ExecRejectedError('too_many_active_execs', 'too many active execs');
    }

    let req;
    try {
      req = this.validate(input);
    } catch (err) {
      if (err instanceof ExecRejectedError) this.bumpRejected(err.code);
      throw err;
    }
    this.active++;
    const execId = randomUUID();
    let seq = 0;
    let stdoutBytes = 0;
    let stderrBytes = 0;
    let forwardedBytes = 0;
    let truncated = false;
    let timedOut = false;
    let killedSignal = null;
    let childExited = false;
    const startedAt = new Date();
    const stdoutTail = new RingBuffer(this.config.ringBufferBytes);
    const stderrTail = new RingBuffer(this.config.ringBufferBytes);

    const send = (event) => emit({ exec_id: execId, ...event });

    let child;
    try {
      const spawned = spawnCommand(this.config, req);
      child = spawned.child;
      if (spawned.stdin) child.stdin.end(spawned.stdin);
    } catch (err) {
      this.active--;
      this.bumpRejected('spawn_failed');
      throw new ExecRejectedError('spawn_failed', err.message);
    }

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

    const heartbeat = setInterval(() => {
      send({
        type: 'heartbeat',
        elapsed_ms: Date.now() - startedAt.getTime(),
        stdout_bytes: stdoutBytes,
        stderr_bytes: stderrBytes
      });
    }, this.config.heartbeatSeconds * 1000);
    heartbeat.unref?.();

    const killGroup = (signal) => {
      if (childExited || !child.pid) return;
      killedSignal = signal;
      try {
        process.kill(-child.pid, signal);
        this.bumpMap(this.metrics.killedTotal, signal);
      } catch {
        try { child.kill(signal); } catch {}
      }
    };

    const timeout = setTimeout(() => {
      timedOut = true;
      this.metrics.timeoutTotal++;
      send({ type: 'timeout', timeout_seconds: req.timeoutSeconds, action: 'sigterm' });
      killGroup('SIGTERM');
      setTimeout(() => {
        if (!childExited) {
          send({ type: 'timeout', timeout_seconds: req.timeoutSeconds, action: 'sigkill' });
          killGroup('SIGKILL');
        }
      }, this.config.killGraceSeconds * 1000).unref?.();
    }, req.timeoutSeconds * 1000);
    timeout.unref?.();

    const abortSignal = options.abortSignal;
    const onAbort = () => {
      this.metrics.streamDisconnectTotal++;
      killGroup('SIGTERM');
      setTimeout(() => killGroup('SIGKILL'), this.config.killGraceSeconds * 1000).unref?.();
    };
    abortSignal?.addEventListener('abort', onAbort, { once: true });

    return await new Promise((resolveRun) => {
      child.on('error', (err) => {
        send({ type: 'error', code: 'spawn_failed', message: err.message });
      });

      child.on('exit', (code, signal) => {
        childExited = true;
        clearTimeout(timeout);
        clearInterval(heartbeat);
        abortSignal?.removeEventListener('abort', onAbort);
        const durationMs = Date.now() - startedAt.getTime();
        this.metrics.durationMsTotal += durationMs;
        this.bumpMap(this.metrics.exitCodeTotal, String(code ?? signal ?? 'null'));
        this.active--;

        const summary = {
          type: 'exit',
          code,
          signal: signal || killedSignal,
          duration_ms: durationMs,
          stdout_bytes: stdoutBytes,
          stderr_bytes: stderrBytes,
          truncated,
          timed_out: timedOut,
          stdout_tail: redact(stdoutTail.toString()),
          stderr_tail: redact(stderrTail.toString())
        };
        send(summary);
        resolveRun({ exec_id: execId, ...summary });
      });
    });
  }

  bumpRejected(reason) {
    this.bumpMap(this.metrics.rejectedTotal, reason);
  }

  bumpMap(map, key) {
    map.set(key, (map.get(key) || 0) + 1);
  }
}



function spawnCommand(config, req) {
  if (config.execMode === 'remote') {
    if (!config.remote.host || !config.remote.keyPath) {
      throw new Error('remote mode requires REMOTE_HOST and REMOTE_KEY_PATH');
    }
    const bin = config.remote.bin || String.fromCharCode(115, 115, 104);
    const destination = `${config.remote.user}@${config.remote.host}`;
    const args = [
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
    const child = spawn(bin, args, {
      cwd: '/tmp',
      env: sanitizedEnv({}),
      detached: true,
      stdio: ['pipe', 'pipe', 'pipe']
    });
    return { child, stdin: buildRemoteScript(req) };
  }

  return {
    child: spawn('/bin/sh', ['-c', req.command], {
      cwd: req.cwd,
      env: sanitizedEnv(req.env),
      detached: true,
      stdio: ['ignore', 'pipe', 'pipe']
    })
  };
}

function buildRemoteScript(req) {
  const lines = [];
  lines.push('set -eu');
  lines.push(`CWD_B64='${b64(req.cwd)}'`);
  lines.push(`CMD_B64='${b64(req.command)}'`);
  lines.push('CWD=$(printf %s "$CWD_B64" | base64 -d)');
  lines.push('CMD=$(printf %s "$CMD_B64" | base64 -d)');
  for (const [key, value] of Object.entries(req.env || {})) {
    lines.push(`${key}_B64='${b64(value)}'`);
    lines.push(`export ${key}=$(printf %s "$${key}_B64" | base64 -d)`);
  }
  lines.push('cd "$CWD"');
  lines.push('exec /bin/sh -c "$CMD"');
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
