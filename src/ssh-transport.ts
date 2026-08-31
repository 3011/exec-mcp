import { spawn } from 'node:child_process';
import type { ChildProcessWithoutNullStreams } from 'node:child_process';
import type { ExecMcpConfig } from './config.js';

export interface ProcessCloseResult {
  code: number | null;
  signal: NodeJS.Signals | null;
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
    env: sanitizedTransportEnv(),
    detached: true,
    stdio: ['pipe', 'pipe', 'pipe']
  });
}

export function terminateLocalProcessGroup(child: ChildProcessWithoutNullStreams, signal: NodeJS.Signals): void {
  try {
    if (child.pid) process.kill(-child.pid, signal);
  } catch {
    try { child.kill(signal); } catch {}
  }
}

export function waitForProcessClose(child: ChildProcessWithoutNullStreams): Promise<ProcessCloseResult> {
  return new Promise((resolve, reject) => {
    child.once('error', reject);
    child.once('close', (code, signal) => resolve({ code, signal }));
  });
}

function shellQuote(value: string): string {
  return `'${String(value).replace(/'/g, `'"'"'`)}'`;
}

function sanitizedTransportEnv(): NodeJS.ProcessEnv {
  const env = { ...process.env };
  delete env.ENV;
  delete env.BASH_ENV;
  return env;
}
