import { fileURLToPath } from 'node:url';

const fakeSshPath = fileURLToPath(new URL('./fake-ssh.js', import.meta.url));

export function remoteTestEnv(overrides = {}) {
  return {
    REMOTE_BIN: process.execPath,
    REMOTE_BIN_ARGS: `--no-warnings ${fakeSshPath}`,
    REMOTE_HOST: 'fake-remote',
    REMOTE_KEY_PATH: '/tmp/fake-ssh-key',
    ...overrides
  };
}
