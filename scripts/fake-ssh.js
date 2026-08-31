import { spawn } from 'node:child_process';

const args = process.argv.slice(2);
let index = 0;
while (index < args.length) {
  const arg = args[index];
  if (arg === '-i' || arg === '-p' || arg === '-o') {
    index += 2;
    continue;
  }
  if (arg?.startsWith('-')) {
    index += 1;
    continue;
  }
  index += 1; // destination
  break;
}
const command = args.slice(index).join(' ');
const simulateRemoteIsolation = Boolean(command);
const child = command
  ? spawn('/bin/sh', ['-c', command], { stdio: ['pipe', 'pipe', 'pipe'], detached: simulateRemoteIsolation })
  : spawn('/bin/sh', ['-s'], { stdio: ['pipe', 'pipe', 'pipe'], detached: simulateRemoteIsolation });

process.stdin.pipe(child.stdin);
child.stdout.pipe(process.stdout);
child.stderr.pipe(process.stderr);
child.on('close', (code, signal) => {
  if (signal) process.kill(process.pid, signal);
  process.exit(code ?? 1);
});
child.on('error', (err) => {
  console.error(err.message);
  process.exit(1);
});
