import { spawn } from 'node:child_process';

const child = spawn('/bin/sh', ['-s'], {
  stdio: ['pipe', 'inherit', 'inherit']
});

process.stdin.pipe(child.stdin);
child.on('close', (code, signal) => {
  if (signal) process.kill(process.pid, signal);
  process.exit(code ?? 1);
});
child.on('error', (err) => {
  console.error(err.message);
  process.exit(1);
});
