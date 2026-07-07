import test from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { mkdtemp, rm, symlink, writeFile } from 'node:fs/promises';
import { Buffer } from 'node:buffer';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createServer } from '../src/server.js';
import { parseConfig } from '../src/config.js';
import { remoteTestEnv } from '../scripts/helpers.js';

test('MCP exec tool schema includes operational context', async () => {
  const config = parseConfig({
    HOST: '127.0.0.1',
    PORT: '0',
    ALLOWED_CWDS: '/tmp',
    DEFAULT_CWD: '/tmp',
    ...remoteTestEnv()
  });
  const { server } = createServer(config);
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const base = `http://127.0.0.1:${server.address().port}`;

  try {
    const resp = await fetch(`${base}/mcp`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/list', params: {} })
    });
    assert.equal(resp.status, 200);
    const body = await resp.json();
    const tool = body.result.tools.find((item) => item.name === 'exec');
    assert.ok(tool);
    assert.ok(body.result.tools.find((item) => item.name === 'download_file'));
    assert.ok(body.result.tools.find((item) => item.name === 'upload_file'));
    assert.equal(body.result.tools.find((item) => item.name === 'read_file'), undefined);
    assert.equal(body.result.tools.find((item) => item.name === 'write_file'), undefined);
    assert.equal(tool.outputSchema.type, 'object');
    assert.deepEqual(tool.outputSchema.required, [
      'exec_id',
      'type',
      'code',
      'signal',
      'duration_ms',
      'stdout_bytes',
      'stderr_bytes',
      'truncated',
      'timed_out',
      'stdout_tail',
      'stderr_tail'
    ]);
    assert.equal(body.result.tools.find((item) => item.name === 'download_file').outputSchema.properties.data_base64.type, 'string');
    assert.equal(body.result.tools.find((item) => item.name === 'upload_file').outputSchema.properties.action.enum.includes('append'), true);
    assert.match(tool.description, /test execution environment/);
    assert.match(tool.description, /\/bin\/sh -c/);
    assert.match(tool.description, /cwd allowlist validation/);
    assert.match(tool.description, /high-risk commands/);
    assert.match(tool.description, /\[exec summary\]/);
    assert.match(tool.description, /too_many_active_execs/);
    assert.match(tool.description, /oldest_age_seconds/);
    assert.match(tool.inputSchema.properties.command.description, /Non-interactive command/);
    assert.match(tool.inputSchema.properties.command.description, /explicit quoting/);
    assert.match(tool.inputSchema.properties.cwd.description, /allowlist/);
    assert.match(tool.inputSchema.properties.timeout_seconds.description, /SIGTERM/);
    assert.match(tool.inputSchema.properties.max_output_bytes.description, /truncation/);
    assert.match(tool.inputSchema.properties.env.description, /BASH_ENV/);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test('MCP download_file and upload_file transfer binary files inside allowlist', async () => {
  const root = await mkdtemp(join(tmpdir(), 'exec-mcp-files-'));
  const config = parseConfig({
    HOST: '127.0.0.1',
    PORT: '0',
    ALLOWED_CWDS: root,
    DEFAULT_CWD: root,
    FILE_MAX_DOWNLOAD_BYTES: '1024',
    FILE_MAX_UPLOAD_BYTES: '1024',
    ...remoteTestEnv()
  });
  const { server } = createServer(config);
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const base = `http://127.0.0.1:${server.address().port}`;

  try {
    const bytes = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0xff]);
    const upload = await mcpCall(base, 1, 'upload_file', {
      path: 'sample.png',
      data_base64: bytes.toString('base64'),
      mime_type: 'image/png'
    });
    assert.equal(upload.result.isError, false);
    const uploadBody = JSON.parse(upload.result.content[0].text);
    assert.deepEqual(upload.result.structuredContent, uploadBody);
    assert.equal(uploadBody.action, 'write');
    assert.equal(uploadBody.bytes, bytes.length);
    assert.equal(uploadBody.mime_type, 'image/png');

    const download = await mcpCall(base, 2, 'download_file', { path: 'sample.png' });
    assert.equal(download.result.isError, false);
    const downloadBody = JSON.parse(download.result.content[0].text);
    assert.deepEqual(download.result.structuredContent, downloadBody);
    assert.equal(downloadBody.bytes, bytes.length);
    assert.equal(downloadBody.mime_type, 'image/png');
    assert.equal(downloadBody.data_base64, bytes.toString('base64'));

    const rejected = await mcpCall(base, 3, 'download_file', { path: '/etc/passwd' });
    assert.equal(rejected.result.isError, true);
    assert.match(rejected.result.content[0].text, /invalid_path/);

    const invalid = await mcpCall(base, 4, 'upload_file', { path: 'bad.bin', data_base64: 'not-base64!' });
    assert.equal(invalid.result.isError, true);
    assert.match(invalid.result.content[0].text, /invalid_base64/);

    const empty = await mcpCall(base, 5, 'upload_file', { path: 'empty.bin', data_base64: '' });
    assert.equal(empty.result.isError, false);
    const emptyDownload = await mcpCall(base, 6, 'download_file', { path: 'empty.bin' });
    assert.equal(emptyDownload.result.isError, false);
    assert.equal(JSON.parse(emptyDownload.result.content[0].text).data_base64, '');

    await mcpCall(base, 7, 'upload_file', { path: 'append.bin', data_base64: Buffer.from([1, 2]).toString('base64') });
    const append = await mcpCall(base, 8, 'upload_file', { path: 'append.bin', data_base64: Buffer.from([3]).toString('base64'), append: true });
    assert.equal(append.result.isError, false);
    assert.equal(append.result.structuredContent.action, 'append');
    const appended = await mcpCall(base, 9, 'download_file', { path: 'append.bin' });
    assert.equal(JSON.parse(appended.result.content[0].text).data_base64, Buffer.from([1, 2, 3]).toString('base64'));

    const tooLargeUpload = await mcpCall(base, 10, 'upload_file', {
      path: 'large-upload.bin',
      data_base64: Buffer.alloc(1025).toString('base64')
    });
    assert.equal(tooLargeUpload.result.isError, true);
    assert.match(tooLargeUpload.result.content[0].text, /file_too_large/);

    await writeFile(join(root, 'large-download.bin'), Buffer.alloc(1025));
    const tooLargeDownload = await mcpCall(base, 11, 'download_file', { path: 'large-download.bin' });
    assert.equal(tooLargeDownload.result.isError, true);
    assert.match(tooLargeDownload.result.content[0].text, /file_too_large/);

    await symlink('/etc/passwd', join(root, 'passwd-link'));
    const symlinkDownload = await mcpCall(base, 12, 'download_file', { path: 'passwd-link' });
    assert.equal(symlinkDownload.result.isError, true);
    assert.match(symlinkDownload.result.content[0].text, /invalid_path/);

    const symlinkUpload = await mcpCall(base, 13, 'upload_file', {
      path: 'passwd-link',
      data_base64: Buffer.from('x').toString('base64')
    });
    assert.equal(symlinkUpload.result.isError, true);
    assert.match(symlinkUpload.result.content[0].text, /symlink_not_allowed/);
  } finally {
    await new Promise((resolve) => server.close(resolve));
    await rm(root, { recursive: true, force: true });
  }
});

test('MCP exec call returns structured content matching output schema', async () => {
  const config = parseConfig({
    HOST: '127.0.0.1',
    PORT: '0',
    ALLOWED_CWDS: '/tmp',
    DEFAULT_CWD: '/tmp',
    ...remoteTestEnv()
  });
  const { server } = createServer(config);
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const base = `http://127.0.0.1:${server.address().port}`;

  try {
    const result = await mcpCall(base, 1, 'exec', {
      command: 'printf hello',
      cwd: '/tmp',
      timeout_seconds: 5,
      max_output_bytes: 1024
    });
    assert.equal(result.result.isError, false);
    assert.match(result.result.content[0].text, /hello/);
    assert.equal(result.result.structuredContent.type, 'exit');
    assert.equal(result.result.structuredContent.code, 0);
    assert.equal(result.result.structuredContent.signal, null);
    assert.equal(result.result.structuredContent.timed_out, false);
    assert.equal(result.result.structuredContent.truncated, false);
    assert.match(result.result.structuredContent.stdout_tail, /hello/);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

async function mcpCall(base, id, name, args) {
  const resp = await fetch(`${base}/mcp`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      jsonrpc: '2.0',
      id,
      method: 'tools/call',
      params: { name, arguments: args }
    })
  });
  assert.equal(resp.status, 200);
  return await resp.json();
}
