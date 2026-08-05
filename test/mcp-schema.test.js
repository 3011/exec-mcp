import test from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createServer } from '../dist/src/server.js';
import { parseConfig } from '../dist/src/config.js';
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
    assert.equal(body.result.tools.find((item) => item.name === 'download_file'), undefined);
    assert.equal(body.result.tools.find((item) => item.name === 'upload_file'), undefined);
    const importArtifact = body.result.tools.find((item) => item.name === 'import_chatgpt_file');
    const exportArtifact = body.result.tools.find((item) => item.name === 'export_remote_file');
    assert.ok(importArtifact);
    assert.ok(exportArtifact);
    assert.deepEqual(importArtifact._meta['openai/fileParams'], ['file']);
    assert.deepEqual(importArtifact.inputSchema.$defs.OpenAIFile.required, ['download_url', 'file_id']);
    assert.deepEqual(Object.keys(importArtifact.inputSchema.$defs.OpenAIFile.properties), ['download_url', 'file_id', 'mime_type', 'file_name']);
    assert.equal(exportArtifact.outputSchema.properties.download_url.type, 'string');
    assert.equal(exportArtifact.outputSchema.properties.file_uri, undefined);
    assert.equal(exportArtifact.outputSchema.properties.embedded.type, 'boolean');
    assert.match(importArtifact.description, /atomically commits/);
    assert.match(exportArtifact.description, /embedded MCP binary resource/);
    assert.match(exportArtifact.description, /ARTIFACT_EMBED_MAX_BYTES/);
    assert.match(exportArtifact.description, /Files above the embed limit are not embedded/);
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
    assert.match(tool.description, /remote test environment/);
    assert.match(tool.description, /\/bin\/sh -c/);
    assert.match(tool.description, /ALLOWED_CWDS/);
    assert.match(tool.inputSchema.properties.command.description, /Non-interactive shell command/);
    assert.match(tool.inputSchema.properties.command.description, /explicit quoting/);
    assert.match(tool.inputSchema.properties.cwd.description, /allowlist/);
    assert.match(tool.inputSchema.properties.timeout_seconds.description, /SIGTERM/);
    assert.match(tool.inputSchema.properties.max_output_bytes.description, /omitted/);
    assert.match(tool.inputSchema.properties.env.description, /BASH_ENV/);

    const removedDownload = await mcpCall(base, 2, 'download_file', { path: '/tmp/x' });
    assert.equal(removedDownload.error.code, -32602);
    assert.match(removedDownload.error.message, /Unknown tool: download_file/);
    const removedUpload = await mcpCall(base, 3, 'upload_file', {});
    assert.equal(removedUpload.error.code, -32602);
    assert.match(removedUpload.error.message, /Unknown tool: upload_file/);
  } finally {
    await new Promise((resolve) => server.close(resolve));
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
