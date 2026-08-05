import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';
import { once } from 'node:events';
import { createServer as createHttpServer } from 'node:http';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createServer } from '../dist/src/server.js';
import { parseConfig } from '../dist/src/config.js';
import { remoteTestEnv } from '../scripts/helpers.js';

function sha256(value) {
  return createHash('sha256').update(value).digest('hex');
}

async function listen(server) {
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  return `http://127.0.0.1:${server.address().port}`;
}

async function mcpCall(base, id, name, args) {
  const response = await fetch(`${base}/mcp`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id, method: 'tools/call', params: { name, arguments: args } })
  });
  assert.equal(response.status, 200);
  return response.json();
}

test('ChatGPT file references and resource links transfer random binary bytes in both directions', async () => {
  const root = await mkdtemp(join(tmpdir(), 'exec-mcp-artifacts-root-'));
  const spool = await mkdtemp(join(tmpdir(), 'exec-mcp-artifacts-spool-'));
  const bytes = randomBytes(256 * 1024 + 37);
  const expectedSha = sha256(bytes);
  const differentBytes = Buffer.from('different-content');
  const source = createHttpServer((req, res) => {
    if (req.url === '/redirect') {
      res.writeHead(302, { location: '/payload' });
      res.end();
      return;
    }
    if (req.url === '/payload') {
      res.writeHead(200, {
        'content-type': 'application/octet-stream',
        'content-length': bytes.length
      });
      res.end(bytes);
      return;
    }
    if (req.url === '/different') {
      res.writeHead(200, {
        'content-type': 'application/octet-stream',
        'content-length': differentBytes.length
      });
      res.end(differentBytes);
      return;
    }
    res.writeHead(404).end();
  });
  const sourceBase = await listen(source);

  const config = parseConfig({
    HOST: '127.0.0.1',
    PORT: '0',
    ALLOWED_CWDS: root,
    DEFAULT_CWD: root,
    ARTIFACT_MAX_BYTES: String(1024 * 1024),
    ARTIFACT_TOOL_BRIDGE_TOKEN: 'tool_bridge_test_token_0123456789abcdef',
    ARTIFACT_SPOOL_DIR: spool,
    ARTIFACT_PUBLIC_BASE_URL: 'http://placeholder.invalid',
    ARTIFACT_IMPORT_ALLOW_HTTP: 'true',
    ARTIFACT_IMPORT_ALLOWED_HOSTS: '127.0.0.1',
    ARTIFACT_DOWNLOAD_TTL_SECONDS: '300',
    ARTIFACT_MAX_DOWNLOADS: '5',
    ARTIFACT_TRANSFER_TIMEOUT_SECONDS: '10',
    ...remoteTestEnv()
  });
  const instance = createServer(config);
  const base = await listen(instance.server);
  config.artifactPublicBaseUrl = base;

  try {
    const imported = await mcpCall(base, 1, 'import_chatgpt_file', {
      file: {
        download_url: `${sourceBase}/redirect`,
        file_id: 'file_test_binary',
        mime_type: 'application/octet-stream',
        file_name: 'random.bin'
      },
      target_path: 'received.bin',
      expected_sha256: expectedSha
    });
    assert.equal(imported.result.isError, false);
    assert.equal(imported.result.structuredContent.bytes, bytes.length);
    assert.equal(imported.result.structuredContent.sha256, expectedSha);
    assert.equal(imported.result.structuredContent.verified, true);
    assert.deepEqual(await readFile(join(root, 'received.bin')), bytes);

    const idempotentRetry = await mcpCall(base, 2, 'import_chatgpt_file', {
      file: { download_url: `${sourceBase}/payload`, file_id: 'file_test_binary' },
      target_path: 'received.bin'
    });
    assert.equal(idempotentRetry.result.isError, false);
    assert.equal(idempotentRetry.result.structuredContent.sha256, expectedSha);
    assert.deepEqual(await readFile(join(root, 'received.bin')), bytes);

    const conflictingRetry = await mcpCall(base, 21, 'import_chatgpt_file', {
      file: { download_url: `${sourceBase}/different`, file_id: 'file_test_binary_different' },
      target_path: 'received.bin'
    });
    assert.equal(conflictingRetry.result.isError, true);
    assert.match(conflictingRetry.result.content[0].text, /already_exists/);
    assert.deepEqual(await readFile(join(root, 'received.bin')), bytes);

    const wrongHash = await mcpCall(base, 3, 'import_chatgpt_file', {
      file: { download_url: `${sourceBase}/payload`, file_id: 'file_test_binary_3' },
      target_path: 'wrong-hash.bin',
      expected_sha256: '0'.repeat(64)
    });
    assert.equal(wrongHash.result.isError, true);
    assert.match(wrongHash.result.content[0].text, /checksum_mismatch/);

    const rejectedHost = await mcpCall(base, 31, 'import_chatgpt_file', {
      file: {
        download_url: 'https://files.example.invalid/private/path?signature=must-not-leak',
        file_id: 'file_test_rejected_host'
      },
      target_path: 'rejected-host.bin'
    });
    assert.equal(rejectedHost.result.isError, true);
    assert.equal(rejectedHost.result.structuredContent.code, 'download_host_not_allowed');
    assert.equal(rejectedHost.result.structuredContent.download_host, 'files.example.invalid');
    assert.doesNotMatch(JSON.stringify(rejectedHost), /must-not-leak|private\/path/);

    await writeFile(join(root, 'result ü.pptx'), bytes);
    const exported = await mcpCall(base, 4, 'export_remote_file', {
      path: 'result ü.pptx'
    });
    assert.equal(exported.result.isError, false);
    assert.equal(exported.result.structuredContent.bytes, bytes.length);
    assert.equal(exported.result.structuredContent.sha256, expectedSha);
    assert.equal(exported.result.structuredContent.mime_type, 'application/vnd.openxmlformats-officedocument.presentationml.presentation');
    assert.deepEqual(exported.result.content.map((item) => item.type), ['text', 'resource_link', 'resource']);
    assert.equal(exported.result.content[1].uri, exported.result.structuredContent.download_url);
    assert.equal(exported.result.content[1].name, 'result ü.pptx');
    assert.equal(exported.result.content[1].size, bytes.length);
    assert.equal(exported.result.content[2].resource.uri, `${base}/embedded/${expectedSha}/result%20%C3%BC.pptx`);
    assert.deepEqual(Buffer.from(exported.result.content[2].resource.blob, 'base64'), bytes);
    assert.equal(exported.result.structuredContent.download_url, `${base}/tool-container/tool_bridge_test_token_0123456789abcdef/current`);
    const head = await fetch(exported.result.structuredContent.download_url, { method: 'HEAD' });
    assert.equal(head.status, 200);
    assert.equal(Number(head.headers.get('content-length')), bytes.length);
    assert.match(head.headers.get('content-disposition'), /filename\*=/);
    assert.equal(head.headers.get('digest'), `sha-256=${Buffer.from(expectedSha, 'hex').toString('base64')}`);

    const downloaded = await fetch(exported.result.structuredContent.download_url);
    assert.equal(downloaded.status, 200);
    assert.deepEqual(Buffer.from(await downloaded.arrayBuffer()), bytes);

    const ranged = await fetch(exported.result.structuredContent.download_url, { headers: { range: 'bytes=100-199' } });
    assert.equal(ranged.status, 206);
    assert.equal(ranged.headers.get('content-range'), `bytes 100-199/${bytes.length}`);
    assert.deepEqual(Buffer.from(await ranged.arrayBuffer()), bytes.subarray(100, 200));
  } finally {
    instance.runner.registry.close();
    await new Promise((resolve) => instance.server.close(resolve));
    await new Promise((resolve) => source.close(resolve));
    await rm(root, { recursive: true, force: true });
    await rm(spool, { recursive: true, force: true });
  }
});
