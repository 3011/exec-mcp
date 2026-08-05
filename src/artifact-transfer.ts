import { createHash, randomBytes } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { mkdir, open, readFile, rename, rm, stat, unlink } from 'node:fs/promises';
import type { IncomingMessage, ServerResponse } from 'node:http';
import { basename, extname, join } from 'node:path';
import { pipeline } from 'node:stream/promises';
import type { ExecMcpConfig } from './config.js';
import { spawnRemoteProcess } from './exec-runner.js';

type UnknownRecord = Record<string, unknown>;

type OpenAIFileReference = {
  download_url: string;
  file_id: string;
  mime_type?: string;
  file_name?: string;
};

export interface ImportedArtifact {
  path: string;
  bytes: number;
  sha256: string;
  mime_type: string;
  source_file_id: string;
  source_file_name: string;
  verified: true;
}

export interface ExportedArtifact {
  path: string;
  bytes: number;
  sha256: string;
  mime_type: string;
  file_name: string;
  download_url: string;
  expires_at: string;
  downloads_remaining: number;
  embedded: boolean;
  delivery_mode: 'embedded_resource' | 'resource_link_only';
}

interface ArtifactRecord extends ExportedArtifact {
  token: string;
  local_path: string;
  expires_at_ms: number;
  downloads: number;
  max_downloads: number;
}

interface RemoteWriteResult {
  ok: true;
  path: string;
  bytes: number;
  sha256: string;
}

interface RemoteReadMetadata {
  ok: true;
  path: string;
  bytes: number;
}

interface ProcessCloseResult {
  code: number | null;
  signal: NodeJS.Signals | null;
}

export class ArtifactTransferError extends Error {
  readonly code: string;
  readonly statusCode: number | undefined;
  readonly details: Record<string, unknown> | undefined;

  constructor(code: string, message: string, statusCode?: number, details?: Record<string, unknown>) {
    super(message);
    this.name = 'ArtifactTransferError';
    this.code = code;
    this.statusCode = statusCode;
    this.details = details;
  }
}

export class ArtifactTransferManager {
  private readonly records = new Map<string, ArtifactRecord>();
  private readonly cleanupTimer: NodeJS.Timeout;
  private activeTransfers = 0;

  constructor(readonly config: ExecMcpConfig) {
    this.cleanupTimer = setInterval(() => { void this.cleanupExpired(); }, 60_000);
    this.cleanupTimer.unref?.();
  }

  close(): void {
    clearInterval(this.cleanupTimer);
  }

  async importChatgptFile(args: UnknownRecord, signal?: AbortSignal): Promise<ImportedArtifact> {
    return this.withTransferSlot(async () => {
      const file = requireOpenAIFile(args.file);
      const targetPath = requirePath(args.target_path, 'target_path');
      const overwrite = args.overwrite === true;
      const expectedSha256 = optionalSha256(args.expected_sha256);
      const sourceUrl = validateImportUrl(file.download_url, this.config);
      const temp = await this.allocateTempPath('import');
      const guard = createTransferGuard(signal, this.config.artifactTransferTimeoutSeconds);

      try {
        const response = await fetchFollowingRedirects(sourceUrl, this.config, guard.signal);
        if (!response.ok || !response.body) {
          throw new ArtifactTransferError('source_download_failed', `source download failed: HTTP ${response.status}`);
        }
        const declaredLength = parseContentLength(response.headers.get('content-length'));
        if (declaredLength !== null && declaredLength > this.config.artifactMaxBytes) {
          throw new ArtifactTransferError('file_too_large', `file_too_large: ${declaredLength} > ${this.config.artifactMaxBytes}`);
        }

        const local = await writeHttpBodyToFile(response.body as unknown as AsyncIterable<Uint8Array>, temp.tmpPath, this.config.artifactMaxBytes, guard.signal);
        if (expectedSha256 && local.sha256 !== expectedSha256) {
          throw new ArtifactTransferError('checksum_mismatch', `checksum_mismatch: expected ${expectedSha256}, got ${local.sha256}`);
        }

        const remote = await writeLocalFileToRemote(
          temp.tmpPath,
          targetPath,
          local.sha256,
          overwrite,
          this.config,
          guard.signal
        );
        if (remote.bytes !== local.bytes || remote.sha256 !== local.sha256) {
          throw new ArtifactTransferError('remote_verification_failed', 'remote size or sha256 differs from downloaded source');
        }

        return {
          path: remote.path,
          bytes: remote.bytes,
          sha256: remote.sha256,
          mime_type: normalizeMimeType(file.mime_type) || normalizeMimeType(response.headers.get('content-type')) || detectMimeType(file.file_name || remote.path),
          source_file_id: file.file_id,
          source_file_name: file.file_name || basename(remote.path),
          verified: true
        };
      } catch (error) {
        throw guard.mapError(error);
      } finally {
        guard.close();
        await rm(temp.dir, { recursive: true, force: true });
      }
    });
  }

  async exportRemoteFile(args: UnknownRecord, signal?: AbortSignal): Promise<ExportedArtifact> {
    return this.withTransferSlot(async () => {
      if (!this.config.artifactPublicBaseUrl) {
        throw new ArtifactTransferError('artifact_public_base_url_missing', 'ARTIFACT_PUBLIC_BASE_URL is required for export_remote_file');
      }
      const sourcePath = requirePath(args.path, 'path');
      const maxBytes = clampMaxBytes(args.max_bytes, this.config.artifactMaxBytes);
      const requestedName = typeof args.file_name === 'string' && args.file_name.trim() ? safeFileName(args.file_name) : '';
      const fixedToken = normalizeToolBridgeToken(this.config.artifactToolBridgeToken);
      const token = fixedToken || randomBytes(32).toString('hex');
      const guard = createTransferGuard(signal, this.config.artifactTransferTimeoutSeconds);
      const tmpPath = join(this.config.artifactSpoolDir, `.${token}.tmp`);
      const finalPath = join(this.config.artifactSpoolDir, token);

      try {
        await mkdir(this.config.artifactSpoolDir, { recursive: true });
        const remote = await readRemoteFileToLocal(sourcePath, tmpPath, maxBytes, this.config, guard.signal);
        await rename(tmpPath, finalPath);
        const fileName = requestedName || safeFileName(basename(remote.path));
        const mimeType = detectMimeType(fileName);
        const expiresAtMs = Date.now() + this.config.artifactDownloadTtlSeconds * 1000;
        const url = fixedToken
          ? `${this.config.artifactPublicBaseUrl}/tool-container/${token}/current`
          : `${this.config.artifactPublicBaseUrl}/artifacts/${token}/${encodeURIComponent(fileName)}`;
        const record: ArtifactRecord = {
          token,
          local_path: finalPath,
          path: remote.path,
          bytes: remote.bytes,
          sha256: remote.sha256,
          mime_type: mimeType,
          file_name: fileName,
          download_url: url,
          expires_at: new Date(expiresAtMs).toISOString(),
          expires_at_ms: expiresAtMs,
          downloads: 0,
          max_downloads: this.config.artifactMaxDownloads,
          downloads_remaining: this.config.artifactMaxDownloads,
          embedded: remote.bytes <= this.config.artifactEmbedMaxBytes,
          delivery_mode: remote.bytes <= this.config.artifactEmbedMaxBytes ? 'embedded_resource' : 'resource_link_only'
        };
        this.records.set(token, record);
        return publicRecord(record);
      } catch (error) {
        await rm(tmpPath, { force: true });
        throw guard.mapError(error);
      } finally {
        guard.close();
      }
    });
  }

  async embedExportedArtifact(result: ExportedArtifact): Promise<{ uri: string; mimeType: string; blob: string } | null> {
    if (!result.embedded || result.delivery_mode !== 'embedded_resource' || result.bytes > this.config.artifactEmbedMaxBytes) return null;
    const record = [...this.records.values()].find((candidate) =>
      candidate.download_url === result.download_url
      && candidate.sha256 === result.sha256
      && candidate.bytes === result.bytes
    );
    if (!record || record.expires_at_ms <= Date.now()) return null;
    const info = await stat(record.local_path);
    if (!info.isFile() || info.size !== record.bytes || info.size > this.config.artifactEmbedMaxBytes) {
      throw new ArtifactTransferError('artifact_embed_validation_failed', 'exported artifact changed before embedding');
    }
    const bytes = await readFile(record.local_path);
    return {
      uri: `${this.config.artifactPublicBaseUrl}/embedded/${record.sha256}/${encodeURIComponent(record.file_name)}`,
      mimeType: record.mime_type,
      blob: bytes.toString('base64')
    };
  }

  async handleHttp(req: IncomingMessage, res: ServerResponse): Promise<boolean> {
    const parsed = new URL(req.url || '/', 'http://exec-mcp.invalid');
    const match = /^\/(?:artifacts|tool-container)\/([A-Za-z0-9_-]{32,128})(?:\/.*)?$/.exec(parsed.pathname);
    if (!match) return false;
    if (req.method !== 'GET' && req.method !== 'HEAD') {
      res.writeHead(405, { allow: 'GET, HEAD', 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: 'method_not_allowed' }));
      return true;
    }

    const token = match[1] as string;
    const record = this.records.get(token);
    if (!record || record.expires_at_ms <= Date.now() || record.downloads >= record.max_downloads) {
      if (record) await this.deleteRecord(record);
      res.writeHead(404, { 'content-type': 'application/json', 'cache-control': 'no-store' });
      res.end(JSON.stringify({ error: 'artifact_not_found_or_expired' }));
      return true;
    }

    let info;
    try {
      info = await stat(record.local_path);
    } catch {
      await this.deleteRecord(record);
      res.writeHead(404, { 'content-type': 'application/json', 'cache-control': 'no-store' });
      res.end(JSON.stringify({ error: 'artifact_not_found' }));
      return true;
    }

    const range = parseRange(req.headers.range, info.size);
    const status = range ? 206 : 200;
    const start = range?.start ?? 0;
    const end = range?.end ?? Math.max(0, info.size - 1);
    const length = info.size === 0 ? 0 : end - start + 1;
    const headers: Record<string, string | number> = {
      'content-type': record.mime_type,
      'content-length': length,
      'content-disposition': contentDisposition(record.file_name),
      'cache-control': 'private, no-store, max-age=0',
      'accept-ranges': 'bytes',
      'x-content-type-options': 'nosniff',
      etag: `"sha256-${record.sha256}"`,
      digest: `sha-256=${Buffer.from(record.sha256, 'hex').toString('base64')}`
    };
    if (range) headers['content-range'] = `bytes ${start}-${end}/${info.size}`;
    res.writeHead(status, headers);
    if (req.method === 'HEAD' || info.size === 0) {
      res.end();
      return true;
    }

    record.downloads += 1;
    record.downloads_remaining = Math.max(0, record.max_downloads - record.downloads);
    const stream = createReadStream(record.local_path, { start, end });
    stream.on('error', () => {
      if (!res.headersSent) res.writeHead(500, { 'content-type': 'application/json' });
      res.destroy();
    });
    stream.pipe(res);
    return true;
  }

  private async withTransferSlot<T>(operation: () => Promise<T>): Promise<T> {
    if (this.activeTransfers >= this.config.artifactMaxConcurrentTransfers) {
      throw new ArtifactTransferError(
        'too_many_active_transfers',
        `too_many_active_transfers: ${this.activeTransfers}/${this.config.artifactMaxConcurrentTransfers}`
      );
    }
    this.activeTransfers += 1;
    try {
      return await operation();
    } finally {
      this.activeTransfers -= 1;
    }
  }

  private async allocateTempPath(prefix: string): Promise<{ dir: string; tmpPath: string }> {
    await mkdir(this.config.artifactSpoolDir, { recursive: true });
    const dir = join(this.config.artifactSpoolDir, `.${prefix}-${randomBytes(16).toString('hex')}`);
    await mkdir(dir, { mode: 0o700 });
    return { dir, tmpPath: join(dir, 'payload') };
  }

  private async cleanupExpired(): Promise<void> {
    const now = Date.now();
    const expired = [...this.records.values()].filter((record) => record.expires_at_ms <= now || record.downloads >= record.max_downloads);
    await Promise.all(expired.map((record) => this.deleteRecord(record)));
  }

  private async deleteRecord(record: ArtifactRecord): Promise<void> {
    this.records.delete(record.token);
    await unlink(record.local_path).catch(() => undefined);
  }
}


function normalizeToolBridgeToken(value: string): string | null {
  const token = value.trim();
  if (!token) return null;
  if (!/^[A-Za-z0-9_-]{32,128}$/.test(token)) {
    throw new ArtifactTransferError('invalid_tool_bridge_token', 'ARTIFACT_TOOL_BRIDGE_TOKEN must contain 32-128 URL-safe characters');
  }
  return token;
}

function publicRecord(record: ArtifactRecord): ExportedArtifact {
  return {
    path: record.path,
    bytes: record.bytes,
    sha256: record.sha256,
    mime_type: record.mime_type,
    file_name: record.file_name,
    download_url: record.download_url,
    expires_at: record.expires_at,
    downloads_remaining: record.downloads_remaining,
    embedded: record.embedded,
    delivery_mode: record.delivery_mode
  };
}

function requireOpenAIFile(value: unknown): OpenAIFileReference {
  if (!isRecord(value)) throw new ArtifactTransferError('invalid_file_reference', 'file must be a ChatGPT file reference');
  const downloadUrl = typeof value.download_url === 'string' ? value.download_url.trim() : '';
  const fileId = typeof value.file_id === 'string' ? value.file_id.trim() : '';
  if (!downloadUrl || !fileId) {
    throw new ArtifactTransferError('invalid_file_reference', 'file.download_url and file.file_id are required');
  }
  return {
    download_url: downloadUrl,
    file_id: fileId,
    ...(typeof value.mime_type === 'string' && value.mime_type.trim() ? { mime_type: value.mime_type.trim() } : {}),
    ...(typeof value.file_name === 'string' && value.file_name.trim() ? { file_name: safeFileName(value.file_name) } : {})
  };
}

function requirePath(value: unknown, field: string): string {
  if (typeof value !== 'string' || !value.trim()) throw new ArtifactTransferError('invalid_path', `${field} must be a non-empty string`);
  return value;
}

function optionalSha256(value: unknown): string | null {
  if (value === undefined || value === null || value === '') return null;
  const hash = String(value).trim().toLowerCase();
  if (!/^[a-f0-9]{64}$/.test(hash)) throw new ArtifactTransferError('invalid_sha256', 'expected_sha256 must contain 64 lowercase hexadecimal characters');
  return hash;
}

function validateImportUrl(input: string, config: ExecMcpConfig): URL {
  let url: URL;
  try { url = new URL(input); } catch { throw new ArtifactTransferError('invalid_download_url', 'file.download_url must be an absolute URL'); }
  if (url.username || url.password) throw new ArtifactTransferError('invalid_download_url', 'credentials in download URL are not allowed');
  if (url.protocol !== 'https:' && !(config.artifactImportAllowHttp && url.protocol === 'http:')) {
    throw new ArtifactTransferError('invalid_download_url', 'file.download_url must use HTTPS');
  }
  if (!hostAllowed(url.hostname, config.artifactImportAllowedHosts)) {
    throw new ArtifactTransferError('download_host_not_allowed', `download host is not allowed: ${url.hostname}`, undefined, { download_host: url.hostname.toLowerCase() });
  }
  return url;
}

function hostAllowed(hostname: string, allowed: readonly string[]): boolean {
  if (allowed.length === 0) return true;
  const host = hostname.toLowerCase();
  return allowed.some((rule) => {
    const normalized = rule.replace(/^\*\./, '.');
    return normalized.startsWith('.') ? host.endsWith(normalized) || host === normalized.slice(1) : host === normalized;
  });
}

async function fetchFollowingRedirects(initial: URL, config: ExecMcpConfig, signal: AbortSignal): Promise<Response> {
  let current = initial;
  for (let redirects = 0; redirects <= 5; redirects += 1) {
    const response = await fetch(current, { redirect: 'manual', signal, headers: { accept: '*/*' } });
    if (![301, 302, 303, 307, 308].includes(response.status)) return response;
    const location = response.headers.get('location');
    await response.body?.cancel().catch(() => undefined);
    if (!location) throw new ArtifactTransferError('source_redirect_invalid', 'source returned a redirect without Location');
    current = validateImportUrl(new URL(location, current).toString(), config);
  }
  throw new ArtifactTransferError('too_many_redirects', 'source download exceeded 5 redirects');
}

async function writeHttpBodyToFile(body: AsyncIterable<Uint8Array>, path: string, maxBytes: number, signal?: AbortSignal): Promise<{ bytes: number; sha256: string }> {
  const handle = await open(path, 'wx', 0o600);
  const hash = createHash('sha256');
  let bytes = 0;
  try {
    for await (const chunk of body) {
      if (signal?.aborted) throw new ArtifactTransferError('request_cancelled', 'artifact transfer was cancelled');
      const buffer = Buffer.from(chunk);
      bytes += buffer.length;
      if (bytes > maxBytes) throw new ArtifactTransferError('file_too_large', `file_too_large: more than ${maxBytes}`);
      hash.update(buffer);
      await handle.write(buffer);
    }
    await handle.sync();
    return { bytes, sha256: hash.digest('hex') };
  } finally {
    await handle.close();
  }
}

async function writeLocalFileToRemote(localPath: string, targetPath: string, sha256: string, overwrite: boolean, config: ExecMcpConfig, signal?: AbortSignal): Promise<RemoteWriteResult> {
  const child = spawnRemoteProcess(config, [
    'python3', '-c', REMOTE_WRITE_SCRIPT,
    targetPath,
    config.defaultCwd,
    JSON.stringify(config.allowedCwds),
    String(config.artifactMaxBytes),
    sha256,
    overwrite ? '1' : '0'
  ]);
  const stdout: Buffer[] = [];
  const stderr: Buffer[] = [];
  let stdoutBytes = 0;
  let stderrBytes = 0;
  child.stdout.on('data', (chunk) => { if (stdoutBytes < 65536) { stdout.push(chunk); stdoutBytes += chunk.length; } });
  child.stderr.on('data', (chunk) => { if (stderrBytes < 65536) { stderr.push(chunk); stderrBytes += chunk.length; } });
  const stop = installProcessGuards(child, config.artifactTransferTimeoutSeconds, config.killGraceSeconds, signal);
  const close = waitForClose(child);
  const pipeResult = pipeline(createReadStream(localPath), child.stdin).catch((error) => error as Error);
  const [closed, pipeError] = await Promise.all([close, pipeResult]);
  stop();
  if (pipeError instanceof Error && closed.code === 0) throw new ArtifactTransferError('remote_stream_failed', pipeError.message);
  if (closed.code !== 0) throw remoteProcessError(stderr, closed, 'remote_write_failed');
  return parseJsonResult<RemoteWriteResult>(stdout, 'remote_protocol_error');
}

async function readRemoteFileToLocal(sourcePath: string, localPath: string, maxBytes: number, config: ExecMcpConfig, signal?: AbortSignal): Promise<{ path: string; bytes: number; sha256: string }> {
  const child = spawnRemoteProcess(config, [
    'python3', '-c', REMOTE_READ_SCRIPT,
    sourcePath,
    config.defaultCwd,
    JSON.stringify(config.allowedCwds),
    String(maxBytes)
  ]);
  child.stdin.end();
  const stderr: Buffer[] = [];
  let stderrBytes = 0;
  child.stderr.on('data', (chunk) => { if (stderrBytes < 65536) { stderr.push(chunk); stderrBytes += chunk.length; } });
  const stop = installProcessGuards(child, config.artifactTransferTimeoutSeconds, config.killGraceSeconds, signal);
  const close = waitForClose(child);
  const handle = await open(localPath, 'wx', 0o600);
  const hash = createHash('sha256');
  let bytes = 0;
  let streamError: unknown = null;
  try {
    for await (const chunk of child.stdout) {
      const buffer = Buffer.from(chunk);
      bytes += buffer.length;
      if (bytes > maxBytes) {
        terminateProcess(child, 'SIGTERM');
        throw new ArtifactTransferError('file_too_large', `file_too_large: more than ${maxBytes}`);
      }
      hash.update(buffer);
      await handle.write(buffer);
    }
    await handle.sync();
  } catch (error) {
    streamError = error;
  } finally {
    await handle.close();
  }
  const closed = await close;
  stop();
  if (streamError) throw streamError;
  if (closed.code !== 0) throw remoteProcessError(stderr, closed, 'remote_read_failed');
  const metadata = parseRemoteReadMetadata(stderr);
  if (metadata.bytes !== bytes) throw new ArtifactTransferError('remote_verification_failed', `remote announced ${metadata.bytes} bytes but streamed ${bytes}`);
  return { path: metadata.path, bytes, sha256: hash.digest('hex') };
}

function installProcessGuards(
  child: ReturnType<typeof spawnRemoteProcess>,
  timeoutSeconds: number,
  killGraceSeconds: number,
  signal?: AbortSignal
): () => void {
  let forceTimer: NodeJS.Timeout | null = null;
  let stopping = false;
  const requestStop = (): void => {
    if (stopping) return;
    stopping = true;
    terminateProcess(child, 'SIGTERM');
    forceTimer = setTimeout(() => terminateProcess(child, 'SIGKILL'), Math.max(1, killGraceSeconds) * 1000);
    forceTimer.unref?.();
  };
  const timer = setTimeout(requestStop, timeoutSeconds * 1000);
  timer.unref?.();
  const onAbort = (): void => requestStop();
  signal?.addEventListener('abort', onAbort, { once: true });
  return () => {
    clearTimeout(timer);
    if (forceTimer) clearTimeout(forceTimer);
    signal?.removeEventListener('abort', onAbort);
  };
}

function terminateProcess(child: ReturnType<typeof spawnRemoteProcess>, signal: NodeJS.Signals): void {
  try { if (child.pid) process.kill(-child.pid, signal); } catch { try { child.kill(signal); } catch {} }
}

function createTransferGuard(requestSignal: AbortSignal | undefined, timeoutSeconds: number): {
  signal: AbortSignal;
  close: () => void;
  mapError: (error: unknown) => unknown;
} {
  const controller = new AbortController();
  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    controller.abort(new Error('artifact_transfer_timeout'));
  }, timeoutSeconds * 1000);
  timer.unref?.();
  const onAbort = (): void => controller.abort(requestSignal?.reason || new Error('request_cancelled'));
  requestSignal?.addEventListener('abort', onAbort, { once: true });
  if (requestSignal?.aborted) onAbort();
  return {
    signal: controller.signal,
    close: () => {
      clearTimeout(timer);
      requestSignal?.removeEventListener('abort', onAbort);
    },
    mapError: (error: unknown): unknown => {
      if (timedOut) return new ArtifactTransferError('artifact_transfer_timeout', `artifact transfer exceeded ${timeoutSeconds} seconds`);
      if (requestSignal?.aborted) return new ArtifactTransferError('request_cancelled', 'artifact transfer was cancelled by the client');
      return error;
    }
  };
}

function waitForClose(child: ReturnType<typeof spawnRemoteProcess>): Promise<ProcessCloseResult> {
  return new Promise((resolve, reject) => {
    child.once('error', reject);
    child.once('close', (code, signal) => resolve({ code, signal }));
  });
}

function parseRemoteReadMetadata(chunks: Buffer[]): RemoteReadMetadata {
  const text = Buffer.concat(chunks).toString('utf8').trim();
  const marker = text.split('\n').find((line) => line.startsWith('EXECMCP_META '));
  if (!marker) throw new ArtifactTransferError('remote_protocol_error', text || 'remote read returned no metadata');
  let value: unknown;
  try { value = JSON.parse(marker.slice('EXECMCP_META '.length)); } catch { throw new ArtifactTransferError('remote_protocol_error', 'remote read returned invalid metadata'); }
  if (!isRecord(value) || value.ok !== true || typeof value.path !== 'string' || typeof value.bytes !== 'number') {
    throw new ArtifactTransferError('remote_protocol_error', 'remote read metadata is incomplete');
  }
  return value as unknown as RemoteReadMetadata;
}

function remoteProcessError(stderr: Buffer[], closed: ProcessCloseResult, fallback: string): ArtifactTransferError {
  const text = Buffer.concat(stderr).toString('utf8').trim();
  const marker = text.split('\n').find((line) => line.startsWith('EXECMCP_ERROR '));
  if (marker) {
    try {
      const value = JSON.parse(marker.slice('EXECMCP_ERROR '.length)) as { code?: string; message?: string };
      return new ArtifactTransferError(value.code || fallback, value.message || fallback);
    } catch {}
  }
  return new ArtifactTransferError(fallback, text || `${fallback}: exit=${closed.code} signal=${closed.signal || 'null'}`);
}

function parseJsonResult<T>(chunks: Buffer[], code: string): T {
  try { return JSON.parse(Buffer.concat(chunks).toString('utf8')) as T; } catch { throw new ArtifactTransferError(code, 'remote operation returned invalid JSON'); }
}

function clampMaxBytes(value: unknown, hardMax: number): number {
  if (value === undefined || value === null) return hardMax;
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) throw new ArtifactTransferError('invalid_max_bytes', 'max_bytes must be a positive integer');
  if (parsed > hardMax) throw new ArtifactTransferError('file_limit_too_large', `file_limit_too_large: ${parsed} > ${hardMax}`);
  return parsed;
}

function parseContentLength(value: string | null): number | null {
  if (!value) return null;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : null;
}

function normalizeMimeType(value: string | null | undefined): string {
  return String(value || '').split(';', 1)[0]?.trim().toLowerCase() || '';
}

function safeFileName(value: string): string {
  const name = basename(value.trim()).replace(/[\x00-\x1f\x7f]/g, '_');
  if (!name || name === '.' || name === '..') return 'artifact.bin';
  return name.slice(0, 255);
}

function detectMimeType(filePath: string): string {
  switch (extname(filePath).toLowerCase()) {
    case '.txt': return 'text/plain';
    case '.md': return 'text/markdown';
    case '.json': return 'application/json';
    case '.csv': return 'text/csv';
    case '.html': return 'text/html';
    case '.xml': return 'application/xml';
    case '.png': return 'image/png';
    case '.jpg':
    case '.jpeg': return 'image/jpeg';
    case '.gif': return 'image/gif';
    case '.webp': return 'image/webp';
    case '.svg': return 'image/svg+xml';
    case '.pdf': return 'application/pdf';
    case '.doc': return 'application/msword';
    case '.docx': return 'application/vnd.openxmlformats-officedocument.wordprocessingml.document';
    case '.xls': return 'application/vnd.ms-excel';
    case '.xlsx': return 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';
    case '.ppt': return 'application/vnd.ms-powerpoint';
    case '.pptx': return 'application/vnd.openxmlformats-officedocument.presentationml.presentation';
    case '.zip': return 'application/zip';
    case '.gz': return 'application/gzip';
    case '.tar': return 'application/x-tar';
    default: return 'application/octet-stream';
  }
}

function parseRange(header: string | undefined, size: number): { start: number; end: number } | null {
  if (!header || size === 0) return null;
  const match = /^bytes=(\d*)-(\d*)$/.exec(header.trim());
  if (!match) return null;
  const startText = match[1] || '';
  const endText = match[2] || '';
  if (!startText && !endText) return null;
  let start: number;
  let end: number;
  if (!startText) {
    const suffix = Number(endText);
    if (!Number.isInteger(suffix) || suffix <= 0) return null;
    start = Math.max(0, size - suffix);
    end = size - 1;
  } else {
    start = Number(startText);
    end = endText ? Number(endText) : size - 1;
  }
  if (!Number.isInteger(start) || !Number.isInteger(end) || start < 0 || end < start || start >= size) return null;
  return { start, end: Math.min(end, size - 1) };
}

function contentDisposition(name: string): string {
  const ascii = name.replace(/[^\x20-\x7e]/g, '_').replace(/["\\]/g, '_');
  return `attachment; filename="${ascii}"; filename*=UTF-8''${encodeURIComponent(name)}`;
}

function isRecord(value: unknown): value is UnknownRecord {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

const REMOTE_WRITE_SCRIPT = String.raw`
import errno, hashlib, json, os, stat, sys, tempfile

def emit_error(code, message):
    sys.stderr.write('EXECMCP_ERROR ' + json.dumps({'code': code, 'message': message}, separators=(',', ':')) + '\n')
    sys.exit(64)

input_path, default_cwd, allowed_json, max_bytes_text, expected_sha, overwrite_text = sys.argv[1:7]
allowed_cwds = json.loads(allowed_json)
max_bytes = int(max_bytes_text)
overwrite = overwrite_text == '1'

def allowed(path):
    for base in allowed_cwds:
        real_base = os.path.realpath(base)
        prefix = real_base if real_base == os.sep else real_base + os.sep
        if path == real_base or path.startswith(prefix):
            return True
    return False

target = input_path if os.path.isabs(input_path) else os.path.join(default_cwd, input_path)
parent = os.path.dirname(target) or '.'
name = os.path.basename(target.rstrip(os.sep))
if not name:
    emit_error('invalid_path', 'path must name a file: ' + target)
try:
    real_parent = os.path.realpath(parent)
    info = os.stat(real_parent)
except FileNotFoundError:
    emit_error('parent_not_found', 'parent directory does not exist for: ' + target)
except OSError as exc:
    emit_error('remote_error', str(exc))
if not stat.S_ISDIR(info.st_mode) or not allowed(real_parent):
    emit_error('invalid_path', 'real parent path is not allowed: ' + real_parent)
real_target = os.path.join(real_parent, name)
if os.path.islink(real_target):
    emit_error('symlink_not_allowed', 'symlink path is not allowed: ' + real_target)
if os.path.exists(real_target) and not stat.S_ISREG(os.stat(real_target).st_mode):
    emit_error('already_exists', 'target exists and is not a regular file: ' + real_target)

def same_existing(path, expected_bytes, expected_sha):
    existing_info = os.stat(path)
    if not stat.S_ISREG(existing_info.st_mode) or existing_info.st_size != expected_bytes:
        return False
    existing_hash = hashlib.sha256()
    with open(path, 'rb') as existing:
        while True:
            existing_chunk = existing.read(1024 * 1024)
            if not existing_chunk:
                break
            existing_hash.update(existing_chunk)
    return existing_hash.hexdigest() == expected_sha

fd, temp_path = tempfile.mkstemp(prefix='.' + name + '.execmcp-', dir=real_parent)
count = 0
hash_value = hashlib.sha256()
try:
    with os.fdopen(fd, 'wb') as output:
        while True:
            chunk = sys.stdin.buffer.read(1024 * 1024)
            if not chunk:
                break
            count += len(chunk)
            if count > max_bytes:
                emit_error('file_too_large', 'file_too_large: more than %d' % max_bytes)
            hash_value.update(chunk)
            output.write(chunk)
        output.flush()
        os.fsync(output.fileno())
    actual_sha = hash_value.hexdigest()
    if expected_sha and actual_sha != expected_sha:
        emit_error('checksum_mismatch', 'checksum_mismatch: expected %s, got %s' % (expected_sha, actual_sha))
    if overwrite:
        os.replace(temp_path, real_target)
    else:
        if os.path.exists(real_target):
            if same_existing(real_target, count, actual_sha):
                os.unlink(temp_path)
                temp_path = ''
            else:
                emit_error('already_exists', 'target exists with different content: ' + real_target)
        else:
            try:
                os.link(temp_path, real_target)
                os.unlink(temp_path)
                temp_path = ''
            except FileExistsError:
                if same_existing(real_target, count, actual_sha):
                    os.unlink(temp_path)
                    temp_path = ''
                else:
                    emit_error('already_exists', 'target exists with different content: ' + real_target)
    dir_fd = os.open(real_parent, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)
    print(json.dumps({'ok': True, 'path': real_target, 'bytes': count, 'sha256': actual_sha}, separators=(',', ':')))
finally:
    try:
        if temp_path and os.path.exists(temp_path):
            os.unlink(temp_path)
    except OSError:
        pass
`;

const REMOTE_READ_SCRIPT = String.raw`
import hashlib, json, os, stat, sys

def emit_error(code, message):
    sys.stderr.write('EXECMCP_ERROR ' + json.dumps({'code': code, 'message': message}, separators=(',', ':')) + '\n')
    sys.exit(64)

input_path, default_cwd, allowed_json, max_bytes_text = sys.argv[1:5]
allowed_cwds = json.loads(allowed_json)
max_bytes = int(max_bytes_text)

def allowed(path):
    for base in allowed_cwds:
        real_base = os.path.realpath(base)
        prefix = real_base if real_base == os.sep else real_base + os.sep
        if path == real_base or path.startswith(prefix):
            return True
    return False

target = input_path if os.path.isabs(input_path) else os.path.join(default_cwd, input_path)
try:
    real = os.path.realpath(target)
    info = os.stat(real)
except FileNotFoundError:
    emit_error('not_found', 'file not found: ' + target)
except OSError as exc:
    emit_error('remote_error', str(exc))
if not allowed(real):
    emit_error('invalid_path', 'real path is not allowed: ' + real)
if not stat.S_ISREG(info.st_mode):
    emit_error('not_file', 'path is not a file: ' + real)
if info.st_size > max_bytes:
    emit_error('file_too_large', 'file_too_large: %d > %d' % (info.st_size, max_bytes))
count = 0
with open(real, 'rb') as source:
    while True:
        chunk = source.read(1024 * 1024)
        if not chunk:
            break
        count += len(chunk)
        if count > max_bytes:
            emit_error('file_too_large', 'file_too_large: more than %d' % max_bytes)
        sys.stdout.buffer.write(chunk)
sys.stdout.buffer.flush()
sys.stderr.write('EXECMCP_META ' + json.dumps({'ok': True, 'path': real, 'bytes': count}, separators=(',', ':')) + '\n')
`;
