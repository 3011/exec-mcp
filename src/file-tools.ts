import { extname } from 'node:path';
import type { ExecMcpConfig } from './config.js';
import { spawnRemoteShell } from './exec-runner.js';

const DEFAULT_MAX_FILE_DOWNLOAD_BYTES = 10 * 1024 * 1024;
const DEFAULT_MAX_FILE_UPLOAD_BYTES = 10 * 1024 * 1024;

type UnknownRecord = Record<string, unknown>;

interface RemoteFileSuccess {
  ok: true;
  path: string;
  bytes: number;
}

interface RemoteDownloadSuccess extends RemoteFileSuccess {
  data_base64: string;
}

interface ProcessCloseResult {
  code: number | null;
  signal: NodeJS.Signals | null;
}

export class FileToolError extends Error {
  readonly code: string;

  constructor(code: string, message: string) {
    super(message);
    this.name = 'FileToolError';
    this.code = code;
  }
}

export async function downloadFileTool(args: UnknownRecord, config: ExecMcpConfig): Promise<{ path: string; bytes: number; mime_type: string; data_base64: string }> {
  const inputPath = requireInputPath(args?.path);
  const maxBytes = clampFileLimit(args?.max_bytes, config.fileMaxDownloadBytes || DEFAULT_MAX_FILE_DOWNLOAD_BYTES, 'invalid_max_bytes');
  const maxStdoutBytes = Math.ceil(maxBytes * 4 / 3) + 8192;
  const body = await runRemoteFileScript<RemoteDownloadSuccess>(config, buildRemoteDownloadScript(inputPath, maxBytes, config), maxStdoutBytes);
  return {
    path: body.path,
    bytes: body.bytes,
    mime_type: detectMimeType(body.path),
    data_base64: body.data_base64
  };
}

export async function uploadFileTool(args: UnknownRecord, config: ExecMcpConfig): Promise<{ path: string; bytes: number; action: 'write' | 'append'; mime_type: string }> {
  const inputPath = requireInputPath(args?.path);
  const data = decodeBase64(args?.data_base64, config.fileMaxUploadBytes || DEFAULT_MAX_FILE_UPLOAD_BYTES);
  const body = await runRemoteFileScript<RemoteFileSuccess>(
    config,
    buildRemoteUploadScript(inputPath, data.toString('base64'), args.append === true, config),
    8192
  );
  return {
    path: body.path,
    bytes: body.bytes,
    action: args.append === true ? 'append' : 'write',
    mime_type: typeof args?.mime_type === 'string' && args.mime_type.trim() ? args.mime_type.trim() : detectMimeType(body.path)
  };
}

function requireInputPath(inputPath: unknown): string {
  if (typeof inputPath !== 'string' || !inputPath.trim()) {
    throw new FileToolError('invalid_path', 'path must be a non-empty string');
  }
  return inputPath;
}

function clampFileLimit(value: unknown, max: number, errorCode: string): number {
  if (value === undefined || value === null) return max;
  const n = Number.parseInt(String(value), 10);
  if (!Number.isFinite(n) || n <= 0) throw new FileToolError(errorCode, `${errorCode}: ${value}`);
  if (n > max) throw new FileToolError('file_limit_too_large', `file_limit_too_large: ${n} > ${max}`);
  return n;
}

function decodeBase64(value: unknown, maxBytes: number): Buffer {
  if (typeof value !== 'string') {
    throw new FileToolError('invalid_base64', 'data_base64 must be a string');
  }
  const compact = value.replace(/\s+/g, '');
  if (compact.length % 4 === 1 || !/^[A-Za-z0-9+/]*={0,2}$/.test(compact)) {
    throw new FileToolError('invalid_base64', 'data_base64 is not valid base64');
  }
  const padding = compact.endsWith('==') ? 2 : compact.endsWith('=') ? 1 : 0;
  const decodedBytes = Math.floor((compact.length * 3) / 4) - padding;
  if (decodedBytes > maxBytes) throw new FileToolError('file_too_large', `file_too_large: ${decodedBytes} > ${maxBytes}`);

  const data = Buffer.from(compact, 'base64');
  if (data.length !== decodedBytes) throw new FileToolError('invalid_base64', 'data_base64 is not valid base64');
  return data;
}

async function runRemoteFileScript<T extends RemoteFileSuccess>(config: ExecMcpConfig, script: string, maxStdoutBytes: number): Promise<T> {
  let spawned;
  try {
    spawned = spawnRemoteShell(config, script);
  } catch (err) {
    throw new FileToolError('remote_config_error', errorMessage(err));
  }

  const { child, stdin } = spawned;
  let stdoutBytes = 0;
  let stderrBytes = 0;
  const stdout: Buffer[] = [];
  const stderr: Buffer[] = [];
  let outputTooLarge = false;
  let timedOut = false;

  const killRemote = (signal: NodeJS.Signals): void => {
    try {
      if (child.pid) process.kill(-child.pid, signal);
    } catch {
      try { child.kill(signal); } catch {}
    }
  };

  const timer = setTimeout(() => {
    timedOut = true;
    killRemote('SIGTERM');
  }, config.defaultTimeoutSeconds * 1000);
  timer.unref?.();

  child.stdout.on('data', (chunk) => {
    stdoutBytes += chunk.length;
    if (stdoutBytes > maxStdoutBytes) {
      outputTooLarge = true;
      killRemote('SIGTERM');
      return;
    }
    stdout.push(chunk);
  });
  child.stderr.on('data', (chunk) => {
    stderrBytes += chunk.length;
    if (stderrBytes <= 65536) stderr.push(chunk);
  });

  const close = new Promise<ProcessCloseResult>((resolve, reject) => {
    child.on('error', reject);
    child.on('close', (code: number | null, signal: NodeJS.Signals | null) => resolve({ code, signal }));
  });
  child.stdin.end(stdin);

  const { code, signal } = await close;
  clearTimeout(timer);

  if (timedOut) throw new FileToolError('remote_timeout', 'remote file operation timed out');
  if (outputTooLarge) throw new FileToolError('remote_output_too_large', `remote stdout exceeded ${maxStdoutBytes} bytes`);
  if (code !== 0) {
    const errText = Buffer.concat(stderr).toString('utf8').trim();
    throw new FileToolError('remote_failed', errText || `remote file operation failed: exit=${code} signal=${signal || 'null'}`);
  }

  let body: unknown;
  try {
    body = JSON.parse(Buffer.concat(stdout).toString('utf8'));
  } catch {
    throw new FileToolError('remote_protocol_error', 'remote file operation returned invalid JSON');
  }
  if (!isRecord(body) || body.ok !== true) {
    throw new FileToolError(
      isRecord(body) && typeof body.code === 'string' ? body.code : 'remote_failed',
      isRecord(body) && typeof body.message === 'string' ? body.message : 'remote file operation failed'
    );
  }
  return body as T;
}

function buildRemoteDownloadScript(inputPath: string, maxBytes: number, config: ExecMcpConfig): string {
  return `python3 - <<'PY'
import base64, json, os, stat, sys

INPUT_PATH = ${JSON.stringify(inputPath)}
DEFAULT_CWD = ${JSON.stringify(config.defaultCwd)}
ALLOWED_CWDS = ${JSON.stringify(config.allowedCwds)}
MAX_BYTES = ${maxBytes}

def emit(obj):
    print(json.dumps(obj, separators=(',', ':')))

def fail(code, message):
    emit({'ok': False, 'code': code, 'message': message})
    sys.exit(0)

def target_path(path):
    return path if os.path.isabs(path) else os.path.join(DEFAULT_CWD, path)

def allowed(path):
    for base in ALLOWED_CWDS:
        try:
            real_base = os.path.realpath(base)
        except OSError:
            continue
        prefix = real_base if real_base == os.sep else real_base + os.sep
        if path == real_base or path.startswith(prefix):
            return True
    return False

target = target_path(INPUT_PATH)
try:
    real = os.path.realpath(target)
    info = os.stat(real)
except FileNotFoundError:
    fail('not_found', 'file not found: ' + target)
except OSError as exc:
    fail('remote_error', str(exc))

if not allowed(real):
    fail('invalid_path', 'real path is not allowed: ' + real)
if not stat.S_ISREG(info.st_mode):
    fail('not_file', 'path is not a file: ' + real)
if info.st_size > MAX_BYTES:
    fail('file_too_large', 'file_too_large: %d > %d' % (info.st_size, MAX_BYTES))

with open(real, 'rb') as fh:
    data = fh.read(MAX_BYTES + 1)
if len(data) > MAX_BYTES:
    fail('file_too_large', 'file_too_large: more than %d' % MAX_BYTES)

emit({'ok': True, 'path': real, 'bytes': len(data), 'data_base64': base64.b64encode(data).decode('ascii')})
PY
`;
}

function buildRemoteUploadScript(inputPath: string, dataBase64: string, append: boolean, config: ExecMcpConfig): string {
  return `python3 - <<'PY'
import base64, binascii, errno, json, os, stat, sys

INPUT_PATH = ${JSON.stringify(inputPath)}
DEFAULT_CWD = ${JSON.stringify(config.defaultCwd)}
ALLOWED_CWDS = ${JSON.stringify(config.allowedCwds)}
DATA_BASE64 = ${JSON.stringify(dataBase64)}
APPEND = ${append ? 'True' : 'False'}

def emit(obj):
    print(json.dumps(obj, separators=(',', ':')))

def fail(code, message):
    emit({'ok': False, 'code': code, 'message': message})
    sys.exit(0)

def target_path(path):
    return path if os.path.isabs(path) else os.path.join(DEFAULT_CWD, path)

def allowed(path):
    for base in ALLOWED_CWDS:
        try:
            real_base = os.path.realpath(base)
        except OSError:
            continue
        prefix = real_base if real_base == os.sep else real_base + os.sep
        if path == real_base or path.startswith(prefix):
            return True
    return False

try:
    data = base64.b64decode(DATA_BASE64.encode('ascii'), validate=True)
except (binascii.Error, ValueError) as exc:
    fail('invalid_base64', str(exc))

target = target_path(INPUT_PATH)
parent = os.path.dirname(target) or '.'
name = os.path.basename(target.rstrip(os.sep))
if not name:
    fail('invalid_path', 'path must name a file: ' + target)

try:
    real_parent = os.path.realpath(parent)
    parent_info = os.stat(real_parent)
except FileNotFoundError:
    fail('parent_not_found', 'parent directory does not exist for: ' + target)
except OSError as exc:
    fail('remote_error', str(exc))

if not stat.S_ISDIR(parent_info.st_mode):
    fail('parent_not_found', 'parent path is not a directory: ' + parent)
if not allowed(real_parent):
    fail('invalid_path', 'real parent path is not allowed: ' + real_parent)

real_target = os.path.join(real_parent, name)
if os.path.islink(real_target):
    fail('symlink_not_allowed', 'symlink path is not allowed: ' + real_target)

flags = os.O_WRONLY | os.O_CREAT | (os.O_APPEND if APPEND else os.O_TRUNC)
if hasattr(os, 'O_NOFOLLOW'):
    flags |= os.O_NOFOLLOW
try:
    fd = os.open(real_target, flags, 0o666)
except IsADirectoryError:
    fail('not_file', 'path is not a file: ' + real_target)
except OSError as exc:
    if exc.errno == errno.ELOOP:
        fail('symlink_not_allowed', 'symlink path is not allowed: ' + real_target)
    fail('remote_error', str(exc))

with os.fdopen(fd, 'ab' if APPEND else 'wb') as fh:
    fh.write(data)

emit({'ok': True, 'path': real_target, 'bytes': len(data)})
PY
`;
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

function isRecord(value: unknown): value is UnknownRecord {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
