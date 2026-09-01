export const REMOTE_SUPERVISOR_PROTOCOL_VERSION = 1;

const MAX_FRAME_BYTES = 16 * 1024 * 1024;

export type RemoteSupervisorFrameType = 'O' | 'E' | 'S' | 'D' | 'R' | 'X';
export type RemoteSupervisorControlType = 'C' | 'A' | 'K';

export interface RemoteSupervisorConfig {
  protocol: 1;
  exec_id: string;
  command: string;
  shell: 'sh' | 'bash';
  cwd: string;
  timeout_seconds: number;
  kill_grace_seconds: number;
  allowed_cwds: string[];
  env: Record<string, string>;
}

export type RemoteSupervisorOutcomeReason =
  | 'exit'
  | 'request_timeout'
  | 'manual_cancel'
  | 'mcp_notification_cancel'
  | 'http_disconnect'
  | 'reaper_grace_exceeded'
  | 'executor_shutdown'
  | 'transport_closed'
  | 'supervisor_signal';

export interface RemoteSupervisorStarted {
  protocol: 1;
  pid: number;
  pgid: number;
}

export interface RemoteSupervisorDecision {
  protocol: 1;
  exec_id: string;
  reason: RemoteSupervisorOutcomeReason;
  pid: number;
  pgid: number;
  decision_ms: number;
}

export interface RemoteSupervisorResult {
  protocol: 1;
  exec_id: string;
  reason: RemoteSupervisorOutcomeReason;
  exit_code: number | null;
  signal: string | null;
  pid: number;
  pgid: number;
  decision_ms: number;
  duration_ms: number;
}

export interface RemoteSupervisorError {
  protocol: 1;
  code: string;
  message: string;
  exit_code: number;
}

export interface DecodedRemoteFrame {
  type: RemoteSupervisorFrameType;
  payload: Buffer;
}

export function encodeSupervisorFrame(type: RemoteSupervisorControlType, payload: Buffer | string): Buffer {
  const body = Buffer.isBuffer(payload) ? payload : Buffer.from(payload, 'utf8');
  if (body.length > MAX_FRAME_BYTES) throw new Error(`remote supervisor frame too large: ${body.length}`);
  const frame = Buffer.allocUnsafe(5 + body.length);
  frame[0] = type.charCodeAt(0);
  frame.writeUInt32BE(body.length, 1);
  body.copy(frame, 5);
  return frame;
}

export function encodeSupervisorConfig(config: RemoteSupervisorConfig): Buffer {
  return encodeSupervisorFrame('C', JSON.stringify(config));
}

export function encodeSupervisorAbort(reason: string): Buffer {
  return encodeSupervisorFrame('A', JSON.stringify({ reason }));
}

export function encodeSupervisorAck(execId: string): Buffer {
  return encodeSupervisorFrame('K', JSON.stringify({ exec_id: execId }));
}

export class RemoteSupervisorFrameDecoder {
  private pending = Buffer.alloc(0);

  push(chunk: Buffer): DecodedRemoteFrame[] {
    if (chunk.length === 0) return [];
    this.pending = this.pending.length === 0 ? Buffer.from(chunk) : Buffer.concat([this.pending, chunk]);
    const frames: DecodedRemoteFrame[] = [];
    let offset = 0;
    while (this.pending.length - offset >= 5) {
      const typeByte = this.pending[offset];
      if (typeByte === undefined) break;
      const type = String.fromCharCode(typeByte) as RemoteSupervisorFrameType;
      if (type !== 'O' && type !== 'E' && type !== 'S' && type !== 'D' && type !== 'R' && type !== 'X') {
        throw new Error(`invalid remote supervisor frame type: ${typeByte}`);
      }
      const length = this.pending.readUInt32BE(offset + 1);
      if (length > MAX_FRAME_BYTES) throw new Error(`remote supervisor frame exceeds limit: ${length}`);
      if (this.pending.length - offset < 5 + length) break;
      frames.push({ type, payload: Buffer.from(this.pending.subarray(offset + 5, offset + 5 + length)) });
      offset += 5 + length;
    }
    if (offset > 0) this.pending = Buffer.from(this.pending.subarray(offset));
    return frames;
  }

  finish(): void {
    if (this.pending.length !== 0) throw new Error(`truncated remote supervisor frame: ${this.pending.length} buffered bytes`);
  }
}

export function parseSupervisorJson<T>(payload: Buffer): T {
  return JSON.parse(payload.toString('utf8')) as T;
}

// A single remote process owns command launch, deadline enforcement, cancellation,
// process-group cleanup, output framing and the authoritative final result. Command
// output backpressure can block the command, but never the supervisor control loop.
export const REMOTE_SUPERVISOR_PY = String.raw`
import collections, json, os, re, selectors, signal, stat, struct, subprocess, sys, time

PROTOCOL = 1
MAX_FRAME = 16 * 1024 * 1024
OUT_HIGH_WATER = 1024 * 1024
OUT_LOW_WATER = 512 * 1024
RESULT_TTL = 86400
VALID_ABORTS = {
    'request_timeout', 'manual_cancel', 'mcp_notification_cancel',
    'http_disconnect', 'reaper_grace_exceeded', 'executor_shutdown'
}


def read_exact(fd, size):
    out = bytearray()
    while len(out) < size:
        chunk = os.read(fd, size - len(out))
        if not chunk:
            raise EOFError('unexpected EOF')
        out.extend(chunk)
    return bytes(out)


def read_frame_blocking(fd):
    header = read_exact(fd, 5)
    size = struct.unpack('>I', header[1:5])[0]
    if size > MAX_FRAME:
        raise ValueError('frame too large')
    return header[:1], read_exact(fd, size)


def blocking_write_frame(kind, payload=b''):
    if isinstance(payload, str):
        payload = payload.encode('utf-8')
    frame = kind + struct.pack('>I', len(payload)) + payload
    view = memoryview(frame)
    try:
        while view:
            written = os.write(1, view)
            view = view[written:]
        return True
    except OSError:
        return False


def blocking_write_json(kind, value):
    return blocking_write_frame(kind, json.dumps(value, separators=(',', ':')).encode('utf-8'))


def parse_control(buffer):
    messages = []
    offset = 0
    while len(buffer) - offset >= 5:
        kind = buffer[offset:offset + 1]
        size = struct.unpack('>I', buffer[offset + 1:offset + 5])[0]
        if size > MAX_FRAME:
            raise ValueError('control frame too large')
        if len(buffer) - offset < 5 + size:
            break
        messages.append((kind, bytes(buffer[offset + 5:offset + 5 + size])))
        offset += 5 + size
    return messages, bytearray(buffer[offset:])


def shell_exit_code(returncode):
    if returncode is None:
        return None
    return returncode if returncode >= 0 else 128 + (-returncode)


def signal_name(returncode):
    if returncode is None or returncode >= 0:
        return None
    try:
        return signal.Signals(-returncode).name
    except Exception:
        return 'SIG' + str(-returncode)


def safe_real_cwd(cwd, allowed):
    real = os.path.realpath(cwd)
    if not os.path.isdir(real):
        raise RuntimeError('invalid_cwd: cwd does not exist or is not accessible: ' + cwd)
    for base in allowed:
        real_base = os.path.realpath(base)
        if not os.path.isdir(real_base):
            continue
        try:
            if os.path.commonpath([real, real_base]) == real_base:
                return real
        except ValueError:
            continue
    raise RuntimeError('invalid_cwd: real cwd is not allowed: ' + real)


def result_dir():
    path = '/tmp/exec-mcp-runtime-results-' + str(os.geteuid())
    try:
        os.mkdir(path, 0o700)
    except FileExistsError:
        pass
    info = os.lstat(path)
    if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode) or info.st_uid != os.geteuid():
        raise RuntimeError('unsafe result journal directory')
    if stat.S_IMODE(info.st_mode) != 0o700:
        os.chmod(path, 0o700)
    return path


def result_path(base, exec_id):
    if not re.match(r'^exec-[0-9a-fA-F-]+$', exec_id):
        raise ValueError('invalid exec id')
    return base + '/' + exec_id + '.json'


def fsync_dir(base):
    try:
        fd = os.open(base, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass


def cleanup_stale_results(base):
    now = time.time()
    try:
        names = os.listdir(base)
    except OSError:
        return
    for name in names:
        if not name.startswith('exec-') or not name.endswith('.json'):
            continue
        path = base + '/' + name
        try:
            info = os.lstat(path)
            if stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid() and now - info.st_mtime > RESULT_TTL:
                os.unlink(path)
        except OSError:
            pass


def write_result_journal(base, exec_id, result):
    path = result_path(base, exec_id)
    tmp = path + '.tmp.' + str(os.getpid()) + '.' + str(time.time_ns())
    data = json.dumps(result, separators=(',', ':')).encode('utf-8')
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, 'O_NOFOLLOW'):
        flags |= os.O_NOFOLLOW
    fd = os.open(tmp, flags, 0o600)
    try:
        view = memoryview(data)
        while view:
            written = os.write(fd, view)
            view = view[written:]
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(tmp, path)
    fsync_dir(base)


def remove_result_journal(base, exec_id):
    try:
        os.unlink(result_path(base, exec_id))
        fsync_dir(base)
    except OSError:
        pass


def fatal(code, message, exit_code=127):
    blocking_write_json(b'X', {'protocol': PROTOCOL, 'code': code, 'message': message, 'exit_code': exit_code})
    raise SystemExit(exit_code)


try:
    kind, payload = read_frame_blocking(0)
    if kind != b'C':
        fatal('protocol_error', 'first frame must be config')
    cfg = json.loads(payload.decode('utf-8'))
    if cfg.get('protocol') != PROTOCOL:
        fatal('protocol_error', 'unsupported protocol version')
    exec_id = str(cfg['exec_id'])
    base = result_dir()
    result_path(base, exec_id)
    cleanup_stale_results(base)
    remove_result_journal(base, exec_id)
    cwd = safe_real_cwd(str(cfg['cwd']), list(cfg.get('allowed_cwds') or []))
    command = str(cfg['command'])
    shell = str(cfg['shell'])
    if shell not in ('sh', 'bash'):
        fatal('protocol_error', 'unsupported shell', 126)
    shell_path = '/bin/bash' if shell == 'bash' else '/bin/sh'
    timeout_seconds = float(cfg['timeout_seconds'])
    grace = max(0.1, float(cfg['kill_grace_seconds']))
    env = os.environ.copy()
    for key, value in dict(cfg.get('env') or {}).items():
        if re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', key) and key not in ('ENV', 'BASH_ENV'):
            env[key] = str(value)
except SystemExit:
    raise
except Exception as exc:
    fatal('bootstrap_failed', str(exc), 126)

try:
    child = subprocess.Popen(
        [shell_path, '-c', command], cwd=cwd, env=env,
        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        start_new_session=True, bufsize=0, close_fds=True
    )
except Exception as exc:
    fatal('spawn_failed', str(exc), 127)

pgid = child.pid
started_monotonic = time.monotonic()
selector = selectors.DefaultSelector()
control_state = bytearray()
out_queue = collections.deque()
out_bytes = 0
stdout_registered = False
child_reads_paused = False
input_closed = False
output_closed = False
pending_abort = None
acked = False

stdout_fd = 1
os.set_blocking(stdout_fd, False)
os.set_blocking(0, False)
selector.register(0, selectors.EVENT_READ, b'I')
child_fds = []
for stream, kind in ((child.stdout, b'O'), (child.stderr, b'E')):
    if stream is not None:
        fd = stream.fileno()
        os.set_blocking(fd, False)
        selector.register(fd, selectors.EVENT_READ, kind)
        child_fds.append((fd, kind))


def sync_stdout_registration():
    global stdout_registered
    want = bool(out_queue) and not output_closed
    if want and not stdout_registered:
        selector.register(stdout_fd, selectors.EVENT_WRITE, b'W')
        stdout_registered = True
    elif not want and stdout_registered:
        try:
            selector.unregister(stdout_fd)
        except Exception:
            pass
        stdout_registered = False


def sync_child_backpressure():
    global child_reads_paused
    if not child_reads_paused and out_bytes >= OUT_HIGH_WATER:
        for fd, kind in child_fds:
            if kind == b'Z':
                continue
            try:
                selector.unregister(fd)
            except Exception:
                pass
        child_reads_paused = True
    elif child_reads_paused and out_bytes <= OUT_LOW_WATER:
        for fd, kind in child_fds:
            if kind == b'Z':
                continue
            try:
                selector.register(fd, selectors.EVENT_READ, kind)
            except KeyError:
                pass
            except OSError:
                pass
        child_reads_paused = False


def queue_frame(kind, payload=b''):
    global out_bytes
    if output_closed:
        return False
    if isinstance(payload, str):
        payload = payload.encode('utf-8')
    frame = kind + struct.pack('>I', len(payload)) + payload
    out_queue.append(memoryview(frame))
    out_bytes += len(frame)
    sync_stdout_registration()
    sync_child_backpressure()
    return True


def queue_json(kind, value):
    return queue_frame(kind, json.dumps(value, separators=(',', ':')).encode('utf-8'))


def flush_output():
    global out_bytes, output_closed
    while out_queue and not output_closed:
        view = out_queue[0]
        try:
            written = os.write(stdout_fd, view)
        except BlockingIOError:
            break
        except OSError:
            output_closed = True
            out_queue.clear()
            out_bytes = 0
            break
        if written <= 0:
            output_closed = True
            out_queue.clear()
            out_bytes = 0
            break
        out_bytes -= written
        if written == len(view):
            out_queue.popleft()
        else:
            out_queue[0] = view[written:]
    sync_stdout_registration()
    sync_child_backpressure()


def consume_control_messages():
    global control_state, pending_abort, acked
    messages, remaining = parse_control(control_state)
    control_state = remaining
    for kind, payload in messages:
        try:
            value = json.loads(payload.decode('utf-8'))
        except Exception:
            continue
        if kind == b'A' and pending_abort is None:
            reason = value.get('reason')
            if reason in VALID_ABORTS:
                pending_abort = reason
        elif kind == b'K' and value.get('exec_id') == exec_id:
            acked = True


def pump(timeout):
    global input_closed, control_state, output_closed
    try:
        events = selector.select(max(0.0, timeout))
    except OSError:
        events = []
    for key, _mask in events:
        if key.data == b'W':
            flush_output()
            continue
        if key.data == b'I':
            try:
                chunk = os.read(0, 65536)
            except BlockingIOError:
                continue
            except OSError:
                input_closed = True
                continue
            if not chunk:
                input_closed = True
                try:
                    selector.unregister(0)
                except Exception:
                    pass
            else:
                control_state.extend(chunk)
                consume_control_messages()
            continue
        if key.data == b'O' or key.data == b'E':
            try:
                chunk = os.read(key.fd, 65536)
            except BlockingIOError:
                continue
            except OSError:
                chunk = b''
            if chunk:
                queue_frame(key.data, chunk)
            else:
                try:
                    selector.unregister(key.fd)
                except Exception:
                    pass
                for idx, (fd, kind) in enumerate(child_fds):
                    if fd == key.fd:
                        child_fds[idx] = (fd, b'Z')
                        break
    flush_output()


def leader_exited(pid):
    try:
        info = os.waitid(os.P_PID, pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
        return info is not None
    except ChildProcessError:
        return True


def live_group_members(group, leader_pid):
    try:
        names = os.listdir('/proc')
    except OSError:
        return True
    for name in names:
        if not name.isdigit():
            continue
        pid = int(name)
        if pid == leader_pid:
            continue
        try:
            text = open('/proc/' + name + '/stat', 'r', encoding='utf-8').read()
            close = text.rfind(')')
            if close < 0:
                continue
            fields = text[close + 2:].split()
            state = fields[0]
            proc_pgid = int(fields[2])
            if proc_pgid == group and state != 'Z':
                return True
        except (OSError, ValueError, IndexError):
            continue
    return False


def kill_group(sig):
    try:
        os.killpg(pgid, sig)
    except ProcessLookupError:
        pass


def wait_group_exit(until):
    while time.monotonic() < until:
        pump(min(0.05, max(0.0, until - time.monotonic())))
        if leader_exited(child.pid) and not live_group_members(pgid, child.pid):
            return True
    return leader_exited(child.pid) and not live_group_members(pgid, child.pid)


def terminate_group():
    kill_group(signal.SIGTERM)
    if wait_group_exit(time.monotonic() + grace):
        return
    kill_group(signal.SIGKILL)
    wait_group_exit(time.monotonic() + 1.0)


def cleanup_after_normal_exit():
    if not live_group_members(pgid, child.pid):
        return
    kill_group(signal.SIGTERM)
    if wait_group_exit(time.monotonic() + 1.0):
        return
    kill_group(signal.SIGKILL)
    wait_group_exit(time.monotonic() + 1.0)


def drain_child_to_eof():
    while not input_closed and not output_closed:
        live_reads = [item for item in child_fds if item[1] != b'Z']
        if not live_reads:
            return True
        pump(0.05)
    return False


supervisor_signal = None
def on_signal(sig, _frame):
    global supervisor_signal
    if supervisor_signal is None:
        supervisor_signal = sig
for sig in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
    signal.signal(sig, on_signal)

queue_json(b'S', {'protocol': PROTOCOL, 'pid': child.pid, 'pgid': pgid})
deadline = started_monotonic + timeout_seconds
reason = None
while reason is None:
    if leader_exited(child.pid):
        reason = 'exit'
        break
    if supervisor_signal is not None:
        reason = 'supervisor_signal'
        break
    if pending_abort is not None:
        reason = pending_abort
        break
    if input_closed or output_closed:
        reason = 'transport_closed'
        break
    now = time.monotonic()
    if now >= deadline:
        reason = 'request_timeout'
        break
    pump(min(0.05, max(0.0, deadline - now)))

decision_monotonic = time.monotonic()
if reason != 'exit' and not output_closed:
    queue_json(b'D', {
        'protocol': PROTOCOL,
        'exec_id': exec_id,
        'reason': reason,
        'pid': child.pid,
        'pgid': pgid,
        'decision_ms': int((decision_monotonic - started_monotonic) * 1000),
    })
    flush_output()
if reason == 'exit':
    cleanup_after_normal_exit()
else:
    terminate_group()

try:
    returncode = child.wait(timeout=1.0)
except subprocess.TimeoutExpired:
    kill_group(signal.SIGKILL)
    returncode = child.wait()

result = {
    'protocol': PROTOCOL,
    'exec_id': exec_id,
    'reason': reason,
    'exit_code': shell_exit_code(returncode),
    'signal': signal_name(returncode),
    'pid': child.pid,
    'pgid': pgid,
    'decision_ms': int((decision_monotonic - started_monotonic) * 1000),
    'duration_ms': int((time.monotonic() - started_monotonic) * 1000),
}
# Persist the authoritative lifecycle fact before any data-plane wait.
write_result_journal(base, exec_id, result)
# Preserve stream ordering: all command output is framed before the final result.
# The queue is bounded; when the consumer resumes, child reads resume automatically.
drained = drain_child_to_eof()
if drained and not input_closed and not output_closed:
    queue_json(b'R', result)
# No arbitrary send deadline: after the child is reaped there is no execution risk.
# Wait for ACK or an actual transport close so already-buffered output is not lost.
while not acked and not input_closed and not output_closed:
    pump(0.05)
if acked:
    remove_result_journal(base, exec_id)
raise SystemExit(0)
`;

// Exceptional-path recovery only. Normal executions use the single framed SSH session.
export const REMOTE_SUPERVISOR_RECONCILE_PY = String.raw`
import json, os, re, stat, sys, time
exec_id = sys.argv[1]
wait_seconds = max(0.0, float(sys.argv[2]))
if not re.match(r'^exec-[0-9a-fA-F-]+$', exec_id):
    raise SystemExit(2)
base = '/tmp/exec-mcp-runtime-results-' + str(os.geteuid())
try:
    info = os.lstat(base)
    if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode) or info.st_uid != os.geteuid():
        raise SystemExit(3)
except FileNotFoundError:
    pass
path = base + '/' + exec_id + '.json'
deadline = time.monotonic() + wait_seconds
while True:
    try:
        with open(path, 'rb') as handle:
            data = handle.read(1024 * 1024)
        value = json.loads(data.decode('utf-8'))
        if value.get('protocol') != 1 or value.get('exec_id') != exec_id:
            raise SystemExit(3)
        sys.stdout.write(json.dumps(value, separators=(',', ':')))
        sys.stdout.flush()
        try:
            os.unlink(path)
        except OSError:
            pass
        raise SystemExit(0)
    except FileNotFoundError:
        pass
    except (OSError, ValueError, UnicodeDecodeError, json.JSONDecodeError):
        raise SystemExit(3)
    if time.monotonic() >= deadline:
        raise SystemExit(4)
    time.sleep(0.05)
`;
