export function parseConfig(env = process.env) {
  const allowedCwds = (env.ALLOWED_CWDS || '/root,/root/config-git,/root/exec-mcp,/tmp,/app')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);

  return {
    host: env.HOST || '0.0.0.0',
    port: Number.parseInt(env.PORT || '8080', 10),
    allowedCwds,
    defaultCwd: env.DEFAULT_CWD || allowedCwds[0] || '/tmp',
    defaultTimeoutSeconds: positiveInt(env.DEFAULT_TIMEOUT_SECONDS, 120),
    maxTimeoutSeconds: positiveInt(env.MAX_TIMEOUT_SECONDS, 600),
    defaultMaxOutputBytes: positiveInt(env.DEFAULT_MAX_OUTPUT_BYTES, 5 * 1024 * 1024),
    hardMaxOutputBytes: positiveInt(env.HARD_MAX_OUTPUT_BYTES, 20 * 1024 * 1024),
    mcpMaxRequestBytes: positiveInt(env.MCP_MAX_REQUEST_BYTES, 16 * 1024 * 1024),
    fileMaxDownloadBytes: positiveInt(env.FILE_MAX_DOWNLOAD_BYTES, 10 * 1024 * 1024),
    fileMaxUploadBytes: positiveInt(env.FILE_MAX_UPLOAD_BYTES, 10 * 1024 * 1024),
    ringBufferBytes: positiveInt(env.RING_BUFFER_BYTES, 65536),
    maxConcurrentExecs: positiveInt(env.MAX_CONCURRENT_EXECS, 2),
    recentHistoryLimit: positiveInt(env.RECENT_EXEC_HISTORY_LIMIT, 100),
    registryReapGraceSeconds: positiveInt(env.REGISTRY_REAP_GRACE_SECONDS, 30),
    emergencyReapSeconds: positiveInt(env.EMERGENCY_REAP_SECONDS, 30),
    exposeRedactedCommandPreview: String(env.EXPOSE_REDACTED_COMMAND_PREVIEW || 'false').toLowerCase() === 'true',
    commandPreviewMaxChars: positiveInt(env.COMMAND_PREVIEW_MAX_CHARS, 160),
    lifecycleLogs: String(env.LIFECYCLE_LOGS || 'true').toLowerCase() !== 'false',
    heartbeatSeconds: positiveInt(env.HEARTBEAT_SECONDS, 15),
    killGraceSeconds: positiveInt(env.KILL_GRACE_SECONDS, 5),
    remote: {
      bin: env.REMOTE_BIN || 'ssh',
      binArgs: splitArgs(env.REMOTE_BIN_ARGS || ''),
      host: env.REMOTE_HOST || '',
      port: positiveInt(env.REMOTE_PORT, 22),
      user: env.REMOTE_USER || 'root',
      keyPath: env.REMOTE_KEY_PATH || '',
      connectTimeoutSeconds: positiveInt(env.REMOTE_CONNECT_TIMEOUT_SECONDS, 10),
      strictHostKeyChecking: env.REMOTE_STRICT_HOST_KEY_CHECKING || 'no',
      knownHostsPath: env.REMOTE_KNOWN_HOSTS_PATH || '/app/known_hosts'
    }
  };
}

function positiveInt(value, fallback) {
  const n = Number.parseInt(String(value), 10);
  return Number.isFinite(n) && n > 0 ? n : fallback;
}

function splitArgs(value) {
  return String(value).split(/\s+/).filter(Boolean);
}
