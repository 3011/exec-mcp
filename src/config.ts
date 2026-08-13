export const ARTIFACT_EMBED_HARD_MAX_BYTES = 1_450_000;

export interface RemoteConfig {
  bin: string;
  binArgs: string[];
  host: string;
  port: number;
  user: string;
  keyPath: string;
  connectTimeoutSeconds: number;
  strictHostKeyChecking: string;
  knownHostsPath: string;
}

export interface ExecMcpConfig {
  host: string;
  port: number;
  allowedCwds: string[];
  defaultCwd: string;
  defaultTimeoutSeconds: number;
  maxTimeoutSeconds: number;
  defaultMaxOutputBytes: number;
  hardMaxOutputBytes: number;
  mcpMaxRequestBytes: number;
  artifactMaxBytes: number;
  artifactEmbedMaxBytes: number;
  artifactMaxConcurrentTransfers: number;
  artifactSpoolDir: string;
  artifactEmbedUriBase: string;
  artifactTransferTimeoutSeconds: number;
  artifactImportAllowHttp: boolean;
  artifactImportAllowedHosts: string[];
  ringBufferBytes: number;
  maxConcurrentExecs: number;
  syncMaxConcurrentExecs: number;
  asyncMaxConcurrentExecs: number;
  globalMaxConcurrentExecs: number;
  maxQueuedExecs: number;
  jobLogBytes: number;
  jobRetentionSeconds: number;
  statusDefaultMaxOutputBytes: number;
  statusHardMaxOutputBytes: number;
  statusMaxWaitSeconds: number;
  recentHistoryLimit: number;
  registryReapGraceSeconds: number;
  emergencyReapSeconds: number;
  exposeRedactedCommandPreview: boolean;
  commandPreviewMaxChars: number;
  lifecycleLogs: boolean;
  heartbeatSeconds: number;
  killGraceSeconds: number;
  remote: RemoteConfig;
}

export function parseConfig(env: NodeJS.ProcessEnv = process.env): ExecMcpConfig {
  const allowedCwds = (env.ALLOWED_CWDS || '/workspace,/tmp')
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);

  const legacyMaxConcurrentExecs = positiveInt(env.MAX_CONCURRENT_EXECS, 2);

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
    artifactMaxBytes: positiveInt(env.ARTIFACT_MAX_BYTES, 256 * 1024 * 1024),
    artifactEmbedMaxBytes: Math.min(positiveInt(env.ARTIFACT_EMBED_MAX_BYTES, ARTIFACT_EMBED_HARD_MAX_BYTES), ARTIFACT_EMBED_HARD_MAX_BYTES),
    artifactMaxConcurrentTransfers: positiveInt(env.ARTIFACT_MAX_CONCURRENT_TRANSFERS, 2),
    artifactSpoolDir: env.ARTIFACT_SPOOL_DIR || '/tmp/exec-mcp-artifacts',
    artifactEmbedUriBase: String(env.ARTIFACT_EMBED_URI_BASE || 'https://exec-mcp.invalid/embedded').replace(/\/+$/, ''),
    artifactTransferTimeoutSeconds: positiveInt(env.ARTIFACT_TRANSFER_TIMEOUT_SECONDS, 600),
    artifactImportAllowHttp: String(env.ARTIFACT_IMPORT_ALLOW_HTTP || 'false').toLowerCase() === 'true',
    artifactImportAllowedHosts: splitCsv(env.ARTIFACT_IMPORT_ALLOWED_HOSTS || ''),
    ringBufferBytes: positiveInt(env.RING_BUFFER_BYTES, 65536),
    maxConcurrentExecs: legacyMaxConcurrentExecs,
    syncMaxConcurrentExecs: positiveInt(env.SYNC_MAX_CONCURRENT_EXECS, legacyMaxConcurrentExecs),
    asyncMaxConcurrentExecs: positiveInt(env.ASYNC_MAX_CONCURRENT_EXECS, legacyMaxConcurrentExecs),
    globalMaxConcurrentExecs: positiveInt(env.GLOBAL_MAX_CONCURRENT_EXECS, legacyMaxConcurrentExecs),
    maxQueuedExecs: positiveInt(env.MAX_QUEUED_EXECS, 20),
    jobLogBytes: positiveInt(env.JOB_LOG_BYTES, 1024 * 1024),
    jobRetentionSeconds: positiveInt(env.JOB_RETENTION_SECONDS, 3600),
    statusDefaultMaxOutputBytes: positiveInt(env.STATUS_DEFAULT_MAX_OUTPUT_BYTES, 32768),
    statusHardMaxOutputBytes: positiveInt(env.STATUS_HARD_MAX_OUTPUT_BYTES, 262144),
    statusMaxWaitSeconds: positiveInt(env.STATUS_MAX_WAIT_SECONDS, 30),
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
      user: env.REMOTE_USER || 'execmcp',
      keyPath: env.REMOTE_KEY_PATH || '',
      connectTimeoutSeconds: positiveInt(env.REMOTE_CONNECT_TIMEOUT_SECONDS, 10),
      strictHostKeyChecking: env.REMOTE_STRICT_HOST_KEY_CHECKING || 'yes',
      knownHostsPath: env.REMOTE_KNOWN_HOSTS_PATH || '/run/secrets/known_hosts'
    }
  };
}

function positiveInt(value: string | undefined, fallback: number): number {
  const parsed = Number.parseInt(String(value), 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function splitArgs(value: string): string[] {
  return value.split(/\s+/).filter(Boolean);
}

function splitCsv(value: string): string[] {
  return value.split(',').map((item) => item.trim().toLowerCase()).filter(Boolean);
}
