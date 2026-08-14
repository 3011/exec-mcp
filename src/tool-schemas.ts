const executionStateValues = [
  'queued',
  'starting',
  'running',
  'timeout_aborting',
  'cancel_aborting',
  'client_closed_aborting',
  'killing'
];

const finalExecutionStateValues = [
  'completed',
  'failed',
  'cancelled',
  'timed_out',
  'client_closed',
  'spawn_failed',
  'unconfirmed_reaped'
];

const publicJobStatusValues = ['queued', 'running', 'completed', 'failed', 'cancelled', 'timed_out'];
const executionClassValues = ['sync', 'async'];

const abortReasonValues = [
  'request_timeout',
  'manual_cancel',
  'mcp_notification_cancel',
  'http_disconnect',
  'reaper_grace_exceeded',
  'executor_shutdown'
];

function executionInputProperties() {
  return {
    command: { type: 'string', minLength: 1, description: 'Non-interactive shell command evaluated by /bin/sh -c. Use explicit quoting for pipelines, redirection, &&, and environment expansion. Do not use this tool for an interactive shell, an intentionally unbounded service, or file transfer.' },
    cwd: { type: 'string', description: 'Absolute remote working directory. Its resolved real path must remain inside the ALLOWED_CWDS allowlist. If omitted, DEFAULT_CWD is used.' },
    timeout_seconds: { type: 'integer', minimum: 1, description: 'Maximum runtime after the job starts running. Values above MAX_TIMEOUT_SECONDS are rejected. On expiry, the server sends SIGTERM and then SIGKILL after KILL_GRACE_SECONDS.' },
    max_output_bytes: { type: 'integer', minimum: 1, description: 'Maximum combined stdout/stderr bytes forwarded by synchronous exec and retained in its final tail. Output beyond this limit is drained but omitted; Job status output uses its own bounded query limit.' },
    env: { type: 'object', additionalProperties: { type: 'string' }, description: 'Additional environment variables. Invalid names are ignored, ENV plus BASH_ENV are removed, and env values are never exposed through job metadata, history, list, or lifecycle logs.' },
    label: { type: 'string', maxLength: 120, description: 'Optional sanitized operator label for status and lifecycle logs. Do not include credentials or secrets.' }
  };
}

function execToolSchema() {
  return {
    name: 'exec',
    title: 'Run short remote command synchronously',
    description: 'Run one bounded, non-interactive shell command synchronously and wait for its terminal result. Agent selection guidance: prefer exec only when the command is expected to finish quickly (roughly within 5 seconds) AND the next reasoning or action depends on its result. Typical exec cases are pwd, ls, cat/sed/grep, git status/diff, kubectl get, and other small deterministic probes or edits. For builds, test suites, package installs, image builds, scans, sleeps/waits, commands with uncertain duration, or work that can run in parallel, prefer start_exec so useful agent work can continue while the job runs. Never emulate background execution inside exec with nohup, disown, or a trailing/background ampersand; submit the foreground command through start_exec so Job Manager status, cancellation, timeout, and remote process-group cleanup remain authoritative. Do not use exec merely to wait for an asynchronous job; use get_exec_status at the synchronization point. Internally exec submits a sync job through the same Job Manager and runner used by start_exec, waits for a terminal state, then returns the final summary. Commands are evaluated by /bin/sh -c. The server validates cwd against ALLOWED_CWDS, enforces runtime and output limits, filters environment variables, enforces dedicated sync/global concurrency limits, and terminates the managed remote process group on timeout or cancellation.',
    inputSchema: { type: 'object', properties: executionInputProperties(), required: ['command'], additionalProperties: false },
    outputSchema: {
      type: 'object',
      properties: {
        exec_id: { type: 'string', description: 'Unique execution identifier used by the execution-control tools.' },
        type: { type: 'string', enum: ['exit'], description: 'Final event type.' },
        code: { type: ['integer', 'null'], description: 'Remote process exit code, or null when no exit code was reported.' },
        signal: { type: ['string', 'null'], description: 'Termination signal, or null.' },
        duration_ms: { type: 'integer', minimum: 0, description: 'Observed duration from job acceptance through terminal finalization.' },
        stdout_bytes: { type: 'integer', minimum: 0, description: 'Total stdout bytes observed before redaction.' },
        stderr_bytes: { type: 'integer', minimum: 0, description: 'Total stderr bytes observed before redaction.' },
        truncated: { type: 'boolean', description: 'True when output exceeded max_output_bytes for the synchronous response.' },
        timed_out: { type: 'boolean', description: 'True when timeout_seconds was exceeded after execution started.' },
        stdout_tail: { type: 'string', description: 'Bounded, redacted stdout tail.' },
        stderr_tail: { type: 'string', description: 'Bounded, redacted stderr tail.' }
      },
      required: ['exec_id', 'type', 'code', 'signal', 'duration_ms', 'stdout_bytes', 'stderr_bytes', 'truncated', 'timed_out', 'stdout_tail', 'stderr_tail'],
      additionalProperties: false
    },
    annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: false, openWorldHint: true },
    _meta: {
      'openai/toolInvocation/invoking': 'Running remote command',
      'openai/toolInvocation/invoked': 'Remote command finished'
    }
  };
}

function startExecToolSchema() {
  return {
    name: 'start_exec',
    title: 'Start asynchronous remote job',
    description: 'Submit one bounded remote command as an asynchronous Job Manager job and return as soon as the job is registered. Agent selection guidance: prefer start_exec whenever runtime is unknown, may exceed a few seconds, or the command can run in parallel with other useful work. Typical start_exec cases are builds, test suites, package installs, container/image builds, scans, migrations, long scripts, sleeps/waits, and multiple independent jobs. Pass the real foreground command to start_exec; do not add nohup, disown, or shell backgrounding, because Job Manager itself owns the background lifecycle. When starting several independent jobs, use concise labels so their results are easy to reconcile. Do not use start_exec for trivial probes whose result is required immediately; exec is simpler for those. After start_exec returns, keep the exec_id and continue independent reasoning or tool work instead of immediately high-frequency polling. At a synchronization point, call get_exec_status with the returned cursors and optionally wait_seconds up to 30 seconds; use cancel_exec if the job is no longer needed. Returning exec_id means the job is registered and immediately queryable; it does not mean the command succeeded or has started running. Jobs may be queued before entering the async execution pool. Finalized records remain queryable only while bounded Job Manager/history retention keeps them, and in-memory state is lost on service restart.',
    inputSchema: { type: 'object', properties: executionInputProperties(), required: ['command'], additionalProperties: false },
    outputSchema: {
      type: 'object',
      properties: {
        exec_id: { type: 'string', description: 'Registered execution identifier.' },
        status: { type: 'string', enum: publicJobStatusValues, description: 'Observed job status at response time.' },
        label: { type: ['string', 'null'], description: 'Optional sanitized operator label.' },
        created_at: { type: 'string', description: 'UTC timestamp when the Job Manager registered the job.' },
        queue_position: { type: ['integer', 'null'], minimum: 1, description: 'Current 1-based queue position, or null when not queued.' }
      },
      required: ['exec_id', 'status', 'label', 'created_at', 'queue_position'],
      additionalProperties: false
    },
    annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: false, openWorldHint: true },
    _meta: {
      'openai/toolInvocation/invoking': 'Starting background remote job',
      'openai/toolInvocation/invoked': 'Background remote job accepted'
    }
  };
}

function activeExecutionSchema() {
  return {
    type: 'object',
    properties: {
      exec_id: { type: 'string', description: 'Unique execution identifier.' },
      status: { type: 'string', enum: ['queued', 'running'], description: 'Stable public job status.' },
      state: { type: 'string', enum: executionStateValues, description: 'Detailed current execution lifecycle state.' },
      execution_class: { type: 'string', enum: executionClassValues, description: 'Admission class used for concurrency accounting.' },
      label: { type: ['string', 'null'], description: 'Optional sanitized operator label.' },
      command_preview: { type: ['string', 'null'], description: 'Optional redacted command preview when explicitly enabled; otherwise null.' },
      command_sha256: { type: ['string', 'null'], description: 'SHA-256 fingerprint of the submitted command, or null when unavailable.' },
      command_length: { type: 'integer', minimum: 0, description: 'Submitted command length in UTF-8 bytes.' },
      cwd: { type: ['string', 'null'], description: 'Validated remote working directory.' },
      timeout_seconds: { type: 'integer', minimum: 1, description: 'Configured runtime timeout.' },
      elapsed_seconds: { type: 'integer', minimum: 0, description: 'Whole seconds elapsed since Job Manager registration.' },
      created_at: { type: 'string', description: 'UTC timestamp when the Job Manager registered the job.' },
      transport_started_at: { type: ['string', 'null'], description: 'UTC timestamp when the local SSH transport started, or null.' },
      running_at: { type: ['string', 'null'], description: 'UTC timestamp when the execution entered running state, or null.' },
      transport_pid: { type: ['integer', 'null'], description: 'Local SSH transport process ID, or null.' },
      remote_pid: { type: ['integer', 'null'], description: 'Observed remote shell process ID, or null.' },
      remote_pgid: { type: ['integer', 'null'], description: 'Observed remote process-group ID, or null.' },
      abort_reason: { enum: [...abortReasonValues, null], description: 'First accepted abort reason, or null.' },
      transport_exit_confirmed: { type: 'boolean', description: 'Whether local SSH transport exit has been confirmed.' },
      remote_exit_confirmed: { type: ['boolean', 'null'], description: 'Whether remote process exit has been confirmed, or null when unknown.' },
      queue_position: { type: ['integer', 'null'], minimum: 1, description: 'Current 1-based queue position, or null when not queued.' }
    },
    required: ['exec_id', 'status', 'state', 'execution_class', 'label', 'command_preview', 'command_sha256', 'command_length', 'cwd', 'timeout_seconds', 'elapsed_seconds', 'created_at', 'transport_started_at', 'running_at', 'transport_pid', 'remote_pid', 'remote_pgid', 'abort_reason', 'transport_exit_confirmed', 'remote_exit_confirmed', 'queue_position'],
    additionalProperties: false
  };
}

function executionHistorySchema() {
  return {
    type: 'object',
    properties: {
      exec_id: { type: 'string', description: 'Unique execution identifier.' },
      status: { type: 'string', enum: ['completed', 'failed', 'cancelled', 'timed_out'], description: 'Stable terminal job status.' },
      execution_class: { type: 'string', enum: executionClassValues, description: 'Admission class used by this job.' },
      label: { type: ['string', 'null'], description: 'Optional sanitized operator label.' },
      command_sha256: { type: ['string', 'null'], description: 'SHA-256 fingerprint of the submitted command, or null.' },
      command_length: { type: 'integer', minimum: 0, description: 'Submitted command length in UTF-8 bytes.' },
      final_state: { type: 'string', enum: finalExecutionStateValues, description: 'Detailed final execution lifecycle state.' },
      abort_reason: { enum: [...abortReasonValues, null], description: 'First accepted abort reason, or null.' },
      abort_source: { type: ['string', 'null'], description: 'Subsystem that requested the abort, or null.' },
      created_at: { type: 'string', description: 'UTC timestamp when the Job Manager registered the job.' },
      started_at: { type: ['string', 'null'], description: 'UTC timestamp when execution entered running state, or null if it never started.' },
      running_at: { type: ['string', 'null'], description: 'UTC timestamp when execution entered running state, or null if it never started.' },
      finished_at: { type: 'string', description: 'UTC timestamp when runner finalization completed.' },
      duration_ms: { type: 'integer', minimum: 0, description: 'Observed duration from acceptance through finalization.' },
      exit_code: { type: ['integer', 'null'], description: 'Observed process exit code, or null.' },
      signal: { type: ['string', 'null'], description: 'Observed termination signal, or null.' },
      timed_out: { type: 'boolean', description: 'True when the final state is timed_out.' },
      transport_exit_confirmed: { type: 'boolean', description: 'Whether local SSH transport exit was confirmed at finalization.' },
      remote_exit_confirmed: { type: ['boolean', 'null'], description: 'Whether remote process exit was confirmed, or null when unknown.' },
      failure_reason: { type: 'string', description: 'Optional machine-readable reason for infrastructure-level failure.' },
      diagnostic: { type: 'string', description: 'Optional retained lifecycle diagnostic.' },
      late_exit_observed_at: { type: 'string', description: 'Optional UTC timestamp for a transport exit observed after forced finalization.' }
    },
    required: ['exec_id', 'status', 'execution_class', 'label', 'command_sha256', 'command_length', 'final_state', 'abort_reason', 'abort_source', 'created_at', 'started_at', 'running_at', 'finished_at', 'duration_ms', 'exit_code', 'signal', 'timed_out', 'transport_exit_confirmed', 'remote_exit_confirmed'],
    additionalProperties: false
  };
}

function listActiveExecsToolSchema() {
  return {
    name: 'list_active_execs',
    title: 'List active remote executions',
    description: 'List every non-terminal Job Manager execution, including queued and running jobs, without consuming an execution slot. The capacity fields expose the sync, async, and global admission pools. Control-plane calls remain available when execution pools are full.',
    inputSchema: { type: 'object', properties: {}, additionalProperties: false },
    outputSchema: {
      type: 'object',
      properties: {
        active: { type: 'integer', minimum: 0, description: 'Executions currently consuming a running slot.' },
        queued: { type: 'integer', minimum: 0, description: 'Executions waiting for admission.' },
        total_active: { type: 'integer', minimum: 0, description: 'All non-terminal jobs: running plus queued/aborting jobs.' },
        max_concurrent: { type: 'integer', minimum: 1, description: 'Compatibility alias for global_max_concurrent.' },
        sync_running: { type: 'integer', minimum: 0 },
        async_running: { type: 'integer', minimum: 0 },
        sync_max_concurrent: { type: 'integer', minimum: 1 },
        async_max_concurrent: { type: 'integer', minimum: 1 },
        global_max_concurrent: { type: 'integer', minimum: 1 },
        max_queue: { type: 'integer', minimum: 1 },
        circuit_open: { type: 'boolean', description: 'True when new jobs are blocked pending transport-lifecycle investigation.' },
        tasks: { type: 'array', items: activeExecutionSchema(), description: 'Queued and running jobs ordered from oldest to newest.' }
      },
      required: ['active', 'queued', 'total_active', 'max_concurrent', 'sync_running', 'async_running', 'sync_max_concurrent', 'async_max_concurrent', 'global_max_concurrent', 'max_queue', 'circuit_open', 'tasks'],
      additionalProperties: false
    },
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    _meta: {
      'openai/toolInvocation/invoking': 'Reading active remote executions',
      'openai/toolInvocation/invoked': 'Active remote executions read'
    }
  };
}

function statusOutputProperties() {
  return {
    logs_available: { type: 'boolean', description: 'Whether incremental logs are still retained by this Job Manager instance.' },
    stdout: { type: 'string', description: 'Incremental redacted stdout from stdout_cursor, bounded by max_output_bytes.' },
    stderr: { type: 'string', description: 'Incremental redacted stderr from stderr_cursor, bounded by max_output_bytes.' },
    stdout_cursor: { type: 'integer', minimum: 0, description: 'Absolute redacted stdout byte cursor for the next query.' },
    stderr_cursor: { type: 'integer', minimum: 0, description: 'Absolute redacted stderr byte cursor for the next query.' },
    has_more_stdout: { type: 'boolean', description: 'True when more currently retained stdout can be read after stdout_cursor.' },
    has_more_stderr: { type: 'boolean', description: 'True when more currently retained stderr can be read after stderr_cursor.' },
    stdout_log_truncated: { type: 'boolean', description: 'True when the bounded stdout log has permanently discarded some earlier bytes.' },
    stderr_log_truncated: { type: 'boolean', description: 'True when the bounded stderr log has permanently discarded some earlier bytes.' }
  };
}

function getExecStatusToolSchema() {
  const outputProperties = statusOutputProperties();
  const outputRequired = Object.keys(outputProperties);
  return {
    name: 'get_exec_status',
    title: 'Get remote execution status',
    description: 'Read one Job Manager execution by exec_id and optionally long-poll for a terminal state. This is the primary synchronization/follow-up tool for start_exec: prefer calling it when independent work is finished or a job result is needed, rather than immediately busy-polling after submission. Incremental stdout/stderr use independent absolute cursors so repeated calls do not resend complete logs. wait_seconds is bounded by the server and never turns this tool into an unbounded wait.',
    inputSchema: {
      type: 'object',
      properties: {
        exec_id: { type: 'string', minLength: 1, description: 'Execution identifier returned by exec, start_exec, or an execution-control tool.' },
        stdout_cursor: { type: 'integer', minimum: 0, description: 'Previously returned stdout_cursor. Omit or use 0 for the first read.' },
        stderr_cursor: { type: 'integer', minimum: 0, description: 'Previously returned stderr_cursor. Omit or use 0 for the first read.' },
        max_output_bytes: { type: 'integer', minimum: 1, description: 'Maximum combined incremental stdout/stderr bytes returned by this query. The server enforces a hard ceiling.' },
        wait_seconds: { type: 'integer', minimum: 0, maximum: 30, description: 'Long-poll for terminal completion for up to this many seconds. 0 returns immediately; server hard maximum is 30 seconds.' }
      },
      required: ['exec_id'],
      additionalProperties: false
    },
    outputSchema: {
      type: 'object',
      properties: {
        found: { type: 'boolean', description: 'Whether the execution was found.' },
        source: { type: 'string', enum: ['active', 'recent', 'unconfirmed'], description: 'Registry that supplied task when found=true.' },
        task: { oneOf: [activeExecutionSchema(), executionHistorySchema()], description: 'Current or finalized execution metadata.' },
        result: { type: 'string', enum: ['exec_not_found'], description: 'Machine-readable missing-result code when found=false.' },
        exec_id: { type: 'string', description: 'Requested execution identifier when found=false.' },
        ...outputProperties
      },
      required: ['found'],
      oneOf: [
        { properties: { found: { enum: [true] }, source: { enum: ['active'] }, task: activeExecutionSchema() }, required: ['found', 'source', 'task', ...outputRequired] },
        { properties: { found: { enum: [true] }, source: { enum: ['recent', 'unconfirmed'] }, task: executionHistorySchema() }, required: ['found', 'source', 'task', ...outputRequired] },
        { properties: { found: { enum: [false] }, result: { enum: ['exec_not_found'] }, exec_id: { type: 'string' } }, required: ['found', 'result', 'exec_id'] }
      ],
      additionalProperties: false
    },
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    _meta: {
      'openai/toolInvocation/invoking': 'Reading remote execution status',
      'openai/toolInvocation/invoked': 'Remote execution status read'
    }
  };
}

function cancelExecToolSchema() {
  return {
    name: 'cancel_exec',
    title: 'Cancel remote execution',
    description: 'Idempotently request cancellation of one queued or running execution. Queued jobs are terminalized without consuming a slot. Running jobs receive process-group termination. Terminal states are immutable: cancelling a finished job reports already_finished and never rewrites its final state.',
    inputSchema: { type: 'object', properties: { exec_id: { type: 'string', minLength: 1, description: 'Execution identifier to cancel.' } }, required: ['exec_id'], additionalProperties: false },
    outputSchema: {
      type: 'object',
      properties: {
        exec_id: { type: 'string', description: 'Requested execution identifier.' },
        result: { type: 'string', enum: ['accepted', 'idempotent', 'conflicting_abort_reason', 'already_finished', 'exec_not_found'], description: 'Machine-readable cancellation outcome.' },
        accepted: { type: 'boolean', description: 'Whether this cancellation request was accepted. This does not confirm process exit for a running job.' },
        idempotent: { type: 'boolean', description: 'True when the same cancellation had already been accepted.' },
        state: { type: 'string', enum: executionStateValues, description: 'Current detailed state after processing the request.' },
        abort_reason: { type: 'string', enum: abortReasonValues, description: 'Existing abort reason when a conflicting cancellation is rejected.' },
        final_state: { type: 'string', enum: finalExecutionStateValues, description: 'Final state when the execution had already finished.' }
      },
      required: ['exec_id', 'result', 'accepted'],
      additionalProperties: false
    },
    annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: true, openWorldHint: false },
    _meta: {
      'openai/toolInvocation/invoking': 'Cancelling remote execution',
      'openai/toolInvocation/invoked': 'Remote cancellation request processed'
    }
  };
}

function openAIFileSchema() {
  return {
    type: 'object',
    description: 'ChatGPT-authorized temporary reference for one conversation or generated file.',
    properties: {
      download_url: { type: 'string', description: 'Temporary HTTPS download URL supplied and authorized by ChatGPT.' },
      file_id: { type: 'string', description: 'Real ChatGPT file identifier supplied by the host.' },
      mime_type: { type: 'string', description: 'Optional MIME type supplied by ChatGPT.' },
      file_name: { type: 'string', description: 'Optional original file name supplied by ChatGPT.' }
    },
    required: ['download_url', 'file_id'],
    additionalProperties: false
  };
}

function importChatgptFileToolSchema() {
  return {
    name: 'import_chatgpt_file',
    title: 'Import ChatGPT file to remote',
    description: 'Transfer one file from the current ChatGPT session into target_path in the configured remote test environment. Use this tool for ChatGPT-to-remote file transfer instead of exec. ChatGPT supplies an authorized temporary file reference through openai/fileParams. The server downloads into a bounded local spool, computes SHA-256, streams raw bytes through SSH, writes a same-directory temporary file, fsyncs it, verifies the remote size and hash, and atomically commits target_path. An identical existing destination is treated as an idempotent success; different content requires overwrite=true.',
    inputSchema: {
      type: 'object',
      $defs: { OpenAIFile: openAIFileSchema() },
      properties: {
        file: { $ref: '#/$defs/OpenAIFile', description: 'File attached to or generated in the current ChatGPT session. The host replaces this value with an authorized temporary file reference.' },
        target_path: { type: 'string', minLength: 1, description: 'Destination remote file path. Relative paths resolve from DEFAULT_CWD; the resolved parent must remain inside ALLOWED_CWDS. Parent directories must already exist.' },
        expected_sha256: { type: 'string', pattern: '^[a-fA-F0-9]{64}$', description: 'Optional expected SHA-256 for an additional source-to-remote integrity check.' },
        overwrite: { type: 'boolean', default: false, description: 'Atomically replace an existing regular file only when true. Symlink targets are rejected.' }
      },
      required: ['file', 'target_path'],
      additionalProperties: false
    },
    outputSchema: {
      type: 'object',
      properties: {
        path: { type: 'string', description: 'Resolved remote destination path.' },
        bytes: { type: 'integer', minimum: 0, description: 'Verified file size.' },
        sha256: { type: 'string', pattern: '^[a-f0-9]{64}$', description: 'Verified SHA-256 of the committed remote file.' },
        mime_type: { type: 'string', description: 'Source MIME type or best-effort inferred type.' },
        source_file_id: { type: 'string', description: 'Real ChatGPT source file identifier.' },
        source_file_name: { type: 'string', description: 'Source file name supplied by ChatGPT or derived from target_path.' },
        verified: { type: 'boolean', enum: [true], description: 'Always true on success.' }
      },
      required: ['path', 'bytes', 'sha256', 'mime_type', 'source_file_id', 'source_file_name', 'verified'],
      additionalProperties: false
    },
    annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: false, openWorldHint: true },
    _meta: {
      'openai/fileParams': ['file'],
      'openai/toolInvocation/invoking': 'Importing ChatGPT file to remote',
      'openai/toolInvocation/invoked': 'ChatGPT file imported and verified'
    }
  };
}

function exportRemoteFileToolSchema() {
  return {
    name: 'export_remote_file',
    title: 'Export remote file to ChatGPT',
    description: 'Transfer one allowed regular file from the configured remote test environment into the current ChatGPT session as one embedded MCP binary resource. The server streams raw bytes over SSH into a bounded local spool, verifies size and SHA-256, and returns the complete file for host-side materialization into /mnt/data. Remote exports have a hard ceiling of 1.45 MB (1,450,000 bytes); ARTIFACT_EMBED_MAX_BYTES may lower but cannot raise that ceiling. Larger files are rejected with file_too_large, and there is no resource-link or external-URL fallback. The embedded blob is Base64 only at the MCP protocol layer; the model does not need to copy or decode it.',
    inputSchema: {
      type: 'object',
      properties: {
        path: { type: 'string', minLength: 1, description: 'Remote file path. Relative paths resolve from DEFAULT_CWD; the resolved regular file must remain inside ALLOWED_CWDS.' },
        file_name: { type: 'string', minLength: 1, description: 'Optional safe output file name used when ChatGPT materializes the embedded resource. Defaults to the remote basename.' },
        max_bytes: { type: 'integer', minimum: 1, description: 'Optional per-call file-size ceiling. It cannot exceed the server limit, whose hard maximum is 1.45 MB (1,450,000 bytes). Oversized files are rejected, never truncated.' }
      },
      required: ['path'],
      additionalProperties: false
    },
    outputSchema: {
      type: 'object',
      properties: {
        path: { type: 'string', description: 'Resolved remote source path.' },
        bytes: { type: 'integer', minimum: 0, description: 'Verified source size.' },
        sha256: { type: 'string', pattern: '^[a-f0-9]{64}$', description: 'Verified SHA-256 of the embedded bytes.' },
        mime_type: { type: 'string', description: 'Best-effort MIME type derived from file_name.' },
        file_name: { type: 'string', description: 'Safe file name used for host-side materialization.' },
        embedded: { type: 'boolean', enum: [true], description: 'Always true on success.' },
        delivery_mode: { type: 'string', enum: ['embedded_resource'], description: 'Always embedded_resource on success.' }
      },
      required: ['path', 'bytes', 'sha256', 'mime_type', 'file_name', 'embedded', 'delivery_mode'],
      additionalProperties: false
    },
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: false, openWorldHint: true },
    _meta: {
      'openai/toolInvocation/invoking': 'Exporting remote file to ChatGPT',
      'openai/toolInvocation/invoked': 'Remote file embedded and verified'
    }
  };
}

export const TOOL_SCHEMAS = [
  execToolSchema(),
  startExecToolSchema(),
  listActiveExecsToolSchema(),
  getExecStatusToolSchema(),
  cancelExecToolSchema(),
  importChatgptFileToolSchema(),
  exportRemoteFileToolSchema()
];
