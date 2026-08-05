const executionStateValues = [
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

const abortReasonValues = [
  'request_timeout',
  'manual_cancel',
  'mcp_notification_cancel',
  'http_disconnect',
  'reaper_grace_exceeded'
];

function execToolSchema() {
  return {
    name: 'exec',
    title: 'Run remote command',
    description: 'Run one bounded, non-interactive shell command in the configured remote test environment. Use this tool for remote command execution, not for transferring files. Commands are evaluated by /bin/sh -c, so quote pipelines, redirections, &&, and variable expansion explicitly. The server validates cwd against ALLOWED_CWDS, enforces runtime and output limits, filters environment variables, limits concurrency, and terminates the SSH process group on timeout or cancellation. Output may be truncated while the process is still fully drained. The final structured result is authoritative: a non-zero code, a signal, or timed_out=true means failure; stderr by itself does not. Use list_active_execs, get_exec_status, and cancel_exec for execution control.',
    inputSchema: {
      type: 'object',
      properties: {
        command: { type: 'string', minLength: 1, description: 'Non-interactive shell command evaluated by /bin/sh -c. Use explicit quoting for pipelines, redirection, &&, and environment expansion. Do not use this tool for an interactive shell, an intentionally unbounded service, or file transfer.' },
        cwd: { type: 'string', description: 'Absolute remote working directory. Its resolved real path must remain inside the ALLOWED_CWDS allowlist. If omitted, DEFAULT_CWD is used.' },
        timeout_seconds: { type: 'integer', minimum: 1, description: 'Maximum runtime. Values above MAX_TIMEOUT_SECONDS are rejected. On expiry, the server sends SIGTERM and then SIGKILL after KILL_GRACE_SECONDS.' },
        max_output_bytes: { type: 'integer', minimum: 1, description: 'Maximum combined stdout/stderr bytes forwarded to the result. Output beyond this limit is drained but omitted; retained stdout_tail plus stderr_tail remains bounded.' },
        env: { type: 'object', additionalProperties: { type: 'string' }, description: 'Additional environment variables. Invalid names are ignored, and ENV plus BASH_ENV are always removed.' },
        label: { type: 'string', maxLength: 120, description: 'Optional sanitized operator label for status and lifecycle logs. Do not include credentials or secrets.' }
      },
      required: ['command'],
      additionalProperties: false
    },
    outputSchema: {
      type: 'object',
      properties: {
        exec_id: { type: 'string', description: 'Unique execution identifier used by the execution-control tools.' },
        type: { type: 'string', enum: ['exit'], description: 'Final event type.' },
        code: { type: ['integer', 'null'], description: 'Remote process exit code, or null when no exit code was reported.' },
        signal: { type: ['string', 'null'], description: 'Termination signal, or null.' },
        duration_ms: { type: 'integer', minimum: 0, description: 'Observed runtime in milliseconds.' },
        stdout_bytes: { type: 'integer', minimum: 0, description: 'Total stdout bytes observed before redaction.' },
        stderr_bytes: { type: 'integer', minimum: 0, description: 'Total stderr bytes observed before redaction.' },
        truncated: { type: 'boolean', description: 'True when output exceeded max_output_bytes.' },
        timed_out: { type: 'boolean', description: 'True when timeout_seconds was exceeded.' },
        stdout_tail: { type: 'string', description: 'Bounded, redacted stdout tail.' },
        stderr_tail: { type: 'string', description: 'Bounded, redacted stderr tail.' }
      },
      required: ['exec_id', 'type', 'code', 'signal', 'duration_ms', 'stdout_bytes', 'stderr_bytes', 'truncated', 'timed_out', 'stdout_tail', 'stderr_tail'],
      additionalProperties: false
    },
    annotations: {
      readOnlyHint: false,
      destructiveHint: true,
      idempotentHint: false,
      openWorldHint: true
    },
    _meta: {
      'openai/toolInvocation/invoking': 'Running remote command',
      'openai/toolInvocation/invoked': 'Remote command finished'
    }
  };
}

function activeExecutionSchema() {
  return {
    type: 'object',
    properties: {
      exec_id: { type: 'string', description: 'Unique execution identifier.' },
      state: { type: 'string', enum: executionStateValues, description: 'Current execution lifecycle state.' },
      label: { type: ['string', 'null'], description: 'Optional sanitized operator label.' },
      command_preview: { type: ['string', 'null'], description: 'Optional redacted command preview when explicitly enabled; otherwise null.' },
      command_sha256: { type: ['string', 'null'], description: 'SHA-256 fingerprint of the submitted command, or null when unavailable.' },
      command_length: { type: 'integer', minimum: 0, description: 'Submitted command length in UTF-8 bytes.' },
      cwd: { type: ['string', 'null'], description: 'Validated remote working directory.' },
      timeout_seconds: { type: 'integer', minimum: 1, description: 'Configured timeout for this execution.' },
      elapsed_seconds: { type: 'integer', minimum: 0, description: 'Whole seconds elapsed since the execution was accepted.' },
      created_at: { type: 'string', description: 'UTC timestamp when the execution was accepted.' },
      transport_started_at: { type: ['string', 'null'], description: 'UTC timestamp when the local SSH transport started, or null.' },
      running_at: { type: ['string', 'null'], description: 'UTC timestamp when the execution entered running state, or null.' },
      transport_pid: { type: ['integer', 'null'], description: 'Local SSH transport process ID, or null.' },
      remote_pid: { type: ['integer', 'null'], description: 'Observed remote shell process ID, or null.' },
      remote_pgid: { type: ['integer', 'null'], description: 'Observed remote process-group ID, or null.' },
      abort_reason: { enum: [...abortReasonValues, null], description: 'First accepted abort reason, or null.' },
      transport_exit_confirmed: { type: 'boolean', description: 'Whether local SSH transport exit has been confirmed.' },
      remote_exit_confirmed: { type: ['boolean', 'null'], description: 'Whether remote process exit has been confirmed, or null when unknown.' }
    },
    required: ['exec_id', 'state', 'label', 'command_preview', 'command_sha256', 'command_length', 'cwd', 'timeout_seconds', 'elapsed_seconds', 'created_at', 'transport_started_at', 'running_at', 'transport_pid', 'remote_pid', 'remote_pgid', 'abort_reason', 'transport_exit_confirmed', 'remote_exit_confirmed'],
    additionalProperties: false
  };
}

function executionHistorySchema() {
  return {
    type: 'object',
    properties: {
      exec_id: { type: 'string', description: 'Unique execution identifier.' },
      label: { type: ['string', 'null'], description: 'Optional sanitized operator label.' },
      command_sha256: { type: ['string', 'null'], description: 'SHA-256 fingerprint of the submitted command, or null.' },
      command_length: { type: 'integer', minimum: 0, description: 'Submitted command length in UTF-8 bytes.' },
      final_state: { type: 'string', enum: finalExecutionStateValues, description: 'Final execution lifecycle state.' },
      abort_reason: { enum: [...abortReasonValues, null], description: 'First accepted abort reason, or null.' },
      abort_source: { type: ['string', 'null'], description: 'Subsystem that requested the abort, or null.' },
      started_at: { type: 'string', description: 'UTC timestamp when execution was accepted.' },
      finished_at: { type: 'string', description: 'UTC timestamp when runner finalization completed.' },
      duration_ms: { type: 'integer', minimum: 0, description: 'Observed execution duration in milliseconds.' },
      exit_code: { type: ['integer', 'null'], description: 'Observed process exit code, or null.' },
      signal: { type: ['string', 'null'], description: 'Observed termination signal, or null.' },
      timed_out: { type: 'boolean', description: 'True when the final state is timed_out.' },
      transport_exit_confirmed: { type: 'boolean', description: 'Whether local SSH transport exit was confirmed at finalization.' },
      remote_exit_confirmed: { type: ['boolean', 'null'], description: 'Whether remote process exit was confirmed, or null when unknown.' },
      diagnostic: { type: 'string', description: 'Optional retained lifecycle diagnostic.' },
      late_exit_observed_at: { type: 'string', description: 'Optional UTC timestamp for a transport exit observed after forced finalization.' }
    },
    required: ['exec_id', 'label', 'command_sha256', 'command_length', 'final_state', 'abort_reason', 'abort_source', 'started_at', 'finished_at', 'duration_ms', 'exit_code', 'signal', 'timed_out', 'transport_exit_confirmed', 'remote_exit_confirmed'],
    additionalProperties: false
  };
}

function listActiveExecsToolSchema() {
  return {
    name: 'list_active_execs',
    title: 'List active remote executions',
    description: 'List every currently active remote execution without consuming an execution slot. Use this tool to inspect running or aborting commands before calling get_exec_status or cancel_exec. The result also reports configured capacity and whether the safety circuit is open because a prior SSH transport was force-reaped before its exit could be confirmed. This is an operator-wide control-plane view intended for a trusted single-tenant connection.',
    inputSchema: { type: 'object', properties: {}, additionalProperties: false },
    outputSchema: {
      type: 'object',
      properties: {
        active: { type: 'integer', minimum: 0, description: 'Current number of active executions.' },
        max_concurrent: { type: 'integer', minimum: 1, description: 'Configured maximum active executions.' },
        circuit_open: { type: 'boolean', description: 'True when new exec calls are blocked pending transport-lifecycle investigation.' },
        tasks: { type: 'array', items: activeExecutionSchema(), description: 'Active executions ordered from oldest to newest.' }
      },
      required: ['active', 'max_concurrent', 'circuit_open', 'tasks'],
      additionalProperties: false
    },
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    _meta: {
      'openai/toolInvocation/invoking': 'Reading active remote executions',
      'openai/toolInvocation/invoked': 'Active remote executions read'
    }
  };
}

function getExecStatusToolSchema() {
  return {
    name: 'get_exec_status',
    title: 'Get remote execution status',
    description: 'Look up one remote execution by exec_id. Use this after exec, list_active_execs, or cancel_exec to inspect an active execution, a bounded recent-history record, or a retained unconfirmed-transport diagnostic. The source field identifies which record set supplied the result. A missing ID returns found=false with result=exec_not_found.',
    inputSchema: {
      type: 'object',
      properties: { exec_id: { type: 'string', minLength: 1, description: 'Execution identifier returned by exec or an execution-control tool.' } },
      required: ['exec_id'],
      additionalProperties: false
    },
    outputSchema: {
      type: 'object',
      properties: {
        found: { type: 'boolean', description: 'Whether the execution was found.' },
        source: { type: 'string', enum: ['active', 'recent', 'unconfirmed'], description: 'Registry that supplied task when found=true.' },
        task: { oneOf: [activeExecutionSchema(), executionHistorySchema()], description: 'Active execution details or finalized execution history, selected by source.' },
        result: { type: 'string', enum: ['exec_not_found'], description: 'Machine-readable missing-result code when found=false.' },
        exec_id: { type: 'string', description: 'Requested execution identifier when found=false.' }
      },
      required: ['found'],
      oneOf: [
        {
          properties: {
            found: { enum: [true] },
            source: { enum: ['active'] },
            task: activeExecutionSchema()
          },
          required: ['found', 'source', 'task']
        },
        {
          properties: {
            found: { enum: [true] },
            source: { enum: ['recent', 'unconfirmed'] },
            task: executionHistorySchema()
          },
          required: ['found', 'source', 'task']
        },
        {
          properties: {
            found: { enum: [false] },
            result: { enum: ['exec_not_found'] },
            exec_id: { type: 'string' }
          },
          required: ['found', 'result', 'exec_id']
        }
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
    description: 'Idempotently request cancellation of one active remote execution. Use exec_id from exec, list_active_execs, or get_exec_status. The request records manual_cancel and begins process-group termination, but accepted=true does not mean the process has already exited: capacity is released only after runner finalization confirms the transport lifecycle. Repeating the same cancellation is safe. Finished or unknown executions are not modified.',
    inputSchema: {
      type: 'object',
      properties: { exec_id: { type: 'string', minLength: 1, description: 'Execution identifier to cancel.' } },
      required: ['exec_id'],
      additionalProperties: false
    },
    outputSchema: {
      type: 'object',
      properties: {
        exec_id: { type: 'string', description: 'Requested execution identifier.' },
        result: { type: 'string', enum: ['accepted', 'idempotent', 'conflicting_abort_reason', 'already_finished', 'exec_not_found'], description: 'Machine-readable cancellation outcome.' },
        accepted: { type: 'boolean', description: 'Whether this cancellation request was accepted. This does not confirm process exit.' },
        idempotent: { type: 'boolean', description: 'True when the same cancellation had already been accepted.' },
        state: { type: 'string', enum: executionStateValues, description: 'Current execution state after processing the request.' },
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
    description: 'Transfer one allowed regular file from the configured remote test environment into the current ChatGPT session as one embedded MCP binary resource. The server streams raw bytes over SSH into a bounded local spool, verifies size and SHA-256, and returns the complete file for host-side materialization into /mnt/data. Files larger than ARTIFACT_EMBED_MAX_BYTES are rejected; there is no resource-link or external-URL fallback. The embedded blob is Base64 only at the MCP protocol layer; the model does not need to copy or decode it.',
    inputSchema: {
      type: 'object',
      properties: {
        path: { type: 'string', minLength: 1, description: 'Remote file path. Relative paths resolve from DEFAULT_CWD; the resolved regular file must remain inside ALLOWED_CWDS.' },
        file_name: { type: 'string', minLength: 1, description: 'Optional safe output file name used when ChatGPT materializes the embedded resource. Defaults to the remote basename.' },
        max_bytes: { type: 'integer', minimum: 1, description: 'Optional per-call file-size ceiling. It cannot exceed ARTIFACT_EMBED_MAX_BYTES. Oversized files are rejected, never truncated.' }
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
  listActiveExecsToolSchema(),
  getExecStatusToolSchema(),
  cancelExecToolSchema(),
  importChatgptFileToolSchema(),
  exportRemoteFileToolSchema()
];
