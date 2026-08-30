import type { IncomingMessage, ServerResponse } from 'node:http';
import { readFileSync } from 'node:fs';
import type { ExecRunner } from './exec-runner.js';

const PACKAGE_VERSION = (JSON.parse(readFileSync(new URL('../../package.json', import.meta.url), 'utf8')) as { version: string }).version;
const MAX_LIST_LIMIT = 500;

export async function handleRuntimeRequest(req: IncomingMessage, res: ServerResponse, runner: ExecRunner): Promise<boolean> {
  if (!req.url) return false;
  const url = new URL(req.url, 'http://runtime.local');
  if (url.pathname !== '/runtime' && !url.pathname.startsWith('/runtime/')) return false;

  if (req.method !== 'GET') {
    writeJson(res, 405, { error: 'method_not_allowed', message: 'Runtime Console is read-only.' }, { allow: 'GET' });
    return true;
  }

  if (url.pathname === '/runtime' || url.pathname === '/runtime/') {
    writeAsset(res, 200, 'text/html; charset=utf-8', RUNTIME_HTML);
    return true;
  }
  if (url.pathname === '/runtime/assets/app.css') {
    writeAsset(res, 200, 'text/css; charset=utf-8', RUNTIME_CSS);
    return true;
  }
  if (url.pathname === '/runtime/assets/app.js') {
    writeAsset(res, 200, 'text/javascript; charset=utf-8', RUNTIME_JS);
    return true;
  }
  if (url.pathname === '/runtime/api/overview') {
    writeJson(res, 200, { version: PACKAGE_VERSION, ...runner.runtimeOverview() });
    return true;
  }
  if (url.pathname === '/runtime/api/executions') {
    const limit = parseBoundedInt(url.searchParams.get('limit'), 100, 1, MAX_LIST_LIMIT);
    writeJson(res, 200, { generated_at: new Date().toISOString(), executions: runner.runtimeListExecutions(limit) });
    return true;
  }

  const detailMatch = /^\/runtime\/api\/executions\/(exec-[0-9a-f-]+)$/i.exec(url.pathname);
  if (detailMatch?.[1]) {
    const result = runner.runtimeDetail(detailMatch[1]);
    writeJson(res, result.found ? 200 : 404, result);
    return true;
  }

  const logMatch = /^\/runtime\/api\/executions\/(exec-[0-9a-f-]+)\/logs$/i.exec(url.pathname);
  if (logMatch?.[1]) {
    const stdoutCursor = parseBoundedInt(url.searchParams.get('stdout_cursor'), 0, 0, Number.MAX_SAFE_INTEGER);
    const stderrCursor = parseBoundedInt(url.searchParams.get('stderr_cursor'), 0, 0, Number.MAX_SAFE_INTEGER);
    const maxOutputBytes = parseBoundedInt(
      url.searchParams.get('max_output_bytes'),
      Math.min(65536, runner.config.statusHardMaxOutputBytes),
      1,
      runner.config.statusHardMaxOutputBytes
    );
    const result = runner.runtimeLogs(logMatch[1], { stdoutCursor, stderrCursor, maxOutputBytes });
    writeJson(res, result.found ? 200 : 404, result);
    return true;
  }

  writeJson(res, 404, { error: 'not_found' });
  return true;
}

function parseBoundedInt(value: string | null, fallback: number, min: number, max: number): number {
  if (value === null || value === '') return fallback;
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < min || parsed > max) return fallback;
  return parsed;
}

function securityHeaders(contentType: string): Record<string, string> {
  return {
    'content-type': contentType,
    'cache-control': 'no-store, max-age=0',
    pragma: 'no-cache',
    'x-content-type-options': 'nosniff',
    'x-frame-options': 'DENY',
    'referrer-policy': 'no-referrer',
    'content-security-policy': "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'; object-src 'none'"
  };
}

function writeAsset(res: ServerResponse, status: number, contentType: string, body: string): void {
  res.writeHead(status, securityHeaders(contentType));
  res.end(body);
}

function writeJson(res: ServerResponse, status: number, body: unknown, extra: Record<string, string> = {}): void {
  res.writeHead(status, { ...securityHeaders('application/json; charset=utf-8'), ...extra });
  res.end(JSON.stringify(body));
}

const RUNTIME_HTML = `<!doctype html>
<html lang="en" data-theme="system">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
  <meta name="color-scheme" content="dark light">
  <title>Execution MCP · Runtime</title>
  <link rel="stylesheet" href="/runtime/assets/app.css">
</head>
<body>
  <div class="app-shell">
    <header class="topbar">
      <div class="brand-wrap">
        <div class="brand-mark" aria-hidden="true"><span></span></div>
        <div>
          <div class="brand">Execution MCP</div>
          <div class="brand-sub">Runtime Console</div>
        </div>
      </div>
      <div class="top-actions">
        <span id="updated-at" class="muted top-updated">—</span>
        <span id="health-pill" class="health-pill neutral"><span class="status-dot"></span><span>Loading</span></span>
        <div id="connection-status" class="connection"><span class="status-dot"></span><span>Connecting</span></div>
        <button id="theme-toggle" class="icon-button" type="button" title="Change theme" aria-label="Change theme">◐</button>
      </div>
    </header>

    <main>
      <section id="summary" class="summary-grid" aria-label="Runtime summary">
        <article class="summary-card"><span>Running</span><strong id="count-running">—</strong><small id="cap-running">—</small></article>
        <article class="summary-card"><span>Queued</span><strong id="count-queued">—</strong><small id="cap-queued">—</small></article>
        <article class="summary-card"><span>Completed</span><strong id="count-completed">—</strong><small>recent window</small></article>
        <article class="summary-card"><span>Issues</span><strong id="count-failed">—</strong><small>failed · timeout</small></article>
      </section>

      <section class="capacity-panel">
        <div class="capacity-copy">
          <span>Execution capacity</span>
          <strong id="capacity-copy">—</strong>
        </div>
        <div class="capacity-track" aria-hidden="true"><span id="capacity-bar"></span></div>
        <div id="capacity-detail" class="capacity-detail">—</div>
      </section>

      <section class="workspace">
        <div class="execution-pane panel">
          <div class="panel-header execution-header">
            <div>
              <h2>Tasks</h2>
              <span id="execution-count" class="muted">—</span>
            </div>
            <button id="refresh-button" class="quiet-button" type="button">Refresh</button>
          </div>

          <div class="toolbar">
            <div class="filters" role="tablist" aria-label="Execution filter">
              <button class="filter active" data-filter="all" type="button">All</button>
              <button class="filter" data-filter="running" type="button">Running</button>
              <button class="filter" data-filter="queued" type="button">Queued</button>
              <button class="filter" data-filter="issues" type="button">Issues</button>
              <button class="filter" data-filter="finished" type="button">Finished</button>
            </div>
            <label class="search-wrap">
              <span class="search-icon" aria-hidden="true"></span>
              <input id="search-input" type="search" autocomplete="off" spellcheck="false" placeholder="Search task, label, cwd, id…">
              <kbd>/</kbd>
            </label>
          </div>

          <div id="execution-list" class="execution-list" aria-live="polite"></div>
          <div id="list-empty" class="empty-state hidden"><strong>No executions</strong><span>Nothing matches the current view.</span></div>
        </div>

        <aside id="detail-pane" class="detail-pane panel" aria-label="Execution details">
          <div id="detail-empty" class="detail-empty">
            <div class="empty-orbit"><span></span></div>
            <strong>Select an execution</strong>
            <p>Inspect activity, lifecycle trace and retained logs without changing runtime state.</p>
          </div>
          <div id="detail-content" class="hidden"></div>
        </aside>
      </section>
    </main>

    <footer>
      <span>Read only · no execution controls</span>
      <span id="footer-version">Execution MCP</span>
    </footer>
  </div>
  <script src="/runtime/assets/app.js" defer></script>
</body>
</html>`;

const RUNTIME_CSS = `
:root {
  color-scheme: dark;
  --bg: #0a0b0d;
  --surface: #0f1115;
  --surface-2: #14171c;
  --surface-3: #191d23;
  --line: #252a31;
  --line-soft: #1c2026;
  --text: #f2f4f7;
  --text-2: #a8afb9;
  --text-3: #6f7885;
  --green: #59d49b;
  --green-soft: rgba(89, 212, 155, .11);
  --amber: #e8b75d;
  --amber-soft: rgba(232, 183, 93, .11);
  --red: #ef7777;
  --red-soft: rgba(239, 119, 119, .11);
  --blue: #7ba7ff;
  --blue-soft: rgba(123, 167, 255, .10);
  --radius: 14px;
  --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
  --sans: Inter, ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}
html[data-theme="light"] {
  color-scheme: light;
  --bg: #f6f7f8; --surface: #ffffff; --surface-2: #f8f9fa; --surface-3: #f0f2f4;
  --line: #dfe3e8; --line-soft: #ebedf0; --text: #15181d; --text-2: #555e69; --text-3: #87909b;
  --green: #167d53; --green-soft: rgba(22,125,83,.09); --amber: #996b16; --amber-soft: rgba(153,107,22,.09);
  --red: #bc3f3f; --red-soft: rgba(188,63,63,.08); --blue: #356fd1; --blue-soft: rgba(53,111,209,.08);
}
@media (prefers-color-scheme: light) {
  html[data-theme="system"] { color-scheme: light; --bg:#f6f7f8;--surface:#fff;--surface-2:#f8f9fa;--surface-3:#f0f2f4;--line:#dfe3e8;--line-soft:#ebedf0;--text:#15181d;--text-2:#555e69;--text-3:#87909b;--green:#167d53;--green-soft:rgba(22,125,83,.09);--amber:#996b16;--amber-soft:rgba(153,107,22,.09);--red:#bc3f3f;--red-soft:rgba(188,63,63,.08);--blue:#356fd1;--blue-soft:rgba(53,111,209,.08); }
}
* { box-sizing: border-box; }
html { background: var(--bg); }
body { margin: 0; background: var(--bg); color: var(--text); font-family: var(--sans); font-size: 14px; line-height: 1.45; -webkit-font-smoothing: antialiased; }
button, input { font: inherit; }
button { color: inherit; }
.app-shell { width: min(1540px, 100%); margin: 0 auto; padding: 0 28px 28px; }
.topbar { height: 74px; display:flex; align-items:center; justify-content:space-between; border-bottom:1px solid var(--line-soft); position:sticky; top:0; z-index:20; background:color-mix(in srgb, var(--bg) 90%, transparent); backdrop-filter: blur(14px); }
.brand-wrap, .top-actions, .capacity-copy, .panel-header > div, .execution-title-row, .execution-meta, .detail-title-row, .meta-line, .log-tabs { display:flex; align-items:center; }
.brand-wrap { gap:11px; }
.brand-mark { width:27px; height:27px; border:1px solid var(--line); border-radius:8px; display:grid; place-items:center; background:var(--surface); }
.brand-mark span { width:8px; height:8px; border-radius:50%; background:var(--green); box-shadow:0 0 15px color-mix(in srgb, var(--green) 65%, transparent); }
.brand { font-size:14px; font-weight:650; letter-spacing:-.01em; }
.brand-sub { font: 10px/1.3 var(--mono); color:var(--text-3); text-transform:uppercase; letter-spacing:.08em; margin-top:2px; }
.top-actions { gap:10px; }
.connection { display:flex; gap:7px; align-items:center; color:var(--text-2); font-size:12px; padding:6px 9px; border:1px solid var(--line-soft); border-radius:999px; background:var(--surface); }
.status-dot { width:7px; height:7px; border-radius:50%; background:currentColor; display:inline-block; flex:0 0 auto; }
.connection.online { color:var(--green); }.connection.offline { color:var(--red); }
.icon-button, .quiet-button { border:1px solid var(--line); background:var(--surface); border-radius:8px; cursor:pointer; transition:120ms ease; }
.icon-button { width:32px; height:32px; }.quiet-button { padding:6px 10px; color:var(--text-2); font-size:12px; }
.icon-button:hover,.quiet-button:hover { background:var(--surface-2); border-color:color-mix(in srgb, var(--line) 70%, var(--text-3)); }
.top-updated { font-family:var(--mono); font-size:10px; white-space:nowrap; }
.health-pill { display:flex; align-items:center; gap:7px; border:1px solid var(--line); padding:6px 10px; border-radius:999px; font-size:12px; font-weight:600; }
.health-pill.healthy { color:var(--green); background:var(--green-soft); border-color:color-mix(in srgb, var(--green) 23%, var(--line)); }
.health-pill.degraded { color:var(--red); background:var(--red-soft); border-color:color-mix(in srgb, var(--red) 24%, var(--line)); }
.health-pill.neutral { color:var(--text-3); }
.muted { color:var(--text-3); font-size:12px; }
.summary-grid { margin-top:22px; display:grid; grid-template-columns:repeat(4,1fr); border:1px solid var(--line); border-radius:var(--radius); overflow:hidden; background:var(--surface); }
.summary-card { min-height:105px; padding:19px 20px; display:grid; grid-template-rows:auto 1fr auto; border-right:1px solid var(--line-soft); }
.summary-card:last-child { border-right:0; }
.summary-card > span { color:var(--text-2); font-size:12px; }.summary-card strong { font:500 30px/1 var(--mono); letter-spacing:-.04em; align-self:center; }.summary-card small { color:var(--text-3); font-size:11px; }
.capacity-panel { margin-top:12px; border:1px solid var(--line); background:var(--surface); border-radius:var(--radius); padding:14px 17px; display:grid; grid-template-columns:auto minmax(130px,1fr) auto; gap:18px; align-items:center; }
.capacity-copy { gap:9px; color:var(--text-2); font-size:12px; }.capacity-copy strong { color:var(--text); font-family:var(--mono); font-weight:500; }
.capacity-track { height:5px; border-radius:99px; background:var(--surface-3); overflow:hidden; }.capacity-track span { display:block; width:0; height:100%; border-radius:inherit; background:var(--green); transition:width .25s ease; }
.capacity-detail { color:var(--text-3); font:11px/1 var(--mono); white-space:nowrap; }
.workspace { margin-top:18px; display:grid; grid-template-columns:minmax(390px,.88fr) minmax(540px,1.35fr); gap:14px; align-items:start; }
.panel { border:1px solid var(--line); background:var(--surface); border-radius:var(--radius); overflow:hidden; }
.execution-pane,.detail-pane { min-height:650px; height:calc(100vh - 275px); max-height:860px; }
.panel-header { min-height:62px; padding:0 16px; border-bottom:1px solid var(--line-soft); display:flex; align-items:center; justify-content:space-between; }
.panel-header > div { gap:9px; }.panel-header h2 { font-size:14px; margin:0; letter-spacing:-.01em; }.panel-header .muted { font-family:var(--mono); }
.toolbar { padding:12px; border-bottom:1px solid var(--line-soft); display:grid; gap:10px; }
.filters { display:flex; gap:3px; overflow:auto; scrollbar-width:none; }.filters::-webkit-scrollbar{display:none}.filter { border:0; background:transparent; color:var(--text-3); padding:5px 8px; border-radius:6px; cursor:pointer; font-size:11px; white-space:nowrap; }.filter:hover { color:var(--text-2); background:var(--surface-2); }.filter.active { color:var(--text); background:var(--surface-3); }
.search-wrap { height:34px; display:flex; align-items:center; gap:8px; border:1px solid var(--line-soft); border-radius:8px; background:var(--surface-2); padding:0 9px; color:var(--text-3); }
.search-icon { width:12px; height:12px; border:1px solid currentColor; border-radius:50%; position:relative; flex:0 0 auto; opacity:.75; }.search-icon:after { content:""; position:absolute; width:5px; height:1px; background:currentColor; right:-4px; bottom:-2px; transform:rotate(45deg); transform-origin:left center; }.search-wrap input { flex:1; min-width:0; border:0; outline:0; color:var(--text); background:transparent; font-size:12px; }.search-wrap input::placeholder{color:var(--text-3)} kbd { border:1px solid var(--line); background:var(--surface); border-radius:4px; padding:0 5px; font:10px/18px var(--mono); color:var(--text-3); }
.execution-list { height:calc(100% - 147px); overflow:auto; overscroll-behavior:contain; }
.task-group { border-bottom:1px solid var(--line); }.task-group:last-child{border-bottom:0}.task-group-header { position:sticky; top:0; z-index:2; width:100%; min-height:48px; padding:8px 14px; border:0; border-bottom:1px solid var(--line-soft); background:color-mix(in srgb,var(--surface) 94%,transparent); backdrop-filter:blur(8px); display:flex; align-items:center; justify-content:space-between; gap:12px; text-align:left; cursor:pointer; }.task-group-header:hover{background:color-mix(in srgb,var(--surface-2) 92%,transparent)}.task-group-heading{display:flex;align-items:center;gap:9px;min-width:0}.task-chevron{width:12px;flex:0 0 12px;color:var(--text-3);font:14px/1 var(--mono);transform:rotate(0deg);transition:transform 120ms ease}.task-chevron.expanded{transform:rotate(90deg)}.task-group-title{min-width:0}.task-group-title strong{display:block;font-size:11px;font-weight:650;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.task-group-title span{display:block;color:var(--text-3);font:9px/1.4 var(--mono);margin-top:2px}.task-group-meta{color:var(--text-3);font:9px/1.3 var(--mono);white-space:nowrap;text-align:right}.task-group.active .task-group-header{box-shadow:inset 2px 0 var(--green)}.task-group.queued .task-group-header{box-shadow:inset 2px 0 var(--amber)}.task-group.issues .task-group-header{box-shadow:inset 2px 0 var(--red)}.task-executions{display:block}
.execution-item { width:100%; display:block; text-align:left; border:0; border-bottom:1px solid var(--line-soft); background:transparent; padding:14px 15px 13px; cursor:pointer; transition:background 100ms ease; }
.execution-item:hover { background:var(--surface-2); }.execution-item.selected { background:var(--blue-soft); box-shadow:inset 2px 0 var(--blue); }
.execution-title-row { justify-content:space-between; gap:12px; }.execution-name { font-size:13px; font-weight:610; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }.execution-time { color:var(--text-3); font:11px/1 var(--mono); flex:0 0 auto; }
.execution-command { color:var(--text-2); font:11px/1.45 var(--mono); margin-top:7px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }.execution-path { color:var(--text-3); font:10px/1.4 var(--mono); margin-top:4px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.execution-meta { gap:8px; margin-top:10px; min-width:0; }.state-badge,.activity-badge { display:inline-flex; align-items:center; gap:6px; font-size:10px; font-weight:650; letter-spacing:.03em; text-transform:uppercase; }.state-badge .status-dot { width:6px; height:6px; }
.state-running{color:var(--green)}.state-queued{color:var(--amber)}.state-completed{color:var(--text-2)}.state-failed,.state-timed_out{color:var(--red)}.state-cancelled{color:var(--amber)}
.state-running .status-dot { animation:pulse 1.9s ease infinite; } @keyframes pulse{0%,100%{opacity:1;box-shadow:0 0 0 0 color-mix(in srgb,var(--green) 30%,transparent)}50%{opacity:.75;box-shadow:0 0 0 4px transparent}}
.activity-badge { color:var(--text-3); font-weight:500; text-transform:none; letter-spacing:0; min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }.activity-badge.active{color:var(--green)}.activity-badge.long-quiet{color:var(--amber)}
.execution-id-short { margin-left:auto; color:var(--text-3); font:10px/1 var(--mono); }
.empty-state,.detail-empty { color:var(--text-3); display:flex; flex-direction:column; align-items:center; justify-content:center; text-align:center; }.empty-state { height:280px; gap:4px; }.empty-state strong,.detail-empty strong { color:var(--text-2); font-size:13px; }.empty-state span,.detail-empty p { margin:0; font-size:12px; }
.detail-pane { overflow:auto; position:sticky; top:88px; }
.detail-empty { height:100%; padding:50px; }.detail-empty p { max-width:300px; margin-top:6px; line-height:1.55; }.empty-orbit { width:54px;height:54px;border:1px solid var(--line);border-radius:50%;display:grid;place-items:center;margin-bottom:18px;position:relative}.empty-orbit:after{content:"";position:absolute;width:68px;height:24px;border:1px solid var(--line-soft);border-radius:50%;transform:rotate(-24deg)}.empty-orbit span{width:8px;height:8px;border-radius:50%;background:var(--text-3)}
.detail-section { padding:18px 20px; border-bottom:1px solid var(--line-soft); }.detail-section:last-child{border-bottom:0}.detail-overview { padding-top:20px; }.detail-title-row { justify-content:space-between; align-items:flex-start; gap:16px; }.detail-title { min-width:0; }.detail-title h2 { font-size:18px; line-height:1.25; margin:7px 0 5px; letter-spacing:-.02em; overflow-wrap:anywhere; }.detail-title .mono-line { color:var(--text-3); font:11px/1.45 var(--mono); overflow-wrap:anywhere; }
.big-state { display:inline-flex; gap:7px; align-items:center; font:650 10px/1 var(--sans); letter-spacing:.05em; text-transform:uppercase; }.big-state .status-dot{width:7px;height:7px}.detail-duration { color:var(--text-2); font:12px/1 var(--mono); white-space:nowrap; padding-top:3px; }
.detail-grid { margin-top:18px; display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:1px; border:1px solid var(--line-soft); border-radius:9px; overflow:hidden; background:var(--line-soft); }.metric { min-height:70px; padding:12px; background:var(--surface-2); }.metric span { display:block; color:var(--text-3); font-size:10px; text-transform:uppercase; letter-spacing:.06em; margin-bottom:7px; }.metric strong { font:500 12px/1.3 var(--mono); overflow-wrap:anywhere; }
.section-title { display:flex; align-items:baseline; justify-content:space-between; gap:12px; margin-bottom:13px; }.section-title h3 { margin:0; font-size:12px; font-weight:650; }.section-title span { color:var(--text-3); font-size:10px; }
.meta-stack { display:grid; gap:9px; }.meta-line { min-width:0; gap:10px; }.meta-key { color:var(--text-3); width:92px; flex:0 0 92px; font-size:11px; }.meta-value { color:var(--text-2); font:11px/1.4 var(--mono); min-width:0; overflow-wrap:anywhere; }.meta-value.command { padding:8px 9px; border:1px solid var(--line-soft); border-radius:7px; background:var(--surface-2); width:100%; }
.trace { position:relative; display:grid; gap:0; }.trace-event { display:grid; grid-template-columns:86px 14px minmax(0,1fr); gap:9px; min-height:42px; }.trace-time { color:var(--text-3); font:10px/18px var(--mono); text-align:right; padding-top:1px; }.trace-rail { position:relative; display:flex; justify-content:center; }.trace-rail:before { content:""; position:absolute; top:12px; bottom:-12px; width:1px; background:var(--line); }.trace-event:last-child .trace-rail:before { display:none; }.trace-dot { width:7px;height:7px;border-radius:50%;background:var(--text-3);margin-top:6px;z-index:1;box-shadow:0 0 0 3px var(--surface); }.trace-event.info .trace-dot{background:var(--green)}.trace-event.warning .trace-dot{background:var(--amber)}.trace-event.error .trace-dot{background:var(--red)}.trace-copy strong { display:block; font-size:11px; font-weight:580; line-height:18px; }.trace-copy span { color:var(--text-3); font:10px/1.4 var(--mono); display:block; overflow-wrap:anywhere; }
.log-shell { border:1px solid var(--line-soft); border-radius:9px; overflow:hidden; background:#08090b; color:#d8dee8; }.log-head { min-height:38px; padding:0 10px; display:flex; align-items:center; justify-content:space-between; border-bottom:1px solid #1f2329; background:#0d0f12; }.log-tabs{gap:2px}.log-tab { border:0; background:transparent; color:#7d8795; padding:5px 8px; border-radius:5px; cursor:pointer; font-size:10px; }.log-tab.active{color:#e7ebf0;background:#181b20}.follow-toggle { display:flex; gap:6px; align-items:center; color:#7d8795; font-size:10px; cursor:pointer; user-select:none; }.follow-toggle input{accent-color:#59d49b}.log-output { margin:0; height:280px; overflow:auto; padding:12px 13px 18px; white-space:pre-wrap; overflow-wrap:anywhere; font:11px/1.55 var(--mono); tab-size:2; }.log-empty { color:#69717d; }.log-warning { color:var(--amber); font:10px/1.4 var(--mono); padding:8px 10px; border-top:1px solid #1f2329; background:#0d0f12; }
.copy-button { border:0;background:transparent;color:var(--text-3);padding:2px 4px;cursor:pointer;font:10px/1 var(--mono); }.copy-button:hover{color:var(--text-2)}
.hidden { display:none !important; }
footer { display:flex; justify-content:space-between; color:var(--text-3); font-size:10px; padding:18px 2px 0; }
@media (max-width:1050px){.workspace{grid-template-columns:1fr}.execution-pane{height:540px;min-height:540px}.detail-pane{height:auto;min-height:620px;max-height:none;position:static}.execution-list{height:393px}}
@media (max-width:720px){.app-shell{padding:0 14px 18px}.topbar{height:62px}.top-updated{display:none}.connection span:last-child{display:none}.health-pill{padding:6px 8px}.summary-grid{margin-top:14px;grid-template-columns:1fr 1fr}.summary-card:nth-child(2){border-right:0}.summary-card:nth-child(-n+2){border-bottom:1px solid var(--line-soft)}.summary-card{min-height:92px;padding:15px}.capacity-panel{grid-template-columns:1fr auto}.capacity-track{grid-column:1/-1;grid-row:2}.workspace{margin-top:12px}.execution-pane{height:520px;min-height:520px}.execution-list{height:373px}.task-group-meta{max-width:100px;overflow:hidden;text-overflow:ellipsis}.detail-grid{grid-template-columns:1fr 1fr}.detail-section{padding:16px}.trace-event{grid-template-columns:70px 13px minmax(0,1fr);gap:7px}.log-output{height:240px}}
`;

const RUNTIME_JS = `
(() => {
  'use strict';

  const TASK_EXPANSION_KEY = 'exec-mcp-runtime-task-expansion-v1';

  const state = {
    overview: null,
    executions: [],
    selectedId: null,
    detail: null,
    filter: 'all',
    search: '',
    refreshing: false,
    generation: 0,
    stdoutCursor: null,
    stderrCursor: null,
    stdout: '',
    stderr: '',
    logTab: 'stdout',
    follow: true,
    logScrollTop: 0,
    lastSuccessAt: 0,
    selectedGeneration: 0,
    taskExpansion: loadTaskExpansion()
  };

  function loadTaskExpansion() {
    try {
      const parsed = JSON.parse(localStorage.getItem(TASK_EXPANSION_KEY) || '{}');
      return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {};
    } catch { return {}; }
  }

  function saveTaskExpansion() {
    try { localStorage.setItem(TASK_EXPANSION_KEY, JSON.stringify(state.taskExpansion)); } catch {}
  }

  const byId = (id) => document.getElementById(id);
  const els = {
    connection: byId('connection-status'), health: byId('health-pill'), updated: byId('updated-at'),
    running: byId('count-running'), queued: byId('count-queued'), completed: byId('count-completed'), failed: byId('count-failed'),
    capRunning: byId('cap-running'), capQueued: byId('cap-queued'), capacityCopy: byId('capacity-copy'), capacityBar: byId('capacity-bar'), capacityDetail: byId('capacity-detail'),
    list: byId('execution-list'), listEmpty: byId('list-empty'), executionCount: byId('execution-count'),
    detailEmpty: byId('detail-empty'), detailContent: byId('detail-content'), search: byId('search-input'), refresh: byId('refresh-button'),
    theme: byId('theme-toggle'), version: byId('footer-version')
  };

  function make(tag, className, text) {
    const element = document.createElement(tag);
    if (className) element.className = className;
    if (text !== undefined && text !== null) element.textContent = String(text);
    return element;
  }

  function append(parent, ...children) {
    for (const child of children) if (child) parent.appendChild(child);
    return parent;
  }

  async function api(path) {
    const response = await fetch(path, { cache: 'no-store', headers: { accept: 'application/json' } });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(data.message || data.error || ('HTTP ' + response.status));
    return data;
  }

  function shortId(value, length = 10) {
    if (!value) return '—';
    const text = String(value);
    return text.length <= length ? text : text.slice(0, length) + '…';
  }

  function formatDuration(ms) {
    if (ms === null || ms === undefined || !Number.isFinite(Number(ms))) return '—';
    let seconds = Math.max(0, Math.floor(Number(ms) / 1000));
    const days = Math.floor(seconds / 86400); seconds %= 86400;
    const hours = Math.floor(seconds / 3600); seconds %= 3600;
    const minutes = Math.floor(seconds / 60); seconds %= 60;
    if (days) return days + 'd ' + hours + 'h';
    if (hours) return hours + 'h ' + minutes + 'm';
    if (minutes) return minutes + 'm ' + String(seconds).padStart(2, '0') + 's';
    return seconds + 's';
  }

  function durationFor(item) {
    if (item.lifecycle === 'active') return formatDuration(Number(item.elapsed_seconds || 0) * 1000);
    return formatDuration(item.duration_ms);
  }

  function formatBytes(value) {
    const bytes = Math.max(0, Number(value || 0));
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(bytes < 10240 ? 1 : 0) + ' KB';
    if (bytes < 1024 * 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(bytes < 10 * 1024 * 1024 ? 1 : 0) + ' MB';
    return (bytes / (1024 * 1024 * 1024)).toFixed(1) + ' GB';
  }

  function relativeTime(iso, now = Date.now()) {
    if (!iso) return 'never';
    const delta = Math.max(0, now - Date.parse(iso));
    const seconds = Math.floor(delta / 1000);
    if (seconds < 5) return 'just now';
    if (seconds < 60) return seconds + 's ago';
    const minutes = Math.floor(seconds / 60);
    if (minutes < 60) return minutes + 'm ago';
    const hours = Math.floor(minutes / 60);
    if (hours < 24) return hours + 'h ago';
    return Math.floor(hours / 24) + 'd ago';
  }

  function localTime(iso, millis = false) {
    if (!iso) return '—';
    const date = new Date(iso);
    if (Number.isNaN(date.getTime())) return '—';
    const base = date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
    return millis ? base + '.' + String(date.getMilliseconds()).padStart(3, '0') : base;
  }

  function statusOf(item) {
    return String(item.status || item.state || item.final_state || 'unknown').toLowerCase();
  }

  function statusLabel(status) {
    return String(status || 'unknown').replaceAll('_', ' ');
  }

  function statusClass(status) {
    const safe = ['running', 'queued', 'completed', 'failed', 'timed_out', 'cancelled'].includes(status) ? status : 'completed';
    return 'state-' + safe;
  }

  function activityFor(item) {
    const status = statusOf(item);
    if (status === 'queued') return { label: item.queue_position ? 'Queue #' + item.queue_position : 'Waiting in queue', className: '' };
    if (item.lifecycle === 'finished') return { label: 'Finished ' + relativeTime(item.finished_at), className: '' };
    if (item.last_output_at) {
      const age = Math.max(0, Date.now() - Date.parse(item.last_output_at));
      if (age <= 15000) return { label: 'Output ' + relativeTime(item.last_output_at), className: 'active' };
      if (age <= 300000) return { label: 'Quiet · output ' + relativeTime(item.last_output_at), className: '' };
      return { label: 'Long quiet · output ' + relativeTime(item.last_output_at), className: 'long-quiet' };
    }
    if (item.last_activity_at) return { label: 'No output · activity ' + relativeTime(item.last_activity_at), className: '' };
    return { label: 'No output yet', className: '' };
  }

  function displayName(item) {
    return item.label || item.command_preview || ('Execution ' + shortId(item.exec_id, 18));
  }

  function issueItem(item) {
    const status = statusOf(item);
    return status === 'failed' || status === 'timed_out' || item.final_state === 'unconfirmed_reaped';
  }

  function matchesFilter(item) {
    const status = statusOf(item);
    if (state.filter === 'running') return status === 'running';
    if (state.filter === 'queued') return status === 'queued';
    if (state.filter === 'issues') return issueItem(item);
    if (state.filter === 'finished') return item.lifecycle === 'finished';
    return true;
  }

  function matchesSearch(item) {
    if (!state.search) return true;
    const taskContext = item.task_context || {};
    const haystack = [taskContext.label, taskContext.task_handle, item.task_handle, item.label, item.cwd, item.command_preview, item.exec_id, item.trace_id, item.command_sha256]
      .filter(Boolean).join(' ').toLowerCase();
    return haystack.includes(state.search.toLowerCase());
  }

  function renderSummary() {
    const overview = state.overview;
    if (!overview) return;
    els.running.textContent = overview.counts.running;
    els.queued.textContent = overview.counts.queued;
    els.completed.textContent = overview.counts.recent_completed;
    els.failed.textContent = overview.counts.recent_failed;
    els.capRunning.textContent = overview.capacity.running + ' / ' + overview.capacity.global_max + ' slots';
    els.capQueued.textContent = overview.capacity.queued + ' / ' + overview.capacity.queue_max + ' queue';
    els.capacityCopy.textContent = overview.capacity.running + ' / ' + overview.capacity.global_max;
    const percent = overview.capacity.global_max > 0 ? Math.min(100, overview.capacity.running / overview.capacity.global_max * 100) : 0;
    els.capacityBar.style.width = percent + '%';
    els.capacityBar.style.background = percent >= 100 ? 'var(--amber)' : 'var(--green)';
    els.capacityDetail.textContent = 'sync ' + overview.capacity.sync_running + '/' + overview.capacity.sync_max + ' · async ' + overview.capacity.async_running + '/' + overview.capacity.async_max;
    els.health.className = 'health-pill ' + (overview.health === 'healthy' ? 'healthy' : 'degraded');
    els.health.lastElementChild.textContent = overview.health === 'healthy' ? 'Healthy' : 'Degraded';
    els.version.textContent = 'Execution MCP v' + overview.version;
  }

  function taskGroupStatus(items) {
    if (items.some((item) => issueItem(item))) return 'issues';
    if (items.some((item) => statusOf(item) === 'running')) return 'active';
    if (items.some((item) => statusOf(item) === 'queued')) return 'queued';
    return '';
  }

  function taskGroupLabel(item) {
    const context = item.task_context || {};
    return context.label || (item.task_handle ? 'Task ' + shortId(item.task_handle.replace(/^task-/, ''), 12) : 'Ungrouped / legacy');
  }

  function defaultTaskExpanded(items) {
    return items.some((item) => statusOf(item) === 'running' || statusOf(item) === 'queued' || issueItem(item));
  }

  function taskExpanded(taskHandle, items) {
    const override = state.taskExpansion[taskHandle];
    if (typeof override === 'boolean') return override;
    if (state.search) return true;
    return defaultTaskExpanded(items);
  }

  function toggleTask(taskHandle, items) {
    state.taskExpansion[taskHandle] = !taskExpanded(taskHandle, items);
    saveTaskExpansion();
    renderExecutionList();
  }

  function taskLastActivity(items) {
    let latest = null;
    for (const item of items) {
      const candidate = item.last_activity_at || item.finished_at || item.running_at || item.created_at || null;
      if (candidate && (!latest || Date.parse(candidate) > Date.parse(latest))) latest = candidate;
    }
    return latest;
  }

  function taskGroupMeta(items) {
    const running = items.filter((item) => statusOf(item) === 'running').length;
    const queued = items.filter((item) => statusOf(item) === 'queued').length;
    const issues = items.filter(issueItem).length;
    const count = items.length + (items.length === 1 ? ' execution' : ' executions');
    if (running) return running + ' running · ' + count;
    if (queued) return queued + ' queued · ' + count;
    if (issues) return issues + (issues === 1 ? ' issue · ' : ' issues · ') + count;
    const latest = taskLastActivity(items);
    return count + (latest ? ' · ' + relativeTime(latest) : '');
  }

  function renderExecutionList() {
    const items = state.executions.filter(matchesFilter).filter(matchesSearch);
    const groups = new Map();
    for (const item of items) {
      const key = item.task_handle || '__ungrouped__';
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push(item);
    }
    els.executionCount.textContent = groups.size + (groups.size === 1 ? ' task' : ' tasks') + ' · ' + items.length + (items.length === 1 ? ' execution' : ' executions');
    els.list.replaceChildren();
    els.listEmpty.classList.toggle('hidden', items.length !== 0);

    const fragment = document.createDocumentFragment();
    for (const [taskHandle, taskItems] of groups) {
      const group = make('section', 'task-group ' + taskGroupStatus(taskItems));
      const expanded = taskExpanded(taskHandle, taskItems);
      const header = make('button', 'task-group-header');
      header.type = 'button';
      header.setAttribute('aria-expanded', String(expanded));
      const heading = make('div', 'task-group-heading');
      const chevron = make('span', 'task-chevron' + (expanded ? ' expanded' : ''), '›');
      chevron.setAttribute('aria-hidden', 'true');
      const title = make('div', 'task-group-title');
      const first = taskItems[0] || {};
      append(title,
        make('strong', '', taskGroupLabel(first)),
        make('span', '', taskHandle === '__ungrouped__' ? 'No explicit task context' : shortId(taskHandle, 44))
      );
      append(heading, chevron, title);
      append(header, heading, make('span', 'task-group-meta', taskGroupMeta(taskItems)));
      header.addEventListener('click', () => toggleTask(taskHandle, taskItems));
      group.appendChild(header);

      const children = make('div', 'task-executions' + (expanded ? '' : ' hidden'));
      for (const item of taskItems) {
        const status = statusOf(item);
        const button = make('button', 'execution-item' + (state.selectedId === item.exec_id ? ' selected' : ''));
        button.type = 'button';
        button.dataset.execId = item.exec_id;

        const titleRow = make('div', 'execution-title-row');
        append(titleRow, make('span', 'execution-name', displayName(item)), make('span', 'execution-time', durationFor(item)));
        const command = item.command_preview ? make('div', 'execution-command', item.command_preview) : null;
        const path = make('div', 'execution-path', item.cwd || 'cwd unavailable');

        const meta = make('div', 'execution-meta');
        const badge = make('span', 'state-badge ' + statusClass(status));
        append(badge, make('span', 'status-dot'), make('span', '', statusLabel(status)));
        const activity = activityFor(item);
        append(meta, badge, make('span', 'activity-badge ' + activity.className, activity.label), make('span', 'execution-id-short', shortId(item.exec_id.replace(/^exec-/, ''), 8)));
        append(button, titleRow, command, path, meta);
        button.addEventListener('click', () => selectExecution(item.exec_id));
        children.appendChild(button);
      }
      group.appendChild(children);
      fragment.appendChild(group);
    }
    els.list.appendChild(fragment);
  }

  function metric(label, value) {
    const box = make('div', 'metric');
    append(box, make('span', '', label), make('strong', '', value));
    return box;
  }

  function metaLine(label, value, extraClass) {
    const row = make('div', 'meta-line');
    append(row, make('span', 'meta-key', label), make('span', 'meta-value' + (extraClass ? ' ' + extraClass : ''), value));
    return row;
  }

  function copyableMetaLine(label, value) {
    const row = make('div', 'meta-line');
    const key = make('span', 'meta-key', label);
    const valueEl = make('span', 'meta-value', value || '—');
    const copy = make('button', 'copy-button', 'copy');
    copy.type = 'button';
    copy.addEventListener('click', async () => {
      try { await navigator.clipboard.writeText(String(value || '')); copy.textContent = 'copied'; setTimeout(() => { copy.textContent = 'copy'; }, 1000); } catch { copy.textContent = 'failed'; }
    });
    append(row, key, valueEl, copy);
    return row;
  }

  function eventLabel(event) {
    const labels = {
      tool_request_received: 'Tool request received', request_validated: 'Request validated', job_registered: 'Job registered',
      queued: 'Queued', starting: 'Starting', transport_started: 'SSH transport started', execution_running: 'Execution running',
      first_output: 'First output', abort_requested: 'Abort requested', transport_closed: 'Transport closed', completed: 'Completed',
      failed: 'Failed', cancelled: 'Cancelled', timed_out: 'Timed out', client_closed: 'Client closed', spawn_failed: 'Spawn failed',
      unconfirmed_reaped: 'Termination unconfirmed'
    };
    return labels[event] || String(event).replaceAll('_', ' ');
  }

  function renderTrace(observation) {
    const trace = make('div', 'trace');
    const events = observation && Array.isArray(observation.trace) ? observation.trace : [];
    if (!events.length) {
      trace.appendChild(make('div', 'muted', 'No retained lifecycle events.'));
      return trace;
    }
    for (const event of events) {
      const row = make('div', 'trace-event ' + (event.level || 'info'));
      const time = make('span', 'trace-time', localTime(event.at, true));
      const rail = make('span', 'trace-rail'); rail.appendChild(make('span', 'trace-dot'));
      const copy = make('div', 'trace-copy');
      copy.appendChild(make('strong', '', eventLabel(event.event)));
      if (event.detail) copy.appendChild(make('span', '', event.detail));
      append(row, time, rail, copy);
      trace.appendChild(row);
    }
    return trace;
  }

  function renderLogViewer(detail) {
    const shell = make('div', 'log-shell');
    const head = make('div', 'log-head');
    const tabs = make('div', 'log-tabs');
    for (const stream of ['stdout', 'stderr']) {
      const tab = make('button', 'log-tab' + (state.logTab === stream ? ' active' : ''), stream);
      tab.type = 'button';
      tab.addEventListener('click', () => { state.logTab = stream; renderLogBody(); });
      tabs.appendChild(tab);
    }
    const follow = make('label', 'follow-toggle');
    const checkbox = document.createElement('input'); checkbox.type = 'checkbox'; checkbox.checked = state.follow;
    checkbox.addEventListener('change', () => { state.follow = checkbox.checked; if (state.follow) scrollLogToBottom(); });
    append(follow, checkbox, make('span', '', 'Follow latest'));
    append(head, tabs, follow);

    const pre = make('pre', 'log-output'); pre.id = 'runtime-log-output';
    pre.addEventListener('scroll', () => { if (!state.follow) state.logScrollTop = pre.scrollTop; });
    const warning = make('div', 'log-warning hidden'); warning.id = 'runtime-log-warning';
    append(shell, head, pre, warning);
    queueMicrotask(renderLogBody);
    return shell;
  }

  function renderLogBody() {
    const pre = byId('runtime-log-output');
    const warning = byId('runtime-log-warning');
    if (!pre || !warning) return;
    const text = state.logTab === 'stderr' ? state.stderr : state.stdout;
    pre.textContent = text || (state.detail && state.detail.logs && !state.detail.logs.available ? 'Retained logs are no longer available for this execution.' : 'No ' + state.logTab + ' output yet.');
    pre.classList.toggle('log-empty', !text);
    const truncated = state.detail && state.detail.logs && (state.logTab === 'stderr' ? state.detail.logs.stderr_truncated : state.detail.logs.stdout_truncated);
    warning.classList.toggle('hidden', !truncated);
    warning.textContent = truncated ? 'Older ' + state.logTab + ' output was evicted from the bounded runtime log buffer.' : '';
    if (state.follow) scrollLogToBottom(); else pre.scrollTop = state.logScrollTop;
  }

  function scrollLogToBottom() {
    const pre = byId('runtime-log-output');
    if (pre) pre.scrollTop = pre.scrollHeight;
  }

  function renderDetail() {
    const detail = state.detail;
    if (!detail || !detail.found) {
      els.detailEmpty.classList.remove('hidden');
      els.detailContent.classList.add('hidden');
      els.detailContent.replaceChildren();
      return;
    }
    els.detailEmpty.classList.add('hidden');
    els.detailContent.classList.remove('hidden');
    els.detailContent.replaceChildren();

    const task = detail.task || {};
    const observation = detail.observation || {};
    const status = statusOf(task);
    const overview = make('section', 'detail-section detail-overview');
    const titleRow = make('div', 'detail-title-row');
    const title = make('div', 'detail-title');
    const bigState = make('span', 'big-state ' + statusClass(status));
    append(bigState, make('span', 'status-dot'), make('span', '', statusLabel(status)));
    append(title, bigState, make('h2', '', displayName({ ...observation, ...task })), make('div', 'mono-line', task.cwd || observation.cwd || 'cwd unavailable'));
    append(titleRow, title, make('span', 'detail-duration', durationFor({ ...task, lifecycle: detail.source === 'active' ? 'active' : 'finished' })));
    overview.appendChild(titleRow);

    const grid = make('div', 'detail-grid');
    const activity = activityFor({ ...task, lifecycle: detail.source === 'active' ? 'active' : 'finished' });
    append(grid,
      metric('Activity', activity.label),
      metric('Last output', observation.last_output_at ? relativeTime(observation.last_output_at) : 'No output'),
      metric('Output', formatBytes((observation.stdout_bytes || 0) + (observation.stderr_bytes || 0))),
      metric('Started', task.running_at ? localTime(task.running_at) : (task.created_at ? localTime(task.created_at) : '—')),
      metric('Class', task.execution_class || observation.execution_class || '—'),
      metric(detail.source === 'active' ? 'Timeout' : 'Exit', detail.source === 'active' ? ((task.timeout_seconds || observation.timeout_seconds || '—') + 's') : (task.exit_code === null || task.exit_code === undefined ? (task.signal || '—') : String(task.exit_code)))
    );
    overview.appendChild(grid);
    els.detailContent.appendChild(overview);

    const identity = make('section', 'detail-section');
    const identityTitle = make('div', 'section-title'); append(identityTitle, make('h3', '', 'Identity & origin'), make('span', '', 'observable facts only'));
    const stack = make('div', 'meta-stack');
    const origin = observation.origin || task.origin || null;
    const taskContext = detail.task_context || {};
    append(stack,
      copyableMetaLine('Execution', task.exec_id || observation.exec_id),
      copyableMetaLine('Trace', observation.trace_id || task.trace_id),
      metaLine('Origin', origin ? ((origin.kind === 'mcp' ? 'MCP' : origin.kind) + (origin.tool ? ' · ' + origin.tool : '')) : 'Unavailable'),
      metaLine('Task', taskContext.label || (task.task_handle ? 'Explicit task context' : 'Ungrouped / legacy')),
      copyableMetaLine('Task handle', task.task_handle || (origin && origin.task_handle) || ''),
      metaLine('Conversation', task.task_handle ? 'Explicit model-carried context · internal ChatGPT conversation ID is not exposed' : 'Cannot be identified reliably'),
      metaLine('MCP session', origin && origin.transport_session_id ? shortId(origin.transport_session_id, 42) + ' · transport only, not conversation' : 'Unavailable'),
      metaLine('Request', origin && origin.request_id ? origin.request_id : 'Unavailable')
    );
    const preview = task.command_preview || observation.command_preview;
    stack.appendChild(metaLine('Command', preview || 'Preview disabled by server configuration', 'command'));
    append(identity, identityTitle, stack);
    els.detailContent.appendChild(identity);

    const lifecycle = make('section', 'detail-section');
    const lifeTitle = make('div', 'section-title'); append(lifeTitle, make('h3', '', 'Lifecycle trace'), make('span', '', observation.trace ? observation.trace.length + ' events' : '0 events'));
    append(lifecycle, lifeTitle, renderTrace(observation));
    els.detailContent.appendChild(lifecycle);

    const logs = make('section', 'detail-section');
    const logTitle = make('div', 'section-title'); append(logTitle, make('h3', '', 'Retained output'), make('span', '', detail.logs && detail.logs.available ? 'live · bounded buffer' : 'not retained'));
    append(logs, logTitle, renderLogViewer(detail));
    els.detailContent.appendChild(logs);
  }

  async function loadLogs(selectedGeneration) {
    const detail = state.detail;
    if (!detail || !detail.found || !detail.logs || !detail.logs.available || !state.selectedId) return;
    if (state.stdoutCursor === null || state.stderrCursor === null) {
      const stdoutSpan = Math.min(49152, Math.max(0, detail.logs.stdout_end_cursor - detail.logs.stdout_start_cursor));
      const stderrSpan = Math.min(16384, Math.max(0, detail.logs.stderr_end_cursor - detail.logs.stderr_start_cursor));
      state.stdoutCursor = Math.max(detail.logs.stdout_start_cursor, detail.logs.stdout_end_cursor - stdoutSpan);
      state.stderrCursor = Math.max(detail.logs.stderr_start_cursor, detail.logs.stderr_end_cursor - stderrSpan);
    }
    const query = new URLSearchParams({
      stdout_cursor: String(state.stdoutCursor), stderr_cursor: String(state.stderrCursor), max_output_bytes: '65536'
    });
    const result = await api('/runtime/api/executions/' + encodeURIComponent(state.selectedId) + '/logs?' + query.toString());
    if (selectedGeneration !== state.selectedGeneration) return;
    if (result.logs_available === false) return;
    if (result.stdout) state.stdout = boundBrowserLog(state.stdout + result.stdout);
    if (result.stderr) state.stderr = boundBrowserLog(state.stderr + result.stderr);
    state.stdoutCursor = result.stdout_cursor;
    state.stderrCursor = result.stderr_cursor;
    renderLogBody();
    if (result.has_more_stdout || result.has_more_stderr) queueMicrotask(() => loadLogs(selectedGeneration).catch(markOffline));
  }

  function boundBrowserLog(text) {
    const maxChars = 600000;
    return text.length > maxChars ? text.slice(text.length - maxChars) : text;
  }

  async function refreshDetail() {
    if (!state.selectedId) { state.detail = null; renderDetail(); return; }
    const generation = state.selectedGeneration;
    try {
      const detail = await api('/runtime/api/executions/' + encodeURIComponent(state.selectedId));
      if (generation !== state.selectedGeneration) return;
      state.detail = detail;
      renderDetail();
      await loadLogs(generation);
    } catch (error) {
      if (generation !== state.selectedGeneration) return;
      if (String(error.message || error).includes('not_found')) {
        state.detail = null; renderDetail();
      } else throw error;
    }
  }

  async function selectExecution(execId) {
    if (state.selectedId === execId && state.detail) return;
    const selected = state.executions.find((item) => item.exec_id === execId);
    if (selected && selected.task_handle) { state.taskExpansion[selected.task_handle] = true; saveTaskExpansion(); }
    state.selectedId = execId;
    state.selectedGeneration++;
    state.detail = null; state.stdoutCursor = null; state.stderrCursor = null; state.stdout = ''; state.stderr = ''; state.logScrollTop = 0;
    history.replaceState(null, '', '#exec=' + encodeURIComponent(execId));
    renderExecutionList();
    els.detailEmpty.classList.remove('hidden');
    els.detailContent.classList.add('hidden');
    await refreshDetail().catch(markOffline);
  }

  function selectedFromHash() {
    const match = /^#exec=(.+)$/.exec(location.hash);
    if (!match || !match[1]) return null;
    try { return decodeURIComponent(match[1]); } catch { return null; }
  }

  async function refreshAll() {
    if (state.refreshing) return;
    state.refreshing = true;
    const generation = ++state.generation;
    els.refresh.disabled = true;
    try {
      const [overview, list] = await Promise.all([api('/runtime/api/overview'), api('/runtime/api/executions?limit=150')]);
      if (generation !== state.generation) return;
      state.overview = overview;
      state.executions = Array.isArray(list.executions) ? list.executions : [];
      state.lastSuccessAt = Date.now();
      markOnline();
      renderSummary();

      if (!state.selectedId) {
        const hashed = selectedFromHash();
        const preferred = hashed || (state.executions.find((item) => statusOf(item) === 'running') || state.executions[0] || {}).exec_id || null;
        if (preferred) {
          state.selectedId = preferred;
          state.selectedGeneration++;
          if (hashed) {
            const selected = state.executions.find((item) => item.exec_id === preferred);
            if (selected && selected.task_handle) { state.taskExpansion[selected.task_handle] = true; saveTaskExpansion(); }
          }
        }
      }
      renderExecutionList();
      await refreshDetail();
    } catch (error) {
      markOffline(error);
    } finally {
      state.refreshing = false;
      els.refresh.disabled = false;
      updateFreshness();
    }
  }

  function markOnline() {
    els.connection.className = 'connection online';
    els.connection.lastElementChild.textContent = 'Live';
  }

  function markOffline(error) {
    els.connection.className = 'connection offline';
    els.connection.lastElementChild.textContent = 'Disconnected';
    if (error) console.error(error);
  }

  function updateFreshness() {
    els.updated.textContent = state.lastSuccessAt ? 'Updated ' + relativeTime(new Date(state.lastSuccessAt).toISOString()) : 'Not updated';
  }

  function setFilter(next) {
    state.filter = next;
    document.querySelectorAll('.filter').forEach((button) => button.classList.toggle('active', button.dataset.filter === next));
    renderExecutionList();
  }

  function initTheme() {
    const saved = localStorage.getItem('exec-mcp-theme');
    const valid = ['system', 'dark', 'light'].includes(saved) ? saved : 'system';
    document.documentElement.dataset.theme = valid;
    updateThemeButton(valid);
  }

  function updateThemeButton(theme) {
    els.theme.textContent = theme === 'dark' ? '☾' : theme === 'light' ? '☀' : '◐';
    els.theme.title = 'Theme: ' + theme;
  }

  function cycleTheme() {
    const current = document.documentElement.dataset.theme || 'system';
    const next = current === 'system' ? 'dark' : current === 'dark' ? 'light' : 'system';
    document.documentElement.dataset.theme = next;
    localStorage.setItem('exec-mcp-theme', next);
    updateThemeButton(next);
  }

  document.querySelectorAll('.filter').forEach((button) => button.addEventListener('click', () => setFilter(button.dataset.filter || 'all')));
  els.search.addEventListener('input', () => { state.search = els.search.value.trim(); renderExecutionList(); });
  els.refresh.addEventListener('click', () => refreshAll());
  els.theme.addEventListener('click', cycleTheme);
  document.addEventListener('keydown', (event) => {
    if (event.key === '/' && document.activeElement !== els.search) { event.preventDefault(); els.search.focus(); }
    if (event.key.toLowerCase() === 'r' && !event.metaKey && !event.ctrlKey && !event.altKey && document.activeElement !== els.search) refreshAll();
  });
  document.addEventListener('visibilitychange', () => { if (!document.hidden) refreshAll(); });
  window.addEventListener('hashchange', () => { const id = selectedFromHash(); if (id && id !== state.selectedId) selectExecution(id); });

  initTheme();
  refreshAll();
  setInterval(() => { if (!document.hidden) refreshAll(); }, 2500);
  setInterval(updateFreshness, 1000);
})();
`;
