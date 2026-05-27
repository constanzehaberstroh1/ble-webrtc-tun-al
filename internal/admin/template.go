package admin

const dashboardHTML = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>BLE Tunnel — Admin Panel</title>
    <link rel="stylesheet" href="/admin/static/xterm.min.css">
    <style>
        *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
        :root {
            --bg-primary: #0a0e1a;
            --bg-secondary: #111827;
            --bg-card: #1a1f35;
            --bg-card-hover: #1f2545;
            --border: #2a3050;
            --text-primary: #e8eaf0;
            --text-secondary: #8892b0;
            --text-muted: #5a6380;
            --accent: #6366f1;
            --accent-glow: rgba(99,102,241,0.3);
            --green: #10b981;
            --green-glow: rgba(16,185,129,0.2);
            --red: #ef4444;
            --red-glow: rgba(239,68,68,0.2);
            --yellow: #f59e0b;
            --cyan: #06b6d4;
        }
        body {
            font-family: 'Inter Variable', 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            min-height: 100vh;
            overflow-x: hidden;
        }
        body::before {
            content: '';
            position: fixed;
            inset: 0;
            background: 
                radial-gradient(ellipse at 20% 50%, rgba(99,102,241,0.08) 0%, transparent 50%),
                radial-gradient(ellipse at 80% 20%, rgba(6,182,212,0.06) 0%, transparent 50%),
                radial-gradient(ellipse at 50% 80%, rgba(16,185,129,0.05) 0%, transparent 50%);
            pointer-events: none;
            z-index: 0;
        }
        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 24px;
            position: relative;
            z-index: 1;
        }
        .header {
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 32px;
            padding-bottom: 20px;
            border-bottom: 1px solid var(--border);
        }
        .header-left {
            display: flex;
            align-items: center;
            gap: 16px;
        }
        .logo {
            width: 48px;
            height: 48px;
            background: linear-gradient(135deg, var(--accent), var(--cyan));
            border-radius: 14px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 22px;
            font-weight: 700;
            color: white;
            box-shadow: 0 4px 20px var(--accent-glow);
        }
        .header-title h1 {
            font-size: 22px;
            font-weight: 700;
            background: linear-gradient(135deg, var(--text-primary), var(--cyan));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        .header-title p {
            font-size: 13px;
            color: var(--text-secondary);
            margin-top: 2px;
        }
        .status-badge {
            display: flex;
            align-items: center;
            gap: 8px;
            padding: 8px 16px;
            border-radius: 100px;
            font-size: 13px;
            font-weight: 500;
            transition: all 0.3s;
        }
        .status-badge.connected {
            background: var(--green-glow);
            color: var(--green);
            border: 1px solid rgba(16,185,129,0.3);
        }
        .status-badge.disconnected {
            background: var(--red-glow);
            color: var(--red);
            border: 1px solid rgba(239,68,68,0.3);
        }
        .status-dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            animation: pulse 2s infinite;
        }
        .connected .status-dot { background: var(--green); }
        .disconnected .status-dot { background: var(--red); }
        @keyframes pulse {
            0%, 100% { opacity: 1; transform: scale(1); }
            50% { opacity: 0.5; transform: scale(0.85); }
        }
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
            gap: 14px;
            margin-bottom: 24px;
        }
        .stat-card {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 16px;
            padding: 18px;
            transition: all 0.3s ease;
            position: relative;
            overflow: hidden;
        }
        .stat-card::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 3px;
            border-radius: 16px 16px 0 0;
        }
        .stat-card:nth-child(1)::before { background: linear-gradient(90deg, var(--accent), var(--cyan)); }
        .stat-card:nth-child(2)::before { background: linear-gradient(90deg, var(--green), var(--cyan)); }
        .stat-card:nth-child(3)::before { background: linear-gradient(90deg, var(--cyan), var(--accent)); }
        .stat-card:nth-child(4)::before { background: linear-gradient(90deg, var(--yellow), var(--green)); }
        .stat-card:nth-child(5)::before { background: linear-gradient(90deg, #8b5cf6, var(--accent)); }
        .stat-card:nth-child(6)::before { background: linear-gradient(90deg, var(--red), var(--yellow)); }
        .stat-card:nth-child(7)::before { background: linear-gradient(90deg, var(--green), #8b5cf6); }
        .stat-card:nth-child(8)::before { background: linear-gradient(90deg, var(--cyan), var(--green)); }
        .stat-card:hover {
            border-color: var(--accent);
            transform: translateY(-2px);
            box-shadow: 0 8px 32px rgba(0,0,0,0.3);
        }
        .stat-label {
            font-size: 11px;
            color: var(--text-muted);
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 6px;
        }
        .stat-value {
            font-size: 24px;
            font-weight: 700;
            font-variant-numeric: tabular-nums;
            transition: color 0.3s;
        }
        .stat-sub {
            font-size: 11px;
            color: var(--text-muted);
            margin-top: 4px;
        }
        .stat-unit {
            font-size: 13px;
            color: var(--text-secondary);
            font-weight: 400;
            margin-left: 3px;
        }
        .panels {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
            margin-bottom: 24px;
        }
        @media (max-width: 900px) {
            .panels { grid-template-columns: 1fr; }
        }
        .panel {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 16px;
            overflow: hidden;
            display: flex;
            flex-direction: column;
        }
        .panel-header {
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 16px 20px;
            border-bottom: 1px solid var(--border);
            background: rgba(255,255,255,0.02);
        }
        .panel-header h2 {
            font-size: 15px;
            font-weight: 600;
            display: flex;
            align-items: center;
            gap: 8px;
        }
        .panel-body {
            flex: 1;
            padding: 0;
            min-height: 300px;
            max-height: 450px;
            overflow: hidden;
            display: flex;
            flex-direction: column;
        }
        .log-container {
            flex: 1;
            overflow-y: auto;
            padding: 12px 16px;
            font-family: 'SFMono-Regular', 'Menlo', 'Consolas', 'DejaVu Sans Mono', monospace;
            font-size: 12px;
            line-height: 1.7;
            scrollbar-width: thin;
            scrollbar-color: var(--border) transparent;
        }
        .log-container::-webkit-scrollbar { width: 6px; }
        .log-container::-webkit-scrollbar-track { background: transparent; }
        .log-container::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
        .log-entry {
            display: flex;
            gap: 8px;
            padding: 3px 0;
            border-bottom: 1px solid rgba(255,255,255,0.03);
        }
        .log-time { color: var(--text-muted); white-space: nowrap; }
        .log-level {
            font-weight: 600;
            white-space: nowrap;
            min-width: 44px;
        }
        .log-level.info { color: var(--cyan); }
        .log-level.warn { color: var(--yellow); }
        .log-level.error { color: var(--red); }
        .log-msg { color: var(--text-secondary); word-break: break-all; }
        .panel-terminal { grid-column: 1 / -1; }
        .panel-terminal .panel-body {
            min-height: 420px;
            max-height: 600px;
            padding: 0;
        }
        .terminal-wrapper {
            flex: 1;
            position: relative;
            overflow: hidden;
        }
        .terminal-wrapper .xterm {
            height: 100%;
            padding: 8px;
        }
        .terminal-toolbar {
            display: flex;
            align-items: center;
            gap: 8px;
        }
        .terminal-toolbar .term-btn {
            background: rgba(255,255,255,0.05);
            border: 1px solid var(--border);
            border-radius: 6px;
            color: var(--text-secondary);
            padding: 4px 10px;
            cursor: pointer;
            font-size: 11px;
            font-family: 'Inter Variable', 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
            transition: all 0.15s;
            display: flex;
            align-items: center;
            gap: 5px;
        }
        .terminal-toolbar .term-btn:hover {
            background: var(--bg-card-hover);
            color: var(--text-primary);
            border-color: var(--accent);
        }
        .terminal-toolbar .term-status { font-size: 11px; color: var(--text-muted); margin-left: auto; }
        .terminal-toolbar .term-status.live { color: var(--green); }
        .btn-icon {
            background: none;
            border: 1px solid var(--border);
            border-radius: 8px;
            color: var(--text-secondary);
            padding: 6px 10px;
            cursor: pointer;
            font-size: 12px;
            transition: all 0.2s;
        }
        .btn-icon:hover {
            background: var(--bg-card-hover);
            color: var(--text-primary);
            border-color: var(--accent);
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div class="header-left">
                <div class="logo">BT</div>
                <div class="header-title">
                    <h1>BLE Tunnel</h1>
                    <p>WebRTC VPN over Bale Infrastructure</p>
                </div>
            </div>
            <div id="statusBadge" class="status-badge disconnected">
                <span class="status-dot"></span>
                <span id="statusText">Disconnected</span>
            </div>
        </div>

        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-label">Channels</div>
                <div class="stat-value" style="color:var(--accent)"><span id="activeChannels">0</span><span class="stat-unit">/ <span id="totalChannels">0</span></span></div>
                <div class="stat-sub" id="channelMode">—</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Bale WS</div>
                <div class="stat-value" id="baleStatus" style="font-size:16px;color:var(--red)">Disconnected</div>
                <div class="stat-sub" id="roomInfo">—</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">↑ Sent</div>
                <div class="stat-value" style="color:var(--cyan)"><span id="bytesSent">0</span><span class="stat-unit" id="bytesSentUnit">B</span></div>
                <div class="stat-sub" id="speedUp">0 B/s</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">↓ Received</div>
                <div class="stat-value" style="color:var(--green)"><span id="bytesRecv">0</span><span class="stat-unit" id="bytesRecvUnit">B</span></div>
                <div class="stat-sub" id="speedDown">0 B/s</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Total Transfer</div>
                <div class="stat-value"><span id="totalTransfer">0</span><span class="stat-unit" id="totalTransferUnit">B</span></div>
                <div class="stat-sub" id="totalSpeed">0 B/s total</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Sessions</div>
                <div class="stat-value" id="totalSessions">0</div>
                <div class="stat-sub" id="connectedSince">—</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Active Conns</div>
                <div class="stat-value" style="color:var(--yellow)" id="activeConns">0</div>
                <div class="stat-sub">proxy streams</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Uptime</div>
                <div class="stat-value" id="uptime">0<span class="stat-unit">s</span></div>
                <div class="stat-sub" id="serverRole">—</div>
            </div>
        </div>

        <div class="panels">
            <div class="panel" style="grid-column: 1 / -1;">
                <div class="panel-header">
                    <h2>📋 Live Logs</h2>
                    <button class="btn-icon" onclick="clearLogs()">Clear</button>
                </div>
                <div class="panel-body">
                    <div id="logContainer" class="log-container"></div>
                </div>
            </div>

            <div class="panel panel-terminal">
                <div class="panel-header">
                    <h2>⚡ Terminal</h2>
                    <div class="terminal-toolbar">
                        <button class="term-btn" onclick="reconnectTerminal()" title="Reconnect">🔄 Reconnect</button>
                        <button class="term-btn" onclick="termFitToWindow()" title="Fit">↔ Fit</button>
                        <button class="term-btn" onclick="termClear()" title="Clear scrollback">🗑 Clear</button>
                        <span id="termStatus" class="term-status">Connecting…</span>
                    </div>
                </div>
                <div class="panel-body">
                    <div id="terminalContainer" class="terminal-wrapper"></div>
                </div>
            </div>
        </div>
    </div>

    <script src="/admin/static/xterm.min.js"></script>
    <script src="/admin/static/addon-fit.min.js"></script>
    <script src="/admin/static/addon-web-links.min.js"></script>
    <script src="/admin/static/addon-search.min.js"></script>
    <script>
        function formatBytes(bytes) {
            if (!bytes || bytes === 0) return { value: '0', unit: 'B' };
            const k = 1024;
            const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
            const i = Math.floor(Math.log(Math.abs(bytes)) / Math.log(k));
            const val = parseFloat((bytes / Math.pow(k, i)).toFixed(1));
            return { value: val, unit: sizes[i] };
        }
        function formatSpeed(bytesPerSec) {
            const f = formatBytes(bytesPerSec);
            return f.value + ' ' + f.unit + '/s';
        }
        function parseUptime(str) {
            if (!str) return 0;
            let s = 0;
            const hm = str.match(/(\d+)h/); if (hm) s += parseInt(hm[1])*3600;
            const mm = str.match(/(\d+)m/); if (mm) s += parseInt(mm[1])*60;
            const sm = str.match(/(\d+)s/); if (sm) s += parseInt(sm[1]);
            return s;
        }

        async function pollStatus() {
            try {
                const resp = await fetch('/api/status');
                const data = await resp.json();
                const t = data.tunnel || {};

                const badge = document.getElementById('statusBadge');
                const text = document.getElementById('statusText');
                if (t.bale_connected) {
                    badge.className = 'status-badge connected';
                    text.textContent = t.tunnel_active ? 'Tunnel Active' : 'Waiting for Call';
                } else {
                    badge.className = 'status-badge disconnected';
                    text.textContent = 'Disconnected';
                }

                const baleEl = document.getElementById('baleStatus');
                if (t.bale_connected) {
                    baleEl.textContent = t.tunnel_active ? '🟢 Active' : '🟡 Waiting';
                    baleEl.style.color = t.tunnel_active ? 'var(--green)' : 'var(--yellow)';
                } else {
                    baleEl.textContent = '🔴 Offline';
                    baleEl.style.color = 'var(--red)';
                }
                document.getElementById('roomInfo').textContent = t.room_id ? 'Room: ' + t.room_id.slice(0,8) + '…' : '—';

                document.getElementById('activeChannels').textContent = t.active_channels || 0;
                document.getElementById('totalChannels').textContent = t.total_channels || 0;
                document.getElementById('channelMode').textContent = t.mode || '—';

                const sent = formatBytes(t.bytes_sent || 0);
                document.getElementById('bytesSent').textContent = sent.value;
                document.getElementById('bytesSentUnit').textContent = sent.unit;
                document.getElementById('speedUp').textContent = '↑ ' + formatSpeed(t.speed_up || 0);

                const recv = formatBytes(t.bytes_received || 0);
                document.getElementById('bytesRecv').textContent = recv.value;
                document.getElementById('bytesRecvUnit').textContent = recv.unit;
                document.getElementById('speedDown').textContent = '↓ ' + formatSpeed(t.speed_down || 0);

                const total = (t.bytes_sent||0) + (t.bytes_received||0);
                const tf = formatBytes(total);
                document.getElementById('totalTransfer').textContent = tf.value;
                document.getElementById('totalTransferUnit').textContent = tf.unit;
                document.getElementById('totalSpeed').textContent = formatSpeed((t.speed_up||0) + (t.speed_down||0)) + ' total';

                document.getElementById('totalSessions').textContent = t.total_sessions || 0;
                document.getElementById('connectedSince').textContent = t.connected_since ? 'since ' + t.connected_since : '—';
                document.getElementById('activeConns').textContent = t.active_connections || 0;
                document.getElementById('uptime').innerHTML = data.uptime || '0s';
                document.getElementById('serverRole').textContent = data.server_role || '—';
            } catch (e) {
                console.error('Status poll error:', e);
            }
        }
        setInterval(pollStatus, 2000);
        pollStatus();

        const logContainer = document.getElementById('logContainer');
        function connectLogs() {
            const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
            const ws = new WebSocket(proto + '//' + location.host + '/api/logs/ws');
            ws.onmessage = (e) => { addLogEntry(JSON.parse(e.data)); };
            ws.onclose = () => setTimeout(connectLogs, 3000);
            ws.onerror = () => ws.close();
        }
        function addLogEntry(entry) {
            const div = document.createElement('div');
            div.className = 'log-entry';
            div.innerHTML =
                '<span class="log-time">' + entry.time + '</span>' +
                '<span class="log-level ' + entry.level + '">' + entry.level.toUpperCase() + '</span>' +
                '<span class="log-msg">' + escapeHtml(entry.message) + '</span>';
            logContainer.appendChild(div);
            logContainer.scrollTop = logContainer.scrollHeight;
            while (logContainer.children.length > 500) logContainer.removeChild(logContainer.firstChild);
        }
        function clearLogs() { logContainer.innerHTML = ''; }
        fetch('/api/logs').then(r => r.json()).then(logs => logs.forEach(addLogEntry)).catch(() => {});
        connectLogs();

        let term, fitAddon, termWs;
        const termStatus = document.getElementById('termStatus');
        function initTerminal() {
            const container = document.getElementById('terminalContainer');
            container.innerHTML = '';
            term = new Terminal({
                cursorBlink: true, cursorStyle: 'bar', fontSize: 14,
                fontFamily: "'SFMono-Regular', 'Menlo', 'Consolas', 'DejaVu Sans Mono', monospace",
                theme: {
                    background: '#0d1117', foreground: '#c9d1d9', cursor: '#58a6ff',
                    cursorAccent: '#0d1117', selectionBackground: 'rgba(56,139,253,0.3)',
                    black: '#484f58', red: '#ff7b72', green: '#3fb950', yellow: '#d29922',
                    blue: '#58a6ff', magenta: '#bc8cff', cyan: '#39d353', white: '#b1bac4',
                    brightBlack: '#6e7681', brightRed: '#ffa198', brightGreen: '#56d364',
                    brightYellow: '#e3b341', brightBlue: '#79c0ff', brightMagenta: '#d2a8ff',
                    brightCyan: '#56d364', brightWhite: '#f0f6fc',
                },
                scrollback: 10000, allowProposedApi: true, allowTransparency: true,
                drawBoldTextInBrightColors: true, macOptionIsMeta: true,
            });
            fitAddon = new FitAddon.FitAddon();
            term.loadAddon(fitAddon);
            term.loadAddon(new WebLinksAddon.WebLinksAddon());
            term.loadAddon(new SearchAddon.SearchAddon());
            term.open(container);
            setTimeout(() => { fitAddon.fit(); connectTerminalWS(); }, 100);
            new ResizeObserver(() => { if (fitAddon) { fitAddon.fit(); sendResize(); } }).observe(container);
            term.attachCustomKeyEventHandler((e) => {
                if (e.ctrlKey && e.key === 'l' && e.type === 'keydown') { term.clear(); return false; }
                return true;
            });
        }
        function connectTerminalWS() {
            const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
            termWs = new WebSocket(proto + '//' + location.host + '/api/shell/ws');
            termWs.binaryType = 'arraybuffer';
            termWs.onopen = () => { termStatus.textContent = '● Connected'; termStatus.className = 'term-status live'; setTimeout(sendResize, 200); };
            termWs.onmessage = (e) => { term.write(e.data instanceof ArrayBuffer ? new Uint8Array(e.data) : e.data); };
            termWs.onclose = () => { termStatus.textContent = '○ Disconnected'; termStatus.className = 'term-status'; term.write('\r\n\x1b[33m--- Session ended ---\x1b[0m\r\n'); };
            termWs.onerror = () => { termStatus.textContent = '✕ Error'; termStatus.className = 'term-status'; };
            term.onData((data) => { if (termWs && termWs.readyState === WebSocket.OPEN) termWs.send(data); });
            term.onBinary((data) => { if (termWs && termWs.readyState === WebSocket.OPEN) { const b = new Uint8Array(data.length); for (let i=0;i<data.length;i++) b[i]=data.charCodeAt(i)&0xFF; termWs.send(b); } });
        }
        function sendResize() { if (termWs && termWs.readyState === WebSocket.OPEN && term) termWs.send(JSON.stringify({type:'resize',cols:term.cols,rows:term.rows})); }
        function reconnectTerminal() { if (termWs) termWs.close(); term.reset(); termStatus.textContent = 'Connecting…'; termStatus.className = 'term-status'; setTimeout(connectTerminalWS, 300); }
        function termFitToWindow() { if (fitAddon) { fitAddon.fit(); sendResize(); } }
        function termClear() { if (term) term.clear(); }
        function escapeHtml(str) { const d = document.createElement('div'); d.textContent = str; return d.innerHTML; }
        initTerminal();
    </script>
</body>
</html>`
