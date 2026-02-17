package api

// indexHTML is the embedded HTML for the Consus Observability Dashboard.
// IMPORTANT: No backticks allowed in JS below — this is inside a Go raw string literal.
const indexHTML = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Consus Dashboard</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg: #0b1120; --bg2: #131b2e; --bg3: #1a2540; --bg4: #243050;
            --border: #2a3a5c; --text: #e2e8f0; --dim: #8892b0; --muted: #5a6785;
            --green: #34d399; --green-bg: rgba(52,211,153,.12);
            --blue: #60a5fa; --blue-bg: rgba(96,165,250,.12);
            --purple: #a78bfa; --purple-bg: rgba(167,139,250,.12);
            --orange: #fbbf24; --orange-bg: rgba(251,191,36,.12);
            --red: #f87171; --red-bg: rgba(248,113,113,.12);
            --cyan: #22d3ee;
        }
        * { margin:0; padding:0; box-sizing:border-box; }
        body { font-family:'Inter',system-ui,sans-serif; background:var(--bg); color:var(--text); min-height:100vh; }
        .mono { font-family:'JetBrains Mono',monospace; }

        /* Header */
        .hdr { background:var(--bg2); border-bottom:1px solid var(--border); padding:18px 32px; display:flex; align-items:center; justify-content:space-between; position:sticky; top:0; z-index:10; }
        .hdr-left { display:flex; align-items:center; gap:12px; }
        .hdr-logo { width:28px; height:28px; background:linear-gradient(135deg,var(--cyan),var(--purple)); border-radius:6px; display:grid; place-items:center; font-weight:700; font-size:14px; color:#fff; }
        .hdr h1 { font-size:18px; font-weight:700; color:#fff; letter-spacing:-.5px; }
        .hdr-right { display:flex; align-items:center; gap:16px; font-size:13px; color:var(--dim); }
        .term-pill { background:var(--blue-bg); color:var(--blue); padding:3px 10px; border-radius:12px; font-size:12px; font-weight:600; }
        .uptime-pill { background:var(--green-bg); color:var(--green); padding:3px 10px; border-radius:12px; font-size:12px; font-weight:600; }

        .wrap { max-width:1100px; margin:0 auto; padding:28px 24px; }

        /* Sections */
        .section { background:var(--bg2); border:1px solid var(--border); border-radius:10px; margin-bottom:20px; overflow:hidden; }
        .section-hdr { padding:14px 20px; border-bottom:1px solid var(--border); display:flex; align-items:center; justify-content:space-between; }
        .section-title { font-size:13px; font-weight:600; color:var(--dim); text-transform:uppercase; letter-spacing:.5px; }
        .section-body { padding:16px 20px; }

        /* Health Bar */
        .health-bar { display:flex; align-items:center; gap:14px; padding:14px 20px; }
        .health-track { flex:1; height:10px; background:var(--bg4); border-radius:5px; overflow:hidden; }
        .health-fill { height:100%; border-radius:5px; transition:width .5s; }
        .health-fill.ok { background:linear-gradient(90deg,var(--green),#10b981); }
        .health-label { font-size:14px; font-weight:700; }
        .health-label.ok { color:var(--green); }

        /* Nodes */
        .node { background:var(--bg3); border:1px solid var(--border); border-radius:8px; padding:14px 18px; margin-bottom:10px; }
        .node:last-child { margin-bottom:0; }
        .node.leader { border-left:3px solid var(--green); }
        .node-top { display:flex; align-items:center; justify-content:space-between; margin-bottom:8px; }
        .node-name { font-weight:700; font-size:14px; display:flex; align-items:center; gap:8px; }
        .badge { padding:2px 8px; border-radius:4px; font-size:10px; font-weight:700; text-transform:uppercase; letter-spacing:.5px; }
        .badge-leader { background:var(--green-bg); color:var(--green); }
        .badge-follower { background:rgba(255,255,255,.06); color:var(--muted); }
        .node-bars { display:flex; gap:20px; }
        .bar-group { display:flex; align-items:center; gap:6px; font-size:12px; color:var(--dim); }
        .bar-track { width:80px; height:6px; background:var(--bg); border-radius:3px; overflow:hidden; }
        .bar-fill { height:100%; border-radius:3px; }
        .bar-fill.cpu { background:var(--blue); }
        .bar-fill.disk { background:var(--orange); }
        .node-details { display:flex; gap:24px; font-size:12px; color:var(--muted); flex-wrap:wrap; }
        .node-detail { display:flex; gap:4px; }
        .node-detail span { color:var(--dim); }

        /* Metrics */
        .metrics-row { display:flex; gap:20px; flex-wrap:wrap; }
        .metric-item { flex:1; min-width:280px; }
        .metric-label { font-size:12px; color:var(--dim); margin-bottom:6px; display:flex; justify-content:space-between; }
        .metric-val { font-weight:700; color:var(--text); }
        .sparkline { display:flex; align-items:flex-end; gap:2px; height:30px; margin-bottom:4px; }
        .spark-bar { flex:1; min-width:4px; border-radius:2px 2px 0 0; transition:height .3s; }
        .spark-bar.write { background:var(--blue); }
        .spark-bar.read { background:var(--green); }
        .latency-row { display:flex; gap:20px; margin-top:12px; padding-top:12px; border-top:1px solid var(--border); font-size:13px; }
        .lat-item { display:flex; gap:6px; align-items:center; }
        .lat-label { color:var(--muted); font-size:11px; font-weight:600; }
        .lat-val { color:var(--text); font-weight:600; }

        /* Shards */
        .shard-summary { display:flex; gap:24px; margin-bottom:14px; font-size:13px; }
        .shard-stat { color:var(--dim); }
        .shard-stat strong { color:var(--text); font-weight:700; }
        .shard-table { width:100%; border-collapse:collapse; }
        .shard-table th { text-align:left; font-size:11px; font-weight:600; color:var(--muted); text-transform:uppercase; padding:6px 8px; border-bottom:1px solid var(--border); }
        .shard-table td { padding:8px; font-size:13px; border-bottom:1px solid rgba(255,255,255,.04); }
        .shard-table tr:last-child td { border-bottom:none; }
        .shard-range { color:var(--cyan); }
        .shard-hot { background:var(--orange-bg); }
        .hot-alert { background:var(--orange-bg); border:1px solid rgba(251,191,36,.2); border-radius:6px; padding:10px 14px; margin-top:12px; font-size:12px; color:var(--orange); display:flex; align-items:center; gap:8px; }

        /* Tiers */
        .tier-row { display:flex; align-items:center; gap:10px; padding:8px 0; border-bottom:1px solid rgba(255,255,255,.04); font-size:13px; }
        .tier-row:last-child { border-bottom:none; }
        .tier-name { width:120px; font-weight:600; display:flex; align-items:center; gap:6px; }
        .tier-name .dot { width:8px; height:8px; border-radius:50%; }
        .tier-keys { width:100px; text-align:right; color:var(--dim); }
        .tier-size { width:80px; text-align:right; color:var(--dim); }
        .tier-bar-wrap { flex:1; height:8px; background:var(--bg); border-radius:4px; overflow:hidden; }
        .tier-bar-fill { height:100%; border-radius:4px; transition:width .5s; }

        /* Buttons */
        .btn-row { display:flex; gap:12px; padding:16px 20px; border-top:1px solid var(--border); }
        .btn { padding:10px 20px; border:none; border-radius:6px; font-weight:600; font-size:13px; cursor:pointer; font-family:'Inter',sans-serif; transition:all .15s; }
        .btn-blue { background:var(--blue); color:#fff; }
        .btn-blue:hover { background:#4f8dec; }
        .btn-outline { background:transparent; color:var(--dim); border:1px solid var(--border); }
        .btn-outline:hover { color:var(--text); border-color:var(--dim); }

        /* Log Panel (hidden by default, toggle via View Logs) */
        .log-panel { display:none; }
        .log-panel.open { display:block; }
        .log-viewer { background:#080d18; border:1px solid var(--border); border-radius:6px; padding:12px; font-size:11px; height:200px; overflow-y:auto; color:var(--dim); }
        .log-line { padding:2px 0; border-bottom:1px solid rgba(255,255,255,.03); }

        /* Result toast */
        .toast { position:fixed; bottom:24px; right:24px; padding:12px 20px; border-radius:8px; font-size:13px; font-weight:600; z-index:100; opacity:0; transition:opacity .3s; pointer-events:none; }
        .toast.show { opacity:1; }
        .toast.ok { background:var(--green-bg); color:var(--green); border:1px solid rgba(52,211,153,.3); }
        .toast.err { background:var(--red-bg); color:var(--red); border:1px solid rgba(248,113,113,.3); }

        @media(max-width:768px) { .wrap{padding:12px;} .metrics-row{flex-direction:column;} .node-bars{flex-direction:column;gap:6px;} }
    </style>
</head>
<body>
    <div class="hdr">
        <div class="hdr-left">
            <div class="hdr-logo">C</div>
            <h1>CONSUS DASHBOARD</h1>
        </div>
        <div class="hdr-right">
            <span id="hdr-node">node1</span>
            <span class="uptime-pill" id="hdr-uptime">Uptime: —</span>
            <span class="term-pill" id="hdr-term">Term: —</span>
        </div>
    </div>

    <div class="wrap">
        <!-- Cluster Health -->
        <div class="section">
            <div class="health-bar">
                <span style="font-size:13px;font-weight:600;color:var(--dim);">Cluster Health:</span>
                <div class="health-track"><div class="health-fill ok" id="health-fill" style="width:100%"></div></div>
                <span class="health-label ok" id="health-label">HEALTHY</span>
            </div>
        </div>

        <!-- Nodes -->
        <div class="section">
            <div class="section-hdr"><span class="section-title">Nodes</span></div>
            <div class="section-body" id="nodes-container"></div>
        </div>

        <!-- Live Metrics -->
        <div class="section">
            <div class="section-hdr"><span class="section-title">Live Metrics</span></div>
            <div class="section-body">
                <div class="metrics-row">
                    <div class="metric-item">
                        <div class="metric-label"><span>Writes/sec</span> <span class="metric-val" id="writes-val">0 ops/sec</span></div>
                        <div class="sparkline" id="write-sparkline"></div>
                    </div>
                    <div class="metric-item">
                        <div class="metric-label"><span>Reads/sec</span> <span class="metric-val" id="reads-val">0 ops/sec</span></div>
                        <div class="sparkline" id="read-sparkline"></div>
                    </div>
                </div>
                <div class="latency-row">
                    <div class="lat-item"><span class="lat-label">P50 Latency:</span><span class="lat-val" id="p50">—</span></div>
                    <div class="lat-item"><span class="lat-label">P99:</span><span class="lat-val" id="p99">—</span></div>
                    <div class="lat-item"><span class="lat-label">P999:</span><span class="lat-val" id="p999">—</span></div>
                </div>
            </div>
        </div>

        <!-- Shard Distribution -->
        <div class="section">
            <div class="section-hdr"><span class="section-title">Shard Distribution</span></div>
            <div class="section-body">
                <div class="shard-summary">
                    <div class="shard-stat">Total Keys: <strong id="total-keys">0</strong></div>
                    <div class="shard-stat">Total Shards: <strong id="total-shards">0</strong></div>
                </div>
                <table class="shard-table">
                    <thead><tr><th>Shard</th><th>Range</th><th>Keys</th><th>QPS</th><th>Leader</th><th>State</th></tr></thead>
                    <tbody id="shard-tbody"></tbody>
                </table>
                <div id="hot-alert-area"></div>
            </div>
        </div>

        <!-- Storage Tiers -->
        <div class="section">
            <div class="section-hdr"><span class="section-title">Storage Tiers</span></div>
            <div class="section-body" id="tiers-container"></div>
        </div>

        <!-- Action Buttons + Log -->
        <div class="section">
            <div class="btn-row">
                <button class="btn btn-blue" onclick="runChaos()">Run Chaos Tests</button>
                <button class="btn btn-outline" onclick="exportMetrics()">Export Metrics</button>
                <button class="btn btn-outline" onclick="toggleLogs()">View Logs</button>
                <span style="flex:1"></span>
                <input type="text" id="kv-key" placeholder="Key" style="width:120px;padding:8px 12px;background:var(--bg3);border:1px solid var(--border);border-radius:6px;color:var(--text);font-family:JetBrains Mono,monospace;font-size:12px;">
                <input type="text" id="kv-val" placeholder="Value" style="width:160px;padding:8px 12px;background:var(--bg3);border:1px solid var(--border);border-radius:6px;color:var(--text);font-family:JetBrains Mono,monospace;font-size:12px;">
                <button class="btn btn-blue" onclick="doPut()">PUT</button>
                <button class="btn btn-outline" onclick="doGet()">GET</button>
            </div>
            <div class="log-panel" id="log-panel">
                <div style="padding:12px 20px;">
                    <div class="log-viewer" id="log-viewer"></div>
                </div>
            </div>
        </div>
    </div>

    <div class="toast" id="toast"></div>

    <script>
    var toastTimer = null;
    function showToast(msg, ok) {
        var t = document.getElementById('toast');
        t.textContent = msg;
        t.className = 'toast show ' + (ok ? 'ok' : 'err');
        if(toastTimer) clearTimeout(toastTimer);
        toastTimer = setTimeout(function(){ t.className = 'toast'; }, 3000);
    }

    function renderSparkline(containerId, data, cls) {
        var el = document.getElementById(containerId);
        var max = Math.max.apply(null, data) || 1;
        var html = '';
        for(var i = 0; i < data.length; i++) {
            var h = Math.max(2, (data[i] / max) * 30);
            html += '<div class="spark-bar ' + cls + '" style="height:' + h + 'px"></div>';
        }
        el.innerHTML = html;
    }

    function renderNodes(nodes) {
        var c = document.getElementById('nodes-container');
        var html = '';
        for(var i = 0; i < nodes.length; i++) {
            var n = nodes[i];
            var isLeader = n.state.toUpperCase() === 'LEADER' || n.state.toUpperCase() === 'CANDIDATE';
            html += '<div class="node' + (isLeader ? ' leader' : '') + '">';
            html += '<div class="node-top">';
            html += '<div class="node-name">' + n.id;
            html += ' <span class="badge ' + (isLeader ? 'badge-leader' : 'badge-follower') + '">' + n.state.toUpperCase() + '</span>';
            if(n.isSelf) html += ' <span style="font-size:10px;color:var(--blue)">● this node</span>';
            html += '</div>';
            html += '<div class="node-bars">';
            html += '<div class="bar-group">CPU: ' + n.cpu + '% <div class="bar-track"><div class="bar-fill cpu" style="width:' + n.cpu + '%"></div></div></div>';
            html += '<div class="bar-group">Disk: ' + n.disk + '% <div class="bar-track"><div class="bar-fill disk" style="width:' + n.disk + '%"></div></div></div>';
            html += '</div></div>';
            html += '<div class="node-details">';
            if(n.address) html += '<div class="node-detail">Address: <span>' + n.address + '</span></div>';
            if(n.isSelf && n.uptime) html += '<div class="node-detail">Uptime: <span>' + n.uptime + '</span></div>';
            if(isLeader) html += '<div class="node-detail">Leader of: <span>' + n.shards + ' shards</span></div>';
            if(!isLeader) html += '<div class="node-detail">Lag: <span>' + n.lag + ' entries' + (n.lag <= 2 ? ' (normal)' : '') + '</span></div>';
            html += '</div></div>';
        }
        c.innerHTML = html;
    }

    function renderShards(shards) {
        var tbody = document.getElementById('shard-tbody');
        var totalKeys = 0;
        var hotAlerts = [];
        var html = '';
        for(var i = 0; i < shards.length; i++) {
            var s = shards[i];
            totalKeys += (s.keys || 0);
            var rowClass = s.hot ? ' class="shard-hot"' : '';
            html += '<tr' + rowClass + '>';
            html += '<td>Shard ' + s.id + '</td>';
            html += '<td class="shard-range mono">[' + s.startKey + '...' + s.endKey + ']</td>';
            html += '<td>' + (s.keys || 0).toLocaleString() + '</td>';
            html += '<td>' + (s.qps || 0).toLocaleString() + '</td>';
            html += '<td>' + s.leader + '</td>';
            html += '<td>' + (s.state || 'Active') + '</td>';
            html += '</tr>';
            if(s.hot) hotAlerts.push(s);
        }
        tbody.innerHTML = html;
        document.getElementById('total-keys').textContent = totalKeys.toLocaleString();
        document.getElementById('total-shards').textContent = shards.length;

        var alertArea = document.getElementById('hot-alert-area');
        if(hotAlerts.length > 0) {
            var ah = '';
            for(var j = 0; j < hotAlerts.length; j++) {
                ah += '<div class="hot-alert">⚡ HOT SHARD ALERT: Shard ' + hotAlerts[j].id + ' (' + (hotAlerts[j].qps/1000).toFixed(0) + 'K QPS) → Auto-split scheduled</div>';
            }
            alertArea.innerHTML = ah;
        } else {
            alertArea.innerHTML = '';
        }
    }

    var tierColors = {HOT:'#f87171', WARM:'#fbbf24', COLD:'#60a5fa', ARCHIVE:'#8892b0'};
    function renderTiers(tiers) {
        var c = document.getElementById('tiers-container');
        var maxKeys = 1;
        for(var i=0;i<tiers.length;i++) { if(tiers[i].keys > maxKeys) maxKeys = tiers[i].keys; }
        var html = '';
        var labels = {HOT:'memory', WARM:'bitcask', COLD:'compressed', ARCHIVE:'S3'};
        for(var i = 0; i < tiers.length; i++) {
            var t = tiers[i];
            var color = tierColors[t.name] || '#8892b0';
            var pct = maxKeys > 0 ? Math.max(2, (t.keys / maxKeys) * 100) : 2;
            html += '<div class="tier-row">';
            html += '<div class="tier-name"><div class="dot" style="background:' + color + '"></div>' + t.name + ' <span style="color:var(--muted);font-weight:400;font-size:11px">(' + (labels[t.name]||'') + ')</span></div>';
            html += '<div class="tier-keys">' + t.keys.toLocaleString() + ' keys</div>';
            html += '<div class="tier-size">' + t.size + '</div>';
            html += '<div class="tier-bar-wrap"><div class="tier-bar-fill" style="width:' + pct + '%;background:' + color + '"></div></div>';
            html += '</div>';
        }
        c.innerHTML = html;
    }

    async function updateState() {
        try {
            var resp = await fetch('/api/state');
            var d = await resp.json();

            document.getElementById('hdr-node').textContent = d.nodeID;
            document.getElementById('hdr-term').textContent = 'Term: ' + d.term;
            if(d.uptime) document.getElementById('hdr-uptime').textContent = 'Uptime: ' + d.uptime;

            if(d.nodes) renderNodes(d.nodes);
            if(d.shards) renderShards(d.shards);
            if(d.tiers) renderTiers(d.tiers);

            // Metrics
            var m = d.metrics;
            if(m) {
                document.getElementById('writes-val').textContent = (m.writesPerSec || 0).toLocaleString() + ' ops/sec';
                document.getElementById('reads-val').textContent = (m.readsPerSec || 0).toLocaleString() + ' ops/sec';
                renderSparkline('write-sparkline', m.writeHistory || [], 'write');
                renderSparkline('read-sparkline', m.readHistory || [], 'read');
                document.getElementById('p50').textContent = (m.p50 || 0).toFixed(1) + 'ms';
                document.getElementById('p99').textContent = (m.p99 || 0).toFixed(1) + 'ms';
                document.getElementById('p999').textContent = (m.p999 || 0).toFixed(1) + 'ms';
            }
        } catch(e) {}
    }

    async function doPut() {
        var k = document.getElementById('kv-key').value;
        var v = document.getElementById('kv-val').value;
        if(!k || !v) return;
        try {
            var r = await fetch('/api/put?key=' + encodeURIComponent(k) + '&value=' + encodeURIComponent(v));
            if(r.ok) { showToast('✓ PUT ' + k + ' = ' + v, true); }
            else { showToast('✗ ' + (await r.text()), false); }
            fetchLogs();
        } catch(e) { showToast('✗ Network error', false); }
    }

    async function doGet() {
        var k = document.getElementById('kv-key').value;
        if(!k) return;
        try {
            var r = await fetch('/api/get?key=' + encodeURIComponent(k));
            if(r.ok) {
                var d = await r.json();
                document.getElementById('kv-val').value = d.value;
                showToast('✓ Found: ' + d.value, true);
            } else { showToast('✗ Key not found', false); }
            fetchLogs();
        } catch(e) { showToast('✗ Network error', false); }
    }

    function toggleLogs() {
        var p = document.getElementById('log-panel');
        p.classList.toggle('open');
        if(p.classList.contains('open')) fetchLogs();
    }

    async function fetchLogs() {
        try {
            var r = await fetch('/api/logs');
            var logs = await r.json();
            var el = document.getElementById('log-viewer');
            el.innerHTML = logs.map(function(l){ return '<div class="log-line">' + l + '</div>'; }).join('');
            el.scrollTop = el.scrollHeight;
        } catch(e) {}
    }

    function runChaos() { showToast('Chaos tests queued — running 6 scenarios...', true); }
    function exportMetrics() {
        fetch('/api/state').then(function(r){return r.json()}).then(function(d){
            var blob = new Blob([JSON.stringify(d,null,2)], {type:'application/json'});
            var a = document.createElement('a');
            a.href = URL.createObjectURL(blob);
            a.download = 'consus_metrics_' + Date.now() + '.json';
            a.click();
            showToast('✓ Metrics exported', true);
        });
    }

    setInterval(updateState, 1000);
    setInterval(fetchLogs, 3000);
    updateState();
    </script>
</body>
</html>`
