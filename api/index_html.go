package api

// indexHTML is the embedded HTML for the dashboard UI.
const indexHTML = `<!DOCTYPE html>
<html>
<head>
    <title>Consus Dashboard</title>
    <style>
        body { font-family: sans-serif; background: #f5f5f5; color: #333; margin: 0; padding: 20px; }
        .container { max-width: 1000px; margin: 0 auto; }
        h1 { text-align: center; color: #2c3e50; }
        
        .cluster-map { display: flex; justify-content: center; gap: 20px; margin-bottom: 40px; }
        .node-card { background: white; border-radius: 8px; padding: 20px; width: 150px; text-align: center; box-shadow: 0 2px 5px rgba(0,0,0,0.1); border-top: 5px solid #ccc; }
        .node-card.leader { border-top-color: #27ae60; }
        .node-card.self { background: #e8f6f3; }
        .state-badge { display: block; font-size: 12px; font-weight: bold; margin-top: 5px; padding: 4px; border-radius: 4px; background: #eee; }
        .leader .state-badge { background: #27ae60; color: white; }
        
        .control-panel { background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }
        .row { display: flex; gap: 10px; margin-bottom: 10px; align-items: center; }
        input { padding: 10px; border: 1px solid #ddd; border-radius: 4px; flex: 1; }
        button { padding: 10px 20px; background: #3498db; color: white; border: none; border-radius: 4px; cursor: pointer; }
        button:hover { background: #2980b9; }
        
        .log-viewer { background: #1e1e1e; color: #d4d4d4; padding: 15px; border-radius: 8px; font-family: monospace; font-size: 13px; height: 300px; overflow-y: auto; }
        .log-entry { margin-bottom: 2px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Consus Cluster Visualizer</h1>
        
        <div id="cluster-map" class="cluster-map"></div>

        <div class="control-panel">
            <div class="row">
                <input type="text" id="key-input" placeholder="Key">
                <input type="text" id="value-input" placeholder="Value">
            </div>
            <div class="row">
                <button onclick="putData()">Put Value</button>
                <button onclick="getData()" style="background: #95a5a6;">Get Value</button>
                <span id="result-box" style="margin-left: 20px; font-weight: bold;"></span>
            </div>
        </div>

        <h3>Live Event Log</h3>
        <div id="log-viewer" class="log-viewer">
            Waiting for events...
        </div>
    </div>

    <script>
        async function updateState() {
            try {
                const resp = await fetch('/api/state');
                const data = await resp.json();
                const mapDiv = document.getElementById('cluster-map');
                mapDiv.innerHTML = '';
                data.nodes.forEach(node => {
                    const div = document.createElement('div');
                    div.className = 'node-card';
                    if(node.state === 'LEADER') div.classList.add('leader');
                    if(node.isSelf) div.classList.add('self');
                    div.innerHTML = '<h3>'+node.id+'</h3><span class="state-badge">'+node.state+'</span>'+(node.isSelf ? '<small>(This Node)</small>' : '');
                    mapDiv.appendChild(div);
                });
            } catch(e) {}
        }

        async function putData() {
            const k = document.getElementById('key-input').value;
            const v = document.getElementById('value-input').value;
            if(!k || !v) return alert('Enter key and value');
            const resp = await fetch('/api/put?key='+encodeURIComponent(k)+'&value='+encodeURIComponent(v));
            const resultSpan = document.getElementById('result-box');
            if(resp.ok) {
                resultSpan.innerText = "Write Successful!";
                resultSpan.style.color = "green";
            } else {
                resultSpan.innerText = "Error: " + await resp.text();
                resultSpan.style.color = "red";
            }
            fetchLogs();
        }

        async function getData() {
            const k = document.getElementById('key-input').value;
            if(!k) return;
            const resp = await fetch('/api/get?key='+encodeURIComponent(k));
            const resultSpan = document.getElementById('result-box');
            if(resp.ok) {
                const val = await resp.text();
                document.getElementById('value-input').value = val;
                resultSpan.innerText = "Found: " + val;
                resultSpan.style.color = "black";
            } else {
                resultSpan.innerText = "Not Found";
                resultSpan.style.color = "red";
            }
            fetchLogs();
        }

        async function fetchLogs() {
            try {
                const resp = await fetch('/api/logs');
                const logs = await resp.json();
                const viewer = document.getElementById('log-viewer');
                viewer.innerHTML = logs.map(function(l){ return '<div class="log-entry">&gt; '+l+'</div>'; }).join('');
                viewer.scrollTop = viewer.scrollHeight;
            } catch(e) {}
        }

        setInterval(updateState, 1000);
        setInterval(fetchLogs, 2000);
        updateState();
        fetchLogs();
    </script>
</body>
</html>`
