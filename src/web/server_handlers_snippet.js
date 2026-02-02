
_handleVersionRequest(req, res) {
    if (!req || req.method !== 'GET') {
        res.writeHead(405, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Method Not Allowed' }));
        return;
    }
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({
        version: this.appVersion,
        timezone: this.appTimezone
    }));
}

_handleNodesRequest(req, res) {
    if (!req || req.method !== 'GET') {
        res.writeHead(405, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Method Not Allowed' }));
        return;
    }
    try {
        const nodes = this._buildNodeSnapshot();
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify(nodes));
    } catch (err) {
        console.error('API /api/nodes 處理失敗:', err);
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Internal Server Error' }));
    }
}
