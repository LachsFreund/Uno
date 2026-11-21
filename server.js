const express = require('express');
const http = require('http');
const path = require('path');
const { WebSocketServer } = require('ws');

const app = express();
app.use(express.static(path.join(__dirname, 'public')));

const server = http.createServer(app);
const wss = new WebSocketServer({ server, maxPayload: 256 * 1024 });

const docs = new Map(); // "games/ABCD" -> { data, t }
const subs = new Map(); // "games/ABCD" -> Set<WebSocket>
const okPath = p => typeof p === 'string' && /^games\/[A-Z]{4}$/.test(p);
const send = (ws, o) => ws.readyState === 1 && ws.send(JSON.stringify(o));
const docOf = p => docs.get(p)?.data ?? null;

function broadcast(p) {
  subs.get(p)?.forEach(c => send(c, { t: 'doc', path: p, data: docOf(p), now: Date.now() }));
}

wss.on('connection', ws => {
  ws.alive = true;
  ws.paths = new Set();
  ws.on('pong', () => (ws.alive = true));
  ws.on('message', raw => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }
    if (!okPath(m.path)) return;
    if (m.t === 'get') send(ws, { t: 'res', id: m.id, data: docOf(m.path), now: Date.now() });
    else if (m.t === 'set') {
      const cur = docs.get(m.path)?.data;
      // Veraltete Schreibzugriffe ablehnen (verhindert doppelte Züge/Strafen)
      if (m.ifV !== undefined && cur && (cur.v || 0) !== m.ifV) {
        send(ws, { t: 'res', id: m.id, data: false, now: Date.now() });
        return;
      }
      if (m.data && typeof m.data === 'object') m.data.ts = Date.now(); // Zugstart in Serverzeit
      docs.set(m.path, { data: m.data, t: Date.now() });
      send(ws, { t: 'res', id: m.id, data: true, now: Date.now() });
      broadcast(m.path);
    } else if (m.t === 'sub') {
      if (!subs.has(m.path)) subs.set(m.path, new Set());
      subs.get(m.path).add(ws);
      ws.paths.add(m.path);
      send(ws, { t: 'doc', path: m.path, data: docOf(m.path), now: Date.now() });
    } else if (m.t === 'unsub') {
      subs.get(m.path)?.delete(ws);
      ws.paths.delete(m.path);
    }
  });
  ws.on('close', () => ws.paths.forEach(p => subs.get(p)?.delete(ws)));
});

// Verbindungen wachhalten und tote Clients entfernen
setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws.alive) return ws.terminate();
    ws.alive = false;
    ws.ping();
  });
}, 30000);

// Spiele löschen, die 12 Stunden nicht mehr benutzt wurden
setInterval(() => {
  const cutoff = Date.now() - 12 * 3600 * 1000;
  for (const [p, d] of docs) if (d.t < cutoff) { docs.delete(p); subs.delete(p); }
}, 10 * 60 * 1000);

server.listen(process.env.PORT || 3000, () => console.log('UNO läuft'));
