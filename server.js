/*
 * ACURA MeshCore Hub
 * ------------------
 * A small MQTT broker for the club. MeshCore observers (Chris's AC-RPT2-Observer1
 * and any others we invite) publish the packets they hear here, the same way
 * they publish to LetsMesh. The hub decodes the packet headers, keeps a rolling
 * history, and serves a JSON summary at /live for acurahamradio.org.
 *
 * Endpoints
 *   wss://<railway-domain>/mqtt   MQTT over WebSocket (observers connect here)
 *   tcp  <railway-tcp-proxy>      plain MQTT on port 1883 (optional, Railway TCP proxy)
 *   GET  /live                    JSON summary for the website
 *   GET  /pw/stats, /pw/open, /pw/world   Path Watch status and world map (see pathwatch.js)
 *   GET  /arc/start, /arc/data    worldwide Antenna Report Card tests (key required)
 *   GET  /                        health check
 *
 * Also polls LetsMesh (api.letsmesh.net) for the region, so the page has data
 * from every LetsMesh observer in the region even before any observer sends here.
 *
 * Environment variables (set in Railway > Variables)
 *   HUB_USER        username observers use to connect   (required)
 *   HUB_PASS        password observers use to connect   (required)
 *   REGION          region code to label data, default ACY
 *   CHRIS_OBSERVER  observer name to pin at the top, default AC-RPT2-Observer1
 *   ALLOW_ORIGIN    CORS origin, default https://acurahamradio.org
 *   PORT            set by Railway automatically
 */
'use strict';

const http = require('http');
const net = require('net');
const crypto = require('crypto');

const REGION = process.env.REGION || 'ACY';
const CHRIS = process.env.CHRIS_OBSERVER || 'AC-RPT2-Observer1';
const ALLOW_ORIGIN = process.env.ALLOW_ORIGIN || 'https://acurahamradio.org';
const PORT = parseInt(process.env.PORT || '8080', 10);
const MQTT_TCP_PORT = parseInt(process.env.MQTT_TCP_PORT || '1883', 10);

/* ---------------- MeshCore packet decoding ---------------- */

const ROUTES = ['TransportFlood', 'Flood', 'Direct', 'TransportDirect'];
const TYPES = {
  0: 'Request', 1: 'Response', 2: 'TextMessage', 3: 'Ack', 4: 'Advert', 5: 'GroupText',
  6: 'GroupData', 7: 'AnonRequest', 8: 'Path', 9: 'Trace', 10: 'Multipart', 11: 'Control', 15: 'RawCustom',
};

function decodeRaw(hex) {
  const clean = String(hex || '').replace(/[^0-9a-fA-F]/g, '');
  if (clean.length < 4) return null;
  const b = Buffer.from(clean, 'hex');
  let i = 0;
  const h = b[i++];
  const routeNum = h & 0x03;
  const typeNum = (h >> 2) & 0x0f;
  if (routeNum === 0 || routeNum === 3) i += 4; // transport codes
  if (i >= b.length) return null;
  const pl = b[i++];
  const hashSize = ((pl >> 6) & 0x03) + 1;
  const hops = pl & 0x3f;
  const path = [];
  for (let k = 0; k < hops; k++) {
    if (i + hashSize > b.length) return null;
    path.push(b.slice(i, i + hashSize).toString('hex'));
    i += hashSize;
  }
  const payload = b.slice(i);
  const out = {
    route: ROUTES[routeNum],
    type: TYPES[typeNum] || 'Other',
    path,
    hash: crypto.createHash('sha256').update(Buffer.concat([Buffer.from([typeNum]), payload])).digest('hex').slice(0, 16).toUpperCase(),
  };
  if (typeNum === 4) out.advert = decodeAdvert(payload);
  return out;
}

function decodeAdvert(p) {
  // pubkey(32) timestamp(4) signature(64) flags(1) [lat(4) lon(4)] [feat1(2)] [feat2(2)] [name]
  if (p.length < 101) return null;
  const pk = p.slice(0, 32).toString('hex').toUpperCase();
  let i = 100;
  const flags = p[i++];
  const role = flags & 0x0f; // 1 companion, 2 repeater, 3 room server, 4 sensor
  let lat = null, lon = null, name = '';
  if (flags & 0x10) {
    if (i + 8 > p.length) return null;
    lat = p.readInt32LE(i) / 1e6; lon = p.readInt32LE(i + 4) / 1e6; i += 8;
  }
  if (flags & 0x20) i += 2;
  if (flags & 0x40) i += 2;
  if (flags & 0x80 && i < p.length) name = p.slice(i).toString('utf8').replace(/\0.*$/, '').trim();
  return { pk, role, lat, lon, name };
}

/* ---------------- State ---------------- */

const startedAt = Math.floor(Date.now() / 1000);
const packets = [];          // newest last, each reception: {t,h,ty,rt,pa,o,snr}
const seenHashes = new Map(); // hash -> first-seen time (for unique counts)
const hours = new Map();     // 'YYYYMMDDHH' -> {n, ty:{}, rp:{}}
const nodes = new Map();     // pubkey -> {n, r, lat, lon, ls}
const observers = new Map(); // observer name -> {last, m, on, c}
const clientObserver = new Map(); // mqtt client id -> observer name

function hourKey(t) { return new Date(t * 1000).toISOString().slice(0, 13).replace(/[-T]/g, ''); }

function num(v) { const n = parseFloat(v); return Number.isFinite(n) ? n : null; }

function pick(obj, keys) { for (const k of keys) if (obj && obj[k] !== undefined && obj[k] !== null && obj[k] !== '') return obj[k]; return undefined; }

function handleMessage(topic, buf, clientId) {
  let msg;
  try { msg = JSON.parse(buf.toString('utf8')); } catch (e) { return; }
  if (!msg || typeof msg !== 'object') return;

  const parts = String(topic).split('/'); // meshcore/<IATA>/<PUBKEY>/<kind>
  const kind = (parts[3] || '').toLowerCase();
  const origin = String(pick(msg, ['origin', 'origin_name', 'name']) || parts[2] || 'observer').slice(0, 60);

  if (clientId) clientObserver.set(clientId, origin);
  const ob = observers.get(origin) || { last: 0, m: '', on: true, c: 0 };
  ob.on = true;
  const model = pick(msg, ['model', 'radio_model']);
  if (model) ob.m = String(model).slice(0, 40);
  observers.set(origin, ob);

  if (kind === 'status') return;

  const raw = pick(msg, ['raw', 'raw_data', 'hex', 'data']);
  if (!raw) return;
  const d = decodeRaw(raw);
  if (!d) return;

  const now = Math.floor(Date.now() / 1000);
  const hash = String(pick(msg, ['hash']) || d.hash).replace(/[^0-9A-Fa-f]/g, '').toUpperCase() || d.hash;
  const rec = { t: now, h: hash, ty: d.type, rt: d.route, pa: d.path, o: origin, snr: num(pick(msg, ['SNR', 'snr'])) };
  packets.push(rec);
  if (packets.length > 1500) packets.splice(0, packets.length - 1500);

  ob.last = now; ob.c++;

  if (!seenHashes.has(hash)) {
    seenHashes.set(hash, now);
    const k = hourKey(now);
    const row = hours.get(k) || { n: 0, ty: {}, rp: {} };
    row.n++;
    row.ty[d.type] = (row.ty[d.type] || 0) + 1;
    for (const p of new Set(d.path)) row.rp[p] = (row.rp[p] || 0) + 1;
    hours.set(k, row);
  }

  if (d.advert && d.advert.pk) {
    const a = d.advert;
    const n = nodes.get(a.pk) || { n: '', r: a.role, lat: null, lon: null, ls: 0 };
    if (a.name) n.n = a.name.slice(0, 40);
    n.r = a.role; n.ls = now;
    if (a.lat !== null && !(a.lat === 0 && a.lon === 0)) { n.lat = a.lat; n.lon = a.lon; }
    nodes.set(a.pk, n);
  }
}

function prune() {
  const now = Math.floor(Date.now() / 1000);
  for (const [h, t] of seenHashes) if (now - t > 7200) seenHashes.delete(h);
  const cut = hourKey(now - 49 * 3600);
  for (const k of hours.keys()) if (k < cut) hours.delete(k);
  for (const [pk, n] of nodes) if (now - n.ls > 7 * 86400) nodes.delete(pk);
}
setInterval(prune, 10 * 60 * 1000).unref();

/* ---------------- LetsMesh feed ----------------
 * LetsMesh (letsmesh.net) publishes the packets every observer in a region hears.
 * The hub polls it for our region so the website works even before an observer
 * sends to the hub directly. Packets heard both ways are counted once (by hash).
 */
const LM_API = 'https://api.letsmesh.net/api';
const lmSeen = new Map(); // LetsMesh reception id -> time
let lmStatus = { ok: false, last: 0, err: '' };

function lmTime(s) { const t = Date.parse(s); return Number.isFinite(t) ? Math.floor(t / 1000) : Math.floor(Date.now() / 1000); }

async function lmGet(path) {
  const r = await fetch(LM_API + path, { headers: { 'User-Agent': 'ACURA-MeshCore-Hub (acurahamradio.org)' }, signal: AbortSignal.timeout(20000) });
  if (!r.ok) throw new Error('LetsMesh ' + path.split('?')[0] + ' HTTP ' + r.status);
  return r.json();
}

function lmIngest(p) {
  const t = lmTime(p.heard_at);
  const hash = String(p.hash || '').toUpperCase() || ('LM' + p.id);
  const origin = String(p.origin || 'observer').slice(0, 60);
  const path = Array.isArray(p.path) ? p.path.map((x) => String(x).toLowerCase()) : [];
  const ty = String(p.payload_type || 'Other');
  const rec = { t, h: hash, ty, rt: String(p.route_type || ''), pa: path, o: origin, snr: num(p.snr) };
  packets.push(rec);

  const ob = observers.get(origin) || { last: 0, m: '', on: true, c: 0 };
  if (t > ob.last) ob.last = t;
  ob.c++;
  observers.set(origin, ob);

  if (!seenHashes.has(hash)) {
    seenHashes.set(hash, t);
    const k = hourKey(t);
    const row = hours.get(k) || { n: 0, ty: {}, rp: {} };
    row.n++;
    row.ty[ty] = (row.ty[ty] || 0) + 1;
    for (const x of new Set(path)) row.rp[x] = (row.rp[x] || 0) + 1;
    hours.set(k, row);
  }
}

async function lmPackets() {
  const list = await lmGet('/packets/filtered?region=' + encodeURIComponent(REGION) + '&limit=500');
  if (!Array.isArray(list)) return;
  const fresh = list.filter((p) => p && p.id && !lmSeen.has(String(p.id)));
  fresh.sort((a, b) => lmTime(a.heard_at) - lmTime(b.heard_at));
  for (const p of fresh) { lmSeen.set(String(p.id), lmTime(p.heard_at)); lmIngest(p); }
  packets.sort((a, b) => a.t - b.t);
  if (packets.length > 1500) packets.splice(0, packets.length - 1500);
  const cut = Math.floor(Date.now() / 1000) - 7200;
  for (const [id, t] of lmSeen) if (t < cut) lmSeen.delete(id);
}

async function lmNodes() {
  const j = await lmGet('/nodes?region=' + encodeURIComponent(REGION));
  const list = (j && j.nodes) || (Array.isArray(j) ? j : []);
  for (const x of list) {
    if (!x || !x.public_key) continue;
    const pk = String(x.public_key).toUpperCase();
    const ls = lmTime(x.last_seen);
    const n = nodes.get(pk) || { n: '', r: x.device_role, lat: null, lon: null, ls: 0 };
    if (ls < n.ls) continue; // the hub already has something newer
    if (x.name) n.n = String(x.name).slice(0, 40);
    n.r = parseInt(x.device_role, 10) || n.r;
    n.ls = ls;
    const d = x.decoded_payload || {};
    const lat = num(d.lat), lon = num(d.lon);
    if (lat !== null && lon !== null && !(lat === 0 && lon === 0)) { n.lat = lat; n.lon = lon; }
    nodes.set(pk, n);
  }
}

async function lmObservers() {
  const list = await lmGet('/observers');
  if (!Array.isArray(list)) return;
  for (const x of list) {
    if (!x || !x.name) continue;
    const reg = x.region;
    const inRegion = Array.isArray(reg) ? reg.includes(REGION) : String(reg || '').split(/[,\s]+/).includes(REGION);
    if (!inRegion) continue;
    const name = String(x.name).slice(0, 60);
    const ob = observers.get(name) || { last: 0, m: '', on: false, c: 0 };
    ob.on = !!x.is_mqtt_connected;
    if (x.model) ob.m = String(x.model).slice(0, 40);
    const lp = x.last_packet_ingested ? lmTime(x.last_packet_ingested) : 0;
    if (lp > ob.last) ob.last = lp;
    observers.set(name, ob);
  }
}

function startLetsMesh() {
  const run = async (fn, label) => {
    try { await fn(); lmStatus = { ok: true, last: Math.floor(Date.now() / 1000), err: '' }; }
    catch (e) { lmStatus.err = label + ': ' + e.message; console.error('LetsMesh', label, e.message); }
  };
  const tick = async () => { await run(lmPackets, 'packets'); };
  (async () => { await run(lmNodes, 'nodes'); await run(lmObservers, 'observers'); await tick(); })();
  setInterval(tick, 60 * 1000).unref();
  setInterval(() => run(lmObservers, 'observers'), 2 * 60 * 1000).unref();
  setInterval(() => run(lmNodes, 'nodes'), 10 * 60 * 1000).unref();
}

/* ---------------- /live summary ---------------- */

function prefixNames() {
  const map = {};
  for (const [pk, n] of nodes) {
    if (n.r !== 2 && n.r !== 3) continue;
    for (const L of [2, 4, 6]) {
      const p = pk.slice(0, L).toLowerCase();
      (map[p] = map[p] || new Set()).add(n.n || pk.slice(0, 6));
    }
  }
  const out = {};
  for (const p in map) if (map[p].size === 1) out[p] = [...map[p]][0];
  return out;
}

function live() {
  const now = Math.floor(Date.now() / 1000);
  const pfx = prefixNames();
  const nm = (p) => pfx[p] || '#' + p.toUpperCase();

  // Ticker: newest first, grouped by hash
  const groups = new Map();
  for (let i = packets.length - 1; i >= 0; i--) {
    const p = packets[i];
    let g = groups.get(p.h);
    if (!g) { g = { t: p.t, ty: p.ty, rt: p.rt, pa: p.pa, obs: new Set(), snr: p.snr }; groups.set(p.h, g); }
    if (p.t < g.t) g.t = p.t;
    if (p.pa.length < g.pa.length) g.pa = p.pa;
    if (p.snr !== null && (g.snr === null || p.snr > g.snr)) g.snr = p.snr;
    g.obs.add(p.o);
  }
  const all = [...groups.values()].sort((a, b) => b.t - a.t);
  const tick = all.slice(0, 40).map((g) => ({ t: g.t, ty: g.ty, rt: g.rt, hops: g.pa.length, via: g.pa.map(nm), obs: [...g.obs], snr: g.snr }));
  const lastHour = all.filter((g) => g.t > now - 3600).length;

  // 24-hour history
  const hrs = []; const types = {}; const rp = {}; let tot24 = 0;
  for (let i = 23; i >= 0; i--) {
    const ts = now - i * 3600;
    const row = hours.get(hourKey(ts)) || { n: 0, ty: {}, rp: {} };
    hrs.push({ t: Math.floor(ts / 3600) * 3600, n: row.n });
    tot24 += row.n;
    for (const k in row.ty) types[k] = (types[k] || 0) + row.ty[k];
    for (const k in row.rp) rp[k] = (rp[k] || 0) + row.rp[k];
  }
  const top = Object.entries(rp).sort((a, b) => b[1] - a[1]).slice(0, 10).map(([p, c]) => ({ n: nm(p), c }));

  // Observers
  const obs = [...observers.entries()].map(([n, o]) => ({ n, on: o.on, last: o.last, m: o.m, c: o.c, chris: n === CHRIS }))
    .sort((a, b) => (b.chris - a.chris) || a.n.localeCompare(b.n));

  // Map: repeaters & room servers with a location in South Jersey / Philly area. Companions are never mapped.
  const map = []; const cnt = { rep24: 0, comp24: 0, room24: 0, total: nodes.size };
  for (const [, n] of nodes) {
    if (now - n.ls < 86400) { if (n.r === 2) cnt.rep24++; else if (n.r === 1) cnt.comp24++; else if (n.r === 3) cnt.room24++; }
    if ((n.r === 2 || n.r === 3) && n.lat !== null && n.lat > 38.7 && n.lat < 40.8 && n.lon > -75.9 && n.lon < -73.6) {
      map.push([n.n || 'Unnamed', n.r, Math.round(n.lat * 1e4) / 1e4, Math.round(n.lon * 1e4) / 1e4, n.ls]);
    }
  }

  return { region: REGION, now, since: startedAt, source: lmStatus.ok ? 'hub+letsmesh' : 'hub', tick, lastHour, tot24, hours: hrs, types, top, obs, nodes: map, cnt };
}

/* ---------------- HTTP server ---------------- */

const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/live') {
    res.writeHead(200, {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': ALLOW_ORIGIN,
      'Cache-Control': 'public, max-age=20',
    });
    res.end(JSON.stringify(live()));
    return;
  }
  if (url.indexOf('/pw/') === 0 || url.indexOf('/arc/') === 0) {
    const pw = require('./pathwatch.js');
    const q = new URL(req.url || '/', 'http://x').searchParams;
    const send = (code, obj) => { res.writeHead(code, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': ALLOW_ORIGIN, 'Cache-Control': 'no-store' }); res.end(JSON.stringify(obj)); };
    if (url === '/pw/stats') return send(200, pw.stats);
    if (url === '/pw/open') return send(200, pw.openNow());
    if (url === '/pw/world') return send(200, pw.worldSummary(q.get('minutes')));
    if (url === '/pw/sixm') {
      if (q.get('format') === 'xml') { res.writeHead(200, { 'Content-Type': 'text/xml; charset=utf-8', 'Access-Control-Allow-Origin': ALLOW_ORIGIN, 'Cache-Control': 'no-store' }); res.end(pw.sixXml(q.get('grid'), q.get('minutes'))); return; }
      return send(200, pw.sixSummary(q.get('grid'), q.get('minutes')));
    }
    if (!pw.okSecret(q.get('key') || '')) return send(403, { ok: false, msg: 'bad key' });
    if (url === '/arc/start') return send(200, pw.testStart(q.get('call')));
    if (url === '/arc/data') return send(200, pw.testData(q.get('call')));
    return send(404, { ok: false });
  }
  if (url === '/' || url === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('ACURA MeshCore Hub is running. Observers: ' + observers.size + '. Packets buffered: ' + packets.length + '. LetsMesh feed: ' + (lmStatus.ok ? 'OK' : 'not yet') + (lmStatus.err ? ' (' + lmStatus.err + ')' : '') + '.\n');
    return;
  }
  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('Not found\n');
});

/* ---------------- MQTT broker ---------------- */

function startBroker() {
  const { WebSocketServer, createWebSocketStream } = require('ws');
  const aedesMod = require('aedes');
  const aedes = typeof aedesMod === 'function' ? aedesMod() : aedesMod.createBroker();

  const USER = process.env.HUB_USER || '';
  const PASS = process.env.HUB_PASS || '';
  if (!USER || !PASS) console.warn('WARNING: HUB_USER / HUB_PASS not set. Observers will be refused until they are.');

  function safeEqual(a, b) {
    const x = Buffer.from(String(a)); const y = Buffer.from(String(b));
    return x.length === y.length && crypto.timingSafeEqual(x, y);
  }

  aedes.authenticate = (client, username, password, cb) => {
    const ok = USER && PASS && safeEqual(username || '', USER) && safeEqual(password ? password.toString() : '', PASS);
    if (!ok) console.log('Refused login from client', client && client.id);
    cb(null, !!ok);
  };
  // Observers may only publish MeshCore topics, and nobody can subscribe (keeps the feed private).
  aedes.authorizePublish = (client, packet, cb) => {
    if (client && /^meshcore\//.test(packet.topic)) return cb(null);
    cb(new Error('topic not allowed'));
  };
  aedes.authorizeSubscribe = (client, sub, cb) => cb(new Error('subscriptions are not allowed'));

  aedes.on('publish', (packet, client) => {
    if (!client) return; // ignore broker-internal messages
    try { handleMessage(packet.topic, packet.payload, client.id); } catch (e) { console.error('handle error', e.message); }
  });
  aedes.on('client', (c) => console.log('Observer connected:', c.id));
  aedes.on('clientDisconnect', (c) => {
    console.log('Observer disconnected:', c.id);
    const name = clientObserver.get(c.id);
    if (name && observers.has(name)) observers.get(name).on = false;
  });

  // MQTT over WebSocket on the same port as the website JSON (Railway public domain).
  const wss = new WebSocketServer({ server, path: '/mqtt', handleProtocols: (protocols) => (protocols.has('mqtt') ? 'mqtt' : [...protocols][0] || false) });
  wss.on('connection', (conn, req) => {
    const stream = createWebSocketStream(conn);
    aedes.handle(stream, req);
  });

  // Plain MQTT over TCP (use with a Railway TCP proxy if an observer can't do WebSocket).
  net.createServer(aedes.handle).listen(MQTT_TCP_PORT, () => console.log('MQTT TCP listening on', MQTT_TCP_PORT));
}

if (require.main === module) {
  startBroker();
// OFF: LetsMesh API needs permission (401).   startLetsMesh();
  require('./pathwatch.js').start();
  server.listen(PORT, () => console.log('ACURA MeshCore Hub listening on', PORT, '(region ' + REGION + ')'));
}

module.exports = { decodeRaw, decodeAdvert, handleMessage, live, lmPackets, lmNodes, lmObservers };
