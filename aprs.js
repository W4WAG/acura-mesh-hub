/*
 * ACURA APRS Watch  (part of the ACURA MeshCore Hub)
 * ---------------------------------------------------
 * Listens to the worldwide APRS-IS network (read-only, no callsign or passcode needed)
 * for the areas that website visitors are looking at, decodes every position packet,
 * and serves the stations near any location as JSON for acurahamradio.org/aprs-map/.
 *
 * How the areas work: the ACURA home area (South Jersey) is always on. When a visitor
 * anywhere in the world opens the APRS page, their area is added to the APRS-IS filter
 * on the fly, and the hub starts collecting it. Areas nobody has looked at for 3 hours
 * are dropped again, so the data stays small.
 *
 * Endpoints (added to server.js)
 *   GET /aprs/near?lat=39.4&lon=-74.5&km=150&minutes=60   stations, digipeaters and iGates near a point
 *   GET /aprs/stats                                         connection status
 */
'use strict';

const net = require('net');

const HOME = { lat: 39.43, lon: -74.5, km: 300 };
const SERVERS = ['rotate.aprs2.net', 'noam.aprs2.net', 'euro.aprs2.net'];
const KEEP_SECONDS = 3 * 3600;        // keep station history this long
const AREA_SECONDS = 3 * 3600;        // forget visitor areas after this long
const AREA_KM = 200;
const MAX_AREAS = 25;
const MAX_STATIONS = 30000;

const stations = new Map();           // call -> station record
const areas = new Map();              // "lat,lon" (0.5 degree steps) -> { lat, lon, first, last }
const stats = { connected: false, server: '', since: 0, packets: 0, positions: 0, lastPacket: 0, reconnects: 0, lastError: '', filter: '' };
let sock = null, buf = '', filterTimer = null, sentFilter = '';

/* ---------------- decoding ---------------- */

function num(s) { return parseFloat(String(s).replace(/ /g, '0')); }

function uncompressed(s) {
  const m = /^(\d{2})([\d ]{2}\.[\d ]{2})([NS])(.)(\d{3})([\d ]{2}\.[\d ]{2})([EW])(.)/.exec(s);
  if (!m) return null;
  let lat = num(m[1]) + num(m[2]) / 60; if (m[3] === 'S') lat = -lat;
  let lon = num(m[5]) + num(m[6]) / 60; if (m[7] === 'W') lon = -lon;
  return { lat, lon, table: m[4], code: m[8], rest: s.slice(19) };
}

function compressed(s) {
  if (s.length < 13) return null;
  const t = s[0];
  if (!/[\/\\A-Za-j]/.test(t)) return null;
  const b91 = (x) => { let v = 0; for (let i = 0; i < 4; i++) { const c = x.charCodeAt(i) - 33; if (c < 0 || c > 90) return NaN; v = v * 91 + c; } return v; };
  const la = b91(s.slice(1, 5)), lo = b91(s.slice(5, 9));
  if (isNaN(la) || isNaN(lo)) return null;
  return { lat: 90 - la / 380926, lon: -180 + lo / 190463, table: t, code: s[9], rest: s.slice(13) };
}

function position(s) { return /^\d/.test(s) ? uncompressed(s) : compressed(s); }

function micE(dest, info) {
  if (info.length < 9 || dest.length < 6) return null;
  const d = dest.slice(0, 6).toUpperCase();
  let digits = '';
  for (let i = 0; i < 6; i++) {
    const c = d[i];
    if (c >= '0' && c <= '9') digits += c;
    else if (c >= 'A' && c <= 'J') digits += String(c.charCodeAt(0) - 65);
    else if (c >= 'P' && c <= 'Y') digits += String(c.charCodeAt(0) - 80);
    else if (c === 'K' || c === 'L' || c === 'Z') digits += '0';
    else return null;
  }
  let lat = parseInt(digits.slice(0, 2), 10) + parseFloat(digits.slice(2, 4) + '.' + digits.slice(4, 6)) / 60;
  const north = d[3] >= 'P' && d[3] <= 'Z';
  const off100 = d[4] >= 'P' && d[4] <= 'Z';
  const west = d[5] >= 'P' && d[5] <= 'Z';
  if (!north) lat = -lat;
  let deg = info.charCodeAt(1) - 28; if (off100) deg += 100;
  if (deg >= 180 && deg <= 189) deg -= 80; else if (deg >= 190 && deg <= 199) deg -= 190;
  let min = info.charCodeAt(2) - 28; if (min >= 60) min -= 60;
  const hun = info.charCodeAt(3) - 28;
  let lon = deg + (min + hun / 100) / 60; if (west) lon = -lon;
  const sp = info.charCodeAt(4) - 28, dc = info.charCodeAt(5) - 28, se = info.charCodeAt(6) - 28;
  let speed = sp * 10 + Math.floor(dc / 10); if (speed >= 800) speed -= 800;
  let course = (dc % 10) * 100 + se; if (course >= 400) course -= 400;
  return { lat, lon, table: info[8], code: info[7], rest: info.slice(9), speed: Math.round(speed * 1.15078), course };
}

function decode(line) {
  const i = line.indexOf(':'); const g = line.indexOf('>');
  if (i < 0 || g < 0 || g > i) return null;
  let src = line.slice(0, g), head = line.slice(g + 1, i), info = line.slice(i + 1);
  const parts = head.split(',');
  const dest = parts[0];
  const path = parts.slice(1);
  let name = src, p = null, kind = info[0];
  try {
    if (kind === '!' || kind === '=') p = position(info.slice(1));
    else if (kind === '/' || kind === '@') p = position(info.slice(8));
    else if (kind === ';' && info.length > 18) { name = info.slice(1, 10).trim(); if (info[10] !== '*') return null; p = position(info.slice(18)); }
    else if (kind === ')') { const m = /^\)([^!_]{3,9})[!_](.*)$/.exec(info); if (m) { name = m[1].trim(); p = position(m[2]); } }
    else if (kind === '\x60' || kind === "'") p = micE(dest, info);
  } catch (e) { p = null; }
  if (!p || !isFinite(p.lat) || !isFinite(p.lon) || Math.abs(p.lat) > 90 || Math.abs(p.lon) > 180 || (p.lat === 0 && p.lon === 0)) return null;

  // path: digipeaters actually used (before the last '*'), q-construct and iGate
  let digis = [], hops = 0, igate = null, net = false, lastStar = -1;
  for (let k = 0; k < path.length; k++) { if (/^q[A-Z][A-Za-z]$/.test(path[k])) break; if (/\*$/.test(path[k])) lastStar = k; }
  for (let k = 0; k < path.length; k++) {
    const x = path[k];
    if (/^q[A-Z][A-Za-z]$/.test(x)) { if (/^qA[CXUSI]$/.test(x)) net = true; else igate = path[k + 1] || null; break; }
    if (/^TCPIP/.test(x)) { net = true; continue; }
    if (k <= lastStar) {
      const c = x.replace(/\*$/, '');
      hops++;
      if (!/^(WIDE|TRACE|RELAY|ECHO|GATE|ARISS|RS0ISS)/.test(c) && /^[A-Z0-9]{1,2}\d[A-Z0-9]{1,4}(-\d{1,2})?$/.test(c)) digis.push(c);
    }
  }
  let wx = null;
  if (p.code === '_') { const t = /t(-?\d{1,3})/.exec(p.rest || ''); if (t) wx = parseInt(t[1], 10); }
  let comment = String(p.rest || '').replace(/^\d{3}\/\d{3}/, '').replace(/[^\x20-\x7E]/g, '').trim().slice(0, 60);
  let speed = p.speed;
  if (speed == null) { const cs = /^(\d{3})\/(\d{3})/.exec(p.rest || ''); if (cs) speed = Math.round(parseInt(cs[2], 10) * 1.15078); }
  return { name, src, lat: Math.round(p.lat * 1e5) / 1e5, lon: Math.round(p.lon * 1e5) / 1e5, sym: (p.table || '/') + (p.code || '>'), digis, hops, igate, net, wx, speed: speed || 0, comment, obj: name !== src };
}

function store(d) {
  const now = Math.floor(Date.now() / 1000);
  let s = stations.get(d.name);
  if (!s) {
    if (stations.size >= MAX_STATIONS) prune(true);
    s = { c: d.name, trail: [] };
    stations.set(d.name, s);
  }
  if (!s.trail.length || Math.abs(s.lat - d.lat) > 0.0005 || Math.abs(s.lon - d.lon) > 0.0005) {
    s.trail.push([now, d.lat, d.lon]); if (s.trail.length > 40) s.trail.shift();
  }
  s.lat = d.lat; s.lon = d.lon; s.sym = d.sym; s.t = now; s.d = d.digis; s.h = d.hops; s.ig = d.igate; s.net = d.net; s.cm = d.comment; s.sp = d.speed; s.obj = d.obj;
  if (d.wx != null) { s.wx = d.wx; s.wxt = now; }
  stats.positions++;
}

function prune(hard) {
  const cut = Math.floor(Date.now() / 1000) - (hard ? KEEP_SECONDS / 2 : KEEP_SECONDS);
  for (const [k, s] of stations) {
    if (s.t < cut) stations.delete(k);
    else if (s.trail.length && s.trail[0][0] < cut) s.trail = s.trail.filter((p) => p[0] >= cut);
  }
  const acut = Math.floor(Date.now() / 1000) - AREA_SECONDS;
  for (const [k, a] of areas) if (a.last < acut) { areas.delete(k); scheduleFilter(); }
}

/* ---------------- APRS-IS connection ---------------- */

function filterString() {
  const f = ['r/' + HOME.lat + '/' + HOME.lon + '/' + HOME.km];
  [...areas.values()].sort((a, b) => b.last - a.last).slice(0, MAX_AREAS).forEach((a) => f.push('r/' + a.lat + '/' + a.lon + '/' + AREA_KM));
  return f.join(' ');
}

function scheduleFilter() {
  if (filterTimer) return;
  filterTimer = setTimeout(() => {
    filterTimer = null;
    const f = filterString();
    if (sock && stats.connected && f !== sentFilter) { try { sock.write('#filter ' + f + '\r\n'); sentFilter = f; stats.filter = f; } catch (e) {} }
  }, 3000);
}

function connect() {
  const host = SERVERS[stats.reconnects % SERVERS.length];
  stats.server = host; buf = '';
  sock = net.connect(14580, host);
  sock.setEncoding('latin1');
  sock.setKeepAlive(true, 60000);
  sock.setTimeout(120000);
  sock.on('connect', () => {
    const f = filterString();
    sock.write('user N0CALL pass -1 vers ACURA-Hub 1.0 filter ' + f + '\r\n');
    sentFilter = f; stats.filter = f; stats.connected = true; stats.since = Math.floor(Date.now() / 1000); stats.lastError = '';
    console.log('APRS-IS connected to', host);
  });
  sock.on('data', (chunk) => {
    buf += chunk;
    let n;
    while ((n = buf.indexOf('\n')) >= 0) {
      const line = buf.slice(0, n).replace(/\r$/, ''); buf = buf.slice(n + 1);
      if (!line || line[0] === '#') continue;
      stats.packets++; stats.lastPacket = Math.floor(Date.now() / 1000);
      try { const d = decode(line); if (d) store(d); } catch (e) { stats.lastError = 'decode: ' + e.message; }
    }
    if (buf.length > 65536) buf = '';
  });
  sock.on('timeout', () => { stats.lastError = 'timeout'; sock.destroy(); });
  sock.on('error', (e) => { stats.lastError = e.message; });
  sock.on('close', () => {
    stats.connected = false; stats.reconnects++;
    console.log('APRS-IS closed, reconnecting in 30 s');
    setTimeout(connect, 30000);
  });
}

/* ---------------- website endpoints ---------------- */

function hav(a, b, c, d) {
  const R = Math.PI / 180, x = Math.sin((c - a) * R / 2) ** 2 + Math.cos(a * R) * Math.cos(c * R) * Math.sin((d - b) * R / 2) ** 2;
  return 6371 * 2 * Math.asin(Math.min(1, Math.sqrt(x)));
}

function near(q) {
  const lat = parseFloat(q.get('lat')), lon = parseFloat(q.get('lon'));
  if (!isFinite(lat) || !isFinite(lon) || Math.abs(lat) > 90 || Math.abs(lon) > 180) return { ok: false, msg: 'lat and lon needed' };
  const km = Math.min(300, Math.max(20, parseFloat(q.get('km')) || 150));
  const minutes = Math.min(180, Math.max(5, parseInt(q.get('minutes'), 10) || 60));
  const now = Math.floor(Date.now() / 1000);

  // register the visitor's area (0.5 degree steps) so the hub starts collecting it
  let listening = stats.since;
  if (hav(lat, lon, HOME.lat, HOME.lon) > HOME.km - km) {
    const key = (Math.round(lat * 2) / 2) + ',' + (Math.round(lon * 2) / 2);
    let a = areas.get(key);
    if (!a) { a = { lat: Math.round(lat * 2) / 2, lon: Math.round(lon * 2) / 2, first: now, last: now }; areas.set(key, a); scheduleFilter(); }
    a.last = now; listening = Math.max(a.first, stats.since);
  }

  const cut = now - minutes * 60, list = [], digiUse = {}, igUse = {};
  for (const s of stations.values()) {
    if (s.t < cut) continue;
    const dkm = hav(lat, lon, s.lat, s.lon);
    if (dkm > km) continue;
    list.push(s);
    (s.d || []).forEach((c) => { digiUse[c] = (digiUse[c] || 0) + 1; });
    if (s.ig) igUse[s.ig] = (igUse[s.ig] || 0) + 1;
  }
  list.sort((a, b) => b.t - a.t);
  const out = list.slice(0, 1500).map((s) => [s.c, s.lat, s.lon, s.sym, s.t, (s.d && s.d[0]) || '', s.ig || '', s.net ? 1 : 0, s.wx == null ? null : s.wx, s.sp || 0, s.cm || '', s.trail.filter((p) => p[0] >= cut).map((p) => [p[1], p[2]])]);
  const pos = (c) => { const s = stations.get(c); return s ? [s.lat, s.lon] : null; };
  const digis = Object.keys(digiUse).map((c) => [c, digiUse[c], pos(c)]).sort((a, b) => b[1] - a[1]).slice(0, 80);
  const igates = Object.keys(igUse).map((c) => [c, igUse[c], pos(c)]).sort((a, b) => b[1] - a[1]).slice(0, 80);
  return { ok: true, now, listening, connected: stats.connected, km, minutes, total: list.length, stations: out, digis, igates, fields: ['call', 'lat', 'lon', 'symbol', 'last', 'firstDigi', 'igate', 'viaInternet', 'wxTempF', 'mph', 'comment', 'trail'] };
}

function handle(req, res, origin) {
  const url = (req.url || '/').split('?')[0];
  const q = new URL(req.url || '/', 'http://x').searchParams;
  const send = (code, obj) => { res.writeHead(code, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': origin, 'Cache-Control': 'no-store' }); res.end(JSON.stringify(obj)); };
  if (url === '/aprs/near') return send(200, near(q));
  if (url === '/aprs/stats') return send(200, Object.assign({ stations: stations.size, areas: areas.size }, stats));
  return send(404, { ok: false });
}

function start() {
  try { connect(); } catch (e) { stats.lastError = e.message; setTimeout(start, 60000); return; }
  setInterval(() => prune(false), 300000);
}

module.exports = { start, handle, decode, stats };
