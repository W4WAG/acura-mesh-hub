/*
 * ACURA Signal Doctor - recorder
 * ------------------------------
 * Records a ham's own transmission on a public KiwiSDR receiver far away, so
 * acurahamradio.org/signal-doctor/ can grade it. KiwiSDRs only speak plain
 * ws:// so this has to run here, not in the visitor's (https) browser.
 *
 *   GET /sd/kiwis?lat=&lon=&f=14200        nearby public receivers that cover f (kHz)
 *   GET /sd/start?url=&f=&mode=voice|carrier&call=   start a 30 s recording, returns {id}
 *   GET /sd/job?id=                        status, elapsed, S-meter, sample rate
 *   GET /sd/wav?id=                        the recording (16-bit mono WAV)
 *   GET /sd/debug?url=                     admin test (needs key)
 *
 * Only receivers from the public KiwiSDR list can be used, at most 3 recordings
 * run at once, and each visitor can start 12 per hour. Each recording uses one
 * receiver channel for about 35 seconds and identifies itself as ACURA Signal Doctor.
 */
'use strict';

const WebSocket = require('ws');
const crypto = require('crypto');

const LIST_URLS = [
  'http://rx.linkfanel.net/kiwisdr_com.js',
  'https://rx.linkfanel.net/kiwisdr_com.js',
  'http://kiwisdr.com/public/',
];
const REC_SECS = 30;
const MAX_JOBS = 3;
const PER_HOUR = 12;

let kiwiCache = { t: 0, list: [], src: '', err: '' };
const jobs = new Map();
const ipLog = new Map();

/* ---------------- receiver list ---------------- */

function toNum(v) { const n = parseFloat(v); return Number.isFinite(n) ? n : null; }

function parseGps(s) {
  const m = String(s || '').match(/(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)/);
  if (!m) return null;
  const lat = parseFloat(m[1]), lon = parseFloat(m[2]);
  if (!Number.isFinite(lat) || !Number.isFinite(lon) || (lat === 0 && lon === 0)) return null;
  return { lat, lon };
}

function normUrl(u) {
  const m = String(u || '').trim().match(/^https?:\/\/([a-z0-9.\-]+)(?::(\d+))?\/?$/i);
  if (!m) return null;
  return { host: m[1].toLowerCase(), port: parseInt(m[2] || '80', 10) };
}

function entryFrom(o) {
  const u = normUrl(o.url);
  const g = parseGps(o.gps);
  if (!u || !g) return null;
  const off = String(o.offline || '').toLowerCase() === 'yes' || String(o.status || '').toLowerCase() === 'offline';
  const b = String(o.bands || '0-30000000').match(/(\d+)\s*-\s*(\d+)/);
  return {
    name: String(o.name || u.host).replace(/<[^>]*>/g, '').slice(0, 90),
    host: u.host, port: u.port,
    lat: g.lat, lon: g.lon,
    users: parseInt(o.users || '0', 10) || 0,
    max: parseInt(o.users_max || '4', 10) || 4,
    lo: b ? parseInt(b[1], 10) : 0, hi: b ? parseInt(b[2], 10) : 30000000,
    offline: off,
    snr: String(o.snr || '').split(',').map(toNum).filter((x) => x !== null),
  };
}

function parseJsList(txt) {
  const a = txt.indexOf('['), b = txt.lastIndexOf(']');
  if (a < 0 || b < a) return [];
  let body = txt.slice(a, b + 1).replace(/,\s*([\]}])/g, '$1');
  let arr;
  try { arr = JSON.parse(body); } catch (e) { return []; }
  return arr.map(entryFrom).filter(Boolean);
}

function parseHtmlList(txt) {
  const out = [];
  const parts = txt.split(/class=['"]cl-entry/);
  for (let i = 1; i < parts.length; i++) {
    const p = parts[i];
    const o = {};
    const re = /<!--\s*([a-z_]+)=([\s\S]*?)\s*-->/g;
    let m;
    while ((m = re.exec(p))) o[m[1]] = m[2];
    if (!o.url) { const h = p.match(/href=['"](https?:\/\/[^'"]+)['"]/); if (h) o.url = h[1]; }
    const e = entryFrom(o);
    if (e) out.push(e);
  }
  return out;
}

async function loadKiwis() {
  if (kiwiCache.list.length && Date.now() - kiwiCache.t < 30 * 60 * 1000) return kiwiCache.list;
  let lastErr = '';
  for (const u of LIST_URLS) {
    try {
      const r = await fetch(u, { headers: { 'User-Agent': 'ACURA-Signal-Doctor (acurahamradio.org)' }, signal: AbortSignal.timeout(20000) });
      if (!r.ok) { lastErr = u + ' HTTP ' + r.status; continue; }
      const t = await r.text();
      const list = u.endsWith('.js') ? parseJsList(t) : parseHtmlList(t);
      if (list.length > 50) { kiwiCache = { t: Date.now(), list, src: u, err: '' }; return list; }
      lastErr = u + ' parsed only ' + list.length;
    } catch (e) { lastErr = u + ' ' + e.message; }
  }
  kiwiCache.err = lastErr;
  if (kiwiCache.list.length) return kiwiCache.list;
  throw new Error('Receiver list unavailable: ' + lastErr);
}

function km(a, b, c, d) {
  const R = 6371, r = Math.PI / 180;
  const x = Math.sin((c - a) * r / 2) ** 2 + Math.cos(a * r) * Math.cos(c * r) * Math.sin((d - b) * r / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(x));
}

function bearing(a, b, c, d) {
  const r = Math.PI / 180;
  const y = Math.sin((d - b) * r) * Math.cos(c * r);
  const x = Math.cos(a * r) * Math.sin(c * r) - Math.sin(a * r) * Math.cos(c * r) * Math.cos((d - b) * r);
  return (Math.atan2(y, x) / r + 360) % 360;
}

async function nearby(lat, lon, fkhz) {
  const list = await loadKiwis();
  const fHz = fkhz * 1000;
  const c = [];
  for (const k of list) {
    if (k.offline || k.users >= k.max) continue;
    if (fHz < k.lo || fHz > k.hi) continue;
    const d = km(lat, lon, k.lat, k.lon);
    if (d < 150 || d > 3000) continue;
    // Best skywave distance for a quick test is roughly 400-1300 km; prefer receivers with a good SNR score.
    const distPenalty = d < 400 ? (400 - d) / 4 : d > 1300 ? (d - 1300) / 8 : 0;
    const snr = k.snr.length ? Math.max(...k.snr) : 15;
    c.push({ name: k.name, url: 'http://' + k.host + ':' + k.port, km: Math.round(d), deg: Math.round(bearing(lat, lon, k.lat, k.lon)), free: k.max - k.users, snr, score: snr * 3 - distPenalty });
  }
  c.sort((a, b) => b.score - a.score);
  return c.slice(0, 10);
}

/* ---------------- recording ---------------- */

function imaDecoder() {
  const idx = [-1, -1, -1, -1, 2, 4, 6, 8, -1, -1, -1, -1, 2, 4, 6, 8];
  const step = [7, 8, 9, 10, 11, 12, 13, 14, 16, 17, 19, 21, 23, 25, 28, 31, 34, 37, 41, 45, 50, 55, 60, 66, 73, 80, 88, 97, 107, 118, 130, 143, 157, 173, 190, 209, 230, 253, 279, 307, 337, 371, 408, 449, 494, 544, 598, 658, 724, 796, 876, 963, 1060, 1166, 1282, 1411, 1552, 1707, 1878, 2066, 2272, 2499, 2749, 3024, 3327, 3660, 4026, 4428, 4871, 5358, 5894, 6484, 7132, 7845, 8630, 9493, 10442, 11487, 12635, 13899, 15289, 16818, 18500, 20350, 22385, 24623, 27086, 29794, 32767];
  let si = 0, prev = 0;
  function one(code) {
    const s = step[si];
    let diff = s >> 3;
    if (code & 1) diff += s >> 2;
    if (code & 2) diff += s >> 1;
    if (code & 4) diff += s;
    if (code & 8) diff = -diff;
    prev = Math.max(-32768, Math.min(32767, prev + diff));
    si = Math.max(0, Math.min(88, si + idx[code]));
    return prev;
  }
  return (buf) => { const out = new Int16Array(buf.length * 2); for (let i = 0; i < buf.length; i++) { out[2 * i] = one(buf[i] & 0x0f); out[2 * i + 1] = one(buf[i] >> 4); } return out; };
}

function pickEndian(buf) {
  // Kiwi sends 16-bit samples big-endian; check which reading is smoother in case that ever changes.
  const n = Math.floor(buf.length / 2);
  let be = 0, le = 0, pb = 0, pl = 0;
  for (let i = 0; i < n; i++) {
    const b = buf.readInt16BE(2 * i), l = buf.readInt16LE(2 * i);
    be += Math.abs(b - pb); le += Math.abs(l - pl); pb = b; pl = l;
  }
  return be <= le ? 'BE' : 'LE';
}

function clientIp(req) {
  return String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
}

function startJob(k, opt) {
  const id = crypto.randomBytes(9).toString('base64').replace(/[^a-z0-9]/gi, '').slice(0, 12);
  const job = { id, created: Date.now(), state: 'connecting', msg: '', recStart: 0, fs: 12000, chunks: [], nsamp: 0, sm: [], endian: '', kiwi: k.name || k.host, info: {} };
  jobs.set(id, job);

  const isCarrier = opt.mode === 'carrier';
  const fk = opt.f;
  const tuneK = isCarrier ? fk - 1.0 : fk;
  const lsb = !isCarrier && fk < 10000 && !(fk >= 5330 && fk <= 5410);
  const mod = lsb ? 'lsb' : 'usb';
  const lc = isCarrier ? 50 : (lsb ? -5400 : 50);
  const hc = isCarrier ? 2500 : (lsb ? -50 : 5400);
  job.info = { mod, tune: tuneK, carrier: isCarrier };

  job.log = [];
  const log = (m) => { if (job.log.length < 40) job.log.push(((Date.now() - job.created) / 1000).toFixed(1) + ' ' + String(m).slice(0, 160)); };
  const paths = ['/' + Math.floor(Date.now() / 1000) + '/SND', '/ws/kiwi/' + Math.floor(Date.now() / 1000) + '/SND'];
  let ws = null, ka = null, endT = null, adpcm = null, attempt = 0, waitT = null;

  const send = (m) => { try { ws.send(m); } catch (e) { /* closed */ } };
  const finish = (state, msg) => {
    if (job.state === 'done' || job.state === 'error') return;
    job.state = state; if (msg) job.msg = msg;
    log('finish ' + state + ' ' + (msg || ''));
    clearInterval(ka); clearTimeout(endT); clearTimeout(waitT);
    try { ws.close(); } catch (e) { /* ignore */ }
  };

  function connect() {
    const url = 'ws://' + k.host + ':' + k.port + paths[attempt];
    log('connect ' + paths[attempt].replace(/\d{6,}/, 'T'));
    let me;
    try { me = new WebSocket(url, { handshakeTimeout: 12000, headers: { 'User-Agent': 'Mozilla/5.0 (ACURA Signal Doctor; acurahamradio.org)' } }); }
    catch (e) { log('ctor ' + e.message); return retry('Could not reach that receiver.'); }
    ws = me; job.ws = me;

    me.on('open', () => {
      log('open');
      send('SET auth t=kiwi p=');
      send('SET ident_user=ACURA_Signal_Doctor_' + String(opt.call || '').replace(/[^A-Z0-9]/gi, '').slice(0, 10));
      send('SET mod=' + mod + ' low_cut=' + lc + ' high_cut=' + hc + ' freq=' + tuneK.toFixed(3));
      send('SET agc=1 hang=0 thresh=-110 slope=6 decay=1000 manGain=50');
      send('SET squelch=0 max=0');
      send('SET compression=0');
      clearInterval(ka); ka = setInterval(() => send('SET keepalive'), 5000);
      clearTimeout(waitT);
      waitT = setTimeout(() => { if (job.state === 'connecting') { log('no audio yet'); retry('That receiver did not send any audio. Pick another one.'); } }, 8000);
    });

    me.on('message', (data) => {
      if (me !== ws) return;
      const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
      const tag = buf.slice(0, 3).toString('latin1');
      if (tag === 'MSG') {
        const str = buf.slice(4).toString('utf8');
        if (job.log.length < 30 && !/^(stats_cb|ext_client_init|load_cfg|load_dxcfg|load_adm|client_public_ip|rx_chans|chan_no_pwd|badp=0|version_maj|version_min|debian_ver|is_admin|is_local|kiwi_up)/.test(str)) log('MSG ' + str);
        for (const kv of str.split(' ')) {
          const [key, val] = kv.split('=');
          if (key === 'audio_rate') send('SET AR OK in=' + parseInt(val, 10) + ' out=44100');
          else if (key === 'sample_rate') { const v = parseFloat(val); if (v > 1000) job.fs = v; }
          else if (key === 'too_busy') finish('error', 'That receiver is full right now. Pick another one.');
          else if (key === 'badp' && val !== '0') finish('error', 'That receiver needs a password. Pick another one.');
          else if (key === 'down') finish('error', 'That receiver is down right now. Pick another one.');
        }
        return;
      }
      if (tag !== 'SND' || buf.length < 11) { if (job.log.length < 30) log('frame ' + tag.replace(/[^ -~]/g, '?') + ' len ' + buf.length); return; }
      const flags = buf[3];
      const smeter = buf.readUInt16BE(8);
      const dbm = 0.1 * smeter - 127;
      let pcm;
      const body = buf.slice(10);
      if (flags & 0x10) { adpcm = adpcm || imaDecoder(); pcm = adpcm(body); }
      else {
        if (!job.endian) job.endian = pickEndian(body);
        pcm = new Int16Array(Math.floor(body.length / 2));
        for (let i = 0; i < pcm.length; i++) pcm[i] = job.endian === 'BE' ? body.readInt16BE(2 * i) : body.readInt16LE(2 * i);
      }
      if (job.state === 'connecting') {
        log('audio flags ' + flags + ' bytes ' + body.length);
        clearTimeout(waitT);
        job.state = 'recording'; job.recStart = Date.now();
        endT = setTimeout(() => finish('done'), REC_SECS * 1000 + 300);
      }
      if (job.state !== 'recording') return;
      job.chunks.push(pcm); job.nsamp += pcm.length;
      job.sm.push([+((Date.now() - job.recStart) / 1000).toFixed(2), +dbm.toFixed(1)]);
      if (job.nsamp > job.fs * (REC_SECS + 2)) finish('done');
    });

    me.on('error', (e) => { log('error ' + e.message); if (me === ws) retry('Could not connect to that receiver. Pick another one.'); });
    me.on('close', (code, reason) => {
      log('close ' + code + ' ' + String(reason || '').slice(0, 60));
      if (me !== ws) return;
      if (job.state === 'recording') finish(job.nsamp > job.fs * 8 ? 'done' : 'error', job.nsamp > job.fs * 8 ? '' : 'The receiver hung up early. Pick another one.');
      else if (job.state === 'connecting') retry('The receiver closed the connection. Pick another one.');
    });
  }

  function retry(msg) {
    if (job.state !== 'connecting') return;
    clearInterval(ka); clearTimeout(waitT);
    const old = ws; ws = null;
    try { old && old.close(); } catch (e) { /* ignore */ }
    if (attempt + 1 < paths.length) { attempt++; setTimeout(connect, 300); }
    else finish('error', msg);
  }

  connect();
  setTimeout(() => { if (job.state === 'connecting') finish('error', 'That receiver did not answer. Pick another one.'); }, 30000);
  return job;
}

function wavOf(job) {
  const n = job.nsamp, rate = Math.round(job.fs);
  const b = Buffer.alloc(44 + n * 2);
  b.write('RIFF', 0); b.writeUInt32LE(36 + n * 2, 4); b.write('WAVE', 8);
  b.write('fmt ', 12); b.writeUInt32LE(16, 16); b.writeUInt16LE(1, 20); b.writeUInt16LE(1, 22);
  b.writeUInt32LE(rate, 24); b.writeUInt32LE(rate * 2, 28); b.writeUInt16LE(2, 32); b.writeUInt16LE(16, 34);
  b.write('data', 36); b.writeUInt32LE(n * 2, 40);
  let o = 44;
  for (const c of job.chunks) for (let i = 0; i < c.length; i++) { b.writeInt16LE(c[i], o); o += 2; }
  return b;
}

setInterval(() => {
  const now = Date.now();
  for (const [id, j] of jobs) if (now - j.created > 20 * 60 * 1000) jobs.delete(id);
  for (const [ip, arr] of ipLog) { const k = arr.filter((t) => now - t < 3600 * 1000); if (k.length) ipLog.set(ip, k); else ipLog.delete(ip); }
}, 60 * 1000).unref();

/* ---------------- HTTP ---------------- */

async function handle(req, res, allowOrigin, okSecret) {
  const u = new URL(req.url || '/', 'http://x');
  const q = u.searchParams;
  const hdr = { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': allowOrigin, 'Cache-Control': 'no-store' };
  const send = (code, obj) => { res.writeHead(code, hdr); res.end(JSON.stringify(obj)); };
  try {
    if (u.pathname === '/sd/kiwis') {
      const lat = toNum(q.get('lat')), lon = toNum(q.get('lon')), f = toNum(q.get('f'));
      if (lat === null || lon === null || f === null || f < 1800 || f > 30000) return send(400, { ok: false, msg: 'Need lat, lon and a frequency between 1800 and 30000 kHz.' });
      return send(200, { ok: true, list: await nearby(lat, lon, f) });
    }
    if (u.pathname === '/sd/start') {
      const f = toNum(q.get('f'));
      if (f === null || f < 1800 || f > 30000) return send(400, { ok: false, msg: 'Pick a frequency between 1800 and 30000 kHz.' });
      const want = normUrl(q.get('url'));
      if (!want) return send(400, { ok: false, msg: 'Pick a receiver.' });
      const list = await loadKiwis();
      const k = list.find((x) => x.host === want.host && x.port === want.port);
      if (!k) return send(400, { ok: false, msg: 'That receiver is not on the public KiwiSDR list.' });
      let running = 0; for (const j of jobs.values()) if (j.state === 'connecting' || j.state === 'recording') running++;
      if (running >= MAX_JOBS) return send(429, { ok: false, msg: 'Signal Doctor is busy with other hams right now. Try again in a minute.' });
      const ip = clientIp(req); const log = (ipLog.get(ip) || []).filter((t) => Date.now() - t < 3600 * 1000);
      if (log.length >= PER_HOUR) return send(429, { ok: false, msg: 'You have run ' + PER_HOUR + ' tests this hour. Please try again later.' });
      log.push(Date.now()); ipLog.set(ip, log);
      const mode = q.get('mode') === 'carrier' ? 'carrier' : 'voice';
      const job = startJob(k, { f, mode, call: q.get('call') });
      return send(200, { ok: true, id: job.id, secs: REC_SECS, kiwi: job.kiwi, info: job.info });
    }
    if (u.pathname === '/sd/job') {
      const j = jobs.get(q.get('id') || '');
      if (!j) return send(404, { ok: false, msg: 'That test has expired.' });
      const el = j.recStart ? (Date.now() - j.recStart) / 1000 : 0;
      return send(200, { ok: true, state: j.state, msg: j.msg, elapsed: +el.toFixed(1), secs: REC_SECS, fs: j.fs, samples: j.nsamp, sm: j.state === 'done' ? j.sm : j.sm.slice(-1), kiwi: j.kiwi, info: j.info, log: q.get('log') ? j.log : undefined });
    }
    if (u.pathname === '/sd/wav') {
      const j = jobs.get(q.get('id') || '');
      if (!j || j.state !== 'done') return send(404, { ok: false, msg: 'Recording not ready.' });
      res.writeHead(200, { 'Content-Type': 'audio/wav', 'Access-Control-Allow-Origin': allowOrigin, 'Cache-Control': 'no-store' });
      res.end(wavOf(j));
      return;
    }
    if (u.pathname === '/sd/debug') {
      if (!okSecret || !okSecret(q.get('key') || '')) return send(403, { ok: false });
      let list = [];
      try { list = await loadKiwis(); } catch (e) { return send(200, { ok: false, err: e.message }); }
      return send(200, { ok: true, src: kiwiCache.src, n: list.length, err: kiwiCache.err, sample: list.slice(0, 3), jobs: [...jobs.values()].map((j) => ({ id: j.id, state: j.state, msg: j.msg, n: j.nsamp, fs: j.fs, endian: j.endian, kiwi: j.kiwi })) });
    }
    return send(404, { ok: false });
  } catch (e) {
    return send(500, { ok: false, msg: e.message.slice(0, 200) });
  }
}

module.exports = { handle, parseJsList, parseHtmlList, nearby, loadKiwis, _jobs: jobs, _cache: () => kiwiCache, _set: (l) => { kiwiCache = { t: Date.now(), list: l, src: 'test', err: '' }; } };
