/*
 * ACURA live PSKReporter features (runs on the ACURA MeshCore Hub)
 * ----------------------------------------------------------------
 * One connection to the real-time PSKReporter MQTT feed (mqtt.pskreporter.info, run by Tom M0LTE) powers:
 *
 *  1. PATH WATCH - for the home grid squares people watch on acurahamradio.org/path-watch/, notice when
 *     stations within about 150 miles (the grid plus the 8 around it) are heard in the watcher's target
 *     (a country, grid square or grid field) and tell the website, which sends the email or app alert.
 *  2. ANTENNA TESTS - for the worldwide Antenna Report Card: during a 90-minute test, collect every report
 *     of the ham's callsign plus every report of stations in the 9 grid squares around them.
 *  3. WORLD MAP - a 1% sample of all reports worldwide, tallied into which paths are open on each band.
 *
 * Environment variables (Railway > Variables)
 *   PW_SECRET   shared key; must match the key in WordPress (required)
 *   WP_BASE     website address, default https://acurahamradio.org
 */
'use strict';

const WP_BASE = (process.env.WP_BASE || 'https://acurahamradio.org').replace(/\/$/, '');
const SECRET = process.env.PW_SECRET || '';
const FEED = 'mqtts://mqtt.pskreporter.info:1884';
const WORLD_TOPIC = 'pskr/filter/v2raw_1pc/#';
const OPEN_WINDOW = 15 * 60;   // look back this far for evidence (seconds)
const OPEN_MIN = 2;            // distinct station pairs needed to call a path open
const CLOSE_AFTER = 45 * 60;   // path closes after this long with no evidence
const MAX_GRIDS = 400;         // safety cap on grid subscriptions
const TEST_MINUTES = 90;       // antenna test length
const MAX_TESTS = 40;          // antenna tests running at once
const MAX_TEST_ROWS = 60000;   // per test

const stats = { connected: false, watches: 0, grids: 0, spots: 0, perMin: 0, opens: 0, tests: 0, world: 0, lastLoad: 0, lastError: '' };
let watches = [];
let byGrid = new Map();      // tx grid (4 char) -> [watch]
const state = new Map();     // "id|band" -> {hits, open, last}
const tests = new Map();     // CALL -> {call, created, expires, grid, grids:[], rows:[], keys:Set}
let world = [];              // [{b, s, r, t}] sampled spots (4-char grids)
let subscribed = new Set();  // topic strings
let client = null;
let minuteCount = 0;

function nowS() { return Math.floor(Date.now() / 1000); }
function okSecret(k) { return !!SECRET && typeof k === 'string' && k === SECRET; }

function neighbors(g) {
  g = String(g || '').toUpperCase();
  if (!/^[A-R]{2}[0-9]{2}$/.test(g)) return [];
  const x0 = (g.charCodeAt(0) - 65) * 10 + Number(g[2]);
  const y0 = (g.charCodeAt(1) - 65) * 10 + Number(g[3]);
  const out = [];
  for (let dx = -1; dx <= 1; dx++) for (let dy = -1; dy <= 1; dy++) {
    const x = (x0 + dx + 180) % 180; const y = y0 + dy;
    if (y < 0 || y > 179) continue;
    out.push(String.fromCharCode(65 + Math.floor(x / 10)) + String.fromCharCode(65 + Math.floor(y / 10)) + (x % 10) + (y % 10));
  }
  return out;
}

/* ---------------- Subscriptions ---------------- */

function gridTopic(g) { return 'pskr/filter/v2/+/+/+/+/' + g + '/#'; }
function callTopic(c) { return 'pskr/filter/v2/+/+/' + c + '/#'; }

function wantedTopics() {
  const want = new Set([WORLD_TOPIC]);
  const grids = new Set(byGrid.keys());
  for (const t of tests.values()) { want.add(callTopic(t.call)); for (const g of t.grids) grids.add(g); }
  [...grids].slice(0, MAX_GRIDS).forEach((g) => want.add(gridTopic(g)));
  return want;
}

function syncSubs() {
  if (!client || !client.connected) return;
  const want = wantedTopics();
  for (const t of subscribed) if (!want.has(t)) { client.unsubscribe(t); subscribed.delete(t); }
  for (const t of want) if (!subscribed.has(t)) { client.subscribe(t, { qos: 0 }); subscribed.add(t); }
  stats.grids = [...subscribed].filter((t) => t !== WORLD_TOPIC).length;
  stats.tests = tests.size;
}

/* ---------------- Path Watch ---------------- */

async function loadWatches() {
  if (!SECRET) { stats.lastError = 'PW_SECRET not set'; return; }
  try {
    const r = await fetch(WP_BASE + '/wp-json/acura-pw/v1/watches?key=' + encodeURIComponent(SECRET), { headers: { 'User-Agent': 'ACURA-Hub/1.0' } });
    if (!r.ok) { stats.lastError = 'watch list HTTP ' + r.status; return; }
    const list = await r.json();
    if (!Array.isArray(list)) return;
    setWatches(list);
    stats.lastLoad = nowS(); stats.lastError = '';
    syncSubs();
  } catch (e) { stats.lastError = 'watch list: ' + e.message; }
}

function setWatches(list) {
  watches = list.filter((w) => w && w.id && /^[A-R]{2}[0-9]{2}$/i.test(w.home));
  const m = new Map();
  for (const w of watches) {
    w.home = w.home.toUpperCase(); w.tval = String(w.tval || '').toUpperCase(); w.band = String(w.band || '');
    for (const g of neighbors(w.home)) { if (!m.has(g)) m.set(g, []); m.get(g).push(w); }
  }
  byGrid = m; stats.watches = watches.length;
}

function targetMatch(w, rl, ra) {
  if (w.tkind === 'dxcc') return String(ra) === w.tval;
  const loc = String(rl || '').toUpperCase();
  if (w.tkind === 'field') return loc.slice(0, 2) === w.tval.slice(0, 2);
  return loc.slice(0, 4) === w.tval.slice(0, 4);
}

async function report(w, band, ev, n) {
  stats.opens++;
  try {
    await fetch(WP_BASE + '/wp-json/acura-pw/v1/hit', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'User-Agent': 'ACURA-Hub/1.0' },
      body: JSON.stringify({ key: SECRET, id: w.id, band, tx: ev.tx, txg: ev.txg, rx: ev.rx, rxg: ev.rxg, snr: ev.snr, mode: ev.mode, n }),
    });
  } catch (e) { stats.lastError = 'hit: ' + e.message; }
}

function pathWatch(sp) {
  const list = byGrid.get(sp.txg);
  if (!list) return;
  const now = nowS();
  const ev = { t: now, tx: sp.tx, rx: sp.rx, txg: sp.txg, rxg: sp.rl.slice(0, 6), snr: sp.snr, mode: sp.mode, k: sp.tx + '>' + sp.rx };
  for (const w of list) {
    if (w.band && w.band !== sp.band) continue;
    if (!targetMatch(w, sp.rl, sp.ra)) continue;
    const key = w.id + '|' + sp.band;
    let st = state.get(key);
    if (!st) { st = { hits: [], open: false, last: 0 }; state.set(key, st); }
    st.hits.push(ev); st.last = now;
    st.hits = st.hits.filter((h) => now - h.t <= OPEN_WINDOW).slice(-50);
    const pairs = new Set(st.hits.map((h) => h.k));
    if (!st.open && pairs.size >= OPEN_MIN) { st.open = true; report(w, sp.band, ev, pairs.size); }
  }
}

function openNow() {
  const out = [];
  for (const [k, st] of state) {
    if (!st.open) continue;
    const [id, band] = k.split('|'); const last = st.hits[st.hits.length - 1];
    out.push({ id: Number(id), band, last: st.last, tx: last && last.tx, rx: last && last.rx, txg: last && last.txg, rxg: last && last.rxg, snr: last && last.snr });
  }
  return out;
}

/* ---------------- Antenna tests ---------------- */

function testStart(call) {
  call = String(call || '').toUpperCase();
  if (!/^[A-Z0-9]{3,10}$/.test(call)) return { ok: false, msg: 'Please enter a callsign without slashes, like W4WAG.' };
  const now = nowS();
  let t = tests.get(call);
  if (!t) {
    if (tests.size >= MAX_TESTS) return { ok: false, msg: 'Lots of hams are testing right now. Please try again in a few minutes.' };
    t = { call, created: now, expires: now + TEST_MINUTES * 60, grid: '', grids: [], rows: [], keys: new Set() };
    tests.set(call, t);
  } else if (t.expires - now < 30 * 60) {
    t.expires = now + TEST_MINUTES * 60;
  }
  syncSubs();
  return { ok: true, call, created: t.created, expires: t.expires, grid: t.grid };
}

function testAdd(t, sp) {
  const k = sp.tx + '|' + sp.rx + '|' + sp.t + '|' + sp.band;
  if (t.keys.has(k) || t.rows.length >= MAX_TEST_ROWS) return;
  t.keys.add(k);
  t.rows.push([sp.tx, sp.sl.slice(0, 6), sp.rx, sp.rl.slice(0, 6), sp.ra, sp.band, sp.mode, sp.t, sp.snr]);
}

function antennaTests(sp) {
  if (!tests.size) return;
  const own = tests.get(sp.tx.toUpperCase());
  if (own) {
    if (!own.grid && /^[A-R]{2}[0-9]{2}/i.test(sp.sl)) { own.grid = sp.sl.slice(0, 4).toUpperCase(); own.grids = neighbors(own.grid); syncSubs(); }
    testAdd(own, sp);
  }
  for (const t of tests.values()) {
    if (t === own || !t.grid) continue;
    if (t.grids.indexOf(sp.txg) >= 0) testAdd(t, sp);
  }
}

function testData(call) {
  const t = tests.get(String(call || '').toUpperCase());
  if (!t) return { ok: false, running: false };
  return { ok: true, running: true, call: t.call, grid: t.grid, created: t.created, expires: t.expires, rows: t.rows };
}

/* ---------------- World map (1% sample) ---------------- */

function worldAdd(s) {
  const sl = String(s.sl || '').toUpperCase(), rl = String(s.rl || '').toUpperCase();
  if (!/^[A-R]{2}[0-9]{2}/.test(sl) || !/^[A-R]{2}[0-9]{2}/.test(rl) || sl.slice(0, 4) === rl.slice(0, 4)) return;
  world.push({ b: String(s.b || ''), s: sl.slice(0, 4), r: rl.slice(0, 4), t: nowS() });
  if (world.length > 30000) world = world.slice(-25000);
  stats.world++;
}

function worldSummary(minutes) {
  const since = nowS() - Math.max(5, Math.min(60, Number(minutes) || 20)) * 60;
  const m = new Map(); const bands = {};
  for (const x of world) {
    if (x.t < since || !x.b) continue;
    const k = x.b + '|' + x.s + '|' + x.r;
    const e = m.get(k); if (e) { e[3]++; if (x.t > e[4]) e[4] = x.t; } else m.set(k, [x.b, x.s, x.r, 1, x.t]);
    bands[x.b] = (bands[x.b] || 0) + 1;
  }
  const paths = [...m.values()].sort((a, b) => b[4] - a[4]).slice(0, 2500);
  return { since, now: nowS(), bands, paths };
}

/* ---------------- Feed ---------------- */

function onMessage(topicStr, payload) {
  stats.spots++; minuteCount++;
  let s; try { s = JSON.parse(payload.toString()); } catch (e) { return; }
  if (topicStr.indexOf('pskr/filter/v2raw_1pc') === 0) { worldAdd(s); return; }
  const p = topicStr.split('/'); // pskr filter v2 band mode tx rx txgrid rxgrid txdxcc rxdxcc
  const sp = {
    band: String(s.b || p[3] || ''), mode: String(s.md || p[4] || ''),
    tx: String(s.sc || p[5] || '').toUpperCase(), rx: String(s.rc || p[6] || '').toUpperCase(),
    txg: String(p[7] || s.sl || '').slice(0, 4).toUpperCase(),
    sl: String(s.sl || p[7] || ''), rl: String(s.rl || p[8] || ''),
    ra: s.ra != null ? s.ra : p[10], snr: Number(s.rp), t: Number(s.t_tx || s.t) || nowS(),
  };
  if (!sp.band) return;
  pathWatch(sp);
  antennaTests(sp);
}

function sweep() {
  const now = nowS();
  for (const [k, st] of state) {
    if (st.open && now - st.last > CLOSE_AFTER) st.open = false;
    if (!st.open && now - st.last > 3 * 3600) state.delete(k);
  }
  let changed = false;
  for (const [c, t] of tests) if (t.expires < now) { tests.delete(c); changed = true; }
  if (changed) syncSubs();
  const cut = now - 3600; if (world.length && world[0].t < cut) world = world.filter((x) => x.t >= cut);
  stats.perMin = minuteCount; minuteCount = 0;
}

function start() {
  let mqtt;
  try { mqtt = require('mqtt'); } catch (e) { stats.lastError = 'mqtt package missing'; console.error('Path Watch: mqtt package missing'); return; }
  client = mqtt.connect(FEED, { clientId: 'acura-hub-' + Math.random().toString(16).slice(2, 10), clean: true, reconnectPeriod: 10000, keepalive: 60 });
  client.on('connect', () => { stats.connected = true; subscribed = new Set(); syncSubs(); console.log('PSKReporter feed: connected'); });
  client.on('close', () => { stats.connected = false; });
  client.on('error', (e) => { stats.lastError = 'feed: ' + e.message; });
  client.on('message', onMessage);
  loadWatches();
  setInterval(loadWatches, 3 * 60 * 1000).unref();
  setInterval(sweep, 60 * 1000).unref();
}

module.exports = { start, stats, openNow, neighbors, okSecret, testStart, testData, worldSummary,
  _test: { onMessage, targetMatch, setWatches, state, tests, syncSubs, wantedTopics } };
