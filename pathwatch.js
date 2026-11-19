/*
 * ACURA Path Watch - live path-opening detector
 * ---------------------------------------------
 * Listens to the real-time PSKReporter MQTT feed (mqtt.pskreporter.info, run by Tom M0LTE)
 * for the home grid squares that people are watching on acurahamradio.org/path-watch/.
 * When stations within about 150 miles of a watcher (their grid plus the 8 grids around it)
 * are heard in the watcher's target (a country, grid square or grid field) on the chosen band,
 * the path is "open" and the hub tells the website, which sends the email or app alert.
 *
 * Environment variables (Railway > Variables)
 *   PW_SECRET   shared key; must match the key shown in WordPress (required)
 *   WP_BASE     website address, default https://acurahamradio.org
 */
'use strict';

const WP_BASE = (process.env.WP_BASE || 'https://acurahamradio.org').replace(/\/$/, '');
const SECRET = process.env.PW_SECRET || '';
const FEED = 'mqtts://mqtt.pskreporter.info:1884';
const OPEN_WINDOW = 15 * 60;   // look back this far for evidence (seconds)
const OPEN_MIN = 2;            // distinct station pairs needed to call a path open
const CLOSE_AFTER = 45 * 60;   // path closes after this long with no evidence
const MAX_GRIDS = 400;         // safety cap on feed subscriptions

const stats = { connected: false, watches: 0, grids: 0, spots: 0, perMin: 0, opens: 0, lastLoad: 0, lastError: '' };
let watches = [];            // [{id, home, tkind, tval, band}]
let byGrid = new Map();      // tx grid (4 char) -> [watch]
let subscribed = new Set();
const state = new Map();     // "id|band" -> {hits:[{t,k,tx,rx,txg,rxg,snr,mode}], open:bool, last:0}
let client = null;
let minuteCount = 0;

function neighbors(g) {
  g = String(g || '').toUpperCase();
  if (!/^[A-R]{2}[0-9]{2}$/.test(g)) return [];
  const x0 = (g.charCodeAt(0) - 65) * 10 + Number(g[2]);
  const y0 = (g.charCodeAt(1) - 65) * 10 + Number(g[3]);
  const out = [];
  for (let dx = -1; dx <= 1; dx++) for (let dy = -1; dy <= 1; dy++) {
    let x = (x0 + dx + 180) % 180; const y = y0 + dy;
    if (y < 0 || y > 179) continue;
    out.push(String.fromCharCode(65 + Math.floor(x / 10)) + String.fromCharCode(65 + Math.floor(y / 10)) + (x % 10) + (y % 10));
  }
  return out;
}

async function loadWatches() {
  if (!SECRET) { stats.lastError = 'PW_SECRET not set'; return; }
  try {
    const r = await fetch(WP_BASE + '/wp-json/acura-pw/v1/watches?key=' + encodeURIComponent(SECRET), { headers: { 'User-Agent': 'ACURA-Hub/1.0' } });
    if (!r.ok) { stats.lastError = 'watch list HTTP ' + r.status; return; }
    const list = await r.json();
    if (!Array.isArray(list)) return;
    watches = list.filter((w) => w && w.id && /^[A-R]{2}[0-9]{2}$/i.test(w.home));
    const m = new Map();
    for (const w of watches) {
      w.home = w.home.toUpperCase(); w.tval = String(w.tval || '').toUpperCase(); w.band = String(w.band || '');
      for (const g of neighbors(w.home)) { if (!m.has(g)) m.set(g, []); m.get(g).push(w); }
    }
    byGrid = m;
    stats.watches = watches.length; stats.lastLoad = Math.floor(Date.now() / 1000); stats.lastError = '';
    syncSubs();
  } catch (e) { stats.lastError = 'watch list: ' + e.message; }
}

function topic(g) { return 'pskr/filter/v2/+/+/+/+/' + g + '/#'; }

function syncSubs() {
  if (!client || !client.connected) return;
  const want = new Set([...byGrid.keys()].slice(0, MAX_GRIDS));
  for (const g of subscribed) if (!want.has(g)) { client.unsubscribe(topic(g)); subscribed.delete(g); }
  for (const g of want) if (!subscribed.has(g)) { client.subscribe(topic(g), { qos: 0 }); subscribed.add(g); }
  stats.grids = subscribed.size;
}

function targetMatch(w, rl, ra) {
  if (w.tkind === 'dxcc') return String(ra) === w.tval;
  const loc = String(rl || '').toUpperCase();
  if (w.tkind === 'field') return loc.slice(0, 2) === w.tval.slice(0, 2);
  return loc.slice(0, 4) === w.tval.slice(0, 4); // grid
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

function onSpot(topicStr, payload) {
  stats.spots++; minuteCount++;
  const p = topicStr.split('/'); // pskr filter v2 band mode tx rx txgrid rxgrid txdxcc rxdxcc
  const txg = String(p[7] || '').toUpperCase();
  const list = byGrid.get(txg);
  if (!list) return;
  let s; try { s = JSON.parse(payload.toString()); } catch (e) { return; }
  const band = String(s.b || p[3] || '');
  const now = Math.floor(Date.now() / 1000);
  const ev = { t: now, tx: String(s.sc || p[5] || ''), rx: String(s.rc || p[6] || ''), txg, rxg: String(s.rl || p[8] || '').slice(0, 6), snr: Number(s.rp), mode: String(s.md || p[4] || '') };
  ev.k = ev.tx + '>' + ev.rx;
  for (const w of list) {
    if (w.band && w.band !== band) continue;
    if (!targetMatch(w, s.rl || p[8], s.ra != null ? s.ra : p[10])) continue;
    const key = w.id + '|' + band;
    let st = state.get(key);
    if (!st) { st = { hits: [], open: false, last: 0 }; state.set(key, st); }
    st.hits.push(ev); st.last = now;
    st.hits = st.hits.filter((h) => now - h.t <= OPEN_WINDOW).slice(-50);
    const pairs = new Set(st.hits.map((h) => h.k));
    if (!st.open && pairs.size >= OPEN_MIN) { st.open = true; report(w, band, ev, pairs.size); }
  }
}

function sweep() {
  const now = Math.floor(Date.now() / 1000);
  for (const [k, st] of state) {
    if (st.open && now - st.last > CLOSE_AFTER) st.open = false;
    if (!st.open && now - st.last > 3 * 3600) state.delete(k);
  }
  stats.perMin = minuteCount; minuteCount = 0;
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

function start() {
  let mqtt;
  try { mqtt = require('mqtt'); } catch (e) { stats.lastError = 'mqtt package missing'; console.error('Path Watch: mqtt package missing'); return; }
  client = mqtt.connect(FEED, { clientId: 'acura-pathwatch-' + Math.random().toString(16).slice(2, 10), clean: true, reconnectPeriod: 10000, keepalive: 60 });
  client.on('connect', () => { stats.connected = true; subscribed = new Set(); syncSubs(); console.log('Path Watch: connected to PSKReporter feed'); });
  client.on('close', () => { stats.connected = false; });
  client.on('error', (e) => { stats.lastError = 'feed: ' + e.message; });
  client.on('message', onSpot);
  loadWatches();
  setInterval(loadWatches, 3 * 60 * 1000).unref();
  setInterval(sweep, 60 * 1000).unref();
}

module.exports = { start, stats, openNow, neighbors, _test: { onSpot, targetMatch, setWatches: (l) => { watches = l; byGrid = new Map(); for (const w of l) for (const g of neighbors(w.home)) { if (!byGrid.has(g)) byGrid.set(g, []); byGrid.get(g).push(w); } }, state } };
