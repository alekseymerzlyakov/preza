/* ────────────────────────────────────────────────────────────────────────────
 * preza-sync.js — remote slide sync for the decks (leader → followers).
 *
 * Goal: the presenter (LEAD) drives slides/voice on their own machine; one host
 * shares THEIR browser+audio into the call and mirrors everything the lead does.
 * No screen share from the presenter, no accounts — a public MQTT-over-WSS broker
 * relays a tiny retained state message. Works from static GitHub Pages too.
 *
 * Roles are chosen IN-PAGE via the "📡 Синхронизация" launcher (bottom-left):
 *   • "Вести"        → become LEAD (a room code is generated, you drive)
 *   • "Подключиться" → become a follower/mirror (enter the lead's room code)
 * Under the hood these just set ?room=CODE(&role=lead) and reload.
 *
 * A deck wires itself in with:
 *   PrezaSync.attach({ deckId, getState, applyState })
 *     getState()  -> { slide, voiceOn, playing }   (read current deck state)
 *     applyState(s) -> reconcile the deck to a remote state (followers only)
 *   returns { publish } — the deck calls publish() after any local change.
 * ──────────────────────────────────────────────────────────────────────────── */
(function () {
  'use strict';

  var params = new URLSearchParams(location.search);
  var ROOM = (params.get('room') || '').trim();
  var ROLE = (params.get('role') || '').trim().toLowerCase() === 'lead' ? 'lead' : 'follow';
  var BROKER = params.get('broker') || localStorage.getItem('prezaBroker') || 'wss://broker.emqx.io:8084/mqtt';

  var TOPIC = ROOM ? 'preza/' + ROOM + '/state' : '';
  var client = null, connected = false, adapters = [], started = false, pubTimer = null, lastSentJson = '';

  // Multiple decks can live on one page (SPA). Route by the section that is active.
  // Prefer a gated adapter whose section is active; else a non-gated (standalone) one;
  // else null (e.g. an SPA sitting on its menu → publish nothing).
  function activeAdapter() {
    for (var i = 0; i < adapters.length; i++) { if (adapters[i].isActive && adapters[i].isActive()) return adapters[i]; }
    for (var j = 0; j < adapters.length; j++) { if (!adapters[j].isActive) return adapters[j]; }
    return null;
  }
  function adapterById(id) {
    for (var i = 0; i < adapters.length; i++) { if (adapters[i].deckId === id) return adapters[i]; }
    return null;
  }

  // ── UI helpers ──────────────────────────────────────────────────────────────
  var BOX_CSS =
    'position:fixed;left:14px;bottom:14px;z-index:2147483000;display:flex;flex-direction:column;align-items:flex-start;gap:8px;' +
    'font:600 12px/1.25 -apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif;color:#dfe8f5;';
  var PILL_CSS =
    'display:flex;align-items:center;gap:8px;padding:7px 11px;border-radius:9px;' +
    'background:rgba(6,12,24,.86);border:1px solid rgba(120,150,190,.35);box-shadow:0 4px 18px rgba(0,0,0,.4);backdrop-filter:blur(6px);';
  var BTN_CSS =
    'padding:5px 9px;border-radius:6px;cursor:pointer;font:600 12px inherit;background:rgba(56,140,255,.20);color:#bcd6ff;border:1px solid rgba(90,150,255,.5);';
  var ACT_CSS =
    'display:block;width:100%;text-align:left;margin:0;padding:8px 10px;border-radius:7px;cursor:pointer;font:600 12px inherit;' +
    'background:rgba(56,140,255,.16);color:#cfe0ff;border:1px solid rgba(90,150,255,.4);';

  function el(tag, css, text) { var e = document.createElement(tag); if (css) e.style.cssText = css; if (text != null) e.textContent = text; return e; }
  function onReady(fn) { if (document.body) fn(); else document.addEventListener('DOMContentLoaded', fn); }
  function randRoom() { return Math.floor(Math.random() * 1e9).toString(36).slice(0, 5).replace(/^$/, 'r'); }
  function goTo(room, role) {
    location.href = location.pathname + '?room=' + encodeURIComponent(room) + (role ? '&role=' + role : '') + (location.hash || '');
  }

  // ── launcher (shown when there is NO room yet) ──────────────────────────────
  function buildLauncher() {
    var box = el('div', BOX_CSS);
    var pill = el('div', PILL_CSS);
    var btn = el('button', BTN_CSS, '📡 Синхронизация');
    pill.appendChild(btn);
    box.appendChild(pill);

    var pop = null;
    btn.onclick = function () {
      if (pop) { pop.remove(); pop = null; return; }
      pop = el('div', PILL_CSS + 'flex-direction:column;align-items:stretch;gap:6px;min-width:210px;');
      var lead = el('button', ACT_CSS, '📡 Вести — я управляю');
      lead.onclick = function () { goTo(randRoom(), 'lead'); };
      var lbl = el('div', 'font:600 11px inherit;opacity:.75;margin-top:4px;', 'или подключиться к докладчику:');
      var row = el('div', 'display:flex;gap:6px;');
      var inp = el('input', 'flex:1;min-width:0;padding:6px 8px;border-radius:6px;border:1px solid rgba(120,150,190,.4);background:rgba(0,0,0,.3);color:#dfe8f5;font:600 12px inherit;');
      inp.placeholder = 'код комнаты';
      var join = el('button', BTN_CSS + 'white-space:nowrap;', 'войти');
      var doJoin = function () { var c = (inp.value || '').trim(); if (c) goTo(c, ''); };
      join.onclick = doJoin;
      inp.addEventListener('keydown', function (e) { if (e.key === 'Enter') doJoin(); });
      row.appendChild(inp); row.appendChild(join);
      pop.appendChild(lead); pop.appendChild(lbl); pop.appendChild(row);
      box.insertBefore(pop, pill);
      inp.focus();
    };
    (document.body || document.documentElement).appendChild(box);
  }

  // ── in-room status badge ────────────────────────────────────────────────────
  var dot;
  function buildBadge() {
    var box = el('div', BOX_CSS);
    var pill = el('div', PILL_CSS);
    dot = el('span', 'width:9px;height:9px;border-radius:50%;background:#e0a030;flex:0 0 auto;transition:background .3s;');
    var label = el('span', null, (ROLE === 'lead' ? '📡 Веду' : '🖥 Зеркало') + ' · ' + ROOM);
    pill.appendChild(dot); pill.appendChild(label);

    if (ROLE === 'lead') {
      var copy = el('button', BTN_CSS, 'ссылка зрителя');
      copy.onclick = function () {
        var u = location.origin + location.pathname + '?room=' + encodeURIComponent(ROOM);
        var done = function () { copy.textContent = 'скопировано ✓'; setTimeout(function () { copy.textContent = 'ссылка зрителя'; }, 1600); };
        if (navigator.clipboard && navigator.clipboard.writeText) navigator.clipboard.writeText(u).then(done, function () { prompt('Ссылка для зрителя:', u); });
        else prompt('Ссылка для зрителя:', u);
      };
      pill.appendChild(copy);
    }
    var quit = el('button', BTN_CSS + 'background:rgba(120,130,150,.18);color:#c7d0dd;border-color:rgba(140,150,170,.4);', 'выйти');
    quit.onclick = function () { location.href = location.pathname + (location.hash || ''); };
    pill.appendChild(quit);
    box.appendChild(pill);
    (document.body || document.documentElement).appendChild(box);
  }
  function setDot(c) { if (dot) dot.style.background = c; }

  // ── connect ─────────────────────────────────────────────────────────────────
  function connect() {
    if (typeof mqtt === 'undefined') { console.error('[preza-sync] mqtt.js not loaded — sync disabled'); return; }
    var clientId = 'preza-' + ROLE + '-' + Math.floor(Math.random() * 1e9).toString(36);
    client = mqtt.connect(BROKER, { clientId: clientId, clean: true, keepalive: 30, reconnectPeriod: 2000, connectTimeout: 8000 });

    client.on('connect', function () {
      connected = true; setDot('#2ecc71');
      client.subscribe(TOPIC, { qos: 0 });
      if (ROLE === 'lead') publishNow(); // re-announce so followers catch up (also retained)
    });
    client.on('reconnect', function () { setDot('#e0a030'); });
    client.on('close', function () { connected = false; setDot('#e0a030'); });
    client.on('error', function (e) { console.warn('[preza-sync] mqtt error', e && e.message); setDot('#e74c3c'); });

    client.on('message', function (topic, payload) {
      if (topic !== TOPIC) return;
      var msg; try { msg = JSON.parse(payload.toString()); } catch (e) { return; }
      if (ROLE === 'lead') return; // lead is the source of truth
      if (!msg || !msg.deckId) return;
      var a = adapterById(msg.deckId);
      if (!a) { console.warn('[preza-sync] no local player for deck "' + msg.deckId + '" — open that deck to mirror it'); return; }
      try {
        if (a.activate && a.isActive && !a.isActive()) a.activate(); // switch SPA section to match the lead
        a.applyState(msg);
      } catch (e) { console.warn('[preza-sync] applyState failed', e); }
    });
  }

  // ── publish (lead only) ─────────────────────────────────────────────────────
  function publishNow() {
    if (!client || !connected || ROLE !== 'lead') return;
    var a = activeAdapter();
    if (!a) return;
    var s = a.getState() || {};
    var payload = { deckId: a.deckId, slide: s.slide, voiceOn: !!s.voiceOn, playing: !!s.playing, ts: Date.now() };
    var json = JSON.stringify(payload);
    if (json === lastSentJson) return;
    lastSentJson = json;
    client.publish(TOPIC, json, { qos: 0, retain: true });
  }
  function publishSoon() { if (ROLE !== 'lead') return; clearTimeout(pubTimer); pubTimer = setTimeout(publishNow, 40); }

  // No room yet → just offer the launcher (independent of any deck adapter).
  if (!ROOM) onReady(buildLauncher);

  window.PrezaSync = {
    active: !!ROOM,
    role: ROLE,
    room: ROOM,
    republish: publishSoon, // SPAs call this after switching sections so followers follow the switch
    attach: function (a) {
      adapters.push(a);
      if (!ROOM) return { publish: function () {} };
      if (!started) { started = true; onReady(buildBadge); connect(); }
      return { publish: publishSoon };
    }
  };
})();
