/* ────────────────────────────────────────────────────────────────────────────
 * preza-sync.js — remote slide sync for the decks (leader → followers).
 *
 * Goal: the presenter (LEAD) drives slides/voice on their own machine; one host
 * shares THEIR browser+audio into the call and mirrors everything the lead does.
 * No screen share from the presenter, no accounts — a public MQTT-over-WSS broker
 * relays a tiny retained state message. Works from static GitHub Pages too.
 *
 * NO codes, NO picking a role on entry. Everyone shares ONE fixed room and by
 * default is a MIRROR (follower) — opening any deck already mirrors the lead.
 * A single button in the bottom-right badge flips your role:
 *   • mirror  → "📡 Взять управление"  → become LEAD (you drive)
 *   • lead    → "🖥 Отдать управление" → back to mirror
 *   • either  → "свободно"             → leave sync entirely (solo browsing)
 * Under the hood the button just sets ?role=lead / ?sync=off and reloads.
 * (?room=... in the URL still overrides the fixed room for power users.)
 *
 * A deck wires itself in with:
 *   PrezaSync.attach({ deckId, getState, applyState })
 *     getState()  -> { slide, voiceOn, playing }   (read current deck state)
 *     applyState(s) -> reconcile the deck to a remote state (followers only)
 *   returns { publish } — the deck calls publish() after any local change.
 * ──────────────────────────────────────────────────────────────────────────── */
(function () {
  'use strict';

  // One fixed room everybody joins — no code to type, no room to remember.
  var DEFAULT_ROOM = 'mzlk-preza';

  var params = new URLSearchParams(location.search);
  var SOLO = (params.get('sync') || '').trim().toLowerCase() === 'off'; // opted out of sync → free browsing
  var ROOM = SOLO ? '' : ((params.get('room') || '').trim() || DEFAULT_ROOM);
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
    'position:fixed;right:14px;bottom:14px;z-index:2147483000;display:flex;flex-direction:column;align-items:flex-end;gap:8px;' +
    'font:600 12px/1.25 -apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif;color:#dfe8f5;';
  var PILL_CSS =
    'display:flex;align-items:center;gap:8px;padding:7px 11px;border-radius:9px;' +
    'background:rgba(6,12,24,.86);border:1px solid rgba(120,150,190,.35);box-shadow:0 4px 18px rgba(0,0,0,.4);backdrop-filter:blur(6px);';
  var BTN_CSS =
    'padding:5px 9px;border-radius:6px;cursor:pointer;font:600 12px inherit;background:rgba(56,140,255,.20);color:#bcd6ff;border:1px solid rgba(90,150,255,.5);';

  function el(tag, css, text) { var e = document.createElement(tag); if (css) e.style.cssText = css; if (text != null) e.textContent = text; return e; }
  function onReady(fn) { if (document.body) fn(); else document.addEventListener('DOMContentLoaded', fn); }
  // Reload the SAME page/deck keeping ?room=, applying param overrides ('' deletes a key).
  function goTo(overrides) {
    var p = new URLSearchParams(location.search);
    for (var k in overrides) { if (overrides[k] === '') p.delete(k); else p.set(k, overrides[k]); }
    var qs = p.toString();
    location.href = location.pathname + (qs ? '?' + qs : '') + (location.hash || '');
  }

  // ── status badge (always shown; a single button flips the role) ─────────────
  var dot;
  function buildBadge() {
    var box = el('div', BOX_CSS);
    var pill = el('div', PILL_CSS);

    // collapse toggle: tuck the badge away so it never blocks deck controls
    var caret = el('button', 'flex:0 0 auto;padding:2px 6px;border-radius:6px;cursor:pointer;font:700 12px inherit;background:transparent;color:#9fb4d0;border:1px solid rgba(120,150,190,.3);', '–');
    var body = el('span', 'display:flex;align-items:center;gap:8px;');

    dot = el('span', 'width:9px;height:9px;border-radius:50%;background:#e0a030;flex:0 0 auto;transition:background .3s;');
    var label = el('span', null, SOLO ? '⏸ Синхр. выкл' : (ROLE === 'lead' ? '📡 Веду' : '🖥 Зеркало'));
    body.appendChild(dot); body.appendChild(label);

    if (SOLO) {
      // opted out → single button to rejoin as a mirror
      var on = el('button', BTN_CSS, 'включить синхр.');
      on.onclick = function () { goTo({ sync: '', role: '' }); };
      body.appendChild(on);
    } else if (ROLE === 'lead') {
      var give = el('button', BTN_CSS, '🖥 Отдать управление');
      give.onclick = function () { goTo({ role: '' }); };
      body.appendChild(give);
    } else {
      var take = el('button', BTN_CSS, '📡 Взять управление');
      take.onclick = function () { goTo({ role: 'lead' }); };
      body.appendChild(take);
    }

    if (!SOLO) {
      var quit = el('button', BTN_CSS + 'background:rgba(120,130,150,.18);color:#c7d0dd;border-color:rgba(140,150,170,.4);', 'свободно');
      quit.title = 'выйти из синхронизации — свободный просмотр';
      quit.onclick = function () { goTo({ sync: 'off', role: '' }); };
      body.appendChild(quit);
    }

    pill.appendChild(caret); pill.appendChild(body);
    box.appendChild(pill);

    var collapsed = false;
    caret.onclick = function () {
      collapsed = !collapsed;
      body.style.display = collapsed ? 'none' : 'flex';
      caret.textContent = collapsed ? '📡' : '–';
    };

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

  // Opted out of sync → still show the badge (so you can re-enable), but never connect.
  if (SOLO) onReady(buildBadge);

  window.PrezaSync = {
    active: !!ROOM,       // false in SOLO → decks stay fully interactive (free browsing)
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
