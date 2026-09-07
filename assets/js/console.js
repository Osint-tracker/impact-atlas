/**
 * IMPACT ATLAS — Command Console Controller
 * ----------------------------------------------------------------------------
 * Drives the command bar telemetry (data-age readout, system status) and
 * provides keyboard navigation for the operations rail. Defensive by design:
 * every hook is feature-detected so partial loads never throw.
 *
 * Contract:
 *   #dataAgeBadge  — text + freshness class, sourced from window.eventsMetadata
 *                    (set by map.js when the event payload loads).
 *   #sysStatusPill — flipped to DEGRADED if the clock heartbeat stalls.
 *   Keys 1-7       — switch rail panels via the global navSwitchTab().
 */

(function (global) {
  'use strict';

  var AGE_REFRESH_MS = 30 * 1000;
  var CLOCK_GRACE_MS = 4000;

  var AGE_CLASSES = {
    fresh: 'age-fresh',    // < 6h
    stale: 'age-stale',    // 6-24h
    aged: 'age-aged'       // > 24h
  };

  var PANEL_KEYS = {
    '1': 'layers',
    '2': 'orbat',
    '3': 'intel',
    '4': 'tempo',
    '5': 'losses',
    '6': 'analytics',
    '7': 'strategic_campaigns'
  };

  function getEl(id) {
    return document.getElementById(id);
  }

  /* --- Data age ------------------------------------------------------------- */

  function parseGeneratedAt(metadata) {
    if (!metadata || !metadata.generated_at) return null;
    var ts = Date.parse(metadata.generated_at);
    return Number.isNaN(ts) ? null : ts;
  }

  function formatAge(deltaMs) {
    if (deltaMs < 0) deltaMs = 0;
    var totalSec = Math.floor(deltaMs / 1000);
    var days = Math.floor(totalSec / 86400);
    if (days >= 1) return days + 'D ' + Math.floor((totalSec % 86400) / 3600) + 'H';
    var h = Math.floor(totalSec / 3600);
    var m = Math.floor((totalSec % 3600) / 60);
    var s = totalSec % 60;
    return String(h).padStart(2, '0') + ':' +
      String(m).padStart(2, '0') + ':' +
      String(s).padStart(2, '0');
  }

  function ageClass(deltaMs) {
    if (deltaMs < 6 * 3600 * 1000) return AGE_CLASSES.fresh;
    if (deltaMs < 24 * 3600 * 1000) return AGE_CLASSES.stale;
    return AGE_CLASSES.aged;
  }

  function updateDataAge() {
    var badge = getEl('dataAgeBadge');
    if (!badge) return;
    var generatedAt = parseGeneratedAt(global.eventsMetadata);
    if (generatedAt === null) return; // keep placeholder until data lands
    var delta = Date.now() - generatedAt;
    badge.textContent = formatAge(delta);
    badge.classList.remove(AGE_CLASSES.fresh, AGE_CLASSES.stale, AGE_CLASSES.aged);
    badge.classList.add(ageClass(delta));
  }

  /* --- System heartbeat ------------------------------------------------------- */

  function watchHeartbeat() {
    var pill = getEl('sysStatusPill');
    var clock = getEl('liveClockDisplay');
    if (!pill || !clock) return;
    var lastTick = Date.now();
    var observer = new MutationObserver(function () {
      lastTick = Date.now();
      pill.classList.add('pill--ok');
      pill.classList.remove('pill--crit');
      if (pill.lastChild) pill.lastChild.textContent = 'ONLINE';
    });
    observer.observe(clock, { childList: true, characterData: true, subtree: true });
    setInterval(function () {
      if (Date.now() - lastTick > CLOCK_GRACE_MS) {
        pill.classList.remove('pill--ok');
        pill.classList.add('pill--crit');
        if (pill.lastChild) pill.lastChild.textContent = 'DEGRADED';
      }
    }, CLOCK_GRACE_MS);
  }

  /* --- Keyboard rail navigation ------------------------------------------------- */

  function isTypingContext(target) {
    if (!target) return false;
    var tag = (target.tagName || '').toLowerCase();
    return tag === 'input' || tag === 'select' || tag === 'textarea' || target.isContentEditable;
  }

  function bindKeyboardNav() {
    global.addEventListener('keydown', function (ev) {
      if (ev.ctrlKey || ev.altKey || ev.metaKey || ev.shiftKey) return;
      if (isTypingContext(ev.target)) return;
      var panelId = PANEL_KEYS[ev.key];
      if (!panelId) return;
      if (typeof global.navSwitchTab !== 'function') return;
      var navItem = document.querySelector('.nav-item[data-panel="' + panelId + '"]');
      if (!navItem) return;
      ev.preventDefault();
      global.navSwitchTab(panelId, navItem);
    });
  }

  /* --- Boot ------------------------------------------------------------------------ */

  function boot() {
    updateDataAge();
    setInterval(updateDataAge, AGE_REFRESH_MS);
    watchHeartbeat();
    bindKeyboardNav();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot);
  } else {
    boot();
  }
})(typeof window !== 'undefined' ? window : this);
