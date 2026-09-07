/**
 * Impact Atlas -- centralized DOM sanitization layer.
 *
 * Every dynamic, data-backed string rendered into the DOM must pass through
 * this module. It is loaded before all other project scripts and exposes a
 * small, auditable API:
 *
 *   escapeHtml(value)   -- neutralize &, <, >, ", ' for HTML text/attributes
 *   escapeAttr(value)   -- alias of escapeHtml (semantic clarity at call sites)
 *   sanitizeUrl(url)    -- allow only http/https URLs; '#' otherwise
 *   safeCssUrl(url)     -- sanitizeUrl + CSS-context escaping for url('...')
 *   setText(el, text)   -- set textContent safely (preferred over innerHTML)
 *
 * Never assign data-derived values to innerHTML without escapeHtml, and never
 * place a data-derived URL into href/src/style without sanitizeUrl.
 */
(function (global) {
  'use strict';

  var HTML_ESCAPE_MAP = {
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;'
  };

  function escapeHtml(value) {
    if (value === null || value === undefined) return '';
    return String(value).replace(/[&<>"']/g, function (ch) {
      return HTML_ESCAPE_MAP[ch];
    });
  }

  function sanitizeUrl(url) {
    if (url === null || url === undefined) return '#';
    var raw = String(url).trim();
    if (/^https?:\/\//i.test(raw)) return raw;
    return '#';
  }

  function safeCssUrl(url) {
    var raw = sanitizeUrl(url);
    if (raw === '#') return '';
    return raw.replace(/["'()\\]/g, function (ch) {
      return '\\' + ch.charCodeAt(0).toString(16).toLowerCase() + ' ';
    });
  }

  function setText(el, text) {
    if (el) el.textContent = text === null || text === undefined ? '' : String(text);
  }

  global.SafeDom = {
    escapeHtml: escapeHtml,
    escapeAttr: escapeHtml,
    sanitizeUrl: sanitizeUrl,
    safeCssUrl: safeCssUrl,
    setText: setText
  };

  global.escapeHtml = escapeHtml;
  global.escapeAttr = escapeHtml;
  global.sanitizeUrl = sanitizeUrl;
  global.safeCssUrl = safeCssUrl;
  global.setText = setText;
})(typeof window !== 'undefined' ? window : this);
