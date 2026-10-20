'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

test('existing modal closes from its overlay, close button, nested icon and programmatic call', () => {
  const source = fs.readFileSync(path.join(__dirname, '../assets/js/map.js'), 'utf8');
  const start = source.indexOf('window.closeUnitModal = function');
  const end = source.indexOf('window.openUnitModalFromId', start);
  assert.ok(start >= 0 && end > start);
  let dialog = { style: { display: 'flex' } };
  const context = vm.createContext({ window: {}, document: { getElementById: () => dialog } });
  vm.runInContext(source.slice(start, end), context);
  const close = context.window.closeUnitModal;
  close({ target: { closest: () => null } });
  assert.equal(dialog.style.display, 'flex', 'Content clicks must not dismiss the dialog');
  for (const target of [dialog, { closest: () => ({}) }]) {
    dialog.style.display = 'flex';
    close({ target });
    assert.equal(dialog.style.display, 'none');
  }
  dialog.style.display = 'flex';
  close();
  assert.equal(dialog.style.display, 'none');
  dialog = null;
  assert.doesNotThrow(() => close());
});

test('missing basemap renderer produces one accessible notice without breaking the page', () => {
  let notice = null;
  const host = { querySelector: () => notice, appendChild: node => { notice = node; } };
  const context = vm.createContext({
    window: {}, console: { warn() {} },
    document: { createElement: () => ({ attributes: {}, setAttribute(name, value) { this.attributes[name] = value; } }) }
  });
  vm.runInContext(fs.readFileSync(path.join(__dirname, '../assets/js/basemap.js'), 'utf8'), context);
  const map = { getContainer: () => host };
  assert.equal(context.window.AtlasBasemap.addTo(map), null);
  assert.equal(notice.attributes.role, 'status');
  assert.match(notice.textContent, /Background map unavailable/);
  const first = notice;
  context.window.AtlasBasemap.addTo(map);
  assert.equal(notice, first);
});
