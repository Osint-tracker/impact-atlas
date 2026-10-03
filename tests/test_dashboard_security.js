/* Offline execution of the real renderer with an instrumented DOM boundary. */
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

function setup() {
    const listeners = {};
    const card = {
        dataset: { lossIndex: '0' }, style: {},
        addEventListener: (name, callback) => { listeners[name] = callback; },
        querySelector: () => null
    };
    const content = { innerHTML: '', querySelectorAll: () => [card] };
    const tooltip = { style: {}, replaceChildren: image => { tooltip.image = image; } };
    const context = vm.createContext({
        console: { log() {}, warn() {} },
        document: {
            getElementById: id => id === 'imint-hover-tooltip' ? tooltip : content,
            createElement: () => ({ style: {} })
        }
    });
    context.window = context;
    for (const file of ['safe_dom.js', 'dashboard.js']) {
        vm.runInContext(fs.readFileSync(path.join(__dirname, '../assets/js', file), 'utf8'), context);
    }
    const ticker = vm.runInContext('Object.create(EquipmentTicker.prototype)', context);
    ticker.content = content;
    return { context, ticker, content, tooltip, listeners };
}

test('loss rendering escapes source text, URLs and removes data-backed handlers', () => {
    const { ticker, content, listeners } = setup();
    const attack = '\"><img src=x onerror=alert(1)>';
    ticker.data = [{ model: attack, type: attack, source_tag: attack, date: attack,
        status: attack, proof_url: 'javascript:alert(1)', visual_evidence: attack,
        lat: '0', lon: 0 }];
    ticker.render();
    assert.ok(!content.innerHTML.includes('<img'));
    assert.ok(!content.innerHTML.includes('javascript:'));
    assert.ok(!/\son(?:click|mouseover)="/.test(content.innerHTML));
    assert.ok(content.innerHTML.includes('&lt;img'));
    let clicked;
    ticker._flyToLoss = (...values) => { clicked = values; };
    listeners.click();
    assert.deepEqual(clicked, [0, 0, attack]);
});

test('tooltip URL is assigned as data and active schemes are rejected', () => {
    const { context, tooltip } = setup();
    context.showImintTooltip({ getBoundingClientRect: () => ({ left: 250, top: 10 }) }, 'javascript:alert(1)');
    assert.equal(tooltip.image.src, '#');
    assert.equal(tooltip.innerHTML, undefined);
});

test('map popup keeps the model name as text, including at zero coordinates', () => {
    const { context, ticker } = setup();
    let popup;
    const marker = {
        addTo: () => marker,
        bindPopup: html => { popup = html; return marker; },
        openPopup() {}
    };
    context.map = { flyTo() {}, removeLayer() {} };
    context.L = { circleMarker: () => marker };
    context.setTimeout = () => {};
    ticker._flyToLoss(0, 0, '<img src=x onerror=alert(1)>');
    assert.ok(!popup.includes('<img'));
    assert.ok(popup.includes('&lt;img'));
});

test('inline handler escaping preserves data without executing apostrophes or backslashes', () => {
    const { context } = setup();
    const payload = "'); globalThis.injected = true; //\\\"<>&\n";
    const html = context.escapeJsString(payload);
    assert.ok(!/[<>"']/.test(html));
    const entities = { '&amp;': '&', '&lt;': '<', '&gt;': '>', '&quot;': '"', '&#39;': "'" };
    const decoded = html.replace(/&(amp|lt|gt|quot|#39);/g, entity => entities[entity]);
    assert.equal(vm.runInContext("'" + decoded + "'", context), payload);
    assert.equal(context.injected, undefined);
});

test('intelligence detail escapes source data and tolerates missing coordinates', () => {
    const { context, content } = setup();
    context.Chart = { defaults: { font: {}, scale: { grid: {} } } };
    context.moment = () => ({ format: () => '2026-01-01' });
    vm.runInContext(fs.readFileSync(path.join(__dirname, '../assets/js/charts.js'), 'utf8'), context);
    context.showIntelDetail({ title: '<img src=x>', type: '<script>', description: 'A < B', lat: null, lon: 'bad' });
    assert.ok(!content.innerHTML.includes('<img'));
    assert.ok(!content.innerHTML.includes('<script>'));
    assert.ok(content.innerHTML.includes('Unknown, Unknown'));
    assert.ok(content.innerHTML.includes('A &lt; B'));
});
