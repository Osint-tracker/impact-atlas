/** Browser security checks on the real admin page with isolated hostile fixtures. */
'use strict';
const assert = require('node:assert/strict');
const { chromium } = require(process.env.PLAYWRIGHT_MODULE_PATH || 'playwright');

(async () => {
  const browser = await chromium.launch({ headless: true,
    ...(process.env.BROWSER_EXECUTABLE ? { executablePath: process.env.BROWSER_EXECUTABLE } : {}) });
  const page = await browser.newPage();
  const base = process.env.UI_BASE_URL || 'http://127.0.0.1:8801';
  const errors = [];
  const requests = [];
  const token = 'test-reader-' + 'x'.repeat(40);
  const hostile = `'"><img src=x onerror="window.securityProbe=true">`;
  const event = { event_id: hostile, title: hostile, date: hostile, sector: hostile,
    tie_score: 0, description: hostile };
  page.on('pageerror', error => errors.push(error.message));
  await page.route('https://fonts.googleapis.com/**', route => route.abort());
  await page.route('https://fonts.gstatic.com/**', route => route.abort());
  await page.route(base + '/api/**', async route => {
    const request = route.request();
    requests.push(request);
    assert.equal(request.headers().authorization, 'Bearer ' + token);
    assert.equal(request.method(), 'GET', 'Security checks must never mutate data');
    const pathname = new URL(request.url()).pathname;
    const payload = pathname === '/api/session' ? { role: 'reader' } :
      pathname === '/api/sectors' ? { sectors: [{ name: hostile, count: 1 }] } :
      pathname === '/api/merge-history' ? { changes: [{ merge_id: hostile, created_at: hostile, record_count: 2, reverted_at: null }] } :
      pathname.startsWith('/api/event/') ? event :
      { events: [event], page: 1, pages: 1, total: 1 };
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify(payload) });
  });
  try {
    await page.goto(base + '/admin_merge.html', { waitUntil: 'load' });
    assert.equal(requests.length, 0, 'No API requests before authentication');
    assert.equal(await page.locator('#adminWorkspace').evaluate(node => node.inert), true);
    await page.getByLabel('Admin access token').fill(token);
    await page.getByRole('button', { name: 'Connect', exact: true }).click();
    await page.waitForFunction(() => document.querySelectorAll('#eventTableBody tr').length === 1);
    assert.equal(await page.locator('#eventTableBody .title-text').textContent(), hostile);
    assert.equal(await page.locator('#eventTableBody img').count(), 0);
    assert.equal(await page.locator('#adminToken').inputValue(), '');
    await page.waitForFunction(() => document.querySelectorAll('.history-entry').length === 1);
    assert.equal(await page.locator('#mergeHistoryList img').count(), 0);
    assert.equal(await page.locator('#mergeHistoryList button').isDisabled(), true);
    await page.locator('#eventTableBody input[type=checkbox]').check();
    await page.waitForFunction(() => document.querySelectorAll('.staged-card').length === 1);
    assert.equal(await page.locator('.staged-card .card-title').textContent(), hostile);
    assert.equal(await page.locator('.staged-card img').count(), 0);
    assert.equal(await page.locator('#mergeBtn').isDisabled(), true);
    await page.evaluate(value => setStatus(value, true), hostile);
    assert.equal(await page.locator('#statusBar').textContent(), hostile);
    assert.equal(await page.locator('#statusBar img').count(), 0);
    assert.equal(await page.evaluate(() => window.securityProbe), undefined);
    assert.equal(await page.evaluate(value => Object.values(localStorage).concat(Object.values(sessionStorage)).some(v => v.includes(value)), token), false);
    await page.getByRole('button', { name: 'Disconnect', exact: true }).click();
    assert.equal(await page.locator('#adminWorkspace').evaluate(node => node.inert), true);
    assert.equal(await page.locator('#eventTableBody tr').count(), 0);
    assert.equal(await page.locator('.staged-card').count(), 0);
    assert.equal(await page.locator('.history-entry').count(), 0);
    assert.deepEqual(errors, []);
    console.log('Admin authentication UI, reader permissions, logout and hostile HTML checks passed.');
  } finally {
    await browser.close();
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
