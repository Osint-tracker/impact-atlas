/** Run only through run_admin_recovery_browser.py, which supplies a temporary DB. */
'use strict';
const assert = require('node:assert/strict');
const { chromium } = require(process.env.PLAYWRIGHT_MODULE_PATH || 'playwright');

(async () => {
  if (!process.env.RECOVERY_TEST_TOKEN || !process.env.UI_BASE_URL) throw new Error('Use the disposable database harness.');
  const browser = await chromium.launch({ headless: true,
    ...(process.env.BROWSER_EXECUTABLE ? { executablePath: process.env.BROWSER_EXECUTABLE } : {}) });
  const page = await browser.newPage();
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  // Typography is unrelated to this local recovery test; keep it network-independent.
  await page.route('https://fonts.googleapis.com/**', route => route.abort());
  await page.route('https://fonts.gstatic.com/**', route => route.abort());
  page.on('dialog', dialog => dialog.accept());
  try {
    await page.goto(process.env.UI_BASE_URL + '/admin_merge.html');
    await page.getByLabel('Admin access token').fill(process.env.RECOVERY_TEST_TOKEN);
    await page.getByRole('button', { name: 'Connect', exact: true }).click();
    await page.waitForFunction(() => document.querySelectorAll('#eventTableBody tr').length === 2);
    await page.locator('#eventTableBody input[type=checkbox]').nth(0).check();
    await page.waitForFunction(() => document.querySelectorAll('.staged-card').length === 1);
    await page.locator('#eventTableBody input[type=checkbox]').nth(1).check();
    await page.waitForFunction(() => document.querySelectorAll('.staged-card').length === 2);
    assert.equal(await page.locator('#mergeBtn').isEnabled(), true);
    await page.locator('#mergeBtn').click();
    await page.waitForFunction(() => document.querySelectorAll('.history-entry').length === 1);
    await page.getByText('Recovery history', { exact: true }).click();
    await page.getByRole('button', { name: 'Undo merge', exact: true }).click();
    await page.waitForFunction(() => document.querySelector('#mergeHistoryList button')?.textContent === 'Restored');
    assert.equal(await page.getByRole('button', { name: 'Restored', exact: true }).isDisabled(), true);
    await page.waitForFunction(() => document.getElementById('statusBar').textContent === 'Restored 2 records.');
    assert.equal(await page.locator('.staged-card').count(), 0);
    assert.deepEqual(errors, []);
    console.log('Admin recovery UI and real HTTP round trip passed.');
  } finally {
    await browser.close();
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
