/** Browser checks for the existing application's presentation and shell.
 * Requires Playwright; PLAYWRIGHT_MODULE_PATH can point to an installed package.
 * Start the local static server before running this file. No datasets are mocked.
 */
'use strict';
const assert = require('node:assert/strict');
const { chromium } = require(process.env.PLAYWRIGHT_MODULE_PATH || 'playwright');
const path = require('node:path');

(async () => {
  const browser = await chromium.launch({ headless: true,
    ...(process.env.BROWSER_EXECUTABLE ? { executablePath: process.env.BROWSER_EXECUTABLE } : {}) });
  const base = process.env.UI_BASE_URL || 'http://127.0.0.1:8801';
  const errors = [];
  const failedRequests = [];
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  page.on('pageerror', error => errors.push(error.message));
  page.on('requestfailed', request => failedRequests.push({ url: request.url(), reason: request.failure().errorText }));
  try {
    await page.goto(base + '/index.html', { waitUntil: 'domcontentloaded', timeout: 60000 });
    await page.evaluate(() => document.fonts.ready);
    if (process.env.UI_REQUIRE_FULL_RUNTIME === '1') {
      await page.getByRole('button', { name: 'Understood', exact: true }).click({ timeout: 15000 });
      await page.waitForSelector('#map.leaflet-container', { timeout: 15000 });
      await page.waitForFunction(() => {
        let loaded = false;
        window.map.eachLayer(layer => {
          if (typeof layer.getMaplibreMap !== 'function') return;
          const renderer = layer.getMaplibreMap();
          loaded = renderer.isStyleLoaded() && renderer.areTilesLoaded();
        });
        return loaded;
      }, undefined, { timeout: 45000 });
      assert.equal(await page.locator('.maplibregl-canvas').isVisible(), true);
      assert.equal(await page.locator('.leaflet-control-attribution').getByRole('link', { name: 'OpenFreeMap', exact: true }).isVisible(), true);
      // Check the existing close control independently of domain content.
      await page.evaluate(() => { document.getElementById('unitModal').style.display = 'flex'; });
      await page.getByRole('button', { name: 'Close details', exact: true }).click();
      assert.equal(await page.locator('#unitModal').isVisible(), false);
    }
    const ids = await page.evaluate(() => [...document.querySelectorAll('[id]')].map(node => node.id));
    assert.equal(ids.filter(id => id === 'mainContent').length, 1);
    assert.equal(await page.locator('.nav-rail button').count(), 8);
    assert.equal(await page.locator('.acled-header').isVisible(), true);
    assert.equal(await page.locator('.nav-item.active').getAttribute('aria-current'), 'page');
    const collapse = page.getByRole('button', { name: 'Toggle sidebar', exact: true });
    await collapse.focus();
    await page.keyboard.press('Enter');
    await page.waitForFunction(() => document.getElementById('sidebarShell').inert);
    assert.equal(await collapse.getAttribute('aria-expanded'), 'false');
    await page.keyboard.press('Enter');
    await page.waitForFunction(() => !document.getElementById('sidebarShell').inert);
    assert.equal(await collapse.getAttribute('aria-expanded'), 'true');
    for (const width of [320, 390, 768, 1024, 1440, 1920]) {
      await page.setViewportSize({ width, height: 1000 });
      const geometry = await page.evaluate(() => {
        const box = selector => { const r = document.querySelector(selector).getBoundingClientRect(); return { x: r.x, right: r.right, top: r.top, width: r.width, height: r.height }; };
        return { width: innerWidth, pageWidth: document.documentElement.scrollWidth, header: box('.acled-header'), sidebar: box('#sidebarShell'), main: box('#mainContent'), nav: box('.nav-rail') };
      });
      assert.ok(geometry.pageWidth <= width, 'Page overflow at ' + width);
      for (const name of ['header', 'sidebar', 'main', 'nav']) {
        assert.ok(geometry[name].right <= width + 1, name + ' outside viewport: ' + JSON.stringify(geometry));
        assert.ok(geometry[name].width > 0 && geometry[name].height > 0, name + ' collapsed unexpectedly');
      }
      assert.ok(geometry.main.top >= geometry.header.height - 1);
      if (width === 390) await page.screenshot({ path: path.resolve('reports/current-ui-mobile.png') });
      if (width === 1440) {
        await page.screenshot({ path: path.resolve('reports/current-ui-desktop.png') });
        await page.locator('.drawer-handle').click();
        await page.waitForFunction(() => {
          const drawer = document.getElementById('analyticsDrawer');
          return drawer.classList.contains('expanded') && drawer.getBoundingClientRect().height >= 439;
        });
        assert.equal(await page.locator('.drawer-body').evaluate(node => node.scrollWidth <= node.clientWidth), true);
        await page.screenshot({ path: path.resolve('reports/current-ui-analytics.png') });
        await page.locator('.drawer-handle').click();
      }
    }
    await page.emulateMedia({ reducedMotion: 'reduce' });
    assert.ok(await collapse.evaluate(node => parseFloat(getComputedStyle(node).transitionDuration) <= .001));
    await page.goto(base + '/legal.html', { waitUntil: 'domcontentloaded' });
    for (const width of [390, 1440]) {
      await page.setViewportSize({ width, height: 1000 });
      assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
      assert.equal(await page.getByRole('heading', { name: 'Legal & Terms of Service', exact: true }).isVisible(), true);
    }
    await page.screenshot({ path: path.resolve('reports/current-ui-document.png') });
    await page.goto(base + '/report.html', { waitUntil: 'domcontentloaded' });
    for (const width of [390, 1440]) {
      await page.setViewportSize({ width, height: 1000 });
      assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true, 'Report overflow at ' + width);
    }
    const localFailures = failedRequests.filter(request => request.url.startsWith(base) && request.reason !== 'net::ERR_ABORTED');
    assert.deepEqual(localFailures, [], 'Local assets failed to load');
    console.log(JSON.stringify({ result: 'Shell and reading-page checks passed', runtimeErrors: [...new Set(errors)], externalRequestFailures: failedRequests.filter(request => !request.url.startsWith(base)).length }, null, 2));
    if (process.env.UI_REQUIRE_FULL_RUNTIME === '1') assert.deepEqual(errors, [], 'Application runtime errors');
  } finally {
    await browser.close();
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
