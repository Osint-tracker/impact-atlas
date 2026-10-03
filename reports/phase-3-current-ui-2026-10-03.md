# Phase 3 — Existing application presentation update

Date: 3 October 2026

## Delivered

The visual update is integrated into the existing `index.html`, `report.html`, and `legal.html`. The separate fictional research preview, its JavaScript/CSS, test, screenshots, and report have been removed. The application continues to use its existing datasets, resource URLs, handlers, and processing logic.

This delivery concerns general presentation, responsive layout, and shell accessibility. It does not introduce or modify domain analysis, collection, scoring, filtering algorithms, or operational workflows. It is not a claim that every application view has been redesigned or that the unresolved backend risks from Phase 2 have been resolved.

## Changes

- Added `assets/css/appearance.css` as the final shared presentation layer: neutral slate surfaces, restrained amber accents, system UI text, local monospace typography, consistent borders, visible focus, and reduced-motion support.
- Removed the inline rules that forced the old background colors and hid every header. The existing main header and its links are visible again.
- Replaced the conflicting mobile layout in `assets/css/style.css`. The sidebar and main content now remain within the application layout instead of being positioned independently over the entire viewport. Narrow screens use a vertically scrollable workspace with a persistent navigation rail.
- Converted the existing navigation entries from clickable `div` elements into native buttons, preserving their handlers and panel identifiers. Navigation icons are local inline SVGs.
- Added a skip link, current-view state, expanded/collapsed state, and `inert` handling for the collapsed sidebar. The observer watches only the shell and navigation button classes, not map animations or the entire content subtree.
- Applied the same visual foundation to the existing briefing and information pages, plus the existing first-run guide. Placeholder notification/account controls without handlers are hidden.
- Updated cache versions for the changed shared JavaScript and legacy stylesheet.
- Retained the local font and its accompanying license in `assets/fonts/`.

All original element IDs in `index.html` are preserved. Its inline JavaScript blocks are byte-equivalent after text newline normalization to those at Git HEAD. The added controller code concerns shell accessibility only.

## Verification

`tests/current_ui.browser.cjs` exercises the actual application without fictional datasets or mocked domain APIs. The full-runtime run used headless Microsoft Edge with network access to the existing public CDN dependencies.

Passed:

- Header visibility, eight native navigation buttons, and current-view semantics.
- Sidebar collapse/reopen through the keyboard, expanded state, and focus exclusion of collapsed content.
- Header, sidebar, navigation, and main-content bounds at 320, 390, 768, 1024, 1440, and 1920 pixels.
- Reduced-motion behavior.
- Existing information and briefing page overflow checks at 390 and 1440 pixels.
- Existing guide dismissal and map container initialization during the full-runtime run.
- No JavaScript runtime errors and no failed external requests observed during that run.
- Five existing JavaScript rendering-security regressions.
- Original-ID preservation and unchanged inline JavaScript comparison.
- `tests/verify_dom_ids.py` passed its inline-reference checks; its external-reference findings remain advisory and are not comprehensive DOM validation.
- JavaScript syntax validation and `git diff --check`.
- Source/documentation scan: no occurrence of the prohibited reference name.

Screenshots of the real pages were visually inspected:

- [Desktop](current-ui-desktop.png)
- [Mobile](current-ui-mobile.png)
- [Information page](current-ui-document.png)

## Observed limitation

The base-map provider currently returns images bearing **API KEY REQUIRED**. Those images can arrive successfully over HTTP, so an absence of network errors is not proof that the base-map service is correctly configured. This was observed in the real application screenshot. Provider credentials and tile-source configuration were not changed in this presentation update.

The initial sandboxed browser run could not access external CDN resources and consequently reported missing `L` and `Chart` globals. A subsequent network-enabled run loaded those dependencies and passed without those errors. External-provider availability remains a deployment consideration.

The standalone admin and campaign-detail pages have not received an individual redesign in this delivery. There was no exhaustive screen-reader audit, multi-browser certification, backend/load test, remote CI run, or deployment. Earlier Phase 1/2 risk reports remain applicable.

## Reproduce

Start the existing server with `.venv\Scripts\python.exe scripts/admin_api.py` and open `http://127.0.0.1:8800/index.html`. The active local review server uses port 8801.

With Playwright available to Node, or `PLAYWRIGHT_MODULE_PATH` pointing to an installed package:

```powershell
$env:UI_BASE_URL = 'http://127.0.0.1:8800'
$env:BROWSER_EXECUTABLE = 'C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe'
$env:UI_REQUIRE_FULL_RUNTIME = '1'
node tests/current_ui.browser.cjs
node --test tests/test_dashboard_security.js
.venv\Scripts\python.exe tests/verify_dom_ids.py
git diff --check
```

The full-runtime browser checks require access to the application's existing CDN dependencies. Without `UI_REQUIRE_FULL_RUNTIME=1`, the script checks shell geometry and reports runtime errors separately; such a run must not be described as a successful full application runtime check.
