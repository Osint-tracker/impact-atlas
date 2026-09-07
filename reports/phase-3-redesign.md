# Phase 3 — UI/UX "Commercial-Ready" Redesign

**Status:** ready for review
**Scope:** C4ISR command-console redesign of the main dashboard plus the
secondary pages, built as a layered design system on top of the Phase 2
sanitization foundation. All existing functionality, element IDs, and JS
behavior hooks are preserved and machine-verified.

## 1. Design system (`assets/css/theme.css`) — new

Single source of truth for the visual language:

* **CODEX tokens** — a 13-step slate scale for surfaces, lines, and text.
* **SIGNAL tokens** — the amber accent scale plus operational status hues
  (crit/high/med/ok/info, UA/RU faction colors).
* **Semantic tokens** — surfaces, lines, text, fonts, font sizes, spacing,
  radii, elevation, and motion, all var()-referenced so no component hardcodes a value.
* **Typography law** — Inter for prose, JetBrains Mono for every data-bearing
  element (numbers, timestamps, coordinates, IDs, classifications), with
  tabular numerals on all telemetry values.
* **Component primitives** — `.console-panel`, `.pill` status badges,
  `.btn` family, `.console-input/select` terminal-grade form controls,
  `.u-label`/`.u-value` data readouts, `.bg-grid` ambient tactical texture.
* **Accessibility** — visible amber `:focus-visible` law, instrument
  scrollbars, `prefers-reduced-motion` respected, `color-scheme: dark`.

## 2. Console redesign layer (`assets/css/console.css`) — new

Loaded after the legacy stylesheets; restyles every existing surface onto the
token system without touching markup contracts:

* Command bar (header): amber signal keyline, micro-grid texture, mono
  telemetry blocks with tabular numerals.
* Nav rail: 56px instrument rail, amber active state with 2px signal edge.
* Sidebar: section keylines, layer cards with hover/active signal borders,
  toggle styling.
* Map chrome: top bar, tactical time command bar, quick-search, buttons,
  dividers.
* KPI readouts and dossier stat values forced to mono/tabular numerals.
* Responsive degradation ladder: 1280px (secondary telemetry hides), 1024px
  (classification strip hides, nav links collapse), 768px (compact 44px bar).
* Data-age semantics (`#dataAgeBadge` green < 6h / yellow < 24h / red beyond).

## 3. Command bar (index.html)

The toy-feeling header (fake "Login" button, dead anchors, plain brand) is
replaced with an operational command bar:

| Block | Content |
| --- | --- |
| Brand | `IMPACT▮ATLAS` wordmark, mono letterform, amber diamond pip |
| Classification strip | `OSINT // OPEN SOURCE` (correct labeling for an open-source product) |
| Telemetry | **EVENTS** (`#eventCount`, live from map.js), **DATA CUT** (`#lastUpdate`), **DATA AGE** (`#dataAgeBadge`, live), **TIME ZULU** (`#liveClockDisplay`, live 1s tick), **SYSTEM** pill (heartbeat-driven ONLINE/DEGRADED) |
| Navigation | Briefing (report.html), Methodology (legal.html), Export CSV (real download of `events_export.csv`) |

The redundant KPI strip was removed (its live IDs moved into the command bar —
zero JS changes needed). The duplicated map-topbar clock was consolidated into
the single Zulu clock in the command bar.

## 4. Console controller (`assets/js/console.js`) — new

* **Data age** — computed from the artifact metadata set by map.js
  (`window.eventsMetadata`), refreshed every 30s, colored by staleness class.
  Never issues an extra multi-megabyte fetch.
* **System heartbeat** — a MutationObserver watches the clock; if the tick
  stalls, the SYSTEM pill flips to DEGRADED.
* **Keyboard navigation** — keys 1-7 switch rail panels through the existing
  global `navSwitchTab()`, with typing-context guards; nav items document
  their key hints.
* Fully defensive: every hook is feature-detected; partial loads never throw.

## 5. Secondary pages

* **report.html** — mislabeled `CLASSIFIED // OSINT ONLY` corrected to
  `OSINT // OPEN SOURCE`; fake OPERATOR/LOGOUT profile replaced with a
  functional "Return to Console" button; theme tokens linked; hardcoded
  briefing date now purely dynamic (report.js already fills it).
* **legal.html** — matching command bar, theme tokens remapped from its
  hardcoded palette, JetBrains Mono loaded, grid texture, panel elevation.
* **campaign_dossier_console.html** — verified already on-palette (modal
  overlay, self-styled); left functionally untouched.
* **admin_merge.html** — internal operator tool, deliberately left unstyled
  (out of the product surface).

## 6. Structure and hygiene defects fixed along the way

* `style.css` and `dossier_styles.css` were loaded **twice** with conflicting
  cache versions; `dashboard.css` loaded mid-body. All stylesheets now load
  once, in the head, in dependency order: `theme → legacy styles → console`.
* `<main class="main-content">` was **never closed** (pre-existing since
  before Phase 2, confirmed against git HEAD); fixed before the fixed-position
  tutorial modal.
* Intel feed rows rendered **unescaped** event titles/dates/categories/source
  names into `innerHTML` (last remaining known XSS surface); all now pass
  through `SafeDom.escapeHtml`.
* Broken Font Awesome class (`fa-bullseye-arrow`, FA5 name) replaced with the
  valid `fa-bullseye`.
* Page title/meta rewritten to a product identity: "Impact Atlas — OSINT
  Command Console".

## 7. Machine verification (new standing gates)

* **`tests/verify_dom_ids.py`** — extracts every statically referenced
  element ID from external JS and all inline script blocks and asserts they
  exist in the page. Confirms: 0 missing after the redesign (the only ID
  removed from the page, `categoryStats`, is referenced by no script; three
  dangling references found pre-exist in git HEAD and are null-guarded,
  tracked as a documented baseline).
* **`tests/verify_html_structure.py`** — tolerant tag-balance parse of every
  edited page: 0 findings across index/report/legal.
* Both scripts are wired into CI (`.github/workflows/ci.yml`), documented in
  the README, and pass locally along with the full Phase 2 gate:

```
ruff     → All checks passed (canonical tier incl. tests)
pytest   → 21/21
node     → all 8 JS modules pass --check
verify_dom_ids.py        → PASS
verify_html_structure.py → PASS
```

## 8. Deliberate decisions

* **Layered override, not rewrite.** map.js alone references 129 element IDs
  and ~7,000 lines of behavior; rebuilding the markup from scratch would be
  unverifiable without browser automation. The redesign therefore ships as
  tokens + a restyle layer + surgical markup changes, with the ID and
  structure contracts machine-checked.
* **No fake chrome.** Every visible control now does something real (links,
  exports, panels). No login buttons, no decorative user profiles, no
  mislabeled classification banners.
* **Zero new network cost.** The command bar runs on data the page already
  loads; the data-age readout reads in-memory metadata rather than re-fetching
  the 20 MB payload.

## 9. Verification boundaries

No browser session was driven (no Playwright/puppeteer in this environment);
all guarantees are static — structure, IDs, JS syntax, and the lint/type/test
gates. Visual tuning (exact pixel spacing, chart palettes on real data) should
get one human pass on the live dashboard, and the restored Phase 2 artifacts
(`events_latest.json` set) should be regenerated via `run_tracker.ps1` so the
command bar telemetry populates with fresh values.