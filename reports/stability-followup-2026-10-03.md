# Stability and interface follow-up — 3 October 2026

**Status: scoped fixes implemented in the existing application. The full risk ledger and requested production transition are not complete.**

This report supersedes the status of the items changed here, while preserving the earlier phase reports as historical records. Work is limited to general software security, dependency reproducibility, background cartography and presentation. Operational military-event correlation, positioning, unit analysis and dossier processing were not enhanced.

**Subsequent update:** [Transactional admin recovery](admin-recovery-2026-10-03.md) is now implemented for newly recorded merges. Its verification supersedes the earlier counts below: 126 Python tests and strict typing of 13 modules. The table below reflects that update; the implementation narrative and earlier verification remain a record of the preceding change.

## Implemented in the real project

### Administrator authentication and rendering security

The local API now requires a bearer capability before accessing any API endpoint. Read and write capabilities are distinct; readers cannot POST merge or unmerge requests. Invalid configuration fails closed, token comparisons use `hmac.compare_digest`, and token values are excluded from the configuration object's representation. Unauthorized responses are 401; insufficient write permission is 403. JSON responses are not cacheable.

The admin page starts locked. It stores the token only in memory, clears the password field after submission, and clears displayed records on disconnect. Disconnect aborts pending requests. Source-controlled text, identifiers, dates and sectors are escaped in the table and queue; status messages use text nodes. Event identifiers are passed through listeners rather than interpolated into executable handlers. API path segments are URL encoded.

This remains a localhost tool. It does not implement hosted identity management, individual user audit trails, TLS, session revocation across clients or reversible merge history. Read/write tokens are capabilities, not user identities. Existing same-origin and public-file allowlist protections remain in place.

### Background cartography

The previous CARTO raster endpoint displayed API-key-required tiles during visual verification. The main map now uses the OpenFreeMap dark vector basemap, MapLibre GL 5.6.1 and its Leaflet bridge 0.0.22. Duplicate Leaflet/MarkerCluster imports were removed so they cannot replace the registered bridge. Leaflet's zoom limit remains explicitly 19; the vector layer does not supply the old raster layer's limit automatically.

[OpenFreeMap's public instance](https://openfreemap.org/) is free and requires neither registration nor API keys. It uses OpenStreetMap/OpenMapTiles data; this changes the hosting/rendering provider, not the underlying data's origin. Attribution is visible, including when the analytics drawer is expanded. There is no SLA. Network and WebGL are required; initialization or renderer failures produce a visible notice. External application overlays still have their own availability constraints.

### Presentation and close control

`closeUnitModal` referenced an undefined `modal` variable. It now resolves the actual dialog and handles the close button, its child elements, the overlay and direct calls. Content clicks keep the dialog open. The X is a native button with an accessible name.

The analytics drawer uses a scrollable three-column grid, two columns on narrow layouts, consistent spacing, restrained surfaces and clearer typography. Metric computation and interpretation are unchanged. This is presentation work, not a redesign or validation of the analytical models. ORBAT card data, campaign positions and event dossier processing are not claimed fixed.

### Reproducible dependencies and quality gates

Runtime and CI dependencies now have separate transitive lockfiles with versions and SHA-256 hashes, generated using uv 0.12.22 for Python 3.11+ with universal markers. CI installs with `--require-hashes` and runs for the current feature branch as well as main and codex branches. Its dependency cache follows the CI lockfile.

The CI lock was installed in an isolated Windows Python 3.11.11 environment and the configured gates passed there. The runtime lock was resolved, not fully installed or certified against external providers. The browser still uses some third-party CDN dependencies; these lockfiles cover Python dependencies only.

## Verification

| Check | Result and scope |
| --- | --- |
| Python regressions | 101 passed on both the existing Python 3.13 environment and isolated Python 3.11.11 environment |
| JavaScript regressions | 7 passed, including the real close function and missing-map-renderer handling |
| Ruff | Configured canonical scope passed; this is not a repository-wide clean lint assertion |
| Strict Mypy | 12 shared foundation/ingestion files passed |
| HTML structure and DOM contracts | Passed; external-script missing-ID reports remain advisory |
| Real application browser smoke | Edge/Playwright, widths 320, 390, 768, 1024, 1440 and 1920; no page exceptions or failed external requests in the final run |
| Basemap browser check | Loaded style and tiles, visible canvas and attribution; no API key configured |
| Admin browser security | Real HTML with isolated hostile response fixtures: login, reader mode, logout, escaped text/IDs and no token in browser storage passed |

The main UI test uses the existing public data. Admin HTTP tests use temporary databases; admin browser tests intercept API requests with hostile test fixtures and assert that every request is GET. Production ingestion, AI processing, exports and merges were not executed. These checks are neither an exhaustive browser-security audit nor load/provider certification.

Reproduce:

```powershell
python -m pip install --require-hashes -r requirements-ci.lock
python -m pytest tests -p no:cacheprovider
python -m ruff check impact_atlas ingestion scripts map_loader.py tests
python -m mypy impact_atlas ingestion
python tests/verify_dom_ids.py
python tests/verify_html_structure.py
node --test tests/test_dashboard_security.js tests/test_ui_controls.js
```

Browser tests require Playwright and a local server at `UI_BASE_URL` (default port 8801). Set `PLAYWRIGHT_MODULE_PATH` and `BROWSER_EXECUTABLE` if needed. Run `tests/current_ui.browser.cjs` with `UI_REQUIRE_FULL_RUNTIME=1`, then `tests/admin_security.browser.cjs`. The first needs access to the existing CDN resources and map host.

## Remaining risk ledger

| Original risk | Current status |
| --- | --- |
| Non-atomic artifact sets | **Partial:** atomic CSV replacement exists; coordinated publication of all artifacts and stage-failure propagation remain open. |
| Provider failure versus legitimate emptiness | **Open:** collection/provider semantics were not changed. |
| Split schema/repository ownership | **Partial:** shared transaction rollback and cleanup are fixed; consolidation and migrations remain open. |
| Hosted admin authentication, authorization and reversible merges | **Partial:** local bearer authentication, read/write authorization and transactional undo/history for new admin merges are implemented and tested. Hosted identity/service architecture and recovery of legacy merges without snapshots remain open. |
| Rendering security across all views | **Partial:** earlier escaping fixes plus admin text/identifier protections and browser security regressions. Exhaustive verification of every renderer remains open. |
| AI/provider schemas and public-artifact validation | **Partial:** prior CSV formula protection remains; semantic schemas, publication validation and comprehensive redaction remain open. |
| Monoliths and global/import-time state | **Partial:** explicit shared boundaries for logging, transactions, serialization, admin authorization and row recovery. Domain monoliths remain. |
| Quadratic matching | **Open:** no military-event correlation or fusion optimization. |
| Long-lived writer transactions during network work | **Open:** no orchestration-level transaction redesign. |
| Heavy browser payloads | **Open:** presentation and background cartography changed; application datasets were not reduced or reorganized. |
| Uneven quality gates | **Partial:** more security/recovery regressions and 13 files under strict typing; no full-domain typing/documentation migration. |
| Dependency reproducibility and integration coverage | **Partial:** hashed Python locks and a tested clean CI install added. Full runtime/provider integration and load certification remain open. |

The reported campaign-marker alignment and event-dossier errors remain unresolved. No verified root cause for the latter is asserted here. The generic close-button failure was independently reproduced and corrected. Publicly available data and delayed publication do not by themselves establish that operational military-intelligence features are safe to enhance; those capabilities remain outside this change set.
