# Phase 2 — Global Refactoring & Optimization

**Status:** ready for review
**Scope:** backend consolidation, production hardening, packaging, CI, and the
centralized DOM sanitization layer required before the Phase 3 UI redesign.
No visual/UI redesign was performed (Phase 3 scope).

## 1. Backend consolidation (audit risk #1 — resolved)

The three divergent backend generations are collapsed to one canonical runtime:

| Action | Detail |
| --- | --- |
| Canonical tier | `scripts/` + shared packages `impact_atlas/` and `ingestion/` + root `map_loader.py`. Raw-signal tier remains `war_tracker_v2/scripts2/` (live, scheduled 8h task). |
| Quarantined | The entire superseded `scripts2/` generation (12 modules) moved to `_archive/scripts2/` via `git mv` (history preserved). Includes 5 byte-identical duplicates and 3 stale near-duplicates of `scripts/` files. |
| Re-homed | `geolocator_agent.py` → `scripts/geolocator_agent.py` (rewritten, lazy init). `campaign_strategist.py` → `scripts/` (unique capability, kept). |
| Archived diagnostics | 7 root-level one-off repair scripts → `_archive/diagnostics/`; dead artifacts (`generate_report.py.bak`, `index PREFIX.html`, `debug.js`, `test.js`) → `_archive/dead/`; superseded sync fetchers → `_archive/legacy_ingestion/`. |
| Import graph | Zero imports of root `scripts2/` remain. `ai_agent.py` and `generate_output.py` now resolve geolocator/analytics from the canonical tier. |

**Regression caught and fixed during consolidation:** the newer `scripts/generate_output.py`
(v2.1) had silently stopped producing four artifacts that the dashboard still
consumes — `events_latest.json` (map.js default data source), `sector_anomalies.json`,
`asymmetry_index.json`, `glocs.geojson` (stale on disk since 17/05 while the rest
updated 29/06). The rewritten exporter restores all four outputs.

## 2. Foundation upgrades (`impact_atlas/`)

* `errors.py` — full error taxonomy: `ImpactAtlasError` base with
  `ConfigurationError`, `DataValidationError`, `PersistenceError`,
  `IngestionError`, `ExportError`, `ProviderError`.
* `models.py` (new) — typed, frozen, slotted domain models (`UnitRecord`,
  `KineticEvent`, `SourceRef`, `MarkerStyle`) shared across boundaries.
* `__init__.py` — coherent public API surface for the package.
* `sqlite.py` / `http.py` / `logging.py` / `config.py` — carried forward from
  Phase 1 and adopted by the refactored modules.

## 3. Core module refactors

| Module | Changes |
| --- | --- |
| `scripts/generate_output.py` | Full rewrite: complete type hints + docstrings, structured JSON logging (`logs/generate_output.log`), path resolution via `ProjectPaths`, all bare `except:` removed (typed handlers), OPSEC/PII logic preserved exactly, restored the 4 missing artifacts, exit codes, per-event error isolation with logging. |
| `scripts/v42_analytics.py` | Full rewrite: typing + docstrings, `utcnow()` deprecation removed, `contextlib.suppress`, batched reputation decay (single `executemany` transaction). |
| `scripts/campaigns_engine.py` | Docstrings on every function, module logger with fallback-chain observability, narrowed exception types, no behavior change to matching/scoring. |
| `scripts/master_ingestor.py` | **Write-amplification fix (audit #3):** every source now ingests inside one `db.transaction()` batch (atomic per source, rollback on failure); unit seeding batched; `RuntimeSettings` loading is lazy (no import-time `.env` parse/validation); `safe_request` reads settings lazily and always passes an explicit timeout. |
| `scripts/geolocator_agent.py` | New canonical home: **import-time side effects removed (audit #4)** — `get_geolocator()` / `get_gazetteer()` lazy accessors; typed, documented; no work happens at import. |
| `scripts/ai_agent.py` | Targeted hardening: `sys.stdout.reconfigure`, `logging.basicConfig`, and `load_dotenv` moved into the `__main__` guard; scripts2 import replaced with lazy canonical getters; all 3 bare `except:` blocks replaced with typed handlers; geocode fallback and sector assignment use lazy singletons. |
| `scripts/admin_api.py` | Hardened: typed handlers, `ProjectPaths` resolution, pagination validation (clamped `page`/`per_page`), malformed-JSON 400s, connection `try/finally` + rollback on failed merges, internal errors no longer leak raw exception text (correlation-id references + structured logging). |
| `scripts/smart_fusion.py`, `scripts/geocode_reviewer.py` | Module-level OpenAI clients replaced with lazy `get_*_client()`; `sys.exit(1)` at import removed. |
| `scripts/unify_clusters.py`, `wtv2 generate_report.py` | Dynamic SQL parameterized (audit security-debt #4); bare excepts typed. |
| Legacy tier-wide | Whitespace/formatting normalized, unused imports removed, all 11 remaining bare `except:` in `scripts/` converted to typed handlers, ambiguous names and missing timeouts fixed. |

## 4. Entry points (audit finding — broken launchers)

* `run_tracker.ps1` — rewritten: runs the real canonical pipeline
  (ingest → FIRMS → AI analysis → export) with stage isolation, structured
  logging to `logs/run_tracker.log`, `-SkipIngest` / `-SkipAnalysis` switches,
  and a proper exit code. The previous launcher targeted a nonexistent script
  and auto-pushed to git on every run.
* `run_report.bat` — rewritten: PDF SITREP + artifact export + report console,
  with error handling.
* `README.md` — rewritten around the canonical architecture, quality gates,
  and operational notes (OPSEC, PII, batching, logging).
* `.env.example` — complete template for every consumed variable (12+ vars;
  previously documented only `FIRMS_API_KEY`).

## 5. Packaging, linting, typing, CI (audit debt #5)

* `pyproject.toml` (new): ruff (E/F/W/B/S/UP/SIM, documented per-file-ignores
  for the legacy tier pending Phase 3 decomposition), mypy strict scoped to
  the shared foundation, pytest config.
* `requirements.txt`: every direct dependency declared **with upper bounds**,
  including previously undeclared `httpx`, `orjson`, `tqdm`, `geopandas`,
  `fiona`, `gspread`, `oauth2client`.
* `.github/workflows/ci.yml` (new): ruff + mypy + `compileall` + pytest on
  every push/PR.
* Local verification (`.venv`, Python 3.13):
  `ruff check impact_atlas ingestion scripts map_loader.py tests` → **all checks passed**;
  `mypy impact_atlas ingestion` → **no issues in 10 files**;
  `compileall` over the whole canonical tier → **clean**;
  `pytest tests` → **21/21 passing** (Phase 1 regressions + new Phase 2 suite).

## 6. Test suite (audit debt #5)

`tests/test_phase2_refactors.py` (new) — 19 network-free tests across
v42 analytics (faction/classification extraction, domain normalization,
asymmetry math, anomaly detection, reputation decay + update),
campaigns engine (row normalization, deterministic matching, status,
sheet-URL building), the exporter (PII redaction, source parsing, OPSEC
gating, latest-window selection, marker styles, epoch coercion), the
geolocator (lazy-init contract, no-import-side-effects, sector fallbacks),
and entity resolution (UnitResolver aliases). Optional heavy dependencies
skip gracefully in minimal CI environments.

## 7. DOM sanitization layer (audit prerequisite for Phase 3)

* `assets/js/safe_dom.js` (new): single auditable API — `escapeHtml`,
  `escapeAttr`, `sanitizeUrl` (http/https only), `safeCssUrl`, `setText` —
  loaded before all other scripts in `index.html`.
* `map.js` data-driven `innerHTML` sites migrated: unit activity feed
  (titles/dates/badges), modal source list (names, hrefs, favicons),
  Telegram embed iframe `src` (URL-sanitized), AI strategist analysis text,
  visual-media grid (titles, dates, background-image URLs), and equipment
  tags. Static-only sites left as-is. Cache-busted `map.js?v=3.9`.
* `node --check` passes on all 7 JS modules; no `eval`/`document.write`
  anywhere (confirmed in audit).

## 8. Security posture

* No secret values in source (re-verified). Service-account JSONs remain
  local-only and gitignored; `.env.example` now documents
  `GOOGLE_SERVICE_ACCOUNT_JSON` as an external path.
* SQL: user values parameterized everywhere in the canonical tier; the
  remaining literal-clause constructions in `admin_api` are tagged and
  documented as a vetted allowlist pattern.
* `admin_api` no longer leaks exception internals to HTTP clients.
* **User action required (outside code):** `x.md` in the repo root contains
  plaintext usernames/emails/passwords. It is not tracked by the ignore rules
  you may want; remove or move it out of the repository before any remote
  sharing.

## 9. Deliberate decisions

* Per-event reputation updates (scripts2 v2.0 behavior) stay retired: the
  v2.1 passive-read optimization avoids thousands of per-row commits against
  the 28 GB database; reputation maintenance happens via batched decay.
  `update_event_reputation` remains available and tested for callers that
  need it.
* `ai_agent.py` (3,351 lines) received targeted hardening, not a full split —
  decomposition belongs with the Phase 3 UI/presentation work to avoid
  destabilizing the analysis engine without integration tests.
* `war_tracker_v2/scripts2/` stays live as the scheduled raw-signal tier; its
  report generator was SQL-parameterized and exception-typed in place.

## 10. Verification boundaries

Same as Phase 1: no external providers were contacted, production databases
were only read/created via tests in temp directories, and no browser session
was driven. The restored export artifacts will regenerate on the next
`run_tracker.ps1` / `generate_output.py` execution against live data.
