# Phase 1 — Stability Audit

**Status:** ready for review  
**Scope:** static analysis and focused, non-feature stability fixes only. No Phase 2 refactoring or Phase 3 UI redesign has begun.

## Audit coverage

| Surface | Result |
| --- | --- |
| Python source | 153 files / 33,814 lines parsed successfully |
| Browser source | 8 JavaScript files passed `node --check` |
| HTML resources | 8 HTML files: all referenced local assets exist |
| Application JSON | Parsed successfully |
| GeoJSON | 526 files / 138,332 features: valid `FeatureCollection` structure |
| SQLite | `impact_atlas.db`, `osint_tracker.db`, and `test_debug.db` passed `PRAGMA integrity_check` |
| Virtual environment | Required installed packages have no broken dependency relationships |

## Defects fixed

| ID | Severity | Resolution |
| --- | --- | --- |
| P1-01 | Blocking | `ingestion/db_manager.py` inserted into `raw_signals.url` without creating or migrating that column. The schema now creates `url`, migrates pre-existing databases, validates malformed events, closes connections reliably, and resolves its DB path from the project root. |
| P1-02 | Blocking | `map_loader.py` processed only the last CSV row in each NASA FIRMS response because the parse block was outside its loop. It now uses `csv.DictReader`, handles quoted fields, processes every row, and counts detections correctly. |
| P1-03 | High | The NASA FIRMS credential was embedded in source. It is now read only from `FIRMS_API_KEY`; `.env.example` documents the required setting without exposing a value. |
| P1-04 | High | The HVT carousel inserted OSINT-supplied title, date, and vector fields into HTML without escaping. Those fields are now escaped before rendering. |
| P1-05 | High | Unit-emblem URLs were injected into an HTML string and only rendered when they contained a legacy substring. The renderer now accepts validated HTTPS URLs through DOM APIs, preserves a safe fallback, and supports the 741 current Google-hosted emblem records. |
| P1-06 | Medium | Unit-description cleanup parsed untrusted strings through `innerHTML`. It now strips tags without creating a DOM parse sink. |
| P1-07 | Medium | `test.js` contained invalid JavaScript syntax. It now parses successfully. |
| P1-08 | Medium | `test_audio.py` and `test_audio_post.py` issued live, credentialed API calls at import time. They are now explicit manual diagnostics with `__main__` guards, missing-credential checks, timeouts, and network-error handling. |

## Regression verification

Focused smoke checks were run in the project virtual environment without contacting external services:

- Fresh raw-event database: insert, duplicate de-duplication, and `url` persistence passed.
- Legacy raw-event database: the automatic `url` migration passed.
- Mocked multi-row FIRMS CSV with quoted content: all rows were retained and written as GeoJSON.
- A two-test standard-library regression suite now covers both fixed backend failure modes without contacting external services.
- Python AST parsing, JavaScript syntax checking, JSON/GeoJSON parsing, database integrity checks, and whitespace checks passed.

## Findings for Phase 2

### Important architecture and performance risks

1. **Three divergent backend generations.** `scripts/`, `scripts2/`, and `war_tracker_v2/scripts2/` contain overlapping pipeline, output, reporting, and campaign logic. Their counterparts have different hashes and imports; there is no single authoritative runtime path. This is the dominant maintainability and regression risk.
2. **Large, stateful modules.** The primary ingestor is roughly 1,470 lines and output generators are roughly 1,000–1,500 lines. Network, parsing, persistence, business rules, and CLI control flow are interleaved.
3. **SQLite write amplification.** Active database managers commit after individual unit/event upserts. Batch transactions are needed to avoid poor throughput and contention during high-volume ingest.
4. **Import-time side effects.** Examples include instantiated geolocation/cache objects and OpenAI clients at module import. This complicates test isolation, configuration validation, and CLI reuse.
5. **Runtime packaging is incomplete.** The dependency manifest lacks version bounds for several runtime libraries and does not explicitly declare every direct optional import, such as `geopandas`, `fiona`, `gspread`, `oauth2client`, `httpx`, `orjson`, and `tqdm`.

### Reliability and security debt

1. **76 bare `except:` blocks** can catch process-control exceptions and hide failures. Error handling must use explicit exception types with context-rich logs.
2. **Documentation/type coverage is low:** 455 of 732 functions and 21 of 43 classes have no docstrings. Type annotations are inconsistent across pipeline boundaries.
3. **DOM rendering needs a systematic policy.** The UI still has a broad `innerHTML` surface. The fixed HVT and emblem pathways were directly exposed to source data, but Phase 2 should centralize escaping, URL validation, and safe element construction for every data-backed view.
4. **Dynamic SQL requires review.** The inspected admin search path parameterizes user values correctly, but static analysis found dynamic SQL construction in admin and utility scripts. Each must be restricted to vetted identifiers or converted to parameterized queries.
5. **Test coverage and CI are still immature.** This phase adds a two-test standard-library regression suite, but there is no broader integration suite, coverage target, linting configuration, or continuous validation workflow.

## Phase 2 recommended order

1. Select one canonical backend and retire or quarantine duplicate generations.
2. Establish package layout, typed domain models, configuration validation, structured logging, and error taxonomy.
3. Split ingestion, persistence, normalization, exports, and presentation adapters into testable modules; add transaction batching.
4. Add unit/integration tests, linting, formatting, type checking, dependency locking, and CI checks.
5. Apply a centralized browser rendering/sanitization layer before UI redesign work begins.

## Validation boundaries

This phase did not call live OSINT or AI providers, mutate production databases, or open the browser UI. External provider availability, credentials, rate limits, and visual behavior remain outside static/smoke verification and will need controlled integration and browser tests in later phases.
