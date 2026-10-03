# Phase 1 review — 2 October 2026

**Status: ready for user review. Phase 2 and Phase 3 have not been started in this task.**

This report describes the current checkout and this change set. The older phase reports and existing shared package/dashboard were already present when this task began; their claims were not treated as current verification. The initial tracked working tree was clean.

The confirmed defects below have focused fixes and offline regression coverage. Passing these checks does **not** establish “100% stability” or commercial release readiness. External-provider integration, real-browser behavior, recovery under load, and the remaining architectural risks still need validation.

## Scope and evidence

| Check | Result |
| --- | --- |
| Workspace Python AST scan | 167 files, 36,114 lines, no syntax errors; includes ignored training/maintenance source and archives |
| JavaScript syntax | 11 files and 5 inline HTML script blocks parsed successfully |
| Tracked JSON/GeoJSON | 558 files parsed successfully; this checks syntax, not factual accuracy or all geometry semantics |
| Python regression suite | 51 passed, no skips; initial baseline was 21 passed |
| JavaScript security regressions | 5 passed using the actual renderer/helper code and an instrumented DOM boundary |
| Configured Ruff gate | Passed for `impact_atlas`, `ingestion`, `scripts`, `map_loader.py`, and `tests` |
| Additional correctness scan | Undefined names, syntax, mutable defaults, and loop closures checked across canonical, scheduled, maintenance, training, and archived Python directories; passed |
| Strict Mypy gate | Passed for the existing 10-file foundation/ingestion scope |
| Existing HTML/DOM checks | Passed within their limited scope; external-script missing IDs remain advisory |
| Patch whitespace | `git diff --check` passed |

The companion [source inventory](phase-1-static-inventory.json) lists per-file size, documentation and exception metrics, syntax results, and the largest public data files. Runtime environments, dependencies, caches, and skill bundles were excluded. Static scanning covered all enumerated source; manual review concentrated on active execution paths and trust boundaries. It is not a claim that every branch was executed or every archived utility semantically verified.

Tests use disposable SQLite databases, temporary artifact directories, mocked providers and a loopback test HTTP server. No live ingestion, AI request, publication, production database mutation, or dataset regeneration was performed.

## Runtime architecture

1. `run_tracker.ps1` invokes canonical source ingestion, FIRMS loading, AI analysis and artifact export.
2. `scripts/master_ingestor.py` writes entities and source events to `impact_atlas.db`.
3. A separate scheduled pipeline under `war_tracker_v2/scripts2` performs Telegram/GDELT backfill, embedding/refinement and event building against `war_tracker_v2/data/raw_events.db`.
4. `scripts/ai_agent.py` analyzes raw-event clusters. `generate_output.py` reads completed reports, joins additional datasets and emits static JSON/GeoJSON/CSV/media.
5. `index.html` and global browser scripts fetch those artifacts. The local admin HTTP server also exposes manual merge operations against the raw-event database.

These are two active ingestion tiers, not simply an archived duplicate. Consolidating them requires preserving scheduler, schema and provider behavior.

## Confirmed defects fixed

| ID | Severity | Before → after | Main evidence |
| --- | --- | --- | --- |
| A01 | High | Admin static serving allowed `.env`, database/source files and sibling paths sharing the root prefix. Resolved-path containment and a public page/asset allowlist now block these requests. | HTTP tests cover public access, hidden files, DB paths and encoded/plain traversal. |
| A02 | High | Wildcard CORS exposed local admin APIs to foreign origins. Requests now require a loopback Host and matching Origin when present; wildcard CORS is removed. The admin page uses its served origin. | Foreign Origin/Host requests return 403. Normal local API access still works. |
| A03 | High | Negative/unbounded body lengths, scalar JSON and malformed IDs could block/crash handlers or cause partial merges. JSON bodies are bounded, timed and type-checked; merge IDs must be distinct strings, bounded in count, and all exist before mutation. | Invalid requests preserve event status; valid merges succeed. |
| A04 | Medium | Huge pagination values overflowed SQLite bindings; array-valued AI reports caused attribute errors; offsets were discarded in date comparisons; malformed vectors could break similarity processing. Added bounds, shape/finite checks and UTC normalization. | HTTP and date regressions; vector checks inspected statically. |
| A05 | High | HTTP failure messages included complete URLs and exception messages containing FIRMS path credentials. Logging now records provider hostname and exception type without URL credentials. | Mocked credential-bearing transport errors do not expose test secrets in logs. |
| A06 | Medium | HTTP timeout overrides produced duplicate keyword errors; rejected responses were not closed; non-finite policies could reach sleep/request operations. Overrides work, failed responses close, and invalid policy values fail early. | Injected-session retry tests. |
| A07 | Medium | An explicit empty configuration mapping loaded ambient credentials; NaN/Infinity backoff passed validation. Explicit mappings are now isolated, default dotenv lookup is rooted, and backoff must be finite and positive. | Environment-isolation and invalid-value tests. |
| A08 | Medium | Media lists were stored with Python `str(list)`, producing invalid JSON for consumers. Media values now persist as valid JSON arrays, including safe handling of malformed input. | SQLite round-trip tests with apostrophes, lists, strings and null values. |
| A09 | High | Anonymous Parabellum IDs used process-randomized `hash()` with a small keyspace, creating duplicates across runs and conflating geometry. IDs now use a deterministic SHA-256 digest over properties and geometry. Provider response shapes and zero/non-finite WarSpotting coordinates are also handled. | Stable-ID, geometry distinction, zero-coordinate and malformed-response tests. |
| A10 | High | Every ingestion run replaced enriched unit names/factions/status with seed defaults. Seeding now inserts missing records only. Raised source failures roll back their batch, allow other sources to finish, and result in a failing pipeline exit. | Temporary-DB test preserves enrichment, rolls back a failed source and retains a successful source. |
| A11 | Medium | Bad FIRMS rows reserved a deduplication key before parsing completed; repeated detections on different dates were lost; NaN/Infinity could enter GeoJSON. Keys are recorded after validation and include observation date/time. Missing/failed thermal ingestion now returns a failing CLI exit. | Mocked CSV and loader failure tests. |
| A12 | High | Media filenames incorporated unchecked event/frame IDs and MIME suffixes. Exports now enforce safe identifiers, raster suffixes, resolved containment, bounded encoded size and strict base64 decoding. | Traversal, active-image suffix and invalid-base64 regressions. |
| A13 | High | Unlocated visual events received fabricated coordinates derived from an ID hash. They are now logged and excluded from map exports; underlying database records remain intact. Valid zero coordinates are preserved; malformed/non-finite coordinates and scores are rejected or defaulted. | Full export-path test proves visual evidence cannot invent a location and zero coordinates survive. |
| A14 | Medium | Invalid dossier dates and null recent-event dates could abort unit export. Valid dates are parsed explicitly and recent records are ordered by parsed timestamps. | Mixed malformed/null/valid date tests. |
| A15 | High | Loss cards, tooltips, popups, intelligence cards, campaign assessments and selected ID-backed handlers interpolated source data into HTML/JavaScript. Fixed identified sinks with context-appropriate escaping, safe URL/DOM assignment and event listeners. Numeric/string/missing detail coordinates no longer crash the detail view. | Five JS tests cover injected markup, active URL schemes, string escapes, popup text and malformed coordinates; affected scripts pass syntax checks. |
| A16 | Medium | README recommended `file://` startup despite fetch-based data loading. It now starts the restricted local server. CI lacked dependencies required to exercise the new backend regressions and JS tests; its install/run steps now include them. The DOM checker now describes its advisory scope accurately. | Local gates passed; updated remote CI has not been run. |

### Behavior changes to review

- Open the dashboard/admin UI through `http://localhost:8800/` or the matching loopback IP after starting `scripts/admin_api.py`. Cross-origin pages and direct project-file downloads are intentionally rejected.
- Unlocated visual events no longer appear at synthetic points or in the map-derived CSV/analytics. A dedicated unlocated-events view is a later product decision. Existing generated artifacts were not rewritten, so they retain their previous contents until the next authorized export.
- Stable anonymous-source IDs change on the next ingest. Historical rows created using randomized IDs are not automatically deleted or reconciled; that requires a reviewed data migration.
- FIRMS detections at the same location on distinct acquisition times are retained. Failed FIRMS loads and raised source failures now make the invoking pipeline report failure.
- Invalid media lists become empty arrays; invalid media identifiers are skipped. Existing stored records/files are not retroactively migrated.

## Architecture, performance and edge-case backlog

These are remaining production risks and the proposed Phase 2 scope, not completed refactoring work.

| Priority | Finding and evidence | Required direction |
| --- | --- | --- |
| High | **Artifact sets are not published atomically.** Export functions overwrite files independently; a crash can leave partial JSON or mismatched generations. Some optional exporters log failures without making the main export fail. | Stage artifacts, validate them, publish a generation manifest/atomic swaps, and propagate stage status. Test interruption and disk-full recovery. |
| High | **Provider failure and legitimate emptiness remain conflated.** Several fetchers return `[]`/`None` or catch per-record errors. The new raised-error exit handling cannot detect all silent partial failures. | Typed outcomes, per-source counts/freshness, bounded retry budgets and an explicit degraded-run policy. |
| High | **Schema ownership and writes are split.** The scheduled raw repository has its own connection singleton and schema logic; stages add/migrate columns ad hoc. Root-level and canonical raw DB paths coexist. | One versioned migration authority, explicit repository ownership and backup/restore verification. Review legacy singleton rollback/thread use before concurrency expansion. |
| High | **Local admin is not a hosted commercial API.** It is loopback-bound, synchronous, and has no user authentication, authorization or durable audit trail. “Unmerge” reactivates the victim but does not restore the master's original combined dossier. | Keep it local; define authenticated service boundaries and persist reversible merge history before multi-user deployment. |
| High | **Rendering security is not centrally enforced.** The identified injection paths are fixed, but thousands of lines of mixed inline HTML/JS still require a complete context-by-context review and real-browser security tests. | Replace remaining data-backed inline handlers/templates with shared safe renderers, then adopt an enforceable content policy. Do not treat this patch as an exhaustive penetration test. |
| High | **AI/provider records lack comprehensive schemas.** Nested dictionaries, regex-based redaction and broad catches allow malformed or semantically wrong records to disappear or produce misleading output. | Validate at each boundary, retain rejected records with reasons and test provenance/publication rules. Review CSV spreadsheet-formula handling and all public artifact classes as part of export validation. |
| Medium | **Large monoliths and globals.** AI agent: 3,341 lines; ingestor: 1,537; exporter: about 1,400; map script: 4,914; main HTML: 6,503. Import-time clients, logging/stream changes and mutable state remain in scheduled/legacy modules. | Separate provider adapters, domain rules, orchestration, persistence and presentation; inject external dependencies. |
| Medium | **Quadratic matching.** Admin suggestions construct an N×N similarity matrix and scan pairs; at 5,000 rows the float32 matrix alone is about 100 MB, excluding vectors and pair storage. Event-building/fusion similarly compare candidates in Python. | Bound candidate windows, filter/index before similarity, process blocks, measure peak memory and consider an appropriate vector index. |
| Medium | **Long-lived SQLite writer transactions.** Source-level batch transactions include network/parsing work after writes begin, potentially holding a writer lock during provider delays. | Fetch/normalize before bounded write batches, with idempotency and explicit retry/rollback behavior. |
| Medium | **Heavy browser payloads.** Fortifications ~28.6 MB, current OWL layer ~15.1 MB, events ~6.1 MB, plus history/media. Parsing and Leaflet object creation inflate memory beyond wire sizes. | Measure load/render budgets, simplify/tile geometry, lazy-load bounded windows and cache versioned artifacts. These are measured file sizes, not a runtime benchmark. |
| Medium | **Uneven quality gates.** Strict typing covers 10 files; scheduled/training/archive code is excluded from normal lint. In the 70-file non-archive/non-training/non-maintenance grouping, static counts found 6 bare catches, 139 `except Exception` blocks and 217 undocumented functions. | Expand checks incrementally as modules are isolated; replace blanket suppression with explicit failure contracts. |
| Medium | **Incomplete reproducibility and integration coverage.** Dependencies are bounded but not locked; prior tests allowed dependency-based skips. Current added tests require their dependencies, but CI has not been rerun remotely and live/provider/browser/load tests remain absent. | Reproducible environments, recorded provider fixtures, integration/contract tests and measured failure recovery. |

## Reproduce the current gates

```powershell
.venv\Scripts\python.exe -m pytest tests -p no:cacheprovider
.venv\Scripts\python.exe -m ruff check impact_atlas ingestion scripts map_loader.py tests
.venv\Scripts\python.exe -m mypy impact_atlas ingestion
node --test tests/test_dashboard_security.js
.venv\Scripts\python.exe tests/verify_dom_ids.py
.venv\Scripts\python.exe tests/verify_html_structure.py
git diff --check
```

## Review gate

Review this report and the working-tree diff, especially the behavior changes above. Approval authorizes Phase 2 refactoring; it is not a certification that remaining risks are resolved. Phase 3 remains gated on a separate review after Phase 2.
