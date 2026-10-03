# General software-security follow-up — 3 October 2026

**Status: scoped security refactor implemented and verified. The full Phase 2 request is not complete. Phase 3 has not started.**

The user approved Phase 1 and requested resolution of its remaining risks before moving to Phase 2. This follow-up implements general-purpose security and data-integrity improvements. It does not productionize operational military-intelligence, targeting or troop-tracking functions. That restriction also prevents treating this work as completion of the entire original roadmap.

The [Phase 1 report](phase-1-review-2026-10-02.md) remains the historical audit. Its source inventory is a snapshot from that review, not a current coverage measurement.

## Implemented changes

### SQLite resource and transaction safety

`impact_atlas/sqlite.py` now validates finite, positive lock timeouts against SQLite's millisecond range before creating files. Connections close if setup fails. A separate connection context owns cleanup, while the transaction context explicitly begins a transaction so schema changes and data writes share rollback behavior.

Ordinary exceptions, `KeyboardInterrupt` and `SystemExit` roll back. Cleanup failures attach diagnostic notes to the original exception rather than hiding it. Commit failures also roll back and release the connection. The implementation is typed and documented. Existing callers retain their public `connect()` and `transaction()` interfaces; direct `connect()` callers still own their connections.

This addresses resource leaks and rollback semantics in the shared adapter. It does **not** consolidate the separate scheduled repository, migrate production databases or remove direct SQLite usage elsewhere. Callers must not issue their own transaction-ending commands or `executescript()` within the owned transaction.

### Credential handling and bounded logs

`RuntimeSettings` no longer includes credential fields in its representation, and required-secret validation rejects whitespace-only values.

The shared logger now rotates at 5 MiB by default and retains three backups. `max_bytes` and `backup_count` are configurable positive integers. Repeated setup updates verbosity and redaction without accumulating managed handlers. A failed destination change preserves the previous working handlers.

File and console formatters redact explicit secrets, credential-like environment values present at configuration time, common credential assignments and authorization values. Logged HTTP URLs retain the hostname while dropping userinfo, path and query. Message and exception fields have size limits. Formatters avoid changing the original record. Existing JSON timestamp/level/logger/message fields remain available.

These controls apply to handlers installed by `configure_logging()`. They do not sanitize arbitrary `print()` output, third-party handlers or previously written logs. Newly loaded credentials require reconfiguration or explicit redactor construction. Redaction is defense in depth, not a PII detector or a guarantee that every possible secret format is recognized. Rotation assumes one process per log file; multi-process log aggregation remains unresolved. A single record may exceed a caller-specified rotation threshold.

### CSV formula injection and interrupted writes

`impact_atlas/serialization.py` provides typed, documented helpers for spreadsheet-safe CSV cells and atomic text-file replacement. The existing CSV export uses these helpers.

Formula-like source strings receive a leading apostrophe, including those prefixed by whitespace or a BOM. Existing numeric Python values remain numeric. The apostrophe is intentional and is visible to raw CSV consumers.

The CSV is written to a sibling temporary file, flushed and synchronized, then replaced only after success. Failed serialization, cancellation, flush failures and replacement failures preserve the previous CSV. Temporary files are cleaned up on ordinary error paths without masking the original failure.

This protects **one CSV file**. It does not make the whole JSON/GeoJSON/media/CSV output generation transactional. It does not guarantee directory-entry durability after sudden power loss; a hard process kill can leave a temporary file. Output paths are trusted configuration, not a path-sanitization interface. Replacement uses a new file and does not promise to preserve custom destination ACLs or other metadata.

## Verification

| Check | Result |
| --- | --- |
| Full Python suite | 94 passed, no skips; 43 additional cases beyond the Phase 1 suite |
| JavaScript security regressions | 5 passed; no browser redesign in this follow-up |
| Ruff | Passed for the configured canonical scope |
| Strict Mypy | Passed for 11 foundation/ingestion files, including the new serialization module |
| Patch whitespace | `git diff --check` passed |

Tests cover cancellation and DDL rollback, failed setup/commit/cleanup, invalid lock timeouts, hidden configuration credentials, log rotation and repeated setup, quoted credentials and tracebacks, CSV escaping, and injected disk/replace failures. The existing export regression now also verifies the CSV formula protection is wired into the writer.

Tests use temporary files/databases and mocked failures. Production databases, generated datasets and existing logs were not rewritten. No external-provider requests or remote CI runs were performed. These results are not live-provider, browser, load or deployment certification.

Reproduce with:

```powershell
.venv\Scripts\python.exe -m pytest tests -p no:cacheprovider
.venv\Scripts\python.exe -m ruff check impact_atlas ingestion scripts map_loader.py tests
.venv\Scripts\python.exe -m mypy impact_atlas ingestion
node --test tests/test_dashboard_security.js
git diff --check
```

## Remaining Phase 1 risk ledger

| Risk from the approved audit | Current status |
| --- | --- |
| Non-atomic artifact sets | **Partially addressed:** CSV replacement is atomic; coordinated multi-file publication and stage-failure propagation remain open. |
| Provider failure versus legitimate emptiness | **Open:** no changes to collection/provider semantics. |
| Split schema/repository ownership | **Partially addressed:** shared connection cleanup and transaction rollback are fixed; consolidation and migrations remain open. |
| Hosted admin authentication, authorization and reversible merges | **Open:** the existing server remains local; no hosted service or merge-history redesign was implemented. |
| Rendering security across all views | **Open beyond Phase 1 fixes:** the existing five JS regressions pass, but there is no exhaustive browser security verification. |
| AI/provider schemas and public-artifact validation | **Partially addressed:** CSV formula injection is fixed; semantic schemas, publication validation and comprehensive redaction remain open. |
| Monoliths and global/import-time state | **Partially addressed only in shared utilities:** logging and transaction responsibilities have explicit typed boundaries; domain modules remain unchanged. |
| Quadratic matching | **Open:** no military-event matching or fusion optimization was performed. |
| Long-lived writer transactions during network work | **Open:** explicit transaction ownership does not shorten orchestration-level transactions. |
| Heavy browser payloads | **Open:** no data-layer optimization or UI redesign was performed. |
| Uneven quality gates | **Partially addressed:** added regression cases and one more strictly checked module; broad application typing/documentation remains incomplete. |
| Dependency reproducibility and integration coverage | **Open:** no lockfile or external integration/load certification was added. |

The narrower credential-log exposure, unbounded shared log retention, settings-representation exposure, CSV formula injection and shared-adapter cleanup defects are addressed within the limits above. The ledger deliberately does not relabel the broader unresolved risks as solved.
