# Transactional administrator recovery — 3 October 2026

## Result

The existing admin merge could not be undone faithfully: `/api/unmerge` merely changed one record's status to `PENDING`, leaving combined master text and source lists in place. New admin merges now have recovery history committed atomically with their data changes. The admin page exposes that history and an **Undo merge** control for writers.

This corrects a data-integrity defect. It does not change event selection, correlation, scoring, campaign positions, unit analysis or dossier processing. The remaining risk ledger is still incomplete; this is not a claim that all architectural risks were eliminated.

## Guarantees and boundaries

- The API obtains a SQLite writer transaction before reading the records to be modified. No network/provider work occurs inside this transaction.
- `impact_atlas/change_journal.py` is a typed, documented recovery component. It owns an independently versioned journal table, not the domain schema or commit policy. The HTTP handler owns commit/rollback and connection cleanup.
- Administrator database setup now closes the connection if WAL initialization fails. Recent history uses a resource/time index and returns bounded metadata rather than scanning snapshot bodies into application memory.
- Before-values are retained for the four columns the existing merge modifies. SHA-256 fingerprints cover all columns after the merge, so later edits, missing records and schema drift prevent stale restoration.
- Restoration checks the complete record set before writing. All restored values and the history status are committed together. Errors or cancellation roll back. Unexpected trigger/cascade writes during undo cause refusal and rollback.
- Repeated undo requests report `already_reverted` without rewriting current records. Overlapping changes can be undone in reverse order while their recorded state still matches.
- Read-only clients can inspect metadata but cannot undo. API history excludes stored source text and snapshots. The UI inserts history values as text, not executable HTML.
- The server remains local. This is not hosted user identity, a signed/immutable audit log, database backup or tamper-proof history. A process with direct database write access remains trusted.
- Older merges without recorded before-values cannot be safely reconstructed and are refused. This implementation does not infer lost originals from combined text.
- Undo affects the private database. Previously generated public files require a later export to reflect the restored rows. Coordinated multi-file publication remains a separate open risk.
- Journal history retains original text/source values in the private database. No automatic deletion is performed; retention and backup policy remain administrative responsibilities.

The journal is created on the first successful new merge, within that merge's transaction. No production database or dataset was changed during development or testing.

## API and UI compatibility

`POST /api/merge` retains its input and previous response fields, adding `merge_id`.

`GET /api/merge-history` returns up to 50 recent entries containing only the operation identifier, record count, creation time and restoration time. It does not create schema when there is no history.

`POST /api/unmerge` now requires `merge_id`, rather than `event_id`. Outcomes:

| HTTP status | Meaning |
| --- | --- |
| 200 | All records restored, or this exact operation was already restored |
| 400 | Missing identifier or unsupported legacy request |
| 401 / 403 | Missing credentials / insufficient write permission |
| 404 | No recorded recovery operation |
| 409 | Later edits, missing rows, schema drift or invalid recovery state; nothing committed |
| 500 | Database failure; transaction rolled back |

The admin confirmation now describes the actual merge behavior. The previous message incorrectly claimed that the master was reset for re-analysis, although the implementation did not do that.

## Verification

- **126 Python tests passed** in the isolated, hash-locked Python 3.11.11 environment, including 25 new journal/HTTP/connection cases.
- **7 JavaScript regressions passed.**
- **Ruff passed** in the configured scope; **strict Mypy passed for 13 modules**.
- A real Edge/Playwright test used the existing admin HTML and actual HTTP handler against a temporary database. It selected two test records, merged them, opened history and undid the change. An independent database read then verified that every original column value was restored and exactly one completed recovery operation existed.
- The admin security browser suite also passed with hostile text/identifier fixtures, reader permissions, logout cleanup and no token in browser storage. Font requests were blocked to keep these recovery/security tests independent of external services.

New cases cover reopening the database, original null/binary/text values, idempotence, out-of-order undo, later edits to both tracked and untracked columns, deleted rows, schema changes, failed history writes, partial restoration failure, unexpected trigger writes, corrupted history, cancellation and writer-lock contention. They use disposable databases only.

Reproduce the browser test with Playwright available and, if necessary, `PLAYWRIGHT_MODULE_PATH` and `BROWSER_EXECUTABLE` configured:

```powershell
python tests/run_admin_recovery_browser.py
```

The harness creates its own loopback server and temporary database. No production server or ingestion run is required. Browser checks were run locally; the normal CI continues to run Python, JavaScript, lint, type and structural checks.
