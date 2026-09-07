# Archive — quarantined legacy code

This directory contains retired code kept for reference and forensics only.
**Nothing in `_archive/` is imported or executed by the live pipeline.**
It is excluded from linting, typing, and CI checks.

| Subdirectory | Contents | Why retired |
| --- | --- | --- |
| `scripts2/` | Full copy of the superseded "generation 2" backend (12 modules) | `scripts/` is the canonical backend. Five modules were byte-identical duplicates of `scripts/` copies; `master_ingestor.py`, `generate_output.py`, and `campaigns_engine.py` were stale near-duplicates; `generate_report.py` referenced non-existent font/report paths. `geolocator_agent.py` and `v42_analytics.py` were re-homed into `scripts/`. |
| `diagnostics/` | One-off root-level repair/inspection scripts (`find_table.py`, `fix_mojibake.py`, …) | Single-purpose tools that ran once; superseded by the tested `impact_atlas`/`ingestion` packages. |
| `dead/` | `generate_report.py.bak`, `index PREFIX.html`, `test.js`, `debug.js` | Dead artifacts with no references. |
| `legacy_ingestion/` | Synchronous Telegram/GDELT fetchers from the original `ingestion/` package | Superseded by the async fetchers in `war_tracker_v2/scripts2/` used by the scheduled pipeline. |

Removed permanently rather than archived: nothing. Every retired file is
recoverable from git history or this directory.
