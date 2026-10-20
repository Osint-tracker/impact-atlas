# Impact Atlas — OSINT Conflict Intelligence Platform

An OSINT research pipeline and C4ISR-style dashboard that ingests open-source
conflict data from 11+ sources, fuses and geolocates events with a multi-agent
LLM analysis tier (T.I.E. scoring), and exports sanitized, OPSEC-gated
artifacts for the browser-based operational dashboard.

```
Raw sources ──► master_ingestor ──► impact_atlas.db ─┐
Telegram/GDELT ─► scheduled pipeline ─► raw_events.db ─┼─► ai_agent (7-agent swarm)
FIRMS thermal ──► map_loader ─► thermal_firms.geojson ┘        │ T.I.E. scoring
                                                               ▼
                                              generate_output ─► assets/data/* ─► index.html
```

## Quick start

```powershell
# 1. Create the virtual environment and install dependencies
python -m venv .venv
.venv\Scripts\pip install --require-hashes -r requirements.lock

# 2. Configure credentials
copy .env.example .env      # then fill in the required keys

# 3. Initialize the database schema without network calls
.venv\Scripts\python scripts\master_ingestor.py --dry-run

# 4. Run the full pipeline (ingest -> thermal -> AI analysis -> export)
.\run_tracker.ps1

# 5. Serve the dashboard (file:// cannot reliably fetch its data)
.venv\Scripts\python scripts\admin_api.py
# Open http://localhost:8800/index.html
```

## Canonical pipeline (scripts/)

| Stage | Command | Purpose |
| --- | --- | --- |
| Ingestion | `python scripts\master_ingestor.py [--source ws ml ox ...]` | 11 OSINT sources into `impact_atlas.db` with entity resolution |
| Thermal | `python map_loader.py` | NASA FIRMS thermal detections to `assets\data\thermal_firms.geojson` |
| Analysis | `python scripts\ai_agent.py` | 7-agent LLM swarm; T.I.E. scoring; writes `ai_report_json` |
| Fusion | `python scripts\smart_fusion.py` / `scripts\unify_clusters.py` | duplicate fusion and cluster unification |
| Export | `python scripts\generate_output.py` | all `assets\data\*` artifacts (GeoJSON/JSON/CSV) |
| Reports | `run_report.bat` | PDF SITREP + `report.html` console |
| Admin | `python scripts\admin_api.py` → `http://localhost:8800/admin_merge.html` | manual event merge tooling |

Raw-signal ingestion (Telegram + GDELT → `raw_events.db`) runs on the
scheduled 8-hour task registered by `war_tracker_v2\scripts2\schedule_pipeline.ps1`
(`auto_pipeline.py`: backfill → refinement → event building).

## Project layout

```
impact_atlas/        Shared production foundation: typed config, validated
                     settings, resilient HTTP, SQLite transactions, JSON
                     logging, error taxonomy, domain models.
ingestion/           Connector library (WarSpotting, Parabellum) and the
                     raw-event repository.
scripts/             THE canonical backend (ingestor, ai_agent, analytics,
                     exporters, admin API). Legacy generations live in
                     _archive/ and are not executed.
war_tracker_v2/      Live raw-signal tier + scheduled pipeline + PDF SITREP.
assets/              Dashboard JS/CSS and generated data artifacts.
tests/               Standard-library-safe regression suite (pytest).
_archive/            Quarantined legacy code. Not imported, not executed.
reports/             Phase audit and refactoring reports.
```

## Configuration

All secrets are read from environment variables (see `.env.example`).
Nothing is hardcoded. The pipeline resolves every database and output path
through `impact_atlas.config.ProjectPaths`, so it can run from any working
directory.

## Quality gates

```powershell
.venv\Scripts\python -m pytest tests               # regression suite
.venv\Scripts\python -m ruff check impact_atlas ingestion scripts map_loader.py tests
.venv\Scripts\python -m mypy impact_atlas ingestion
.venv\Scripts\python tests\verify_dom_ids.py       # every JS-referenced DOM id resolves
.venv\Scripts\python tests\verify_html_structure.py # page markup is structurally sound
```

CI (`.github/workflows/ci.yml`) runs these checks on pushes to `main`,
`feature/**`, and `codex/**`, and on pull requests targeting `main`.
The configured lint scope excludes legacy and experimental tiers; strict
typing covers `impact_atlas` and `ingestion`, not the whole application.

### Reproducible dependencies

`requirements.txt` and `requirements-ci.in` declare direct dependencies.
`requirements.lock` and `requirements-ci.lock` pin transitive dependencies
with SHA-256 hashes. Regenerate both intentionally with uv 0.12.22:

```powershell
uv pip compile requirements.txt --python-version 3.11 --universal --generate-hashes --output-file requirements.lock
uv pip compile requirements-ci.in --python-version 3.11 --universal --generate-hashes --output-file requirements-ci.lock
```

The CI lock has been installed and tested in a clean Windows Python 3.11.11
environment. The full runtime lock has been resolved but has not undergone
external-provider integration or load testing. Exact versions and hashes
improve reproducibility; they are not a vulnerability certification.

### Local administrator access

The server binds to localhost. Public pages remain readable; every `/api/`
request requires an `Authorization: Bearer ...` header. Enter the session
token printed by the server in `admin_merge.html`. Each restart generates
a new token unless `IMPACT_ADMIN_WRITE_TOKEN` is configured in the process
environment. Optional `IMPACT_ADMIN_READ_TOKEN` grants API read access only.
Use separate, randomly generated tokens with at least 32 URL-safe characters.
The server does not load these two settings from `.env` automatically.

The page keeps its token in memory, clears it on disconnect, and does not
put it in URLs or browser storage. This is local capability authentication,
not a hosted identity service. Hosted deployment remains unresolved;
see the current risk ledger in
[the follow-up report](reports/stability-followup-2026-10-03.md).

New admin merges now record recovery history in the same SQLite transaction.
Open **Recovery history** in the admin page to undo a recorded merge. Undo
restores the original values only if none of its records have changed since
the merge; conflicting, missing or schema-changed records cause the entire
operation to be refused. Repeating a completed undo is harmless. Overlapping
merges must be undone in reverse order.

The API returns `merge_id` from `POST /api/merge`. Read recent metadata at
`GET /api/merge-history`; send `{"merge_id":"..."}` to `POST /api/unmerge`.
Undo requires write permission. The old `event_id` unmerge request is rejected:
merges made before recovery history existed cannot be reconstructed safely.
Undo restores database records; it does not regenerate previously exported
public files. Protect the database and its history, which retains original
text and source values. See [recovery guarantees and tests](reports/admin-recovery-2026-10-03.md).

### Background map

The main dashboard uses the [OpenFreeMap](https://openfreemap.org/)
dark vector basemap via pinned MapLibre GL and Leaflet integration scripts.
Its public instance requires neither registration nor an API key. It still
uses OpenStreetMap/OpenMapTiles data, with their required attribution.
It needs network access and WebGL; free public hosting has no availability
guarantee. The UI reports background-map failures rather than silently
showing a blank map. Application data layers are separate from the basemap.

## Console operations

* Keys **1-7** switch the rail panels (Maps, ORBAT, Intel, Tempo, Losses,
  Stats, Campaigns).
* The command bar shows live telemetry: event count, data cut, data age
  (green < 6h / yellow < 24h / red beyond), Zulu clock, and a system pill
  that flips to DEGRADED if the clock heartbeat stalls.

## Operational notes

* **OPSEC**: sensitive movement events are withheld for 24h
  (`OPSEC_CUTOFF_HOURS`); counts are surfaced in artifact metadata.
* **PII**: person names and license plates are redacted from every public
  artifact (`generate_output.sanitize_public_*`).
* **Write amplification**: each ingestion source commits in one batch
  transaction; per-row commits are a compatibility fallback only.
* **Logging**: JSON records under `logs/` per stage
  (`master_ingestor.log`, `generate_output.log`, `admin_api.log`, ...).
