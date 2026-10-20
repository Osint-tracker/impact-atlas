"""
IMPACT ATLAS — Admin Manual Merge API
Lightweight HTTP server for browsing events and manually merging duplicates.
Uses the same merge protocol as smart_fusion.py.

Usage: python -u scripts/admin_api.py
Then open http://localhost:8800/admin_merge.html in your browser.
"""

from __future__ import annotations

import contextlib
import json
import logging
import math
import os
import mimetypes
import sqlite3
import sys
import uuid
from datetime import datetime, UTC
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, parse_qs, unquote

# Resolve the project root so the shared packages import when this file is
# executed directly (python scripts\admin_api.py).
_PROJECT_ROOT = str(Path(__file__).resolve().parents[1])
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# These imports require the repository-path bootstrap for direct script execution.
from impact_atlas.config import ProjectPaths  # noqa: E402
from impact_atlas.logging import configure_logging  # noqa: E402
from impact_atlas.admin_security import AdminAccess  # noqa: E402
from impact_atlas.change_journal import ChangeNotFound, JournalConflict, RowChangeJournal  # noqa: E402

PATHS = ProjectPaths.discover()
DB_PATH = PATHS.raw_events_database
STATIC_DIR = PATHS.root

PORT = 8800
MAX_PER_PAGE = 500
MAX_BODY_BYTES = 64 * 1024
logger = logging.getLogger("admin_api")


def merge_journal(connection: sqlite3.Connection) -> RowChangeJournal:
    """Bind generic recovery to the columns the existing admin merge modifies."""
    return RowChangeJournal(connection, table="unique_events", key="event_id", columns=(
        "full_text_dossier", "urls_list", "sources_list", "ai_analysis_status",
    ))

class AdminHTTPServer(HTTPServer):
    """HTTP server that tolerates immediate port reuse across restarts."""

    allow_reuse_address = True

    def __init__(self, server_address: tuple[str, int], handler: type[BaseHTTPRequestHandler],
                 *, access: AdminAccess | None = None) -> None:
        """Start with authenticated APIs; static public pages stay public."""
        self.access = access if access is not None else AdminAccess()
        super().__init__(server_address, handler)


def get_db() -> sqlite3.Connection:
    """Open a WAL-mode connection to the raw-events database."""
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False, timeout=10)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
    except BaseException:
        with contextlib.suppress(sqlite3.Error):
            conn.close()
        raise
    return conn


def _error_id(error: Exception) -> str:
    """Log an exception with a correlation id and return the id."""
    error_id = uuid.uuid4().hex[:8]
    logger.error("admin_api error %s: %s", error_id, error, exc_info=error)
    return error_id


class AdminAPIHandler(BaseHTTPRequestHandler):
    """Request handler exposing the manual merge REST endpoints."""

    def _authorize(self, *, write: bool = False) -> bool:
        """Require a valid capability before any API database access."""
        role = self.server.access.role_for(self.headers.get('Authorization'))
        if role is None:
            self._json_response(401, {"error": "Admin authentication required"})
            return False
        if write and role != 'writer':
            self._json_response(403, {"error": "Write permission required"})
            return False
        return True

    def _allow_request(self) -> bool:
        """Reject foreign hosts/origins on this loopback-only administrative service."""
        port = self.server.server_port
        allowed_hosts = {f"localhost:{port}", f"127.0.0.1:{port}", f"[::1]:{port}"}
        host = self.headers.get("Host", "").lower()
        origin = self.headers.get("Origin")
        if host not in allowed_hosts or (origin is not None and origin != f"http://{host}"):
            self._json_response(403, {"error": "Only same-origin localhost requests are allowed"})
            return False
        return True

    def do_GET(self) -> None:
        """Route GET requests to the API or the static file server."""
        if not self._allow_request():
            return
        parsed = urlparse(self.path)
        path = parsed.path

        if path.startswith('/api/') and not self._authorize():
            return

        if path == '/api/session':
            self._json_response(200, {"role": self.server.access.role_for(self.headers.get('Authorization'))})
        elif path == '/api/events':
            self._handle_list_events(parse_qs(parsed.query))
        elif path.startswith('/api/event/'):
            event_id = unquote(path.split('/api/event/')[-1])
            self._handle_get_event(event_id)
        elif path == '/api/sectors':
            self._handle_list_sectors()
        elif path == '/api/merge-history':
            self._handle_merge_history()
        elif path == '/api/suggestions':
            self._handle_suggestions(parse_qs(parsed.query))
        else:
            self._serve_static(path)

    def do_OPTIONS(self) -> None:
        """Reject cross-origin preflight requests."""
        if not self._allow_request():
            return
        self.send_response(204)
        self.end_headers()

    def do_POST(self) -> None:
        """Route POST bodies to merge/unmerge endpoints."""
        if not self._allow_request():
            return
        if not self._authorize(write=True):
            return
        parsed = urlparse(self.path)
        if self.headers.get_content_type() != "application/json":
            self._json_response(415, {"error": "Content-Type must be application/json"})
            return
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            if not 0 < content_length <= MAX_BODY_BYTES or self.headers.get('Transfer-Encoding'):
                self._json_response(400, {"error": "Invalid or oversized request body"})
                return
            self.connection.settimeout(10)
            body = json.loads(self.rfile.read(content_length))
        except (ValueError, UnicodeError, TimeoutError):
            self._json_response(400, {"error": "Malformed JSON body"})
            return
        if not isinstance(body, dict):
            self._json_response(400, {"error": "JSON body must be an object"})
            return

        if parsed.path == '/api/merge':
            self._handle_merge(body)
        elif parsed.path == '/api/unmerge':
            self._handle_unmerge(body)
        else:
            self._json_response(404, {"error": "Not found"})

    # â”€â”€â”€ Static file serving â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def _serve_static(self, path: str) -> None:
        """Serve static files from the project root with traversal protection."""
        if path == '/' or path == '':
            path = '/admin_merge.html'

        # Only public UI artifacts may be served, even within the project root.
        root = STATIC_DIR.resolve()
        try:
            safe_path = (root / unquote(path).lstrip('/')).resolve()
            relative = safe_path.relative_to(root)
        except (ValueError, OSError):
            self.send_error(403, "Forbidden")
            return
        public_pages = {"admin_merge.html", "index.html", "report.html", "legal.html",
                        "campaign_dossier_console.html"}
        public_extensions = {".css", ".js", ".json", ".geojson", ".csv", ".png", ".jpg",
                             ".jpeg", ".svg", ".webp", ".gif", ".ico", ".woff", ".woff2", ".ttf"}
        if (any(part.startswith('.') for part in relative.parts)
                or not (relative.as_posix() in public_pages
                        or (relative.parts and relative.parts[0] == 'assets'
                            and safe_path.suffix.lower() in public_extensions))):
            self.send_error(403, "Forbidden")
            return

        if not safe_path.is_file():
            self.send_error(404, "File not found")
            return

        try:
            content_type, _ = mimetypes.guess_type(str(safe_path))
            if not content_type:
                content_type = 'application/octet-stream'

            with open(safe_path, 'rb') as f:
                content = f.read()

            self.send_response(200)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(content)))
            self.send_header('X-Content-Type-Options', 'nosniff')
            self.end_headers()
            self.wfile.write(content)
            self.wfile.flush()
        except OSError as e:
            self.send_error(500, f"File access error (ref {_error_id(e)})")

    # â”€â”€â”€ JSON helper â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def _json_response(self, status: int, data: Any) -> None:
        """Write a JSON response, tolerating client disconnects."""
        try:
            body = json.dumps(data, ensure_ascii=False).encode('utf-8')
            self.send_response(status)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Content-Length', str(len(body)))
            self.send_header('X-Content-Type-Options', 'nosniff')
            self.send_header('Cache-Control', 'no-store')
            if status == 401:
                self.send_header('WWW-Authenticate', 'Bearer realm="Local administration"')
            self.end_headers()
            self.wfile.write(body)
            self.wfile.flush()
        except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
            pass

    # â”€â”€â”€ API: List Events â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def _handle_list_events(self, params: dict[str, list[str]]) -> None:
        """List events with search/sector/status filters and pagination."""
        conn: sqlite3.Connection | None = None
        try:
            conn = get_db()
            cursor = conn.cursor()

            search = params.get('search', [''])[0].strip()
            sector = params.get('sector', [''])[0].strip()
            status = params.get('status', [''])[0].strip()
            try:
                page = max(1, int(params.get('page', ['1'])[0]))
                per_page = min(MAX_PER_PAGE, max(1, int(params.get('per_page', ['50'])[0])))
            except ValueError:
                page, per_page = 1, 50
            offset = (page - 1) * per_page
            if offset > 2**63 - 1:
                self._json_response(400, {"error": "Page exceeds supported range"})
                return

            conditions = []
            bind_params: list[Any] = []

            if status:
                conditions.append("ai_analysis_status = ?")
                bind_params.append(status)
            if sector:
                conditions.append("operational_sector = ?")
                bind_params.append(sector)
            if search:
                conditions.append("(title LIKE ? OR description LIKE ?)")
                like = f"%{search}%"
                bind_params.extend([like, like])

            where_clause = " AND ".join(conditions) if conditions else "1=1"

            cursor.execute(f"SELECT COUNT(*) FROM unique_events WHERE {where_clause}", bind_params)  # noqa: S608
            total = cursor.fetchone()[0]

            cursor.execute(f"""
                SELECT event_id, title, description, last_seen_date,
                       tie_score, ai_analysis_status, operational_sector, ai_summary
                FROM unique_events
                WHERE {where_clause}
                ORDER BY last_seen_date DESC
                LIMIT ? OFFSET ?
            """, bind_params + [per_page, offset])

            events = []
            for row in cursor.fetchall():
                events.append({
                    "event_id": row["event_id"],
                    "title": row["title"] or "(No title)",
                    "description": (row["description"] or "")[:200],
                    "date": row["last_seen_date"] or "",
                    "tie_score": row["tie_score"] or 0,
                    "status": row["ai_analysis_status"],
                    "sector": row["operational_sector"] or "UNKNOWN",
                    "ai_summary": (row["ai_summary"] or "")[:300]
                })

            self._json_response(200, {
                "events": events, "total": total,
                "page": page, "per_page": per_page,
                "pages": max(1, (total + per_page - 1) // per_page)
            })
        except sqlite3.Error as e:
            self._json_response(500, {"error": f"Database error (ref {_error_id(e)})"})
        finally:
            if conn is not None:
                conn.close()

    # â”€â”€â”€ API: Get Event Detail â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def _handle_get_event(self, event_id: str) -> None:
        """Return the full dossier for a single event."""
        conn: sqlite3.Connection | None = None
        try:
            conn = get_db()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT event_id, title, description, last_seen_date,
                       full_text_dossier, ai_report_json, tie_score,
                       ai_analysis_status, operational_sector, ai_summary,
                       urls_list, sources_list, kinetic_score, target_score, effect_score
                FROM unique_events WHERE event_id = ?
            """, (event_id,))
            row = cursor.fetchone()

            if not row:
                self._json_response(404, {"error": "Event not found"})
                return

            lat, lon = None, None
            ai_data: dict[str, Any] = {}
            if row["ai_report_json"]:
                try:
                    parsed_report = json.loads(row["ai_report_json"])
                    ai_data = parsed_report if isinstance(parsed_report, dict) else {}
                    geo = ai_data.get("tactics", {}).get("geo_location", {})
                    expl = geo.get("explicit", {})
                    lat = expl.get("lat") or geo.get("inferred", {}).get("lat")
                    lon = expl.get("lon") or geo.get("inferred", {}).get("lon")
                except (json.JSONDecodeError, AttributeError, TypeError):
                    pass

            self._json_response(200, {
                "event_id": row["event_id"],
                "title": row["title"] or "",
                "description": row["description"] or "",
                "date": row["last_seen_date"] or "",
                "full_text": (row["full_text_dossier"] or "")[:2000],
                "tie_score": row["tie_score"] or 0,
                "k_score": row["kinetic_score"] or 0,
                "t_score": row["target_score"] or 0,
                "e_score": row["effect_score"] or 0,
                "status": row["ai_analysis_status"],
                "sector": row["operational_sector"] or "UNKNOWN",
                "ai_summary": row["ai_summary"] or "",
                "lat": lat, "lon": lon,
                "sources": row["sources_list"] or "",
                "classification": ai_data.get("classification", "")
            })
        except sqlite3.Error as e:
            self._json_response(500, {"error": f"Database error (ref {_error_id(e)})"})
        finally:
            if conn is not None:
                conn.close()

    # â”€â”€â”€ API: List Sectors â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def _handle_list_sectors(self) -> None:
        """Return event counts grouped by operational sector."""
        conn: sqlite3.Connection | None = None
        try:
            conn = get_db()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT operational_sector, COUNT(*) as cnt
                FROM unique_events
                WHERE operational_sector IS NOT NULL
                GROUP BY operational_sector
                ORDER BY cnt DESC
            """)
            sectors = [{"name": r[0], "count": r[1]} for r in cursor.fetchall()]
            self._json_response(200, {"sectors": sectors})
        except sqlite3.Error as e:
            self._json_response(500, {"error": f"Database error (ref {_error_id(e)})"})
        finally:
            if conn is not None:
                conn.close()

    # â”€â”€â”€ API: Merge â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def _handle_merge(self, data: dict[str, Any]) -> None:
        """Merge duplicate events: oldest becomes master, rest are marked MERGED."""
        conn: sqlite3.Connection | None = None
        try:
            event_ids = data.get("event_ids", [])
            if (not isinstance(event_ids, list) or not 2 <= len(event_ids) <= MAX_PER_PAGE
                    or any(not isinstance(value, str) or not value.strip() for value in event_ids)
                    or len(set(event_ids)) != len(event_ids)):
                self._json_response(400, {"error": "Need 2 to 500 distinct, non-empty event IDs"})
                return

            conn = get_db()
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.cursor()
            placeholders = ','.join(['?'] * len(event_ids))
            cursor.execute(f"""
                SELECT event_id, last_seen_date, full_text_dossier, title,
                       urls_list, sources_list
                FROM unique_events WHERE event_id IN ({placeholders})
            """, event_ids)
            events = cursor.fetchall()

            if len(events) != len(event_ids):
                self._json_response(400, {"error": f"Only {len(events)} events found"})
                return

            journal = merge_journal(conn)
            before = journal.capture(event_ids)

            sorted_events = sorted(events, key=lambda e: e["last_seen_date"] or "")
            master = sorted_events[0]
            victims = sorted_events[1:]

            merged_text = master["full_text_dossier"] or ""
            for v in victims:
                merged_text += f" ||| [MERGED]: {v['full_text_dossier'] or ''}"

            all_urls: set[str] = set()
            all_sources: set[str] = set()
            for ev in sorted_events:
                for url in (ev["urls_list"] or "").split(","):
                    url = url.strip()
                    if url:
                        all_urls.add(url)
                for src in (ev["sources_list"] or "").split(","):
                    src = src.strip()
                    if src:
                        all_sources.add(src)

            merged_urls = ", ".join(sorted(all_urls))
            merged_sources = ", ".join(sorted(all_sources))

            for v in victims:
                cursor.execute(
                    "UPDATE unique_events SET ai_analysis_status='MERGED' WHERE event_id=?",
                    (v["event_id"],)
                )

            cursor.execute("""
                UPDATE unique_events
                SET full_text_dossier=?,
                    urls_list=?,
                    sources_list=?
                WHERE event_id=?
            """, (merged_text, merged_urls, merged_sources, master["event_id"]))

            merge_id = journal.record(before)
            conn.commit()

            self._json_response(200, {
                "status": "ok",
                "merge_id": merge_id,
                "master_id": master["event_id"],
                "master_title": master["title"],
                "merged_count": len(victims),
                "merged_ids": [v["event_id"] for v in victims]
            })
            logger.info(
                "Merged %d events into master %s",
                len(victims), master["event_id"],
            )
        except JournalConflict as error:
            if conn is not None:
                conn.rollback()
            self._json_response(409, {"error": str(error)})
        except sqlite3.Error as e:
            if conn is not None:
                conn.rollback()
            self._json_response(500, {"error": f"Database error (ref {_error_id(e)})"})
        finally:
            if conn is not None:
                conn.close()

    # â”€â”€â”€ API: Unmerge â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def _handle_merge_history(self) -> None:
        """Return recent recovery metadata without exposing stored row snapshots."""
        conn: sqlite3.Connection | None = None
        try:
            conn = get_db()
            history = merge_journal(conn).history()
            self._json_response(200, {"changes": [
                {"merge_id": item.change_id, "record_count": item.record_count,
                 "created_at": item.created_at, "reverted_at": item.reverted_at}
                for item in history
            ]})
        except sqlite3.Error as error:
            self._json_response(500, {"error": f"Database error (ref {_error_id(error)})"})
        finally:
            if conn is not None:
                conn.close()

    def _handle_unmerge(self, data: dict[str, Any]) -> None:
        """Restore a recorded merge, or reject stale/legacy operations without mutation."""
        merge_id = data.get("merge_id")
        if not isinstance(merge_id, str) or not merge_id:
            self._json_response(400, {"error": "merge_id is required; legacy merges cannot be safely undone"})
            return
        conn: sqlite3.Connection | None = None
        try:
            conn = get_db()
            conn.execute("BEGIN IMMEDIATE")
            result = merge_journal(conn).restore(merge_id)
            conn.commit()
            self._json_response(200, {"status": "ok", "merge_id": result.change_id,
                                      "restored_count": result.record_count,
                                      "already_reverted": result.already_reverted})
            logger.info("Restored recorded merge %s (%d records)", result.change_id, result.record_count)
        except ChangeNotFound:
            if conn is not None:
                conn.rollback()
            self._json_response(404, {"error": "No recovery history exists for this merge"})
        except JournalConflict as error:
            if conn is not None:
                conn.rollback()
            self._json_response(409, {"error": str(error)})
        except sqlite3.Error as error:
            if conn is not None:
                conn.rollback()
            self._json_response(500, {"error": f"Database error (ref {_error_id(error)})"})
        finally:
            if conn is not None:
                conn.close()

    @staticmethod
    def _parse_local_iso(value: str) -> datetime | None:
        """Parse an ISO date string to UTC, treating legacy naive values as UTC."""
        if not value:
            return None
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)

    def _handle_suggestions(self, params: dict[str, list[str]]) -> None:
        """Cluster similar events via embedding cosine similarity."""
        conn: sqlite3.Connection | None = None
        try:
            import numpy as np  # Lazy import to avoid blocking server startup

            try:
                sim_threshold = float(params.get('threshold', ['0.85'])[0])
                max_hours = float(params.get('hours', ['72'])[0])
                limit = min(5000, max(2, int(params.get('limit', ['2000'])[0])))
            except ValueError:
                self._json_response(400, {"error": "threshold, hours and limit must be numeric"})
                return
            if not math.isfinite(sim_threshold) or not 0 <= sim_threshold <= 1 or not math.isfinite(max_hours) or max_hours < 0:
                self._json_response(400, {"error": "threshold must be 0..1 and hours must be finite and non-negative"})
                return
            sector = params.get('sector', [''])[0].strip()
            search = params.get('search', [''])[0].strip()
            status = params.get('status', [''])[0].strip()

            # â”€â”€ Build context-aware filter â”€â”€
            conditions = [
                "embedding_vector IS NOT NULL",
                "ai_analysis_status IN ('COMPLETED', 'PENDING')",
                "title IS NOT NULL",
                "TRIM(title) != ''",
                "title != '(No title)'",
            ]
            bind_params: list[Any] = []

            if sector:
                conditions.append("operational_sector = ?")
                bind_params.append(sector)
            if search:
                conditions.append("(title LIKE ? OR description LIKE ?)")
                like = f"%{search}%"
                bind_params.extend([like, like])
            if status:
                conditions.append("ai_analysis_status = ?")
                bind_params.append(status)

            where_clause = " AND ".join(conditions)
            bind_params.append(limit)

            conn = get_db()
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT event_id, title, last_seen_date, tie_score,
                       operational_sector, embedding_vector, ai_summary, description
                FROM unique_events
                WHERE {where_clause}
                ORDER BY last_seen_date DESC
                LIMIT ?
            """, bind_params)
            rows = cursor.fetchall()

            if len(rows) < 2:
                self._json_response(200, {"suggestions": [], "total_scanned": len(rows)})
                return

            events: list[dict[str, Any]] = []
            vectors: list[list[float]] = []
            for r in rows:
                try:
                    vec = json.loads(r["embedding_vector"])
                    if not isinstance(vec, list) or len(vec) < 10:
                        continue
                    if any(not isinstance(value, (int, float)) or not math.isfinite(value) for value in vec):
                        continue
                    if vectors and len(vec) != len(vectors[0]):
                        continue

                    raw_title = r["title"]
                    title_text = str(raw_title).strip().lower()
                    if (
                        not raw_title
                        or title_text == ""
                        or "(no title)" in title_text
                        or title_text in ("none", "null", "[no title]")
                    ):
                        continue

                    events.append({
                        "event_id": r["event_id"],
                        "title": r["title"] or "(No title)",
                        "date": r["last_seen_date"] or "",
                        "tie_score": r["tie_score"] or 0,
                        "sector": r["operational_sector"] or "UNKNOWN",
                        "summary": (r["ai_summary"] or r["description"] or "")[:200]
                    })
                    vectors.append(vec)
                except (json.JSONDecodeError, TypeError):
                    continue

            if len(events) < 2:
                self._json_response(200, {"suggestions": [], "total_scanned": len(rows)})
                return

            # Cosine similarity
            matrix = np.array(vectors, dtype=np.float32)
            norms = np.linalg.norm(matrix, axis=1, keepdims=True)
            matrix = matrix / (norms + 1e-10)
            sim_matrix = np.dot(matrix, matrix.T)
            np.fill_diagonal(sim_matrix, 0)

            # Find pairs above threshold
            pairs: list[tuple[int, int, float]] = []
            for i in range(len(events)):
                for j in range(i + 1, len(events)):
                    if sim_matrix[i, j] < sim_threshold:
                        continue
                    try:
                        dt_i = self._parse_local_iso(events[i]["date"])
                        dt_j = self._parse_local_iso(events[j]["date"])
                        if dt_i and dt_j and abs((dt_i - dt_j).total_seconds()) / 3600 > max_hours:
                            continue
                    except ValueError:
                        pass
                    pairs.append((i, j, float(sim_matrix[i, j])))

            # Union-find clustering
            parent = list(range(len(events)))

            def find(x: int) -> int:
                while parent[x] != x:
                    parent[x] = parent[parent[x]]
                    x = parent[x]
                return x

            for i, j, _ in pairs:
                ri, rj = find(i), find(j)
                if ri != rj:
                    parent[ri] = rj

            clusters_map: dict[int, dict[str, Any]] = {}
            for i, j, sim in pairs:
                root = find(i)
                if root not in clusters_map:
                    clusters_map[root] = {"members": set(), "max_sim": 0}
                clusters_map[root]["members"].update([i, j])
                clusters_map[root]["max_sim"] = max(clusters_map[root]["max_sim"], sim)

            suggestions = []
            for _root, cluster in clusters_map.items():
                members = sorted(cluster["members"], key=lambda idx: events[idx]["date"] or "")
                suggestions.append({
                    "max_similarity": round(cluster["max_sim"], 3),
                    "count": len(members),
                    "events": [events[idx] for idx in members]
                })

            suggestions.sort(key=lambda s: s["max_similarity"], reverse=True)

            self._json_response(200, {
                "suggestions": suggestions,
                "total_scanned": len(events),
                "threshold": sim_threshold,
                "max_hours": max_hours
            })
        except sqlite3.Error as e:
            self._json_response(500, {"error": f"Database error (ref {_error_id(e)})"})
        except Exception as e:
            self._json_response(500, {"error": f"Internal error (ref {_error_id(e)})"})
        finally:
            if conn is not None:
                conn.close()

    # â”€â”€â”€ Logging â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def log_message(self, format: str, *args: Any) -> None:
        """Log only API requests, silencing noisy static file access logs."""
        if args and '/api/' in str(args[0]):
            super().log_message(format, *args)


def main() -> None:
    """Start the admin API server on localhost."""
    with contextlib.suppress(AttributeError, OSError):
        sys.stdout.reconfigure(encoding='utf-8')
    configure_logging("admin_api", PATHS.logs / "admin_api.log")
    print(f"[*] IMPACT ATLAS Admin API on http://localhost:{PORT}")
    print(f"[*] Static: {STATIC_DIR}")
    print(f"[*] DB: {DB_PATH}")
    print(f"[*] Open http://localhost:{PORT}/admin_merge.html")
    configured_writer = os.environ.get('IMPACT_ADMIN_WRITE_TOKEN')
    access = AdminAccess(**({"writer_token": configured_writer} if configured_writer else {}),
                         reader_token=os.environ.get('IMPACT_ADMIN_READ_TOKEN'))
    server = AdminHTTPServer(('localhost', PORT), AdminAPIHandler, access=access)
    if not configured_writer:
        print(f"[*] Session admin token (enter in the admin page): {access.writer_token}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[!] Stopped.")
        server.server_close()


if __name__ == '__main__':
    main()
