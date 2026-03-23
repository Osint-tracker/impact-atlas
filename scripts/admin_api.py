"""
IMPACT ATLAS — Admin Manual Merge API
Lightweight HTTP server for browsing events and manually merging duplicates.
Uses the same merge protocol as smart_fusion.py.

Usage: python -u scripts/admin_api.py
Then open http://localhost:8787/admin_merge.html in your browser.
"""

import sqlite3
import json
import os
import sys
import mimetypes
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs, unquote
from datetime import datetime, timedelta

sys.stdout.reconfigure(encoding='utf-8')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, '..', 'war_tracker_v2', 'data', 'raw_events.db')
STATIC_DIR = os.path.normpath(os.path.join(BASE_DIR, '..'))

PORT = 8800


class AdminHTTPServer(HTTPServer):
    allow_reuse_address = True


def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


class AdminAPIHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path == '/api/events':
            self._handle_list_events(parse_qs(parsed.query))
        elif path.startswith('/api/event/'):
            event_id = unquote(path.split('/api/event/')[-1])
            self._handle_get_event(event_id)
        elif path == '/api/sectors':
            self._handle_list_sectors()
        elif path == '/api/suggestions':
            self._handle_suggestions(parse_qs(parsed.query))
        else:
            self._serve_static(path)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == '/api/merge':
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length)
            self._handle_merge(json.loads(body))
        elif parsed.path == '/api/unmerge':
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length)
            self._handle_unmerge(json.loads(body))
        else:
            self._json_response(404, {"error": "Not found"})

    # ─── Static file serving ─────────────────────────────────────────

    def _serve_static(self, path):
        """Serve static files from the project root."""
        if path == '/' or path == '':
            path = '/admin_merge.html'

        # Security: prevent path traversal
        safe_path = os.path.normpath(os.path.join(STATIC_DIR, path.lstrip('/')))
        if not safe_path.startswith(STATIC_DIR):
            self.send_error(403, "Forbidden")
            return

        if not os.path.isfile(safe_path):
            self.send_error(404, "File not found")
            return

        try:
            content_type, _ = mimetypes.guess_type(safe_path)
            if not content_type:
                content_type = 'application/octet-stream'

            with open(safe_path, 'rb') as f:
                content = f.read()

            self.send_response(200)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(content)))
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(content)
            self.wfile.flush()
        except Exception as e:
            self.send_error(500, str(e))

    # ─── JSON helper ─────────────────────────────────────────────────

    def _json_response(self, status, data):
        try:
            body = json.dumps(data, ensure_ascii=False).encode('utf-8')
            self.send_response(status)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Content-Length', str(len(body)))
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(body)
            self.wfile.flush()
        except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
            pass  # Client disconnected, ignore

    # ─── API: List Events ────────────────────────────────────────────

    def _handle_list_events(self, params):
        try:
            conn = get_db()
            cursor = conn.cursor()

            search = params.get('search', [''])[0].strip()
            sector = params.get('sector', [''])[0].strip()
            status = params.get('status', [''])[0].strip()
            page = int(params.get('page', ['1'])[0])
            per_page = int(params.get('per_page', ['50'])[0])
            offset = (page - 1) * per_page

            conditions = []
            bind_params = []

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

            cursor.execute(f"SELECT COUNT(*) FROM unique_events WHERE {where_clause}", bind_params)
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

            conn.close()
            self._json_response(200, {
                "events": events, "total": total,
                "page": page, "per_page": per_page,
                "pages": max(1, (total + per_page - 1) // per_page)
            })
        except Exception as e:
            self._json_response(500, {"error": str(e)})

    # ─── API: Get Event Detail ───────────────────────────────────────

    def _handle_get_event(self, event_id):
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
            conn.close()

            if not row:
                self._json_response(404, {"error": "Event not found"})
                return

            lat, lon = None, None
            ai_data = {}
            if row["ai_report_json"]:
                try:
                    ai_data = json.loads(row["ai_report_json"])
                    geo = ai_data.get("tactics", {}).get("geo_location", {})
                    expl = geo.get("explicit", {})
                    lat = expl.get("lat") or geo.get("inferred", {}).get("lat")
                    lon = expl.get("lon") or geo.get("inferred", {}).get("lon")
                except Exception:
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
        except Exception as e:
            self._json_response(500, {"error": str(e)})

    # ─── API: List Sectors ───────────────────────────────────────────

    def _handle_list_sectors(self):
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
            conn.close()
            self._json_response(200, {"sectors": sectors})
        except Exception as e:
            self._json_response(500, {"error": str(e)})

    # ─── API: Merge ──────────────────────────────────────────────────

    def _handle_merge(self, data):
        try:
            event_ids = data.get("event_ids", [])
            if len(event_ids) < 2:
                self._json_response(400, {"error": "Need at least 2 events"})
                return

            conn = get_db()
            cursor = conn.cursor()
            placeholders = ','.join(['?'] * len(event_ids))
            cursor.execute(f"""
                SELECT event_id, last_seen_date, full_text_dossier, title,
                       urls_list, sources_list
                FROM unique_events WHERE event_id IN ({placeholders})
            """, event_ids)
            events = cursor.fetchall()

            if len(events) < 2:
                conn.close()
                self._json_response(400, {"error": f"Only {len(events)} events found"})
                return

            sorted_events = sorted(events, key=lambda e: e["last_seen_date"] or "")
            master = sorted_events[0]
            victims = sorted_events[1:]

            # ── Merge full_text_dossier ──
            merged_text = master["full_text_dossier"] or ""
            for v in victims:
                merged_text += f" ||| [MERGED]: {v['full_text_dossier'] or ''}"

            # ── Merge urls_list and sources_list ──
            all_urls = set()
            all_sources = set()
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

            # ── Mark victims as MERGED ──
            for v in victims:
                cursor.execute(
                    "UPDATE unique_events SET ai_analysis_status='MERGED' WHERE event_id=?",
                    (v["event_id"],)
                )

            # ── Update master: preserve AI intelligence, just enrich data ──
            cursor.execute("""
                UPDATE unique_events
                SET full_text_dossier=?,
                    urls_list=?,
                    sources_list=?
                WHERE event_id=?
            """, (merged_text, merged_urls, merged_sources, master["event_id"]))

            conn.commit()
            conn.close()

            self._json_response(200, {
                "status": "ok",
                "master_id": master["event_id"],
                "master_title": master["title"],
                "merged_count": len(victims),
                "merged_ids": [v["event_id"] for v in victims]
            })
        except Exception as e:
            self._json_response(500, {"error": str(e)})

    # ─── API: Unmerge ────────────────────────────────────────────────

    def _handle_unmerge(self, data):
        try:
            event_id = data.get("event_id", "")
            if not event_id:
                self._json_response(400, {"error": "Missing event_id"})
                return

            conn = get_db()
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE unique_events SET ai_analysis_status='PENDING' WHERE event_id=? AND ai_analysis_status='MERGED'",
                (event_id,)
            )
            affected = cursor.rowcount
            conn.commit()
            conn.close()

            if affected == 0:
                self._json_response(404, {"error": "Not MERGED or not found"})
            else:
                self._json_response(200, {"status": "ok", "event_id": event_id})
        except Exception as e:
            self._json_response(500, {"error": str(e)})

    # ─── API: AI Suggestions ─────────────────────────────────────────

    def _handle_suggestions(self, params):
        try:
            import numpy as np  # Lazy import to avoid blocking server startup

            sim_threshold = float(params.get('threshold', ['0.85'])[0])
            max_hours = float(params.get('hours', ['72'])[0])
            limit = int(params.get('limit', ['2000'])[0])
            sector = params.get('sector', [''])[0].strip()
            search = params.get('search', [''])[0].strip()
            status = params.get('status', [''])[0].strip()

            # ── Build context-aware filter ──
            conditions = [
                "embedding_vector IS NOT NULL",
                "ai_analysis_status IN ('COMPLETED', 'PENDING')",
                "title IS NOT NULL",
                "TRIM(title) != ''",
                "title != '(No title)'",
            ]
            bind_params = []

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
            conn.close()

            if len(rows) < 2:
                self._json_response(200, {"suggestions": [], "total_scanned": len(rows)})
                return

            events, vectors = [], []
            for r in rows:
                try:
                    vec = json.loads(r["embedding_vector"])
                    if not vec or len(vec) < 10:
                        continue
                    
                    raw_title = r["title"]
                    if not raw_title or str(raw_title).strip() == "" or "(No title)" in str(raw_title) or str(raw_title).lower() in ("none", "null", "[no title]"):
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
                except Exception:
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
            pairs = []
            for i in range(len(events)):
                for j in range(i + 1, len(events)):
                    if sim_matrix[i, j] < sim_threshold:
                        continue
                    try:
                        dt_i = datetime.fromisoformat(events[i]["date"].replace('Z', '+00:00')).replace(tzinfo=None) if events[i]["date"] else None
                        dt_j = datetime.fromisoformat(events[j]["date"].replace('Z', '+00:00')).replace(tzinfo=None) if events[j]["date"] else None
                        if dt_i and dt_j and abs((dt_i - dt_j).total_seconds()) / 3600 > max_hours:
                            continue
                    except Exception:
                        pass
                    pairs.append((i, j, float(sim_matrix[i, j])))

            # Union-find clustering
            parent = list(range(len(events)))

            def find(x):
                while parent[x] != x:
                    parent[x] = parent[parent[x]]
                    x = parent[x]
                return x

            for i, j, _ in pairs:
                ri, rj = find(i), find(j)
                if ri != rj:
                    parent[ri] = rj

            clusters_map = {}
            for i, j, sim in pairs:
                root = find(i)
                if root not in clusters_map:
                    clusters_map[root] = {"members": set(), "max_sim": 0}
                clusters_map[root]["members"].update([i, j])
                clusters_map[root]["max_sim"] = max(clusters_map[root]["max_sim"], sim)

            suggestions = []
            for root, cluster in clusters_map.items():
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
        except Exception as e:
            import traceback
            traceback.print_exc()
            self._json_response(500, {"error": str(e)})

    # ─── Logging ─────────────────────────────────────────────────────

    def log_message(self, format, *args):
        if args and '/api/' in str(args[0]):
            super().log_message(format, *args)


if __name__ == '__main__':
    print(f"[*] IMPACT ATLAS Admin API on http://localhost:{PORT}")
    print(f"[*] Static: {STATIC_DIR}")
    print(f"[*] DB: {DB_PATH}")
    print(f"[*] Open http://localhost:{PORT}/admin_merge.html")
    server = AdminHTTPServer(('localhost', PORT), AdminAPIHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[!] Stopped.")
        server.server_close()
