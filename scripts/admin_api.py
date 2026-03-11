"""
IMPACT ATLAS — Admin Manual Merge API
Lightweight HTTP server for browsing events and manually merging duplicates.
Uses the same merge protocol as smart_fusion.py.

Usage: python scripts/admin_api.py
Then open admin_merge.html in browser.
"""

import sqlite3
import json
import os
import sys
from http.server import HTTPServer, SimpleHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

sys.stdout.reconfigure(encoding='utf-8')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, '../war_tracker_v2/data/raw_events.db')
STATIC_DIR = os.path.join(BASE_DIR, '..')

PORT = 8787


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


class AdminAPIHandler(SimpleHTTPRequestHandler):
    """Serves both static files (from project root) and API endpoints."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=STATIC_DIR, **kwargs)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path == '/api/events':
            self._handle_list_events(parse_qs(parsed.query))
        elif path.startswith('/api/event/'):
            event_id = path.split('/api/event/')[-1]
            self._handle_get_event(event_id)
        elif path == '/api/sectors':
            self._handle_list_sectors()
        else:
            # Serve static files (HTML, JS, CSS, etc.)
            super().do_GET()

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

    def _json_response(self, status, data):
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode('utf-8'))

    def _handle_list_events(self, params):
        """List events with optional search, sector, status filters and pagination."""
        try:
            conn = get_db()
            cursor = conn.cursor()

            search = params.get('search', [''])[0].strip()
            sector = params.get('sector', [''])[0].strip()
            status = params.get('status', ['COMPLETED'])[0].strip()
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
                conditions.append("(title LIKE ? OR description LIKE ? OR full_text_dossier LIKE ?)")
                like = f"%{search}%"
                bind_params.extend([like, like, like])

            where_clause = " AND ".join(conditions) if conditions else "1=1"

            # Count total
            cursor.execute(f"SELECT COUNT(*) FROM unique_events WHERE {where_clause}", bind_params)
            total = cursor.fetchone()[0]

            # Fetch page
            cursor.execute(f"""
                SELECT event_id, title, description, last_seen_date, 
                       tie_score, ai_analysis_status, operational_sector,
                       ai_summary
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
                "events": events,
                "total": total,
                "page": page,
                "per_page": per_page,
                "pages": (total + per_page - 1) // per_page
            })
        except Exception as e:
            self._json_response(500, {"error": str(e)})

    def _handle_get_event(self, event_id):
        """Get full event details."""
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

            # Parse coordinates from ai_report_json
            lat, lon = None, None
            ai_data = {}
            if row["ai_report_json"]:
                try:
                    ai_data = json.loads(row["ai_report_json"])
                    tactics = ai_data.get("tactics", {})
                    geo = tactics.get("geo_location", {}).get("explicit", {})
                    lat = geo.get("lat")
                    lon = geo.get("lon")
                    if not lat or not lon:
                        inferred = tactics.get("geo_location", {}).get("inferred", {})
                        lat = inferred.get("lat")
                        lon = inferred.get("lon")
                except:
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
                "lat": lat,
                "lon": lon,
                "sources": row["sources_list"] or "",
                "classification": ai_data.get("classification", "")
            })
        except Exception as e:
            self._json_response(500, {"error": str(e)})

    def _handle_list_sectors(self):
        """List all distinct sectors."""
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

    def _handle_merge(self, data):
        """
        Manual merge using same protocol as smart_fusion.py:
        - Master = chronologically oldest event
        - Victims = all others → status='MERGED'
        - Master gets concatenated text, reset to PENDING
        """
        try:
            event_ids = data.get("event_ids", [])
            if len(event_ids) < 2:
                self._json_response(400, {"error": "Need at least 2 events to merge"})
                return

            conn = get_db()
            cursor = conn.cursor()

            # Fetch all events
            placeholders = ','.join(['?'] * len(event_ids))
            cursor.execute(f"""
                SELECT event_id, last_seen_date, full_text_dossier, title
                FROM unique_events WHERE event_id IN ({placeholders})
            """, event_ids)
            events = cursor.fetchall()

            if len(events) < 2:
                conn.close()
                self._json_response(400, {"error": f"Only {len(events)} events found"})
                return

            # Sort by date (oldest first) to pick master
            sorted_events = sorted(events, key=lambda e: e["last_seen_date"] or "")
            master = sorted_events[0]
            victims = sorted_events[1:]

            # Build merged text
            merged_text = master["full_text_dossier"] or ""
            for v in victims:
                merged_text += f" ||| [MERGED]: {v['full_text_dossier'] or ''}"

            # Execute merge
            for v in victims:
                cursor.execute(
                    "UPDATE unique_events SET ai_analysis_status='MERGED' WHERE event_id=?",
                    (v["event_id"],)
                )

            cursor.execute("""
                UPDATE unique_events
                SET full_text_dossier=?, ai_analysis_status='PENDING',
                    ai_report_json=NULL, embedding_vector=NULL
                WHERE event_id=?
            """, (merged_text, master["event_id"]))

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

    def _handle_unmerge(self, data):
        """Undo a merge: restore a MERGED event to PENDING."""
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
                self._json_response(404, {"error": "Event not found or not in MERGED status"})
            else:
                self._json_response(200, {"status": "ok", "event_id": event_id})
        except Exception as e:
            self._json_response(500, {"error": str(e)})

    def log_message(self, format, *args):
        """Override to suppress verbose static file logs."""
        if '/api/' in str(args[0]) if args else False:
            super().log_message(format, *args)


if __name__ == '__main__':
    print(f"🔐 IMPACT ATLAS Admin API starting on http://localhost:{PORT}")
    print(f"📂 Serving static files from: {STATIC_DIR}")
    print(f"🗄️  Database: {DB_PATH}")
    print(f"🌐 Open http://localhost:{PORT}/admin_merge.html in your browser")
    server = HTTPServer(('localhost', PORT), AdminAPIHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n🛑 Admin API stopped.")
        server.server_close()
