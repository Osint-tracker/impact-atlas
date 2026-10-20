"""Run the real recovery UI/API against a disposable local SQLite database."""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import admin_api  # noqa: E402


def main() -> None:
    """Exercise the existing admin page without opening the production database."""
    token = "test-browser-writer-" + "a" * 40
    with tempfile.TemporaryDirectory(prefix="atlas-admin-recovery-") as directory:
        path = Path(directory) / "test.sqlite"
        with closing(sqlite3.connect(path)) as connection:
            connection.execute("""CREATE TABLE unique_events (
                event_id TEXT PRIMARY KEY, title TEXT, description TEXT, last_seen_date TEXT,
                full_text_dossier TEXT, ai_report_json TEXT, tie_score REAL, ai_analysis_status TEXT,
                operational_sector TEXT, ai_summary TEXT, urls_list TEXT, sources_list TEXT,
                kinetic_score REAL, target_score REAL, effect_score REAL
            )""")
            for event_id in ("one", "two"):
                connection.execute("""INSERT INTO unique_events
                    (event_id, title, full_text_dossier, ai_report_json, ai_analysis_status)
                    VALUES (?, ?, ?, '{}', 'PENDING')""", (event_id, f"Test record {event_id}", f"Original {event_id}"))
            connection.commit()
            original = connection.execute("SELECT * FROM unique_events ORDER BY event_id").fetchall()
        with patch.object(admin_api, "DB_PATH", path), patch.object(admin_api, "STATIC_DIR", ROOT):
            server = admin_api.AdminHTTPServer(("127.0.0.1", 0), admin_api.AdminAPIHandler,
                                              access=admin_api.AdminAccess(writer_token=token))
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                environment = dict(os.environ, UI_BASE_URL=f"http://127.0.0.1:{server.server_port}",
                                   RECOVERY_TEST_TOKEN=token)
                for script in ("admin_recovery.browser.cjs", "admin_security.browser.cjs"):
                    subprocess.run(["node", str(ROOT / "tests" / script)],
                                   env=environment, check=True, timeout=90)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)
        with closing(sqlite3.connect(path)) as connection:
            assert connection.execute("SELECT * FROM unique_events ORDER BY event_id").fetchall() == original
            rows = connection.execute("SELECT reverted_at FROM _atlas_row_changes").fetchall()
            assert len(rows) == 1 and rows[0][0]
        print("Real UI/API round trip restored every original database value.")


if __name__ == "__main__":
    main()
