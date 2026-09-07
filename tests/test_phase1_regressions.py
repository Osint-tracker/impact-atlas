"""Regression coverage for Phase 1 stability fixes."""

import gc
import json
import shutil
import sqlite3
import tempfile
import time
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import Mock

import ingestion.db_manager as db_manager
from map_loader import MapDataLoader


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class PhaseOneRegressionTests(unittest.TestCase):
    """Verify fixed persistence and FIRMS-ingestion failure modes."""

    def setUp(self) -> None:
        """Create an isolated workspace-local directory for SQLite test files."""
        self.temp_dir = Path(tempfile.mkdtemp(prefix=".phase1-test-", dir=PROJECT_ROOT))
        self.original_db_path = db_manager.DB_PATH

    def tearDown(self) -> None:
        """Restore module state and remove SQLite WAL artifacts on Windows."""
        db_manager.DB_PATH = self.original_db_path
        gc.collect()
        for _ in range(20):
            try:
                shutil.rmtree(self.temp_dir)
                return
            except PermissionError:
                time.sleep(0.15)
        self.fail(f"Could not remove temporary directory: {self.temp_dir}")

    def test_raw_event_schema_migrates_and_deduplicates(self) -> None:
        """Create and migrate raw-event databases without losing URL data."""
        fresh_path = self.temp_dir / "fresh.db"
        db_manager.DB_PATH = fresh_path
        event = {
            "text": "fresh database event",
            "type": "telegram",
            "source": "test",
            "date": "2026-09-07",
            "url": "https://example.test/fresh",
        }
        self.assertEqual(db_manager.save_raw_events([event]), 1)
        self.assertEqual(db_manager.save_raw_events([event]), 0)
        with closing(sqlite3.connect(fresh_path)) as connection:
            columns = {row[1] for row in connection.execute("PRAGMA table_info(raw_signals)")}
            saved_url = connection.execute("SELECT url FROM raw_signals").fetchone()[0]
        self.assertIn("url", columns)
        self.assertEqual(saved_url, event["url"])

        legacy_path = self.temp_dir / "legacy.db"
        with closing(sqlite3.connect(legacy_path)) as connection:
            connection.execute(
                """
                CREATE TABLE raw_signals (
                    event_hash TEXT PRIMARY KEY,
                    source_type TEXT,
                    source_name TEXT,
                    date_published DATETIME,
                    text_content TEXT,
                    tie_score REAL DEFAULT 0,
                    processed BOOLEAN DEFAULT 0,
                    media_urls TEXT
                )
                """
            )
        db_manager.DB_PATH = legacy_path
        legacy_event = {**event, "text": "legacy database event", "url": "https://example.test/legacy"}
        self.assertEqual(db_manager.save_raw_events([legacy_event]), 1)
        with closing(sqlite3.connect(legacy_path)) as connection:
            saved_url = connection.execute("SELECT url FROM raw_signals").fetchone()[0]
        self.assertEqual(saved_url, legacy_event["url"])

    def test_firms_csv_preserves_all_quoted_rows(self) -> None:
        """Parse all FIRMS rows with a standards-compliant CSV reader."""
        loader = MapDataLoader(output_dir=self.temp_dir / "map-output")
        loader.firms_api_key = "test-key"
        loader.borders = []
        response = Mock(status_code=200)
        response.text = (
            "latitude,longitude,bright_ti4,confidence,acq_date,acq_time,frp,note\n"
            "50.0,30.0,310,n,2026-09-07,0100,5,\"first, quoted\"\n"
            "51.0,31.0,320,h,2026-09-07,0200,7,\"second, quoted\"\n"
        )
        loader.fetch_with_retry = Mock(return_value=response)

        self.assertTrue(loader.load_nasa_firms(days=1))
        payload = json.loads((loader.output_dir / "thermal_firms.geojson").read_text(encoding="utf-8"))
        self.assertEqual(len(payload["features"]), 2)
        self.assertEqual(payload["features"][0]["geometry"]["coordinates"], [30.0, 50.0])
