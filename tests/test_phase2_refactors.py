"""Phase 2 unit coverage for the refactored canonical backend.

All tests are network-free and exercise pure functions or local SQLite
databases created inside a temporary directory. Optional heavy dependencies
are detected at import time and skipped gracefully in minimal CI runners.
"""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from datetime import UTC

PROJECT_ROOT = Path(__file__).resolve().parents[1]

try:
    import scripts.v42_analytics as v42
    HAS_V42 = True
except ImportError:
    HAS_V42 = False

try:
    from scripts import campaigns_engine as ce
    HAS_CE = True
except ImportError:
    HAS_CE = False

try:
    from scripts import generate_output as go
    HAS_GO = True
except ImportError:
    HAS_GO = False

try:
    from scripts.geolocator_agent import GeolocatorAgent
    import scripts.geolocator_agent as geo_mod
    HAS_GEO = True
except ImportError:
    HAS_GEO = False

try:
    from scripts.master_ingestor import UnitResolver
    HAS_ING = True
except ImportError:
    HAS_ING = False


@unittest.skipUnless(HAS_V42, "v42_analytics import unavailable")
class V42AnalyticsTests(unittest.TestCase):
    """Analytic derivations on in-memory feature payloads."""

    def test_extract_faction_prefers_report_over_text(self) -> None:
        ai = {"tactics": {"actors": {"aggressor": {"side": "RUS"}}}}
        self.assertEqual(v42.extract_faction(ai, "Ukrainian forces"), "RU")
        self.assertEqual(v42.extract_faction({}, "AFU struck a depot in Kyiv"), "UA")
        self.assertEqual(v42.extract_faction({}, ""), "UNK")

    def test_extract_classification_walks_nested_candidates(self) -> None:
        ai = {"tactics": {"event_analysis": {"classification": "strike"}}}
        self.assertEqual(v42.extract_classification(ai), "STRIKE")
        self.assertEqual(v42.extract_classification(None), "UNKNOWN")
        self.assertEqual(v42.extract_classification({}), "UNKNOWN")

    def test_normalize_and_domains(self) -> None:
        self.assertEqual(v42.normalize_domain("https://WWW.ISW.pub/article?x=1"), "isw.pub")
        self.assertEqual(v42.normalize_domain("t.me/channel"), "t.me")
        sources = [{"name": "a", "url": "https://isw.pub/x"}, {"name": "mod.gov.ua"}]
        self.assertEqual(v42.domains_from_structured_sources(sources), ["isw.pub", "mod.gov.ua"])

    def test_asymmetry_index_math(self) -> None:
        features = [
            {"properties": {"operational_sector": "S", "faction": "UA", "vec_k": 5, "vec_e": 4, "vec_t": 2}},
            {"properties": {"operational_sector": "S", "faction": "UA", "vec_k": 3, "vec_e": 2, "vec_t": 2}},
        ]
        out = v42.compute_asymmetry_index(features)
        self.assertEqual(out["sectors"]["S"]["UA"]["events"], 2)
        self.assertEqual(out["global"]["UA"], round((5 * 4 + 3 * 2) / 4, 4))

    def test_sector_anomaly_detection(self) -> None:
        features = []
        for day in range(1, 16):
            features.append({"properties": {"operational_sector": "CALM", "date": f"2026-08-{day:02d}"}})
        features.append({"properties": {"operational_sector": "HOT", "date": f"2026-08-{d:02d}"}} for d in range(1, 16))
        features = [f for f in features if isinstance(f, dict)]
        features += [{"properties": {"operational_sector": "CALM", "date": "2026-08-15"}}] * 5
        anomalies = v42.compute_sector_volume_anomalies(features, lookback_days=14)
        self.assertIn("CALM", anomalies)
        flagged = v42.apply_anomaly_flags(list(features), anomalies)
        self.assertTrue(flagged[0]["properties"]["is_anomaly_sector"])

    def test_reputation_decay_pulls_toward_center(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            conn = sqlite3.connect(Path(tmp) / "rep.db")
            conn.execute("CREATE TABLE unique_events (event_id TEXT PRIMARY KEY)")
            v42.ensure_sources_reputation_schema(conn)
            conn.execute(
                "INSERT INTO sources_reputation (domain, score, last_verified) VALUES (?, ?, ?)",
                ("example.com", 90, "2026-01-01T00:00:00"),
            )
            v42.apply_reputation_decay(conn)
            decayed = conn.execute("SELECT score FROM sources_reputation").fetchone()[0]
            self.assertLess(decayed, 90)
            score = v42.update_event_reputation(conn, "evt-1", ["example.com"], institutional=True)
            self.assertGreaterEqual(score, 0)
            conn.close()


@unittest.skipUnless(HAS_CE, "campaigns_engine import unavailable")
class CampaignsEngineTests(unittest.TestCase):
    """Definition normalization and deterministic event tagging."""

    def test_normalize_campaign_rows(self) -> None:
        rows = [
            {"campaign_id": "energy", "name": "Energy Coercion", "target_types": "power|grid", "keywords": "drone, strike", "color": "f59e0b"},
            {"campaign_id": "", "name": "broken", "target_types": "x", "keywords": "y"},
        ]
        out = ce.normalize_campaign_rows(rows)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["color"], "#f59e0b")
        self.assertEqual(out[0]["target_types"], ["power", "grid"])

    def test_match_event_campaign(self) -> None:
        campaigns = [{
            "campaign_id": "energy", "name": "Energy Coercion", "color": "#f59e0b",
            "target_types": ["power plant"], "keywords": ["drone", "strike"],
        }]
        match = ce.match_event_campaign(campaigns, "POWER PLANT", "Night drone strike on substation")
        self.assertIsNotNone(match)
        self.assertEqual(match["campaign_id"], "energy")
        self.assertIsNone(ce.match_event_campaign(campaigns, "tank", "drone strike"))

    def test_status_and_sheet_url(self) -> None:
        from datetime import datetime, timedelta

        self.assertEqual(ce._campaign_status(None), "STANDBY")
        recent = datetime.now(UTC) - timedelta(days=1)
        self.assertEqual(ce._campaign_status(recent), "LIVE")
        url = ce.build_campaign_sheet_csv_url(
            "https://docs.google.com/spreadsheets/d/ABC123/edit#gid=0", "campaign_definitions"
        )
        self.assertIn("ABC123", url)
        self.assertTrue(url.endswith("sheet=campaign_definitions"))


@unittest.skipUnless(HAS_GO, "generate_output import unavailable")
class GenerateOutputTests(unittest.TestCase):
    """Sanitization, source parsing, and export shaping helpers."""

    def test_pii_redaction(self) -> None:
        self.assertEqual(go.sanitize_public_text("Colonel Ivan Petrov arrived"), "[REDACTED] arrived")
        sanitized = go.sanitize_public_object({"name": "Secret", "kind": "strike", "note": "Plate AA-1234-BB seen"})
        self.assertNotIn("name", sanitized)
        self.assertIn("[REDACTED]", sanitized["note"])

    def test_parse_sources_to_list(self) -> None:
        srcs = go.parse_sources_to_list('["https://t.me/warmonitor/123", "isw.pub", "GDELT_Network", "x", ""]')
        names = [s["name"] for s in srcs]
        self.assertIn("warmonitor", names)
        self.assertIn("isw.pub", names)
        self.assertIn("GDELT", names)
        self.assertNotIn("x", names)
        pipes = go.parse_sources_to_list("chan1 ||| chan2")
        self.assertEqual([s["name"] for s in pipes], ["chan1", "chan2"])

    def test_marker_style_thresholds(self) -> None:
        self.assertEqual(go.get_marker_style(10, 9)[1], "#ef4444")
        self.assertEqual(go.get_marker_style(0, 0)[1], "#64748b")
        self.assertEqual(go.get_marker_style(None, "junk")[1], "#64748b")

    def test_classify_sector(self) -> None:
        self.assertEqual(go.classify_sector(49.0, 35.0, "power plant"), "ENERGY_COERCION")
        self.assertEqual(go.classify_sector(51.0, 38.0, "depot"), "DEEP_STRIKES_RU")
        self.assertEqual(go.classify_sector(None, None, "depot"), "SOUTHERN_FRONT")

    def test_opsec_gating_and_latest_window(self) -> None:
        import datetime as dt

        now = dt.datetime.now(dt.UTC)
        fresh = (now - dt.timedelta(hours=2)).isoformat()
        old = (now - dt.timedelta(days=3)).isoformat()
        self.assertFalse(go._should_publish_event(fresh, "MANOEUVRE", "", "", "", now))
        self.assertTrue(go._should_publish_event(old, "MANOEUVRE", "", "", "", now))
        self.assertTrue(go._should_publish_event(fresh, "STRIKE", "", "", "", now))

        features = [
            {"properties": {"date": "2026-09-06"}},
            {"properties": {"date": "2026-09-02"}},
            {"properties": {"date": "2026-08-20"}},
        ]
        latest = go.build_latest_features(features)
        self.assertEqual(len(latest), 2)
        self.assertEqual(latest[0]["properties"]["date"], "2026-09-06")

    def test_date_to_epoch_ms(self) -> None:
        self.assertEqual(go._date_to_epoch_ms(None), 0)
        self.assertEqual(go._date_to_epoch_ms("nat"), 0)
        self.assertGreater(go._date_to_epoch_ms("2026-09-06"), 0)
        self.assertGreater(go._date_to_epoch_ms("2026-09-06T12:00:00Z"), 0)


@unittest.skipUnless(HAS_GEO, "geolocator_agent import unavailable (needs shapely)")
class GeolocatorAgentTests(unittest.TestCase):
    """Deterministic sector assignment without importing singletons."""

    def test_import_has_no_side_effects(self) -> None:
        self.assertIsNone(geo_mod._geolocator)
        self.assertIsNone(geo_mod._gazetteer)

    def test_assign_sector_without_geofences(self) -> None:

        with tempfile.TemporaryDirectory() as tmp:
            agent = GeolocatorAgent(Path(tmp) / "missing.geojson", Path(tmp) / "borders.geojson")
            self.assertEqual(agent.sectors, [])
            self.assertEqual(agent.assign_sector(37.0, 49.0), "Rear_Area_UA")
            self.assertEqual(agent.assign_sector(None, None), "UNKNOWN_SECTOR")

    def test_lazy_singletons_are_cached(self) -> None:
        first = geo_mod.get_geolocator()
        self.assertIs(first, geo_mod.get_geolocator())


@unittest.skipUnless(HAS_ING, "master_ingestor import unavailable")
class UnitResolverTests(unittest.TestCase):
    """Entity resolution from raw unit naming conventions."""

    def test_resolve_known_aliases(self) -> None:
        resolver = UnitResolver()
        self.assertEqual(resolver.resolve_unit_id("93rd Mechanized Brigade"), "ua_93_mech")
        self.assertEqual(resolver.resolve_unit_id("3rd Assault"), "ua_3_assault")
        self.assertEqual(resolver.resolve_unit_id("Wagner Group"), "ru_wagner")
        self.assertIsNone(resolver.resolve_unit_id(""))
        self.assertIsNone(resolver.resolve_unit_id("Totally Unknown Battalion 999"))


if __name__ == "__main__":
    unittest.main()
