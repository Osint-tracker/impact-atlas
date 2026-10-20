"""Offline regressions for the current Phase 1 audit; never open project databases."""

import http.client
import json
import os
import sqlite3
import threading
from contextlib import closing
from unittest.mock import Mock, patch

import pytest
import requests

from impact_atlas.config import RuntimeSettings
from impact_atlas.config import ProjectPaths
from impact_atlas.errors import ConfigurationError, IngestionError
from impact_atlas.http import ResilientHttpClient, RetryPolicy
from ingestion.connectors import ParabellumGeoExtractor, WarSpottingClient
from ingestion.db_manager import RawEventRepository
from scripts import admin_api, generate_output, master_ingestor
from map_loader import MapDataLoader


def test_explicit_empty_environment_is_isolated():
    with patch.dict(os.environ, {"FIRMS_API_KEY": "ambient-secret"}):
        assert RuntimeSettings.from_environment({}).firms_api_key is None


@pytest.mark.parametrize("value", ["nan", "inf", "-inf", "0", "-1"])
def test_backoff_must_be_finite_and_positive(value):
    with pytest.raises(ConfigurationError):
        RuntimeSettings.from_environment({"IMPACT_ATLAS_RETRY_BACKOFF_SECONDS": value})


def test_http_timeout_override_and_failed_response_cleanup():
    session = Mock()
    transient, success = Mock(status_code=503), Mock(status_code=200)
    session.request.side_effect = [transient, success]
    client = ResilientHttpClient(name="test", user_agent="test", session=session, sleeper=Mock())
    assert client.request("GET", "https://example.test", timeout=5) is success
    assert session.request.call_args.kwargs["timeout"] == 5
    transient.close.assert_called_once()
    success.close.assert_not_called()


def test_http_rejects_nonfinite_policy():
    with pytest.raises(ValueError):
        ResilientHttpClient(name="test", user_agent="test", retry_policy=RetryPolicy(backoff_seconds=float("nan")))


def test_http_errors_do_not_log_provider_credentials(caplog):
    url = "https://example.test/api/secret-token/data?api_key=query-secret"
    session = Mock()
    session.request.side_effect = requests.ConnectionError(url)
    client = ResilientHttpClient(name="test", user_agent="test", session=session, sleeper=Mock())
    assert client.request("GET", url) is None
    assert "example.test" in caplog.text
    assert "secret-token" not in caplog.text
    assert "query-secret" not in caplog.text


def test_firms_bad_rows_do_not_hide_good_rows_or_distinct_observation_times(tmp_path):
    loader = MapDataLoader(tmp_path, settings=RuntimeSettings.from_environment({"FIRMS_API_KEY": "test"}))
    loader.borders = []
    loader.fetch_with_retry = Mock(return_value=Mock(text=(
        "latitude,longitude,bright_ti4,acq_date,acq_time,frp\n"
        "50,30,bad,2026-01-01,0100,5\n"
        "50,30,310,2026-01-01,0100,5\n"
        "50,30,310,2026-01-02,0100,5\n"
        "nan,30,310,2026-01-01,0100,5\n"
        "51,31,310,2026-01-01,0100,inf\n"
    )))
    assert loader.load_nasa_firms()
    payload = json.loads((tmp_path / "thermal_firms.geojson").read_text())
    assert len(payload["features"]) == 2
    loader.load_nasa_firms = Mock(return_value=False)
    loader.convert_kml_to_geojson = Mock()
    loader.create_dummy_geojson = Mock()
    assert loader.run_all() is False


@pytest.mark.parametrize("media", [["https://example.test/a'b.jpg"], '["https://example.test/image.jpg"]', None, "bad-json"])
def test_media_is_always_a_json_array(tmp_path, media):
    path = tmp_path / "raw.db"
    assert RawEventRepository(path).save_many([{"text": "event", "media_urls": media}]) == 1
    with closing(sqlite3.connect(path)) as connection:
        saved = json.loads(connection.execute("SELECT media_urls FROM raw_signals").fetchone()[0])
    assert isinstance(saved, list)
    if isinstance(media, list):
        assert saved == media


def test_generated_source_ids_are_stable_and_include_geometry():
    extractor = ParabellumGeoExtractor.__new__(ParabellumGeoExtractor)
    feature = {"properties": {"name": "test"}, "geometry": {"type": "Point", "coordinates": [30, 50]}}
    with patch("builtins.hash", side_effect=[1, 2]):
        first = extractor._parse_feature(feature, "test")
        second = extractor._parse_feature(feature, "test")
    assert first["event_id"] == second["event_id"]
    other = {**feature, "geometry": {"type": "Point", "coordinates": [31, 50]}}
    assert first["event_id"] != extractor._parse_feature(other, "test")["event_id"]


def test_connector_preserves_zero_and_rejects_invalid_latitude():
    client = WarSpottingClient.__new__(WarSpottingClient)
    client.name = "test"
    event = client._normalize_item({"id": 1, "geo": {"lat": 0, "lng": 0, "lon": 12}})
    assert (event["lat"], event["lon"]) == (0, 0)
    assert client._normalize_item({"id": 1, "geo": {"lat": "nan", "lon": 30}}) is None
    client.safe_request = Mock(return_value=Mock(json=Mock(return_value={"losses": None})))
    assert client.fetch_events() == []


def test_export_malformed_dates_coordinates_and_nonfinite_scores():
    fields = generate_output._build_dossier_fields({"daily_dates": ["not-a-date", None, "2026-01-01"],
                                                  "recent_events": [{"date": None}, {"date": "2026-01-01"}]})
    assert fields["engagement_trend_anchor"] == "2026-01-01"
    assert sum(fields["engagement_trend_30d"]) == 1
    assert generate_output._safe_float(float("nan")) == 0
    assert generate_output._safe_float("inf") == 0
    assert generate_output._safe_float(1e20) == 1e20
    report = {"tactics": {"geo_location": {"explicit": {"lat": 0, "lon": "0"}}}}
    assert generate_output._resolve_event_coordinates(report) == (0, 0)
    assert generate_output._resolve_event_coordinates({"tactics": []}) == (None, None)


def test_media_export_stays_within_its_directory(tmp_path):
    with patch.object(generate_output, "PATHS", ProjectPaths.discover(tmp_path)):
        assert generate_output.save_media_frame("../../outside", 1, "aW1hZ2U=") == ""
        assert generate_output.save_media_frame("event", "../../outside", "aW1hZ2U=") == ""
        assert generate_output.save_media_frame("event", 1, "data:image/svg+xml;base64,aW1hZ2U=") == ""
        assert generate_output.save_media_frame("event", 1, "invalid!") == ""
        relative = generate_output.save_media_frame("event", 1, "aW1hZ2U=")
        assert relative == "assets/data/media/event_1.jpg"
        assert (tmp_path / relative).read_bytes() == b"image"


@pytest.mark.parametrize("point, expected_count", [({}, 0), ({"lat": 0, "lon": 0}, 1)])
def test_full_export_never_fabricates_a_location(tmp_path, monkeypatch, point, expected_count):
    go = generate_output
    monkeypatch.setattr(go, "PATHS", ProjectPaths.discover(tmp_path))
    database = tmp_path / "test.db"
    database.touch()
    monkeypatch.setattr(go, "DB_PATH", database)
    for name in ("GEOJSON_PATH", "EVENTS_LATEST_PATH", "CSV_PATH", "SECTOR_ANOMALIES_PATH", "ASYMMETRY_INDEX_PATH", "GLOCS_PATH", "CAMPAIGNS_GEO_PATH", "CAMPAIGN_REPORTS_PATH"):
        monkeypatch.setattr(go, name, tmp_path / name)
    for name in ("configure_logging", "load_dotenv", "ensure_campaign_columns", "ensure_sources_reputation_schema", "apply_reputation_decay", "export_units", "export_equipment_losses", "build_campaigns_geo", "build_campaign_reports", "generate_strategic_trends"):
        monkeypatch.setattr(go, name, Mock())
    for name in ("load_orbat_data", "load_campaign_definitions"):
        monkeypatch.setattr(go, name, Mock(return_value=[]))
    for name in ("preload_casualties_index", "_fetch_tg_deeplinks"):
        monkeypatch.setattr(go, name, Mock(return_value={}))
    monkeypatch.setattr(go, "_build_visual_analysis", Mock(return_value=[{"frame_id": 1}]))
    connection = Mock()
    connection.cursor.return_value.fetchall.return_value = [{
        "event_id": "test-event", "last_seen_date": "2026-01-01", "title": "=1+1",
        "ai_report_json": json.dumps({"tactics": {"geo_location": {"explicit": point}}})
    }]
    monkeypatch.setattr(go.sqlite3, "connect", Mock(return_value=connection))
    assert go.main() == 0
    payload = json.loads(go.GEOJSON_PATH.read_text(encoding="utf-8"))
    assert len(payload["features"]) == expected_count
    if expected_count:
        assert payload["features"][0]["geometry"]["coordinates"] == [0, 0]
        import csv

        with go.CSV_PATH.open(encoding="utf-8", newline="") as handle:
            assert next(csv.DictReader(handle))["Title"] == "'=1+1"
    connection.close.assert_called_once()


def test_ingestion_preserves_enrichment_rolls_back_and_signals_failure(tmp_path):
    path = tmp_path / "impact.db"
    with master_ingestor.DatabaseManager(path) as db:
        db.upsert_unit("ua_93_mech", official_name="Enriched name", faction="UA", status="INACTIVE")

    def fail(db, resolver):
        db.upsert_event("rolled-back", source="test")
        raise RuntimeError("provider failure")

    def succeed(db, resolver):
        db.upsert_event("kept", source="test")

    factory = master_ingestor.DatabaseManager
    with patch.object(master_ingestor, "DatabaseManager", side_effect=lambda: factory(path)), \
            patch.object(master_ingestor, "configure_runtime_logging"), \
            patch.object(master_ingestor, "SOURCE_REGISTRY", {"fail": ("Fail", fail), "ok": ("OK", succeed)}), \
            pytest.raises(IngestionError):
        master_ingestor.run_pipeline()
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("SELECT event_id FROM kinetic_events").fetchall() == [("kept",)]
        assert connection.execute("SELECT official_name, faction, status FROM units_registry WHERE unit_id='ua_93_mech'").fetchone() == ("Enriched name", "UA", "INACTIVE")


@pytest.fixture
def admin_server(tmp_path):
    root = tmp_path / "public"
    root.mkdir()
    (root / "index.html").write_text("public", encoding="utf-8")
    (root / ".env").write_text("test-secret", encoding="utf-8")
    sibling = tmp_path / "public-private"
    sibling.mkdir()
    (sibling / "secret.txt").write_text("sibling-secret", encoding="utf-8")
    path = tmp_path / "test.db"
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("""CREATE TABLE unique_events (
            event_id TEXT PRIMARY KEY, title TEXT, description TEXT, last_seen_date TEXT,
            full_text_dossier TEXT, ai_report_json TEXT, tie_score REAL, ai_analysis_status TEXT,
            operational_sector TEXT, ai_summary TEXT, urls_list TEXT, sources_list TEXT,
            kinetic_score REAL, target_score REAL, effect_score REAL)""")
        for event_id in ("one", "two"):
            connection.execute("INSERT INTO unique_events (event_id, ai_report_json, ai_analysis_status) VALUES (?, '[]', 'PENDING')", (event_id,))
        connection.commit()
    with patch.object(admin_api, "STATIC_DIR", root), patch.object(admin_api, "DB_PATH", path):
        server = admin_api.AdminHTTPServer(("127.0.0.1", 0), admin_api.AdminAPIHandler,
                                          access=admin_api.AdminAccess(writer_token="test-writer-token-" + "a" * 32,
                                                                       reader_token="test-reader-token-" + "b" * 32))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield server.server_port
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)


def request(port, path, method="GET", body=None, headers=None):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        request_headers = {"Authorization": "Bearer test-writer-token-" + "a" * 32}
        request_headers.update(headers or {})
        connection.request(method, path, body, request_headers)
        response = connection.getresponse()
        return response.status, response.read(), dict(response.getheaders())
    finally:
        connection.close()


def test_admin_public_files_only_and_no_cross_origin_access(admin_server):
    assert request(admin_server, "/index.html")[0] == 200
    for path in ("/.env", "/impact_atlas.db", "/../public-private/secret.txt", "/%2e%2e/public-private/secret.txt"):
        assert request(admin_server, path)[0] == 403
    assert request(admin_server, "/api/events", headers={"Origin": "https://foreign.test"})[0] == 403
    assert request(admin_server, "/api/events", headers={"Host": "foreign.test"})[0] == 403
    assert "Access-Control-Allow-Origin" not in request(admin_server, "/index.html")[2]


@pytest.mark.parametrize("payload", [[], None, {"event_ids": ["one", {}]}, {"event_ids": ["one", "one"]}, {"event_ids": ["one", "two", "missing"]}])
def test_admin_rejects_invalid_merge_without_mutation(admin_server, payload):
    status, _, _ = request(admin_server, "/api/merge", "POST", json.dumps(payload), {"Content-Type": "application/json"})
    assert status == 400
    detail = json.loads(request(admin_server, "/api/event/two")[1])
    assert detail["status"] == "PENDING"


def test_admin_body_bounds_and_valid_merge(admin_server):
    assert request(admin_server, "/api/merge", "POST", b"", {"Content-Type": "application/json", "Content-Length": "-1"})[0] == 400
    assert request(admin_server, "/api/merge", "POST", b"", {"Content-Type": "application/json", "Content-Length": "9999999"})[0] == 400
    assert request(admin_server, "/api/merge", "POST", "{}", {"Content-Type": "text/plain"})[0] == 415
    status, data, _ = request(admin_server, "/api/merge", "POST", '{"event_ids":["one","two"]}', {"Content-Type": "application/json"})
    assert status == 200
    assert json.loads(data)["merged_count"] == 1


def test_admin_handles_array_reports_and_huge_page(admin_server):
    status, data, _ = request(admin_server, "/api/event/one")
    assert status == 200
    assert json.loads(data)["classification"] == ""
    assert request(admin_server, "/api/events?page=" + "9" * 100)[0] == 400


def test_admin_compares_dates_in_utc():
    parse = admin_api.AdminAPIHandler._parse_local_iso
    assert parse("2026-01-01T12:00:00+02:00") == parse("2026-01-01T10:00:00Z")


def test_api_authentication_and_read_only_authorization(admin_server):
    assert request(admin_server, "/index.html", headers={"Authorization": ""})[0] == 200
    status, _, headers = request(admin_server, "/api/events", headers={"Authorization": ""})
    assert status == 401
    assert headers["Cache-Control"] == "no-store"
    assert "Bearer" in headers["WWW-Authenticate"]
    reader = {"Authorization": "Bearer test-reader-token-" + "b" * 32}
    assert request(admin_server, "/api/events", headers=reader)[0] == 200
    assert request(admin_server, "/api/merge", "POST", "{}", reader)[0] == 403
    assert request(admin_server, "/api/unmerge", "POST", "{}", reader)[0] == 403
    assert request(admin_server, "/api/session", headers=reader)[0] == 200
    assert request(admin_server, "/api/merge-history", headers=reader)[0] == 200
    assert request(admin_server, "/api/events", headers={"Authorization": "Bearer " + "z" * 40})[0] == 401


def test_admin_merge_recovery_restores_original_values_and_is_idempotent(admin_server):
    headers = {"Content-Type": "application/json"}
    with closing(sqlite3.connect(admin_api.DB_PATH)) as connection:
        connection.execute("UPDATE unique_events SET full_text_dossier='Original A', urls_list='https://example.org/a', sources_list='A' WHERE event_id='one'")
        connection.execute("UPDATE unique_events SET full_text_dossier='Original B', urls_list='https://example.org/b', sources_list='B' WHERE event_id='two'")
        connection.commit()
        original = connection.execute("SELECT * FROM unique_events ORDER BY event_id").fetchall()
    status, body, _ = request(admin_server, "/api/merge", "POST", '{"event_ids":["one","two"]}', headers)
    assert status == 200
    merge_id = json.loads(body)["merge_id"]
    history = json.loads(request(admin_server, "/api/merge-history")[1])["changes"]
    assert history[0]["merge_id"] == merge_id
    assert set(history[0]) == {"merge_id", "record_count", "created_at", "reverted_at"}
    for already_reverted in (False, True):
        status, body, _ = request(admin_server, "/api/unmerge", "POST", json.dumps({"merge_id": merge_id}), headers)
        assert status == 200
        assert json.loads(body)["already_reverted"] is already_reverted
        with closing(sqlite3.connect(admin_api.DB_PATH)) as connection:
            assert connection.execute("SELECT * FROM unique_events ORDER BY event_id").fetchall() == original
    assert json.loads(request(admin_server, "/api/merge-history")[1])["changes"][0]["reverted_at"]


def test_admin_undo_refuses_later_edits_and_legacy_requests(admin_server):
    headers = {"Content-Type": "application/json"}
    result = json.loads(request(admin_server, "/api/merge", "POST", '{"event_ids":["one","two"]}', headers)[1])
    with closing(sqlite3.connect(admin_api.DB_PATH)) as connection:
        connection.execute("UPDATE unique_events SET title='Later edit' WHERE event_id='one'")
        connection.commit()
        current = connection.execute("SELECT * FROM unique_events ORDER BY event_id").fetchall()
    assert request(admin_server, "/api/unmerge", "POST", json.dumps({"merge_id": result["merge_id"]}), headers)[0] == 409
    assert request(admin_server, "/api/unmerge", "POST", '{"event_id":"two"}', headers)[0] == 400
    assert request(admin_server, "/api/unmerge", "POST", json.dumps({"merge_id": "0" * 32}), headers)[0] == 404
    with closing(sqlite3.connect(admin_api.DB_PATH)) as connection:
        assert connection.execute("SELECT * FROM unique_events ORDER BY event_id").fetchall() == current


def test_admin_journal_failure_rolls_back_the_merge(admin_server):
    with patch.object(admin_api.RowChangeJournal, "record", side_effect=sqlite3.OperationalError("Injected storage failure")):
        status, _, _ = request(admin_server, "/api/merge", "POST", '{"event_ids":["one","two"]}', {"Content-Type": "application/json"})
    assert status == 500
    assert json.loads(request(admin_server, "/api/event/two")[1])["status"] == "PENDING"
    assert json.loads(request(admin_server, "/api/event/one")[1])["full_text"] == ""
    assert json.loads(request(admin_server, "/api/merge-history")[1])["changes"] == []


def test_admin_database_setup_failure_closes_connection():
    connection = Mock()
    error = sqlite3.OperationalError("Injected initialization failure")
    connection.execute.side_effect = error
    with patch.object(admin_api.sqlite3, "connect", return_value=connection), pytest.raises(sqlite3.OperationalError) as caught:
        admin_api.get_db()
    assert caught.value is error
    connection.close.assert_called_once()
