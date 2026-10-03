"""Regression tests for general-purpose configuration, database and log safety."""

from __future__ import annotations

import json
import logging
import sqlite3
import sys
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from impact_atlas.config import RuntimeSettings
from impact_atlas.errors import ConfigurationError
from impact_atlas.logging import CredentialRedactor, JsonFormatter, configure_logging
from impact_atlas.sqlite import SQLiteDatabase
from impact_atlas.serialization import atomic_text_writer, csv_safe_value


@pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf"), float("-inf"), 2**32, True])
def test_sqlite_rejects_invalid_timeout_without_creating_files(tmp_path: Path, timeout: float) -> None:
    """Invalid lock limits fail before a database directory is created."""
    path = tmp_path / "missing" / "test.db"
    with pytest.raises(ValueError):
        SQLiteDatabase(path, timeout_seconds=timeout)
    assert not path.parent.exists()


def test_sqlite_closes_failed_connection_setup(tmp_path: Path) -> None:
    """A failed PRAGMA cannot leak a file handle or connection."""
    connection = Mock()
    connection.execute.side_effect = sqlite3.OperationalError("setup failed")
    with patch("impact_atlas.sqlite.sqlite3.connect", return_value=connection), \
            pytest.raises(sqlite3.OperationalError, match="setup failed"):
        SQLiteDatabase(tmp_path / "test.db").connect()
    connection.close.assert_called_once()


@pytest.mark.parametrize("failure", [RuntimeError("failed"), KeyboardInterrupt(), SystemExit(2)])
def test_transaction_rolls_back_schema_and_data_on_any_exit(tmp_path: Path, failure: BaseException) -> None:
    """Cancellation and ordinary failures must roll back DDL as well as writes."""
    database = SQLiteDatabase(tmp_path / "test.db")
    with pytest.raises(type(failure)), database.transaction() as connection:
        connection.execute("CREATE TABLE records (value TEXT)")
        connection.execute("INSERT INTO records VALUES ('uncommitted')")
        raise failure
    with database.connection() as connection:
        assert connection.execute("SELECT name FROM sqlite_master WHERE name='records'").fetchall() == []
    with database.transaction() as connection:
        connection.execute("CREATE TABLE records (value TEXT)")
        connection.execute("INSERT INTO records VALUES ('committed')")
    with database.connection() as connection:
        assert connection.execute("SELECT value FROM records").fetchone()[0] == "committed"


def test_cleanup_failure_preserves_original_error(tmp_path: Path) -> None:
    """Rollback/close errors must not replace the failure being diagnosed."""
    database = SQLiteDatabase(tmp_path / "test.db")
    connection = Mock()
    connection.rollback.side_effect = sqlite3.OperationalError("rollback failed")
    connection.close.side_effect = sqlite3.OperationalError("close failed")
    original = RuntimeError("original error")
    with patch.object(database, "connect", return_value=connection), \
            pytest.raises(RuntimeError) as raised, database.transaction():
        raise original
    assert raised.value is original
    assert len(original.__notes__) == 2
    connection.rollback.assert_called_once()
    connection.close.assert_called_once()


def test_commit_failure_rolls_back_and_closes(tmp_path: Path) -> None:
    """A failed commit follows the same cleanup contract as a body exception."""
    database = SQLiteDatabase(tmp_path / "test.db")
    connection = Mock()
    connection.commit.side_effect = sqlite3.OperationalError("disk full")
    with patch.object(database, "connect", return_value=connection), \
            pytest.raises(sqlite3.OperationalError, match="disk full"), database.transaction():
        pass
    connection.rollback.assert_called_once()
    connection.close.assert_called_once()


def test_configuration_repr_omits_secrets() -> None:
    """Debugging a settings object must not print its credentials."""
    settings = RuntimeSettings.from_environment({"FIRMS_API_KEY": "secret-one", "OPENROUTER_API_KEY": "secret-two"})
    assert "secret-one" not in repr(settings)
    assert "secret-two" not in repr(settings)
    assert settings.firms_api_key == "secret-one"
    with pytest.raises(ConfigurationError):
        settings.require("EXAMPLE_KEY", "   ")


@pytest.mark.parametrize("text, secret", [
    ('api_key="quoted-value"', "quoted-value"),
    ('password="two secret words"', "secret words"),
    ("password='two secret words'", "secret words"),
    ('{"password": "password-value"}', "password-value"),
    ("Authorization: Bearer token-value", "token-value"),
    ("Authorization: Basic dGVzdDp0ZXN0", "dGVzdDp0ZXN0"),
    ("https://user:user-password@example.test/path-secret?token=query-secret", "user-password"),
    ("https://example.test/path-secret?token=query-secret", "path-secret"),
    ("https://example.test/path-secret?token=query-secret", "query-secret"),
])
def test_redactor_removes_common_credentials(text: str, secret: str) -> None:
    """Messages and exception strings share one credential-redaction policy."""
    assert secret not in CredentialRedactor().redact(text)


def test_json_traceback_and_message_are_redacted_without_mutating_record() -> None:
    """Percent-formatted arguments and traceback values must both be protected."""
    redactor = CredentialRedactor(["literal-secret"])
    try:
        raise ValueError("Request failed with literal-secret https://example.test/path-secret")
    except ValueError:
        record = logging.LogRecord("test", logging.ERROR, __file__, 1, "Credential %s", ("literal-secret",), sys.exc_info())
    output = JsonFormatter(redactor).format(record)
    assert "literal-secret" not in output
    assert "path-secret" not in output
    assert "ValueError" in json.loads(output)["exception"]
    assert record.args == ("literal-secret",)
    assert record.exc_text is None


@pytest.fixture
def isolated_logger():
    """Release log file handles and isolate handler configuration between tests."""
    name = "test.infrastructure.security"
    logger = logging.getLogger(name)
    try:
        yield name
    finally:
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
            handler.close()


def test_logging_rotation_redaction_and_reconfiguration(tmp_path: Path, isolated_logger: str, capsys) -> None:
    """Rotation retains only configured backups and repeated setup updates verbosity."""
    path = tmp_path / "test.log"
    logger = configure_logging(isolated_logger, path, max_bytes=400, backup_count=2, secrets=["literal-secret"])
    initial_handlers = tuple(logger.handlers)
    again = configure_logging(isolated_logger, path, verbose=True, max_bytes=400, backup_count=2,
                              secrets=["literal-secret"])
    assert again is logger
    assert tuple(logger.handlers) == initial_handlers
    logger.debug("visible debug")
    for index in range(20):
        logger.info("entry %d literal-secret api_key=field-secret", index)
    for handler in logger.handlers:
        handler.flush()
    files = sorted(tmp_path.glob("test.log*"))
    assert [file.name for file in files] == ["test.log", "test.log.1", "test.log.2"]
    for file in files:
        text = file.read_text(encoding="utf-8")
        assert "literal-secret" not in text
        assert "field-secret" not in text
        for line in text.splitlines():
            json.loads(line)
    console = capsys.readouterr().out
    assert "visible debug" in console
    assert "literal-secret" not in console
    assert "field-secret" not in console
    assert "DEBUG" in console


def test_failed_log_reconfiguration_keeps_previous_handlers(tmp_path: Path, isolated_logger: str) -> None:
    """An invalid replacement destination cannot disable a working logger."""
    logger = configure_logging(isolated_logger, tmp_path / "working.log")
    original_handlers = tuple(logger.handlers)
    blocker = tmp_path / "file-not-directory"
    blocker.write_text("test", encoding="utf-8")
    with pytest.raises(OSError):
        configure_logging(isolated_logger, blocker / "new.log")
    assert tuple(logger.handlers) == original_handlers
    logger.info("still works")
    assert "still works" in (tmp_path / "working.log").read_text(encoding="utf-8")


@pytest.mark.parametrize("value", ["=1+1", "+SUM(A1)", "-1", "@SUM(A1)", " \t=1+1", "\ufeff=1+1", "\tplain"])
def test_csv_formula_strings_become_literal_text(value: str) -> None:
    """Formula-like source content cannot become an active spreadsheet formula."""
    assert csv_safe_value(value) == "'" + value


@pytest.mark.parametrize("value", [None, 0, -12.5, "ordinary, text", "2026-10-03", ""])
def test_csv_preserves_ordinary_text_and_numeric_values(value: object) -> None:
    """Safe existing cell values are preserved without blanket string coercion."""
    assert csv_safe_value(value) == value


@pytest.mark.parametrize("failure", [RuntimeError("failed"), KeyboardInterrupt()])
def test_atomic_write_preserves_previous_file_on_cancellation(tmp_path: Path, failure: BaseException) -> None:
    """A failed write cannot truncate the last successfully saved file."""
    path = tmp_path / "output.txt"
    path.write_text("previous", encoding="utf-8")
    with pytest.raises(type(failure)), atomic_text_writer(path) as handle:
        handle.write("partial")
        raise failure
    assert path.read_text(encoding="utf-8") == "previous"
    assert list(tmp_path.iterdir()) == [path]
    with atomic_text_writer(path) as handle:
        handle.write("complete")
    assert path.read_text(encoding="utf-8") == "complete"
    assert list(tmp_path.iterdir()) == [path]


def test_atomic_write_preserves_previous_file_if_replace_fails(tmp_path: Path) -> None:
    """Windows sharing violations preserve the old output and clean up staging."""
    path = tmp_path / "output.txt"
    path.write_text("previous", encoding="utf-8")
    with patch("impact_atlas.serialization.os.replace", side_effect=PermissionError("file in use")), \
            pytest.raises(PermissionError), atomic_text_writer(path) as handle:
        handle.write("replacement")
    assert path.read_text(encoding="utf-8") == "previous"
    assert list(tmp_path.iterdir()) == [path]


def test_atomic_write_preserves_previous_file_if_flush_fails(tmp_path: Path) -> None:
    """A disk failure before replacement leaves the previous output intact."""
    path = tmp_path / "output.txt"
    path.write_text("previous", encoding="utf-8")
    with patch("impact_atlas.serialization.os.fsync", side_effect=OSError("disk full")), \
            pytest.raises(OSError, match="disk full"), atomic_text_writer(path) as handle:
        handle.write("replacement")
    assert path.read_text(encoding="utf-8") == "previous"
    assert list(tmp_path.iterdir()) == [path]
