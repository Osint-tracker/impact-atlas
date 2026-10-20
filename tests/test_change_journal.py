"""Recovery tests on disposable, generic notes rather than live application data."""

import json
import sqlite3

import pytest

from impact_atlas.change_journal import ChangeNotFound, JournalConflict, RowChangeJournal


@pytest.fixture
def notes(tmp_path):
    connection = sqlite3.connect(tmp_path / "notes.sqlite", timeout=0)
    connection.execute("""CREATE TABLE notes (
        note_id TEXT PRIMARY KEY, content TEXT, source TEXT, status TEXT,
        payload BLOB, version INTEGER NOT NULL DEFAULT 0
    )""")
    connection.executemany("INSERT INTO notes VALUES (?, ?, ?, ?, ?, ?)", [
        ("a", "Original 'quote' <text>", None, "ready", b"\x00\xff", 0),
        ("b", None, "Source B", "reviewed", None, 0),
    ])
    connection.commit()
    journal = RowChangeJournal(connection, table="notes", key="note_id",
                               columns=("content", "source", "status", "payload"))
    try:
        yield connection, journal
    finally:
        connection.close()


def state(connection):
    return connection.execute("SELECT * FROM notes ORDER BY note_id").fetchall()


def change(connection, journal):
    with connection:
        connection.execute("BEGIN IMMEDIATE")
        before = journal.capture(["a", "b"])
        connection.execute("UPDATE notes SET content = content || ' combined', source = 'A, B', payload = NULL WHERE note_id='a'")
        connection.execute("UPDATE notes SET status = 'combined' WHERE note_id='b'")
        return journal.record(before)


def restore(connection, journal, change_id):
    with connection:
        connection.execute("BEGIN IMMEDIATE")
        return journal.restore(change_id)


def test_exact_round_trip_survives_reopening_and_keeps_history(notes, tmp_path):
    connection, journal = notes
    original = state(connection)
    assert journal.history() == []
    assert connection.execute("SELECT name FROM sqlite_master WHERE name='_atlas_row_changes'").fetchone() is None
    change_id = change(connection, journal)
    assert state(connection) != original
    assert journal.history()[0].change_id == change_id
    connection.close()
    with sqlite3.connect(tmp_path / "notes.sqlite") as reopened:
        recovered = RowChangeJournal(reopened, table="notes", key="note_id", columns=journal.columns)
        result = restore(reopened, recovered, change_id)
        assert result.record_count == 2
        assert not result.already_reverted
        assert state(reopened) == original
        assert recovered.history()[0].reverted_at is not None


def test_repeated_undo_is_idempotent_even_after_later_edits(notes):
    connection, journal = notes
    change_id = change(connection, journal)
    restore(connection, journal, change_id)
    connection.execute("UPDATE notes SET content='New independent edit' WHERE note_id='a'")
    connection.commit()
    current = state(connection)
    assert restore(connection, journal, change_id).already_reverted
    assert state(connection) == current


@pytest.mark.parametrize("edit", [
    "UPDATE notes SET content='Later edit' WHERE note_id='a'",
    "UPDATE notes SET version=1 WHERE note_id='b'",
    "DELETE FROM notes WHERE note_id='b'",
    "ALTER TABLE notes ADD COLUMN new_field TEXT",
])
def test_later_changes_and_schema_drift_refuse_undo_without_overwriting(notes, edit):
    connection, journal = notes
    change_id = change(connection, journal)
    connection.execute(edit)
    connection.commit()
    current = state(connection)
    with pytest.raises(JournalConflict):
        restore(connection, journal, change_id)
    assert state(connection) == current
    assert journal.history()[0].reverted_at is None


def test_overlapping_changes_must_be_undone_in_reverse_order(notes):
    connection, journal = notes
    original = state(connection)
    first = change(connection, journal)
    second = change(connection, journal)
    with pytest.raises(JournalConflict):
        restore(connection, journal, first)
    restore(connection, journal, second)
    restore(connection, journal, first)
    assert state(connection) == original


def test_failed_journal_write_rolls_back_record_changes(notes):
    connection, journal = notes
    initial_change = change(connection, journal)
    restore(connection, journal, initial_change)
    original = state(connection)
    connection.execute("""CREATE TRIGGER fail_history BEFORE INSERT ON _atlas_row_changes
        BEGIN SELECT RAISE(ABORT, 'Injected journal failure'); END""")
    with pytest.raises(sqlite3.IntegrityError, match="Injected journal failure"):
        change(connection, journal)
    assert state(connection) == original
    assert len(journal.history()) == 1


def test_failed_restoration_rolls_back_every_row_and_journal_status(notes):
    connection, journal = notes
    change_id = change(connection, journal)
    current = state(connection)
    connection.execute("""CREATE TRIGGER refuse_update BEFORE UPDATE ON notes
        WHEN NEW.note_id='b' AND NEW.status='reviewed'
        BEGIN SELECT RAISE(ABORT, 'Injected restoration failure'); END""")
    with pytest.raises(sqlite3.IntegrityError, match="Injected restoration failure"):
        restore(connection, journal, change_id)
    assert state(connection) == current
    assert journal.history()[0].reverted_at is None


def test_side_effecting_restore_trigger_is_detected(notes):
    connection, journal = notes
    change_id = change(connection, journal)
    current = state(connection)
    connection.execute("""CREATE TRIGGER unexpected_edit AFTER UPDATE ON notes
        BEGIN UPDATE notes SET version=version+1 WHERE note_id=NEW.note_id; END""")
    with pytest.raises(JournalConflict, match="triggers"):
        restore(connection, journal, change_id)
    assert state(connection) == current


def test_restore_trigger_cannot_change_an_unrelated_table(notes):
    connection, journal = notes
    change_id = change(connection, journal)
    current = state(connection)
    connection.execute("CREATE TABLE other_records (value TEXT)")
    connection.execute("""CREATE TRIGGER unexpected_external_edit AFTER UPDATE ON notes
        BEGIN INSERT INTO other_records VALUES ('Unrelated write'); END""")
    with pytest.raises(JournalConflict, match="recovery boundary"):
        restore(connection, journal, change_id)
    assert state(connection) == current
    assert connection.execute("SELECT * FROM other_records").fetchall() == []


@pytest.mark.parametrize("column,value", [
    ("version", 99), ("before_json", "not json"),
    ("before_json", json.dumps({"a": {"unexpected": "value"}})),
    ("after_hashes_json", "[]"), ("record_count", 0),
    ("columns_json", '["unexpected"]'),
])
def test_corrupt_history_fails_closed(notes, column, value):
    connection, journal = notes
    change_id = change(connection, journal)
    connection.execute(f"UPDATE _atlas_row_changes SET {column}=?", (value,))
    connection.commit()
    current = state(connection)
    with pytest.raises(JournalConflict):
        restore(connection, journal, change_id)
    assert state(connection) == current


def test_cancellation_rolls_back_data_and_new_journal_schema(notes):
    connection, journal = notes
    original = state(connection)
    with pytest.raises(KeyboardInterrupt), connection:
        connection.execute("BEGIN IMMEDIATE")
        before = journal.capture(["a", "b"])
        connection.execute("UPDATE notes SET status='combined'")
        journal.record(before)
        raise KeyboardInterrupt
    assert state(connection) == original
    assert journal.history() == []


def test_requires_owned_transaction_and_rejects_bad_resource_names(notes):
    connection, journal = notes
    with pytest.raises(RuntimeError):
        journal.capture(["a"])
    with pytest.raises(ValueError):
        RowChangeJournal(connection, table='notes; DROP TABLE notes', key='note_id', columns=('content',))
    for keys in (["missing"], ["a", "a"], []):
        with pytest.raises(JournalConflict), connection:
            connection.execute("BEGIN IMMEDIATE")
            journal.capture(keys)
    with pytest.raises(ChangeNotFound):
        restore(connection, journal, "0" * 32)


def test_undeclared_mutations_cannot_be_committed_with_recovery_claim(notes):
    connection, journal = notes
    original = state(connection)
    with pytest.raises(JournalConflict), connection:
        connection.execute("BEGIN IMMEDIATE")
        before = journal.capture(["a"])
        connection.execute("UPDATE notes SET version=version+1 WHERE note_id='a'")
        journal.record(before)
    assert state(connection) == original


def test_writer_lock_covers_capture_through_journal_commit(notes, tmp_path):
    connection, journal = notes
    with sqlite3.connect(tmp_path / "notes.sqlite", timeout=0) as other, connection:
        connection.execute("BEGIN IMMEDIATE")
        journal.capture(["a"])
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            other.execute("UPDATE notes SET content='Concurrent overwrite' WHERE note_id='a'")
