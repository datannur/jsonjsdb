"""Tests for jsonjsdb package - Phase 1: Reading."""

import json
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, TypedDict, Union, cast

import pytest
import polars as pl

import jsonjsdb
from jsonjsdb import Jsonjsdb, Table
from jsonjsdb.writer import file_hash

DB_PATH = Path(__file__).parent / "db"


# TypedDict definitions for typed DB access
class User(TypedDict):
    id: str
    name: str
    status: str
    tag_ids: list[str]


class Tag(TypedDict):
    id: str
    label: str


class Email(TypedDict):
    id: str
    user_id: str
    address: str


class Folder(TypedDict):
    id: str
    name: str
    parent_id: Optional[str]


class TypedDB(Jsonjsdb):
    user: Table[User]
    tag: Table[Tag]
    email: Table[Email]
    folder: Table[Folder]


# Dataclass for entity_type tests
@dataclass
class TagEntity:
    id: str
    label: str


def test_version_exists():
    """Package should expose a version string."""
    assert hasattr(jsonjsdb, "__version__")
    assert isinstance(jsonjsdb.__version__, str)
    assert len(jsonjsdb.__version__) > 0


# --- Loader tests ---


def test_load_database():
    """Should load database from path."""
    db = Jsonjsdb(DB_PATH)
    assert db.path == DB_PATH
    assert set(db.tables) == {"user", "tag", "email", "folder"}


def test_load_nonexistent_path():
    """Should raise FileNotFoundError for missing path."""
    with pytest.raises(FileNotFoundError):
        Jsonjsdb("/nonexistent/path")


def test_untyped_table_access():
    """Should access tables via db['name'] syntax."""
    db = Jsonjsdb(DB_PATH)
    user_table = db["user"]
    assert user_table.name == "user"


def test_untyped_missing_table():
    """Should raise KeyError for missing table."""
    db = Jsonjsdb(DB_PATH)
    with pytest.raises(KeyError):
        db["nonexistent"]


# --- Table.get() tests ---


def test_get_existing_row():
    """Should return row by ID."""
    db = Jsonjsdb(DB_PATH)
    user = db["user"].get("user_1")
    assert user is not None
    assert user["id"] == "user_1"
    assert user["name"] == "Alice"


def test_get_missing_row():
    """Should return None for missing ID."""
    db = Jsonjsdb(DB_PATH)
    user = db["user"].get("nonexistent")
    assert user is None


# --- Table.all() tests ---


def test_all_rows():
    """Should return all rows."""
    db = Jsonjsdb(DB_PATH)
    users = db["user"].all()
    assert len(users) == 3
    assert {u["id"] for u in users} == {"user_1", "user_2", "user_3"}


# --- Conversion: *_ids columns ---


def test_ids_column_conversion():
    """Should convert comma-separated *_ids to list[str]."""
    db = Jsonjsdb(DB_PATH)
    user1 = db["user"].get("user_1")
    assert user1 is not None
    assert user1["tag_ids"] == ["tag_1", "tag_2"]

    user2 = db["user"].get("user_2")
    assert user2 is not None
    assert user2["tag_ids"] == ["tag_1"]


def test_empty_ids_column():
    """Should convert empty *_ids to empty list."""
    db = Jsonjsdb(DB_PATH)
    user3 = db["user"].get("user_3")
    assert user3 is not None
    assert user3["tag_ids"] == []


# --- Table.where() tests ---


def test_where_equals():
    """Should filter by equality."""
    db = Jsonjsdb(DB_PATH)
    active = db["user"].where("status", "==", "active")
    assert len(active) == 2
    assert {u["name"] for u in active} == {"Alice", "Bob"}


def test_where_not_equals():
    """Should filter by inequality."""
    db = Jsonjsdb(DB_PATH)
    not_active = db["user"].where("status", "!=", "active")
    assert len(not_active) == 1
    assert not_active[0]["name"] == "Charlie"


def test_where_in_list():
    """Should filter by membership in list."""
    db = Jsonjsdb(DB_PATH)
    some = db["user"].where("id", "in", ["user_1", "user_3"])
    assert len(some) == 2
    assert {u["name"] for u in some} == {"Alice", "Charlie"}


def test_where_is_null():
    """Should filter by null values."""
    db = Jsonjsdb(DB_PATH)
    roots = db["folder"].where("parent_id", "is_null")
    assert len(roots) == 1
    assert roots[0]["name"] == "Documents"


def test_where_is_not_null():
    """Should filter by non-null values."""
    db = Jsonjsdb(DB_PATH)
    children = db["folder"].where("parent_id", "is_not_null")
    assert len(children) == 3


# --- Typed DB access ---


def test_typed_db_access():
    """Should access tables via typed attributes."""
    db = TypedDB(DB_PATH)
    user = db.user.get("user_1")
    assert user is not None
    assert user["name"] == "Alice"


def test_typed_db_all():
    """Should return typed list from all()."""
    db = TypedDB(DB_PATH)
    tags = db.tag.all()
    assert len(tags) == 3
    assert tags[0]["label"] == "Python"


# --- Empty DB in memory ---


def test_empty_db():
    """Should create empty in-memory database."""
    db = Jsonjsdb()
    assert db.path is None
    assert db.tables == []


# =============================================================================
# Phase 2: Write + CRUD tests
# =============================================================================


def test_add_row():
    """Should add a new row."""
    db = Jsonjsdb(DB_PATH)
    initial_count = len(db["user"].all())

    db["user"].add(
        {"id": "user_new", "name": "New User", "status": "active", "tag_ids": []}
    )

    assert len(db["user"].all()) == initial_count + 1
    new_user = db["user"].get("user_new")
    assert new_user is not None
    assert new_user["name"] == "New User"


def test_add_duplicate_id_raises():
    """Should raise ValueError when adding duplicate ID."""
    db = Jsonjsdb(DB_PATH)
    with pytest.raises(ValueError, match="already exists"):
        db["user"].add(
            {"id": "user_1", "name": "Duplicate", "status": "x", "tag_ids": []}
        )


def test_add_missing_id_raises():
    """Should raise ValueError when adding row without ID."""
    db = Jsonjsdb(DB_PATH)
    with pytest.raises(ValueError, match="must have an 'id'"):
        db["user"].add({"name": "No ID", "status": "x", "tag_ids": []})  # type: ignore[typeddict-item]


def test_add_all():
    """Should add multiple rows."""
    db = Jsonjsdb(DB_PATH)
    initial_count = len(db["tag"].all())

    db["tag"].add_all(
        [
            {"id": "tag_new1", "label": "New Tag 1"},
            {"id": "tag_new2", "label": "New Tag 2"},
        ]
    )

    assert len(db["tag"].all()) == initial_count + 2


def test_get_many():
    """Should reconstruct only the requested rows, in table order."""
    db = Jsonjsdb(DB_PATH)
    users = db["user"].get_many(["user_3", "user_1", "nonexistent"])
    assert [u["id"] for u in users] == ["user_1", "user_3"]


def test_get_many_empty_ids():
    """Should return empty list for no matches."""
    db = Jsonjsdb(DB_PATH)
    assert db["user"].get_many([]) == []
    assert db["user"].get_many(["nope"]) == []


def test_get_many_empty_table():
    """Should return empty list on an empty table."""
    table: Table[dict] = Table("empty")
    assert table.get_many(["x"]) == []


def test_upsert_all_inserts_and_replaces():
    """Should replace existing rows in place and append new ones."""
    db = Jsonjsdb(DB_PATH)
    db["tag"].upsert_all(
        [
            {"id": "tag_1", "label": "Relabelled"},
            {"id": "tag_new", "label": "Brand New"},
        ]
    )

    assert db["tag"].get("tag_1")["label"] == "Relabelled"  # type: ignore[index]
    assert db["tag"].get("tag_new")["label"] == "Brand New"  # type: ignore[index]


def test_upsert_all_preserves_row_order():
    """Replaced rows keep their original position; new rows go last."""
    db = Jsonjsdb(DB_PATH)
    original_ids = db["user"].df["id"].to_list()

    db["user"].upsert_all(
        [
            {"id": "user_2", "name": "Bob 2", "status": "active", "tag_ids": []},
            {"id": "user_z", "name": "Zed", "status": "active", "tag_ids": []},
        ]
    )

    assert db["user"].df["id"].to_list() == original_ids + ["user_z"]
    assert db["user"].get("user_2")["name"] == "Bob 2"  # type: ignore[index]


def test_upsert_all_introduces_new_column():
    """A column present only in the batch should be added to the table."""
    import polars as pl

    table: Table[dict] = Table("item")
    table._df = pl.DataFrame([{"id": "1", "name": "One"}])

    table.upsert_all(
        [{"id": "1", "name": "One", "score": 5}, {"id": "2", "name": "Two", "score": 9}]
    )

    assert "score" in table.df.columns
    assert table.get("1") == {"id": "1", "name": "One", "score": 5}
    assert table.get("2") == {"id": "2", "name": "Two", "score": 9}


def test_upsert_all_duplicate_in_batch_raises():
    """Should raise on duplicate id within the incoming batch."""
    db = Jsonjsdb(DB_PATH)
    with pytest.raises(ValueError, match="Duplicate IDs"):
        db["tag"].upsert_all(
            [{"id": "tag_1", "label": "A"}, {"id": "tag_1", "label": "B"}]
        )


def test_upsert_all_missing_id_raises():
    """Should raise when a row lacks an id."""
    db = Jsonjsdb(DB_PATH)
    with pytest.raises(ValueError, match="must have an 'id'"):
        db["tag"].upsert_all([{"label": "No ID"}])  # type: ignore[list-item]


def test_upsert_all_into_empty_table():
    """Should populate an empty table."""
    db = Jsonjsdb(DB_PATH)
    db["user"].remove_all(db["user"].df["id"].to_list())
    assert db["user"].is_empty

    db["user"].upsert_all(
        [{"id": "user_1", "name": "Alice", "status": "active", "tag_ids": []}]
    )
    assert db["user"].get("user_1")["name"] == "Alice"  # type: ignore[index]


def test_upsert_all_empty_is_noop():
    """Should do nothing for an empty batch."""
    db = Jsonjsdb(DB_PATH)
    before = db["user"].df["id"].to_list()
    db["user"].upsert_all([])
    assert db["user"].df["id"].to_list() == before


def test_update_row():
    """Should update an existing row."""
    db = Jsonjsdb(DB_PATH)
    db["user"].update("user_1", name="Alice Updated", status="inactive")

    user = db["user"].get("user_1")
    assert user is not None
    assert user["name"] == "Alice Updated"
    assert user["status"] == "inactive"


def test_update_ids_field():
    """Should update *_ids field correctly."""
    db = Jsonjsdb(DB_PATH)
    db["user"].update("user_1", tag_ids=["tag_3"])

    user = db["user"].get("user_1")
    assert user is not None
    assert user["tag_ids"] == ["tag_3"]


def test_update_nonexistent_raises():
    """Should raise KeyError when updating nonexistent row."""
    db = Jsonjsdb(DB_PATH)
    with pytest.raises(KeyError):
        db["user"].update("nonexistent", name="X")


def test_remove_row():
    """Should remove an existing row."""
    db = Jsonjsdb(DB_PATH)
    result = db["user"].remove("user_1")

    assert result is True
    assert db["user"].get("user_1") is None


def test_remove_nonexistent():
    """Should return False when removing nonexistent row."""
    db = Jsonjsdb(DB_PATH)
    result = db["user"].remove("nonexistent")
    assert result is False


def test_remove_all():
    """Should remove multiple rows."""
    db = Jsonjsdb(DB_PATH)
    removed = db["user"].remove_all(["user_1", "user_2", "nonexistent"])

    assert removed == 2
    assert db["user"].get("user_1") is None
    assert db["user"].get("user_2") is None


def test_save_to_new_path(tmp_path: Path):
    """Should save database to a new path."""
    db = TypedDB(DB_PATH)
    db.user.add(
        {"id": "user_new", "name": "New", "status": "active", "tag_ids": ["tag_1"]}
    )

    new_path = tmp_path / "new_db"
    db.save(new_path)

    assert new_path.exists()
    assert (new_path / "__table__.json").exists()
    assert (new_path / "__table__.json.js").exists()
    assert (new_path / "user.json").exists()
    assert (new_path / "user.json.js").exists()

    # Verify __table__.json.js content format
    js_content = (new_path / "__table__.json.js").read_text()
    assert js_content.startswith("jsonjs.data['__table__'] = ")
    assert '["name","last_modif"]' in js_content


def test_save_with_write_js_false(tmp_path: Path):
    """Should skip .json.js files when write_js=False."""
    db = TypedDB(DB_PATH)
    db.user.add(
        {"id": "user_new", "name": "New", "status": "active", "tag_ids": ["tag_1"]}
    )

    new_path = tmp_path / "no_js_db"
    db.save(new_path, write_js=False)

    assert new_path.exists()
    assert (new_path / "__table__.json").exists()
    assert not (new_path / "__table__.json.js").exists()
    assert (new_path / "user.json").exists()
    assert not (new_path / "user.json.js").exists()


def test_save_without_changes_preserves_files_and_last_modif(tmp_path: Path):
    """Should not rewrite unchanged exports on no-op save."""
    db = TypedDB(DB_PATH)
    db.save(tmp_path, timestamp=111)

    tracked_paths = [
        tmp_path / "user.json",
        tmp_path / "user.json.js",
        tmp_path / "__table__.json",
        tmp_path / "__table__.json.js",
    ]
    mtimes_before = {path: path.stat().st_mtime_ns for path in tracked_paths}
    table_index_before = json.loads((tmp_path / "__table__.json").read_text())

    db.save(tmp_path, timestamp=222)

    mtimes_after = {path: path.stat().st_mtime_ns for path in tracked_paths}
    table_index_after = json.loads((tmp_path / "__table__.json").read_text())

    assert mtimes_after == mtimes_before
    assert table_index_after == table_index_before


def test_save_without_changes_skips_evolution_comparison(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Should avoid expensive evolution comparisons for unchanged tables."""
    import jsonjsdb.database as database_module

    db = TypedDB(DB_PATH)
    db.save(tmp_path, timestamp=111)

    calls = 0

    def fail_on_compare(*args: object, **kwargs: object) -> list[object]:
        nonlocal calls
        calls += 1
        raise AssertionError("compare_datasets should not run for unchanged tables")

    monkeypatch.setattr(database_module, "compare_datasets", fail_on_compare)

    db.save(tmp_path, timestamp=222)

    assert calls == 0


def test_save_regenerates_missing_jsonjs_for_unchanged_table(tmp_path: Path):
    """Should rewrite derived JSON.js when canonical JSON is unchanged but JS is missing."""
    db = TypedDB(DB_PATH)
    db.save(tmp_path, timestamp=111)
    user_json_mtime = (tmp_path / "user.json").stat().st_mtime_ns

    (tmp_path / "user.json.js").unlink()

    db.save(tmp_path, timestamp=222)

    assert (tmp_path / "user.json").stat().st_mtime_ns == user_json_mtime
    assert (tmp_path / "user.json.js").exists()


def test_save_without_hash_state_uses_existing_json_hash(tmp_path: Path):
    """Should avoid data changes when upgrading a database without hash metadata."""
    db = TypedDB(DB_PATH)
    db.save(tmp_path, timestamp=111)
    (tmp_path / "_meta" / "json-hashes.json").unlink()
    user_json_mtime = (tmp_path / "user.json").stat().st_mtime_ns

    db.save(tmp_path, timestamp=222)

    table_index = {
        entry["name"]: entry["last_modif"]
        for entry in json.loads((tmp_path / "__table__.json").read_text())
    }
    assert (tmp_path / "user.json").stat().st_mtime_ns == user_json_mtime
    assert table_index["user"] == 111


def test_save_with_invalid_hash_state_uses_existing_json_hash(tmp_path: Path):
    """Should recover when hash metadata exists but cannot be decoded."""
    db = TypedDB(DB_PATH)
    db.save(tmp_path, timestamp=111)
    (tmp_path / "_meta" / "json-hashes.json").write_text("{", encoding="utf-8")
    user_json_mtime = (tmp_path / "user.json").stat().st_mtime_ns

    db.save(tmp_path, timestamp=222)

    table_index = {
        entry["name"]: entry["last_modif"]
        for entry in json.loads((tmp_path / "__table__.json").read_text())
    }
    assert (tmp_path / "user.json").stat().st_mtime_ns == user_json_mtime
    assert table_index["user"] == 111


def test_save_prunes_stale_table_hashes(tmp_path: Path):
    """Should remove hash entries for public tables no longer exported."""
    db = TypedDB(DB_PATH)
    db.save(tmp_path, timestamp=111)

    del db._tables["tag"]
    db.save(tmp_path, timestamp=222)

    hashes = json.loads((tmp_path / "_meta" / "json-hashes.json").read_text())
    assert "tag.json" not in hashes
    assert "user.json" in hashes
    assert "__table__.json" in hashes


def test_save_preserves_subdirectory_json_hashes(tmp_path: Path):
    """Should keep hash entries for managed JSON exports in subdirectories."""
    db = TypedDB(DB_PATH)
    db.save(tmp_path, timestamp=111)

    hash_path = tmp_path / "_meta" / "json-hashes.json"
    hashes = json.loads(hash_path.read_text())
    hashes["md-doc/example.json"] = "sha256:old"
    hashes["_custom/state.json"] = "sha256:internal"
    hashes["notes.txt"] = "sha256:notes"
    hashes["obsolete.json"] = "sha256:old"
    hash_path.write_text(json.dumps(hashes), encoding="utf-8")

    db.save(tmp_path, timestamp=222)

    updated_hashes = json.loads(hash_path.read_text())
    assert updated_hashes["md-doc/example.json"] == "sha256:old"
    assert updated_hashes["_custom/state.json"] == "sha256:internal"
    assert updated_hashes["notes.txt"] == "sha256:notes"
    assert "obsolete.json" not in updated_hashes
    assert "user.json" in updated_hashes
    assert "__table__.json" in updated_hashes


def test_save_updates_last_modif_only_for_changed_tables(tmp_path: Path):
    """Should keep previous last_modif values for unchanged table entries."""
    db = TypedDB(DB_PATH)
    db.save(tmp_path, timestamp=111)

    db.user.update("user_1", name="Alice Updated")
    db.save(tmp_path, timestamp=222)

    table_index = {
        entry["name"]: entry["last_modif"]
        for entry in json.loads((tmp_path / "__table__.json").read_text())
    }

    assert table_index["user"] == 222
    assert table_index["tag"] == 111
    assert table_index["email"] == 111
    assert table_index["folder"] == 111
    assert table_index["__table__"] == 222


def test_save_without_changes_keeps_evolution_files_untouched(tmp_path: Path):
    """Should not rewrite evolution files when no new evolution entry is produced."""
    db = TypedDB(DB_PATH)
    db.save(tmp_path, timestamp=111)
    db.user.update("user_1", name="Alice Updated")
    db.save(tmp_path, timestamp=222)

    evolution_paths = [tmp_path / "evolution.json", tmp_path / "evolution.json.js"]
    mtimes_before = {path: path.stat().st_mtime_ns for path in evolution_paths}

    db.save(tmp_path, timestamp=333)

    mtimes_after = {path: path.stat().st_mtime_ns for path in evolution_paths}

    assert mtimes_after == mtimes_before


def test_save_regenerates_evolution_outputs_after_manual_json_edit(tmp_path: Path):
    """Should propagate manual evolution.json edits to derived outputs."""
    db = TypedDB(DB_PATH)
    db.save(tmp_path, timestamp=111)
    db.user.update("user_1", name="Alice Updated", status="inactive")
    db.save(tmp_path, timestamp=222)

    evolution_path = tmp_path / "evolution.json"
    evolution_js_path = tmp_path / "evolution.json.js"
    edited_entries = json.loads(evolution_path.read_text(encoding="utf-8"))[:-1]
    evolution_path.write_text(
        json.dumps(edited_entries, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    db.save(tmp_path, timestamp=333)

    evolution_js = evolution_js_path.read_text(encoding="utf-8")
    table_index = {
        entry["name"]: entry["last_modif"]
        for entry in json.loads((tmp_path / "__table__.json").read_text())
    }
    hashes = json.loads((tmp_path / "_meta" / "json-hashes.json").read_text())

    assert evolution_js.count('"update"') == len(edited_entries)
    assert table_index["evolution"] == 333
    assert hashes["evolution.json"] == file_hash(evolution_path)


def test_save_regenerates_evolution_outputs_after_manual_json_clear(tmp_path: Path):
    """Should allow manual edits that remove all evolution entries."""
    db = TypedDB(DB_PATH)
    db.save(tmp_path, timestamp=111)
    db.user.update("user_1", name="Alice Updated")
    db.save(tmp_path, timestamp=222)

    evolution_path = tmp_path / "evolution.json"
    evolution_js_path = tmp_path / "evolution.json.js"
    evolution_path.write_text("[]\n", encoding="utf-8")

    db.save(tmp_path, timestamp=333)

    evolution_js = evolution_js_path.read_text(encoding="utf-8")
    table_index = {
        entry["name"]: entry["last_modif"]
        for entry in json.loads((tmp_path / "__table__.json").read_text())
    }
    hashes = json.loads((tmp_path / "_meta" / "json-hashes.json").read_text())

    assert json.loads(evolution_path.read_text(encoding="utf-8")) == []
    assert evolution_js.startswith("jsonjs.data['evolution'] = ")
    assert evolution_js.count('"update"') == 0
    assert table_index["evolution"] == 333
    assert hashes["evolution.json"] == file_hash(evolution_path)


def test_save_without_evolution_hash_state_keeps_last_modif(tmp_path: Path):
    """Should initialize evolution hash metadata without treating data as changed."""
    db = TypedDB(DB_PATH)
    db.save(tmp_path, timestamp=111)
    db.user.update("user_1", name="Alice Updated")
    db.save(tmp_path, timestamp=222)

    hash_path = tmp_path / "_meta" / "json-hashes.json"
    hashes = json.loads(hash_path.read_text())
    hashes.pop("evolution.json")
    hash_path.write_text(json.dumps(hashes, indent=2), encoding="utf-8")

    db.save(tmp_path, timestamp=333)

    table_index = {
        entry["name"]: entry["last_modif"]
        for entry in json.loads((tmp_path / "__table__.json").read_text())
    }
    updated_hashes = json.loads(hash_path.read_text())

    assert table_index["evolution"] == 222
    assert updated_hashes["evolution.json"] == file_hash(tmp_path / "evolution.json")


def test_save_regenerates_missing_evolution_jsonjs_without_bumping_last_modif(
    tmp_path: Path,
):
    """Should restore a missing derived evolution JSON.js file without data churn."""
    db = TypedDB(DB_PATH)
    db.save(tmp_path, timestamp=111)
    db.user.update("user_1", name="Alice Updated")
    db.save(tmp_path, timestamp=222)

    evolution_js_path = tmp_path / "evolution.json.js"
    evolution_js_path.unlink()

    db.save(tmp_path, timestamp=333)

    table_index = {
        entry["name"]: entry["last_modif"]
        for entry in json.loads((tmp_path / "__table__.json").read_text())
    }

    assert evolution_js_path.exists()
    assert table_index["evolution"] == 222


def test_save_recovers_from_invalid_existing_table_index(tmp_path: Path):
    """Should save even when an existing target __table__.json is invalid."""
    db = TypedDB()
    db.user.add({"id": "u1", "name": "User", "status": "active", "tag_ids": []})
    (tmp_path / "__table__.json").write_text("{", encoding="utf-8")

    db.save(tmp_path, timestamp=111, track_evolution=False)

    table_index = json.loads((tmp_path / "__table__.json").read_text())
    assert table_index == [
        {"name": "user", "last_modif": 111},
        {"name": "__table__", "last_modif": 111},
    ]


def test_save_and_reload(tmp_path: Path):
    """Should save and reload database correctly."""
    # Create and save
    db1 = TypedDB(DB_PATH)
    db1.user.add(
        {
            "id": "user_new",
            "name": "New",
            "status": "pending",
            "tag_ids": ["tag_2", "tag_3"],
        }
    )
    db1.save(tmp_path)

    # Reload
    db2 = TypedDB(tmp_path)
    users = db2.user.all()
    assert len(users) == 4

    new_user = db2.user.get("user_new")
    assert new_user is not None
    assert new_user["name"] == "New"
    assert new_user["tag_ids"] == ["tag_2", "tag_3"]


def test_save_without_path_raises():
    """Should raise ValueError when saving without path."""
    db = Jsonjsdb()
    with pytest.raises(ValueError, match="No path specified"):
        db.save()


def test_typed_empty_db_add_and_save(tmp_path: Path):
    """Should create empty typed DB, add data, and save."""
    db = TypedDB()
    db.user.add({"id": "u1", "name": "Test", "status": "active", "tag_ids": []})
    db.tag.add({"id": "t1", "label": "Test Tag"})

    db.save(tmp_path)

    # Reload and verify
    db2 = TypedDB(tmp_path)
    assert len(db2.user.all()) == 1
    assert len(db2.tag.all()) == 1


# =============================================================================
# Phase 3: Relations (having) tests
# =============================================================================


def test_having_one_to_many():
    """Should find rows by foreign key (email.user_id)."""
    db = Jsonjsdb(DB_PATH)
    emails = db["email"].having.user("user_1")

    assert len(emails) == 2
    assert {e["address"] for e in emails} == {
        "alice@example.com",
        "alice.pro@example.com",
    }


def test_having_one_to_many_no_results():
    """Should return empty list when no matching foreign key."""
    db = Jsonjsdb(DB_PATH)
    emails = db["email"].having.user("nonexistent")

    assert emails == []


def test_having_many_to_many():
    """Should find rows by list contains (user.tag_ids)."""
    db = Jsonjsdb(DB_PATH)
    users = db["user"].having.tag("tag_1")

    assert len(users) == 2
    assert {u["name"] for u in users} == {"Alice", "Bob"}


def test_having_many_to_many_single():
    """Should find single row by list contains."""
    db = Jsonjsdb(DB_PATH)
    users = db["user"].having.tag("tag_2")

    assert len(users) == 1
    assert users[0]["name"] == "Alice"


def test_having_self_reference_parent():
    """Should find children by parent_id (folder hierarchy)."""
    db = Jsonjsdb(DB_PATH)
    children = db["folder"].having.parent("folder_1")

    assert len(children) == 2
    assert {f["name"] for f in children} == {"Projects", "Archives"}


def test_having_self_reference_nested():
    """Should find nested children."""
    db = Jsonjsdb(DB_PATH)
    children = db["folder"].having.parent("folder_3")

    assert len(children) == 1
    assert children[0]["name"] == "2024"


def test_having_invalid_relation():
    """Should raise AttributeError for unknown relation."""
    db = Jsonjsdb(DB_PATH)
    with pytest.raises(AttributeError, match="No relation 'unknown'"):
        db["user"].having.unknown("x")


def test_having_typed_access():
    """Should work with typed DB access."""
    db = TypedDB(DB_PATH)
    emails = db.email.having.user("user_2")

    assert len(emails) == 1
    assert emails[0]["address"] == "bob@example.com"


# =============================================================================
# Additional coverage tests
# =============================================================================


def test_where_greater_than(tmp_path: Path):
    """Should filter with > operator."""
    # Create test data with numeric field

    data = [
        {"id": "1", "name": "A", "age": 20},
        {"id": "2", "name": "B", "age": 30},
        {"id": "3", "name": "C", "age": 40},
    ]
    table_index = [{"name": "person", "last_modif": 1234567890}]

    (tmp_path / "person.json").write_text(json.dumps(data))
    (tmp_path / "__table__.json").write_text(json.dumps(table_index))

    db = Jsonjsdb(tmp_path)
    result = db["person"].where("age", ">", 25)
    assert len(result) == 2
    assert {r["name"] for r in result} == {"B", "C"}


def test_where_greater_equal(tmp_path: Path):
    """Should filter with >= operator."""

    data = [
        {"id": "1", "age": 20},
        {"id": "2", "age": 30},
        {"id": "3", "age": 40},
    ]
    table_index = [{"name": "person", "last_modif": 1234567890}]

    (tmp_path / "person.json").write_text(json.dumps(data))
    (tmp_path / "__table__.json").write_text(json.dumps(table_index))

    db = Jsonjsdb(tmp_path)
    result = db["person"].where("age", ">=", 30)
    assert len(result) == 2


def test_where_less_than(tmp_path: Path):
    """Should filter with < operator."""

    data = [
        {"id": "1", "age": 20},
        {"id": "2", "age": 30},
        {"id": "3", "age": 40},
    ]
    table_index = [{"name": "person", "last_modif": 1234567890}]

    (tmp_path / "person.json").write_text(json.dumps(data))
    (tmp_path / "__table__.json").write_text(json.dumps(table_index))

    db = Jsonjsdb(tmp_path)
    result = db["person"].where("age", "<", 35)
    assert len(result) == 2


def test_where_less_equal(tmp_path: Path):
    """Should filter with <= operator."""

    data = [
        {"id": "1", "age": 20},
        {"id": "2", "age": 30},
        {"id": "3", "age": 40},
    ]
    table_index = [{"name": "person", "last_modif": 1234567890}]

    (tmp_path / "person.json").write_text(json.dumps(data))
    (tmp_path / "__table__.json").write_text(json.dumps(table_index))

    db = Jsonjsdb(tmp_path)
    result = db["person"].where("age", "<=", 30)
    assert len(result) == 2


def test_where_invalid_operator():
    """Should raise ValueError for unknown operator."""
    db = Jsonjsdb(DB_PATH)
    with pytest.raises(ValueError, match="Unknown operator"):
        db["user"].where("name", "~~", "test")  # type: ignore[arg-type]


def test_remove_on_empty_table():
    """Should return False when removing from empty table."""
    db = Jsonjsdb()

    class EmptyDB(Jsonjsdb):
        item: Table[dict]

    db = EmptyDB()
    assert db.item.remove("x") is False
    assert db.item.remove_all(["x", "y"]) == 0


def test_missing_table_json_error(tmp_path: Path):
    """Should raise FileNotFoundError when __table__.json is missing."""
    tmp_path.mkdir(exist_ok=True)
    with pytest.raises(FileNotFoundError, match="__table__.json"):
        Jsonjsdb(tmp_path)


def test_save_empty_table(tmp_path: Path):
    """Should handle saving empty tables."""

    class EmptyDB(Jsonjsdb):
        empty: Table[dict]

    db = EmptyDB()
    # Empty tables should not be written
    db.save(tmp_path)

    assert (tmp_path / "__table__.json").exists()
    # empty.json should NOT exist (table is empty)
    assert not (tmp_path / "empty.json").exists()


def test_load_empty_json_file(tmp_path: Path):
    """Should handle loading an empty JSON file (returns empty DataFrame)."""

    (tmp_path / "empty.json").write_text("[]")
    (tmp_path / "__table__.json").write_text(
        json.dumps([{"name": "empty", "last_modif": 0}])
    )

    db = Jsonjsdb(tmp_path)
    assert db["empty"].all() == []


def test_having_without_db_context():
    """Should raise RuntimeError when using having without db context."""
    table: Table[dict] = Table("standalone")
    with pytest.raises(RuntimeError, match="without a database context"):
        _ = table.having


def test_ids_column_non_string_type(tmp_path: Path):
    """Should handle _ids column that is already a list type."""

    # Polars will read this as List[Int64], not String
    data = [{"id": "1", "item_ids": [1, 2, 3]}]
    (tmp_path / "test.json").write_text(json.dumps(data))
    (tmp_path / "__table__.json").write_text(
        json.dumps([{"name": "test", "last_modif": 0}])
    )

    db = Jsonjsdb(tmp_path)
    row = db["test"].get("1")
    assert row is not None
    # The list type column is kept as-is
    assert row["item_ids"] == [1, 2, 3]


def test_empty_table_index(tmp_path: Path):
    """Should load database with empty __table__.json (no tables)."""
    (tmp_path / "__table__.json").write_text("[]")

    db = Jsonjsdb(tmp_path)
    assert db.tables == []


def test_table_missing_from_disk(tmp_path: Path):
    """Should skip tables listed in __table__.json but missing from disk."""

    table_index = [{"name": "missing", "last_modif": 0}]
    (tmp_path / "__table__.json").write_text(json.dumps(table_index))

    db = Jsonjsdb(tmp_path)
    # Table is listed but file doesn't exist - should be skipped
    assert db.tables == []


def test_typed_db_with_non_table_hints():
    """Should ignore non-Table type hints in typed DB subclass."""

    class MixedDB(Jsonjsdb):
        user: Table[User]
        name: str  # Non-Table hint, should be ignored
        count: int  # Non-Table hint, should be ignored

    db = MixedDB(DB_PATH)
    # Only Table hints should be processed
    assert "user" in db.tables
    assert db.user.name == "user"


# --- runtime_fields tests ---


def test_runtime_fields_excluded_on_save(tmp_path: Path):
    """Should exclude runtime_fields columns when saving."""
    import polars as pl

    class ItemTable(Table[dict]):
        runtime_fields = {"_seen", "_processed"}

    # Create table with runtime fields
    table = ItemTable("item")
    table._df = pl.DataFrame(
        [{"id": "1", "name": "Test", "_seen": True, "_processed": False}]
    )

    # Runtime fields exist in memory
    item = table.get("1")
    assert item is not None
    assert item["_seen"] is True
    assert item["_processed"] is False

    # Persistable df excludes runtime fields
    persistable = table.get_persistable_df()
    assert "_seen" not in persistable.columns
    assert "_processed" not in persistable.columns
    assert "name" in persistable.columns

    # Simulate save via writer
    from jsonjsdb.writer import write_table_json

    write_table_json(persistable, tmp_path / "item.json")

    # Verify saved file
    saved = json.loads((tmp_path / "item.json").read_text())
    assert saved[0]["name"] == "Test"
    assert "_seen" not in saved[0]
    assert "_processed" not in saved[0]


def test_upsert_all_preserves_runtime_fields():
    """Replacing a row keeps its runtime_fields when the batch omits them."""
    import polars as pl

    class ItemTable(Table[dict]):
        runtime_fields = {"_seen"}

    table = ItemTable("item")
    table._df = pl.DataFrame(
        [
            {"id": "1", "name": "One", "_seen": True},
            {"id": "2", "name": "Two", "_seen": True},
        ]
    )

    table.upsert_all([{"id": "1", "name": "One*"}, {"id": "3", "name": "Three"}])

    assert table.get("1") == {"id": "1", "name": "One*", "_seen": True}
    assert table.get("3") == {"id": "3", "name": "Three", "_seen": None}


def test_runtime_fields_empty_set():
    """Should keep all columns when runtime_fields is empty."""
    table: Table[dict] = Table("test")
    assert table.runtime_fields == set()

    import polars as pl

    table._df = pl.DataFrame([{"id": "1", "a": 1, "b": 2}])
    persistable = table.get_persistable_df()
    assert persistable.columns == ["id", "a", "b"]


def test_runtime_fields_via_init_parameter():
    """Should accept runtime_fields as __init__ parameter."""
    import polars as pl

    table: Table[dict] = Table("test", runtime_fields={"_temp", "_cache"})
    assert table.runtime_fields == {"_temp", "_cache"}

    table._df = pl.DataFrame([{"id": "1", "name": "A", "_temp": 1, "_cache": 2}])
    persistable = table.get_persistable_df()
    assert "_temp" not in persistable.columns
    assert "_cache" not in persistable.columns
    assert "name" in persistable.columns


def test_runtime_fields_get_persistable_df():
    """Should return filtered DataFrame via get_persistable_df()."""
    import polars as pl

    class MyTable(Table[dict]):
        runtime_fields = {"_temp"}

    table = MyTable("test")
    table._df = pl.DataFrame([{"id": "1", "name": "A", "_temp": 123}])

    # Full df still has runtime fields
    assert "_temp" in table.df.columns

    # Persistable df excludes them
    persistable = table.get_persistable_df()
    assert "_temp" not in persistable.columns
    assert "name" in persistable.columns


def test_runtime_fields_partial_match():
    """Should only exclude exact matches, not partial."""
    import polars as pl

    class ItemTable(Table[dict]):
        runtime_fields = {"_seen"}

    table = ItemTable("item")
    table._df = pl.DataFrame(
        [{"id": "1", "_seen": True, "_seen_count": 5, "seen": "yes"}]
    )

    persistable = table.get_persistable_df()
    assert "_seen" not in persistable.columns
    assert "_seen_count" in persistable.columns
    assert "seen" in persistable.columns


# =============================================================================
# Empty table tests (DataFrame without columns)
# =============================================================================


def test_empty_table_get_returns_none():
    """Should return None when calling get() on table without columns."""

    class EmptyDB(Jsonjsdb):
        user: Table[dict]

    db = EmptyDB()
    result = db.user.get("any_id")
    assert result is None


def test_empty_table_where_returns_empty_list():
    """Should return empty list when calling where() on table without columns."""

    class EmptyDB(Jsonjsdb):
        user: Table[dict]

    db = EmptyDB()
    result = db.user.where("name", "==", "test")
    assert result == []


def test_empty_table_having_returns_empty_list():
    """Should return empty list when calling having on table without columns."""

    class EmptyDB(Jsonjsdb):
        user: Table[dict]
        email: Table[dict]

    db = EmptyDB()
    result = db.email.having.user("user_1")
    assert result == []


def test_empty_table_all_returns_empty_list():
    """Should return empty list when calling all() on empty table."""

    class EmptyDB(Jsonjsdb):
        user: Table[dict]

    db = EmptyDB()
    result = db.user.all()
    assert result == []


def test_standalone_empty_table_get():
    """Should return None for get() on standalone empty Table."""
    table: Table[dict] = Table("test")
    assert table.get("any") is None


def test_standalone_empty_table_where():
    """Should return empty list for where() on standalone empty Table."""
    table: Table[dict] = Table("test")
    assert table.where("col", "==", "val") == []


# =============================================================================
# count property tests
# =============================================================================


def test_count_property():
    """Should return number of rows in table."""
    db = Jsonjsdb(DB_PATH)
    assert db["user"].count == 3
    assert db["tag"].count == 3


def test_count_empty_table():
    """Should return 0 for empty table."""
    table: Table[dict] = Table("test")
    assert table.count == 0


# =============================================================================
# update_many tests
# =============================================================================


def test_update_many():
    """Should update multiple rows at once."""
    db = Jsonjsdb(DB_PATH)
    updated = db["user"].update_many(["user_1", "user_2"], status="updated")

    assert updated == 2
    user1 = db["user"].get("user_1")
    user2 = db["user"].get("user_2")
    assert user1 is not None and user1["status"] == "updated"
    assert user2 is not None and user2["status"] == "updated"


def test_update_many_partial():
    """Should update only existing IDs and return actual count."""
    db = Jsonjsdb(DB_PATH)
    updated = db["user"].update_many(["user_1", "nonexistent"], status="changed")

    assert updated == 1
    user1 = db["user"].get("user_1")
    assert user1 is not None and user1["status"] == "changed"


def test_update_many_empty_table():
    """Should return 0 for empty table."""
    table: Table[dict] = Table("test")
    updated = table.update_many(["id1", "id2"], name="test")
    assert updated == 0


def test_update_many_no_matches():
    """Should return 0 when no IDs match."""
    db = Jsonjsdb(DB_PATH)
    updated = db["user"].update_many(["nonexistent1", "nonexistent2"], status="x")
    assert updated == 0


# =============================================================================
# ids_where tests
# =============================================================================


def test_ids_where():
    """Should return IDs matching condition."""
    db = Jsonjsdb(DB_PATH)
    ids = db["user"].ids_where("status", "==", "active")
    assert "user_1" in ids


def test_ids_where_empty_result():
    """Should return empty list when no matches."""
    db = Jsonjsdb(DB_PATH)
    ids = db["user"].ids_where("status", "==", "nonexistent")
    assert ids == []


def test_ids_where_empty_table():
    """Should return empty list for empty table."""
    table: Table[dict] = Table("test")
    ids = table.ids_where("col", "==", "val")
    assert ids == []


def test_ids_where_is_null():
    """Should work with is_null operator."""
    db = Jsonjsdb(DB_PATH)
    ids = db["folder"].ids_where("parent_id", "is_null")
    assert "folder_1" in ids


def test_ids_where_not_equals():
    """Should work with != operator."""
    db = Jsonjsdb(DB_PATH)
    ids = db["user"].ids_where("status", "!=", "active")
    assert "user_3" in ids


def test_ids_where_greater_than():
    """Should work with > operator."""
    db = Jsonjsdb(DB_PATH)
    ids = db["folder"].ids_where("id", ">", "folder_1")
    assert "folder_2" in ids


def test_ids_where_greater_equal():
    """Should work with >= operator."""
    db = Jsonjsdb(DB_PATH)
    ids = db["folder"].ids_where("id", ">=", "folder_2")
    assert "folder_2" in ids


def test_ids_where_less_than():
    """Should work with < operator."""
    db = Jsonjsdb(DB_PATH)
    ids = db["folder"].ids_where("id", "<", "folder_2")
    assert "folder_1" in ids


def test_ids_where_less_equal():
    """Should work with <= operator."""
    db = Jsonjsdb(DB_PATH)
    ids = db["folder"].ids_where("id", "<=", "folder_1")
    assert "folder_1" in ids


def test_ids_where_in():
    """Should work with in operator."""
    db = Jsonjsdb(DB_PATH)
    ids = db["user"].ids_where("id", "in", ["user_1", "user_2"])
    assert "user_1" in ids
    assert "user_2" in ids


def test_ids_where_is_not_null():
    """Should work with is_not_null operator."""
    db = Jsonjsdb(DB_PATH)
    ids = db["folder"].ids_where("parent_id", "is_not_null")
    assert "folder_2" in ids


def test_ids_where_invalid_operator():
    """Should raise ValueError for invalid operator."""
    db = Jsonjsdb(DB_PATH)
    with pytest.raises(ValueError, match="Unknown operator"):
        db["user"].ids_where("status", "invalid", "x")  # type: ignore[arg-type]


# =============================================================================
# remove_where tests
# =============================================================================


def test_remove_where():
    """Should remove rows matching condition."""
    db = Jsonjsdb(DB_PATH)
    removed = db["user"].remove_where("status", "==", "active")

    assert removed >= 1
    assert db["user"].ids_where("status", "==", "active") == []


def test_remove_where_no_matches():
    """Should return 0 when no matches."""
    db = Jsonjsdb(DB_PATH)
    removed = db["user"].remove_where("status", "==", "nonexistent")
    assert removed == 0


def test_remove_where_empty_table():
    """Should return 0 for empty table."""
    table: Table[dict] = Table("test")
    removed = table.remove_where("col", "==", "val")
    assert removed == 0


# =============================================================================
# entity_type with dataclass tests
# =============================================================================


def test_entity_type_get_returns_dataclass():
    """Should return dataclass instance when entity_type is set."""
    from jsonjsdb.loader import load_table

    df = load_table(DB_PATH / "tag.json")
    table: Table[TagEntity] = Table("tag", df=df, entity_type=TagEntity)

    tag = table.get("tag_1")
    assert tag is not None
    assert isinstance(tag, TagEntity)
    assert tag.id == "tag_1"
    assert tag.label == "Python"


def test_entity_type_add_dataclass():
    """Should accept dataclass instance in add()."""
    table: Table[TagEntity] = Table("tag", entity_type=TagEntity)

    tag = TagEntity(id="new_tag", label="New Label")
    table.add(tag)

    result = table.get("new_tag")
    assert result is not None
    assert result.id == "new_tag"
    assert result.label == "New Label"


# =============================================================================
# get_by tests
# =============================================================================


def test_get_by_found():
    """Should return single row matching column value."""
    db = Jsonjsdb(DB_PATH)
    user = db["user"].get_by("name", "Alice")
    assert user is not None
    assert user["id"] == "user_1"


def test_get_by_not_found():
    """Should return None when no match."""
    db = Jsonjsdb(DB_PATH)
    user = db["user"].get_by("name", "NonExistent")
    assert user is None


def test_get_by_empty_table():
    """Should return None for empty table."""
    table: Table[dict] = Table("test")
    result = table.get_by("col", "val")
    assert result is None


# =============================================================================
# add_all batch tests
# =============================================================================


def test_add_all_batch():
    """Should add multiple rows in single batch."""
    table: Table[dict] = Table("test")
    rows = [
        {"id": "1", "name": "A"},
        {"id": "2", "name": "B"},
        {"id": "3", "name": "C"},
    ]
    table.add_all(rows)

    assert table.count == 3
    assert table.get("1") is not None
    assert table.get("2") is not None
    assert table.get("3") is not None


def test_add_all_empty_list():
    """Should handle empty list."""
    table: Table[dict] = Table("test")
    table.add_all([])
    assert table.count == 0


def test_add_all_duplicate_in_input():
    """Should raise ValueError for duplicate IDs in input."""
    table: Table[dict] = Table("test")
    rows = [{"id": "1", "name": "A"}, {"id": "1", "name": "B"}]
    with pytest.raises(ValueError, match="Duplicate IDs"):
        table.add_all(rows)


def test_add_all_conflict_with_existing():
    """Should raise ValueError when ID already exists."""
    table: Table[dict] = Table("test")
    table.add({"id": "1", "name": "Existing"})

    with pytest.raises(ValueError, match="IDs already exist"):
        table.add_all([{"id": "1", "name": "Conflict"}, {"id": "2", "name": "New"}])


def test_add_all_missing_id():
    """Should raise ValueError when row missing id."""
    table: Table[dict] = Table("test")
    with pytest.raises(ValueError, match="must have an 'id' field"):
        table.add_all([{"name": "No ID"}])


# =============================================================================
# ids_having tests
# =============================================================================


def test_ids_having_one_to_many():
    """Should return IDs for one-to-many relation."""
    db = Jsonjsdb(DB_PATH)
    email_ids = db["email"].ids_having.user("user_1")
    assert isinstance(email_ids, list)
    assert len(email_ids) >= 1
    assert all(isinstance(id, str) for id in email_ids)


def test_ids_having_many_to_many():
    """Should return IDs for many-to-many relation."""
    db = Jsonjsdb(DB_PATH)
    user_ids = db["user"].ids_having.tag("tag_1")
    assert isinstance(user_ids, list)
    assert "user_1" in user_ids


def test_ids_having_empty_result():
    """Should return empty list when no matches."""
    db = Jsonjsdb(DB_PATH)
    ids = db["email"].ids_having.user("nonexistent")
    assert ids == []


def test_ids_having_empty_table():
    """Should return empty list for empty table."""

    class EmptyDB(Jsonjsdb):
        email: Table[dict]

    db = EmptyDB()
    ids = db.email.ids_having.user("user_1")
    assert ids == []


def test_ids_having_invalid_relation():
    """Should raise AttributeError for invalid relation."""
    db = Jsonjsdb(DB_PATH)
    with pytest.raises(AttributeError, match="No relation"):
        db["user"].ids_having.nonexistent("x")


def test_ids_having_without_db_context():
    """Should raise RuntimeError when no database context."""
    table: Table[dict] = Table("test")
    with pytest.raises(RuntimeError, match="without a database context"):
        _ = table.ids_having


# =============================================================================
# is_empty property tests
# =============================================================================


def test_is_empty_true():
    """Should return True for empty table."""
    table: Table[dict] = Table("test")
    assert table.is_empty is True


def test_is_empty_false():
    """Should return False for non-empty table."""
    db = Jsonjsdb(DB_PATH)
    assert db["user"].is_empty is False


# =============================================================================
# exists() tests
# =============================================================================


def test_exists_found():
    """Should return True when ID exists."""
    db = Jsonjsdb(DB_PATH)
    assert db["user"].exists("user_1") is True


def test_exists_not_found():
    """Should return False when ID not found."""
    db = Jsonjsdb(DB_PATH)
    assert db["user"].exists("nonexistent") is False


def test_exists_empty_table():
    """Should return False for empty table."""
    table: Table[dict] = Table("test")
    assert table.exists("any") is False


# =============================================================================
# upsert() tests
# =============================================================================


def test_upsert_add():
    """Should add new row and return True."""
    table: Table[dict] = Table("test")
    result = table.upsert({"id": "1", "name": "New"})

    assert result is True
    assert table.count == 1
    row = table.get("1")
    assert row is not None
    assert row["name"] == "New"


def test_upsert_update():
    """Should update existing row and return False."""
    table: Table[dict] = Table("test")
    table.add({"id": "1", "name": "Original"})

    result = table.upsert({"id": "1", "name": "Updated"})

    assert result is False
    assert table.count == 1
    row = table.get("1")
    assert row is not None
    assert row["name"] == "Updated"


def test_upsert_missing_id():
    """Should raise ValueError when row missing id."""
    table: Table[dict] = Table("test")
    with pytest.raises(ValueError, match="must have an 'id' field"):
        table.upsert({"name": "No ID"})


def test_internal_tables_not_loaded():
    """Should skip evolution and __table__ when loading database."""
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir)

        # First save - no evolution.json (initial creation not tracked)
        db1 = Jsonjsdb()
        db1._tables["user"] = Table("user", db1)
        db1["user"].add({"id": "1", "name": "Test"})
        db1.save(path)
        assert not (path / "evolution.json").exists()

        # Modify and save again - evolution.json now created
        db1["user"].update("1", name="Updated")
        db1.save(path)
        assert (path / "evolution.json").exists()

        # Reload - evolution and __table__ should NOT be in _tables
        db2 = Jsonjsdb(path)
        assert "evolution" not in db2._tables
        assert "__table__" not in db2._tables
        assert "user" in db2._tables
        assert db2.tables == ["user"]


def test_save_nan_as_null(tmp_path: Path):
    """Should convert NaN to null in JSON output."""
    db = Jsonjsdb()
    db._tables["data"] = Table("data", db)
    db["data"].add({"id": "1", "value": float("nan"), "score": 42.0})
    db["data"].add({"id": "2", "value": 3.14, "score": float("nan")})
    db.save(tmp_path)

    raw = (tmp_path / "data.json").read_text()
    assert "NaN" not in raw
    assert "nan" not in raw

    rows = json.loads(raw)
    assert rows[0]["value"] is None
    assert rows[0]["score"] == 42.0
    assert rows[1]["value"] == 3.14
    assert rows[1]["score"] is None

    js_raw = (tmp_path / "data.json.js").read_text()
    assert "NaN" not in js_raw


def _load_jsonjs_payload(path: Path, table_name: str) -> list[list[object]]:
    content = path.read_text()
    prefix = f"jsonjs.data['{table_name}'] = "
    assert content.startswith(prefix)
    return json.loads(content[len(prefix) :])


def test_typed_nullable_integer_fields_write_json_integers(tmp_path: Path):
    class Metric(TypedDict):
        id: str
        count: Optional[int]
        flag: Union[int, None]

    class MetricDB(Jsonjsdb):
        metric: Table[Metric]

    db = MetricDB()
    db.metric.add_all(
        [
            {"id": "row-1", "count": 392.0, "flag": 1.0},  # type: ignore[typeddict-item]
            {"id": "row-2", "count": None, "flag": None},
        ]
    )
    db.save(tmp_path, track_evolution=False)

    rows = json.loads((tmp_path / "metric.json").read_text())
    assert type(rows[0]["count"]) is int
    assert rows[0]["count"] == 392
    assert type(rows[0]["flag"]) is int
    assert rows[0]["flag"] == 1
    assert rows[1]["count"] is None
    assert rows[1]["flag"] is None

    js_rows = _load_jsonjs_payload(tmp_path / "metric.json.js", "metric")
    assert type(js_rows[1][1]) is int
    assert js_rows[1][1] == 392
    assert type(js_rows[1][2]) is int
    assert js_rows[1][2] == 1
    assert js_rows[2][1] is None
    assert js_rows[2][2] is None


def test_typed_nullable_integer_fields_preserve_order_independently(tmp_path: Path):
    class Metric(TypedDict):
        id: str
        count: Optional[int]

    class MetricDB(Jsonjsdb):
        metric: Table[Metric]

    for dirname, rows in {
        "int_first": [
            {"id": "row-1", "count": 392.0},  # type: ignore[typeddict-item]
            {"id": "row-2", "count": None},
        ],
        "null_first": [
            {"id": "row-2", "count": None},
            {"id": "row-1", "count": 392},
        ],
    }.items():
        db = MetricDB()
        for row in rows:
            db.metric.add(cast(Metric, row))
        path = tmp_path / dirname
        db.save(path, track_evolution=False)

        json_rows = json.loads((path / "metric.json").read_text())
        integer_row = next(row for row in json_rows if row["id"] == "row-1")
        assert type(integer_row["count"]) is int
        assert integer_row["count"] == 392

        js_rows = _load_jsonjs_payload(path / "metric.json.js", "metric")
        integer_js_row = next(row for row in js_rows[1:] if row[0] == "row-1")
        assert type(integer_js_row[1]) is int
        assert integer_js_row[1] == 392


def test_typed_nullable_integer_updates_write_json_integers(tmp_path: Path):
    class Metric(TypedDict):
        id: str
        count: Optional[int]
        flag: Optional[int]

    class MetricDB(Jsonjsdb):
        metric: Table[Metric]

    db = MetricDB()
    db.metric.add_all(
        [
            {"id": "row-1", "count": None, "flag": None},
            {"id": "row-2", "count": None, "flag": None},
        ]
    )
    db.metric.update("row-1", count=392.0)
    db.metric.update_many(["row-1", "row-2"], flag=1.0)
    db.save(tmp_path, track_evolution=False)

    rows = json.loads((tmp_path / "metric.json").read_text())
    assert type(rows[0]["count"]) is int
    assert rows[0]["count"] == 392
    assert type(rows[0]["flag"]) is int
    assert rows[0]["flag"] == 1
    assert type(rows[1]["flag"]) is int
    assert rows[1]["flag"] == 1

    js_rows = _load_jsonjs_payload(tmp_path / "metric.json.js", "metric")
    assert type(js_rows[1][1]) is int
    assert js_rows[1][1] == 392
    assert type(js_rows[1][2]) is int
    assert js_rows[1][2] == 1
    assert type(js_rows[2][2]) is int
    assert js_rows[2][2] == 1


def test_typed_integer_field_rejects_non_integral_float():
    class Metric(TypedDict):
        id: str
        count: Optional[int]

    class MetricDB(Jsonjsdb):
        metric: Table[Metric]

    db = MetricDB()
    with pytest.raises(ValueError, match="non-integer values"):
        db.metric.add({"id": "row-1", "count": 392.5})  # type: ignore[typeddict-item]


def test_entity_type_schema_supports_dataclass_scalar_and_list_fields():
    @dataclass
    class MetricEntity:
        id: str
        count: int
        active: bool
        tags: list[str]

    table: Table[MetricEntity] = Table("metric", entity_type=MetricEntity)
    table.add(MetricEntity(id="row-1", count=392, active=True, tags=["a", "b"]))

    assert table.df.schema["id"] == pl.Utf8
    assert table.df.schema["count"] == pl.Int64
    assert table.df.schema["active"] == pl.Boolean
    assert table.df.schema["tags"] == pl.List(pl.Utf8)


def test_storage_schema_parses_supported_annotation_forms(monkeypatch):
    import sys
    from typing import ForwardRef

    import jsonjsdb.table as table_module

    assert table_module._storage_schema_from_entity_type(None) == {}
    assert table_module._storage_schema_from_entity_type(dict) == {}
    assert (
        table_module._annotation_to_polars_dtype(ForwardRef("Optional[int]"))
        == pl.Int64
    )
    assert (
        table_module._string_annotation_to_polars_dtype("Union[int, None]") == pl.Int64
    )
    assert table_module._string_annotation_to_polars_dtype("int | None") == pl.Int64
    assert table_module._string_annotation_to_polars_dtype("List[str]") == pl.List(
        pl.Utf8
    )
    assert table_module._string_annotation_to_polars_dtype("list[bool]") == pl.List(
        pl.Boolean
    )
    assert table_module._string_annotation_to_polars_dtype("float") == pl.Float64
    assert table_module._string_annotation_to_polars_dtype("UnknownType") is None
    assert table_module._annotation_to_polars_dtype(object) is None
    assert table_module._unwrap_optional(str) is str
    assert table_module._dtype_is_integer(object()) is False
    assert table_module._dtype_is_float(object()) is False

    if sys.version_info >= (3, 10):
        assert table_module._annotation_to_polars_dtype(eval("int | None")) == pl.Int64

    class MetricWithStringAnnotations:
        count: "Optional[int]"

    def fail_type_hints(_entity_type: type[object]) -> object:
        raise TypeError

    monkeypatch.setattr(table_module, "get_type_hints", fail_type_hints)
    schema = table_module._storage_schema_from_entity_type(MetricWithStringAnnotations)
    assert schema == {"count": pl.Int64}


def test_typed_nullable_float_field_keeps_json_floats(tmp_path: Path):
    class Metric(TypedDict):
        id: str
        ratio: Optional[float]

    class MetricDB(Jsonjsdb):
        metric: Table[Metric]

    db = MetricDB()
    db.metric.add_all(
        [
            {"id": "row-1", "ratio": 1.0},
            {"id": "row-2", "ratio": None},
        ]
    )
    db.save(tmp_path, track_evolution=False)

    rows = json.loads((tmp_path / "metric.json").read_text())
    assert type(rows[0]["ratio"]) is float
    assert rows[0]["ratio"] == 1.0
    assert rows[1]["ratio"] is None

    js_rows = _load_jsonjs_payload(tmp_path / "metric.json.js", "metric")
    assert type(js_rows[1][1]) is float
    assert js_rows[1][1] == 1.0
    assert js_rows[2][1] is None


def test_schema_less_dataframe_writer_preserves_integer_and_float_dtypes(
    tmp_path: Path,
):
    from jsonjsdb.writer import write_table_json, write_table_jsonjs

    df = pl.DataFrame(
        {
            "id": ["row-1", "row-2", "row-3"],
            "count": pl.Series([1, None, 2], dtype=pl.Int64),
            "ratio": pl.Series([1.0, None, 2.0], dtype=pl.Float64),
        }
    )

    write_table_json(df, tmp_path / "data.json")
    write_table_jsonjs(df, "data", tmp_path / "data.json.js")

    rows = json.loads((tmp_path / "data.json").read_text())
    assert type(rows[0]["count"]) is int
    assert rows[0]["count"] == 1
    assert type(rows[0]["ratio"]) is float
    assert rows[0]["ratio"] == 1.0
    assert rows[1]["count"] is None
    assert rows[1]["ratio"] is None

    js_rows = _load_jsonjs_payload(tmp_path / "data.json.js", "data")
    assert type(js_rows[1][1]) is int
    assert js_rows[1][1] == 1
    assert type(js_rows[1][2]) is float
    assert js_rows[1][2] == 1.0
    assert js_rows[2][1] is None
    assert js_rows[2][2] is None


def test_dataframe_writers_do_not_touch_identical_files(tmp_path: Path):
    """Should avoid rewriting identical JSON and JSON.js content."""
    from jsonjsdb.writer import write_table_json, write_table_jsonjs

    df = pl.DataFrame({"id": ["row-1"], "name": ["Alpha"]})
    json_path = tmp_path / "data.json"
    jsonjs_path = tmp_path / "data.json.js"

    write_table_json(df, json_path)
    write_table_jsonjs(df, "data", jsonjs_path)
    mtimes_before = {
        json_path: json_path.stat().st_mtime_ns,
        jsonjs_path: jsonjs_path.stat().st_mtime_ns,
    }

    write_table_json(df, json_path)
    write_table_jsonjs(df, "data", jsonjs_path)

    mtimes_after = {
        json_path: json_path.stat().st_mtime_ns,
        jsonjs_path: jsonjs_path.stat().st_mtime_ns,
    }

    assert mtimes_after == mtimes_before


def test_json_writer_preserves_exact_output_path(tmp_path: Path):
    """Should write exactly the requested JSON path when called directly."""
    from jsonjsdb.writer import write_table_json

    df = pl.DataFrame({"id": ["row-1"], "name": ["Alpha"]})
    json_path = tmp_path / "guide.data.json"

    write_table_json(df, json_path)

    assert json_path.exists()
    assert not (tmp_path / "guide.data.data.json").exists()


def test_dataframe_writers_update_hashes_for_nested_exports(tmp_path: Path):
    """Should track unchanged direct writer exports under the DB root."""
    from jsonjsdb.writer import write_table_json, write_table_jsonjs

    (tmp_path / "__table__.json").write_text("[]", encoding="utf-8")
    df = pl.DataFrame({"id": ["doc-1"], "title": ["Guide"]})
    json_path = tmp_path / "md-doc" / "guide.json"
    jsonjs_path = tmp_path / "md-doc" / "guide.json.js"

    write_table_json(df, json_path)
    write_table_jsonjs(df, "md-doc/guide", jsonjs_path)

    hashes = json.loads((tmp_path / "_meta" / "json-hashes.json").read_text())
    assert hashes["md-doc/guide.json"].startswith("sha256:")

    mtimes_before = {
        json_path: json_path.stat().st_mtime_ns,
        jsonjs_path: jsonjs_path.stat().st_mtime_ns,
    }

    write_table_json(df, json_path)
    write_table_jsonjs(df, "md-doc/guide", jsonjs_path)

    mtimes_after = {
        json_path: json_path.stat().st_mtime_ns,
        jsonjs_path: jsonjs_path.stat().st_mtime_ns,
    }
    assert mtimes_after == mtimes_before


def test_pair_writer_reports_changes_for_nested_exports(tmp_path: Path):
    """Should write paired JSON exports and report logical data changes."""
    from jsonjsdb.writer import write_table_json_pair

    (tmp_path / "__table__.json").write_text("[]", encoding="utf-8")
    df = pl.DataFrame({"id": ["doc-1"], "title": ["Guide"]})

    first_result = write_table_json_pair(df, "md-doc/guide", tmp_path)
    second_result = write_table_json_pair(df, "md-doc/guide", tmp_path)

    assert first_result.data_changed is True
    assert first_result.json_written is True
    assert first_result.jsonjs_written is True
    assert second_result.data_changed is False
    assert second_result.json_written is False
    assert second_result.jsonjs_written is False
    assert (tmp_path / "md-doc" / "guide.json").exists()
    assert (tmp_path / "md-doc" / "guide.json.js").exists()


def test_pair_writer_works_without_export_root(tmp_path: Path):
    """Should still write paired exports when no DB root metadata is present."""
    from jsonjsdb.writer import table_jsonjs_content, write_table_json_pair

    df = pl.DataFrame({"id": ["doc-1"], "title": ["Guide"]})
    result = write_table_json_pair(df, "guide", tmp_path)

    assert result.data_changed is True
    assert table_jsonjs_content(df, "guide").startswith("jsonjs.data['guide'] = ")
    assert not (tmp_path / "_meta" / "json-hashes.json").exists()


def test_pair_writer_hash_session_batches_metadata_updates(tmp_path: Path):
    """Should update hash metadata once for multiple paired exports."""
    from jsonjsdb.writer import export_hash_session, write_table_json_pair

    (tmp_path / "__table__.json").write_text("[]", encoding="utf-8")
    guide_df = pl.DataFrame({"id": ["doc-1"], "title": ["Guide"]})
    intro_df = pl.DataFrame({"id": ["doc-2"], "title": ["Intro"]})

    with export_hash_session(tmp_path) as hashes:
        write_table_json_pair(
            guide_df,
            "md-doc/guide",
            tmp_path,
            export_root=tmp_path,
            hash_session=hashes,
        )
        assert not (tmp_path / "_meta" / "json-hashes.json").exists()
        write_table_json_pair(
            intro_df,
            "md-doc/intro",
            tmp_path,
            export_root=tmp_path,
            hash_session=hashes,
        )

    saved_hashes = json.loads((tmp_path / "_meta" / "json-hashes.json").read_text())
    assert set(saved_hashes) == {"md-doc/guide.json", "md-doc/intro.json"}
    assert saved_hashes["md-doc/guide.json"] == file_hash(
        tmp_path / "md-doc" / "guide.json"
    )
    assert saved_hashes["md-doc/intro.json"] == file_hash(
        tmp_path / "md-doc" / "intro.json"
    )


def test_pair_writer_uses_empty_hash_session_without_loading_manifest(
    tmp_path: Path,
):
    """Should treat an empty hash session as the active hash map."""
    from jsonjsdb.writer import write_table_json_pair

    (tmp_path / "__table__.json").write_text("[]", encoding="utf-8")
    hash_path = tmp_path / "_meta" / "json-hashes.json"
    hash_path.parent.mkdir(parents=True)
    hash_path.write_text(
        json.dumps({"md-doc/guide.json": "sha256:stale"}),
        encoding="utf-8",
    )
    hashes: dict[str, str] = {}
    df = pl.DataFrame({"id": ["doc-1"], "title": ["Guide"]})

    result = write_table_json_pair(
        df,
        "md-doc/guide",
        tmp_path,
        export_root=tmp_path,
        hash_session=hashes,
    )

    assert result.data_changed is True
    assert hashes["md-doc/guide.json"] == file_hash(tmp_path / "md-doc" / "guide.json")


def test_pair_writer_hash_session_preserves_unchanged_exports(tmp_path: Path):
    """Should keep write-if-changed behavior when using a hash session."""
    from jsonjsdb.writer import export_hash_session, write_table_json_pair

    (tmp_path / "__table__.json").write_text("[]", encoding="utf-8")
    df = pl.DataFrame({"id": ["doc-1"], "title": ["Guide"]})

    with export_hash_session(tmp_path) as hashes:
        write_table_json_pair(
            df,
            "md-doc/guide",
            tmp_path,
            export_root=tmp_path,
            hash_session=hashes,
        )

    json_path = tmp_path / "md-doc" / "guide.json"
    jsonjs_path = tmp_path / "md-doc" / "guide.json.js"
    mtimes_before = {
        json_path: json_path.stat().st_mtime_ns,
        jsonjs_path: jsonjs_path.stat().st_mtime_ns,
    }

    with export_hash_session(tmp_path) as hashes:
        result = write_table_json_pair(
            df,
            "md-doc/guide",
            tmp_path,
            export_root=tmp_path,
            hash_session=hashes,
        )

    mtimes_after = {
        json_path: json_path.stat().st_mtime_ns,
        jsonjs_path: jsonjs_path.stat().st_mtime_ns,
    }
    assert result.data_changed is False
    assert result.json_written is False
    assert result.jsonjs_written is False
    assert mtimes_after == mtimes_before


def test_pair_writer_hash_session_recreates_missing_jsonjs(tmp_path: Path):
    """Should recreate missing derived JSON.js files when hash matches."""
    from jsonjsdb.writer import export_hash_session, write_table_json_pair

    (tmp_path / "__table__.json").write_text("[]", encoding="utf-8")
    df = pl.DataFrame({"id": ["doc-1"], "title": ["Guide"]})

    with export_hash_session(tmp_path) as hashes:
        write_table_json_pair(
            df,
            "md-doc/guide",
            tmp_path,
            export_root=tmp_path,
            hash_session=hashes,
        )

    jsonjs_path = tmp_path / "md-doc" / "guide.json.js"
    jsonjs_path.unlink()

    with export_hash_session(tmp_path) as hashes:
        result = write_table_json_pair(
            df,
            "md-doc/guide",
            tmp_path,
            export_root=tmp_path,
            hash_session=hashes,
        )

    assert result.data_changed is False
    assert result.json_written is False
    assert result.jsonjs_written is True
    assert jsonjs_path.exists()


def test_jsonjs_writer_does_not_poison_hash_before_json(tmp_path: Path):
    """Should not mark canonical JSON unchanged before the JSON file is updated."""
    from jsonjsdb.writer import write_table_json, write_table_jsonjs

    (tmp_path / "__table__.json").write_text("[]", encoding="utf-8")
    json_path = tmp_path / "md-doc" / "guide.json"
    jsonjs_path = tmp_path / "md-doc" / "guide.json.js"

    old_df = pl.DataFrame({"id": ["doc-1"], "title": ["Old"]})
    new_df = pl.DataFrame({"id": ["doc-1"], "title": ["New"]})
    write_table_json(old_df, json_path)

    write_table_jsonjs(new_df, "md-doc/guide", jsonjs_path)
    write_table_json(new_df, json_path)

    rows = json.loads(json_path.read_text())
    assert rows == [{"id": "doc-1", "title": "New"}]


def test_writer_hash_and_table_index_helpers(tmp_path: Path):
    """Should expose hash and table index helpers with preserved last_modif values."""
    from jsonjsdb.writer import table_json_hash, write_table_index

    df = pl.DataFrame({"id": ["row-1"], "name": ["Alpha"]})
    assert table_json_hash(df).startswith("sha256:")

    write_table_index(
        ["data"],
        tmp_path / "__table__.json",
        timestamp=222,
        last_modifs={"data": 111, "__table__": 222},
    )

    table_index = json.loads((tmp_path / "__table__.json").read_text())
    assert table_index == [
        {"name": "data", "last_modif": 111},
        {"name": "__table__", "last_modif": 222},
    ]
    assert (tmp_path / "__table__.json.js").exists()


def test_load_nullable_column_with_late_value(tmp_path: Path):
    """Should load table where a column is null in early rows and numeric later."""
    table_index = [{"name": "variable", "last_modif": 0}]
    (tmp_path / "__table__.json").write_text(json.dumps(table_index))

    rows = [{"id": f"v{i}", "key": None} for i in range(120)]
    rows[110]["key"] = 1
    (tmp_path / "variable.json").write_text(json.dumps(rows))

    db = Jsonjsdb(tmp_path)
    all_rows = db["variable"].all()
    assert len(all_rows) == 120
    assert all_rows[110]["key"] == 1
    assert all_rows[0]["key"] is None


# --- Numeric list serialization (bbox) ---


def test_numeric_list_written_as_json_array(tmp_path: Path):
    """Should write List(Float64) columns (e.g. bbox) as JSON arrays, not CSV."""
    from jsonjsdb.writer import write_table_json, write_table_jsonjs

    df = pl.DataFrame(
        {
            "id": ["dataset-1"],
            "bbox": pl.Series([[6.02, 45.8, 10.5, 47.8]], dtype=pl.List(pl.Float64)),
        }
    )

    write_table_json(df, tmp_path / "data.json")
    write_table_jsonjs(df, "data", tmp_path / "data.json.js")

    rows = json.loads((tmp_path / "data.json").read_text())
    assert rows[0]["bbox"] == [6.02, 45.8, 10.5, 47.8]
    assert isinstance(rows[0]["bbox"], list)

    js_rows = _load_jsonjs_payload(tmp_path / "data.json.js", "data")
    assert js_rows[1][1] == [6.02, 45.8, 10.5, 47.8]


def test_integer_list_written_as_json_array(tmp_path: Path):
    """Should write List(Int64) columns as JSON arrays, not CSV."""
    from jsonjsdb.writer import write_table_json

    df = pl.DataFrame(
        {
            "id": ["row-1"],
            "years": pl.Series([[2020, 2021, 2022]], dtype=pl.List(pl.Int64)),
        }
    )

    write_table_json(df, tmp_path / "data.json")

    rows = json.loads((tmp_path / "data.json").read_text())
    assert rows[0]["years"] == [2020, 2021, 2022]
    assert all(isinstance(v, int) for v in rows[0]["years"])


def test_string_list_still_written_as_csv(tmp_path: Path):
    """Should keep List(Utf8) columns (e.g. *_ids) as comma-separated strings."""
    from jsonjsdb.writer import write_table_json

    df = pl.DataFrame(
        {
            "id": ["row-1"],
            "tag_ids": pl.Series([["a", "b", "c"]], dtype=pl.List(pl.Utf8)),
        }
    )

    write_table_json(df, tmp_path / "data.json")

    rows = json.loads((tmp_path / "data.json").read_text())
    assert rows[0]["tag_ids"] == "a,b,c"


def test_numeric_list_round_trip(tmp_path: Path):
    """Should preserve numeric lists as List(Float64) and *_ids as List(Utf8)."""
    from jsonjsdb.loader import load_table
    from jsonjsdb.writer import write_table_json

    df = pl.DataFrame(
        {
            "id": ["row-1"],
            "bbox": pl.Series([[6.02, 45.8, 10.5, 47.8]], dtype=pl.List(pl.Float64)),
            "tag_ids": pl.Series([["a", "b", "c"]], dtype=pl.List(pl.Utf8)),
        }
    )

    write_table_json(df, tmp_path / "data.json")
    loaded = load_table(tmp_path / "data.json")

    assert loaded.schema["bbox"] == pl.List(pl.Float64)
    assert loaded["bbox"].to_list() == [[6.02, 45.8, 10.5, 47.8]]
    assert loaded.schema["tag_ids"] == pl.List(pl.Utf8)
    assert loaded["tag_ids"].to_list() == [["a", "b", "c"]]


def test_nan_in_numeric_list_becomes_null(tmp_path: Path):
    """Should replace NaN inside a float list with null for valid JSON."""
    from jsonjsdb.writer import write_table_json

    df = pl.DataFrame(
        {
            "id": ["row-1"],
            "bbox": pl.Series(
                [[6.02, float("nan"), 10.5, 47.8]], dtype=pl.List(pl.Float64)
            ),
        }
    )

    write_table_json(df, tmp_path / "data.json")

    rows = json.loads((tmp_path / "data.json").read_text())
    assert rows[0]["bbox"] == [6.02, None, 10.5, 47.8]


# --- Buffered appends ---


def test_buffered_adds_are_materialized_in_a_single_chunk():
    """N appends on a fresh table must concat once, not once per row."""
    table: Table[dict] = Table("t")
    for i in range(500):
        table.add({"id": f"i{i}", "v": i})

    assert table.count == 500
    assert table.df.n_chunks() == 1


def test_read_after_write_sees_buffered_rows():
    table: Table[dict] = Table("t")
    table.add({"id": "a", "v": 1})
    table.add_all([{"id": "b", "v": 2}, {"id": "c", "v": 3}])

    assert table.count == 3
    assert not table.is_empty
    assert table.get("b") == {"id": "b", "v": 2}
    assert table.exists("c")
    assert [r["id"] for r in table.all()] == ["a", "b", "c"]
    assert table.ids_where("v", ">", 1) == ["b", "c"]
    assert table.get_by("v", 3) == {"id": "c", "v": 3}
    assert table.get_many(["a", "c"]) == [{"id": "a", "v": 1}, {"id": "c", "v": 3}]


def test_buffered_duplicate_id_raises_immediately():
    table: Table[dict] = Table("t")
    table.add({"id": "a"})

    with pytest.raises(ValueError, match="already exists"):
        table.add({"id": "a"})
    with pytest.raises(ValueError, match="IDs already exist"):
        table.add_all([{"id": "a"}])


def test_insertion_order_preserved_across_flush_boundary():
    table: Table[dict] = Table("t")
    table.add({"id": "a"})
    table.add({"id": "b"})
    table.get("a")  # forces a flush mid-stream
    table.add({"id": "c"})
    table.add_all([{"id": "d"}, {"id": "e"}])

    assert [r["id"] for r in table.all()] == ["a", "b", "c", "d", "e"]


def test_mutations_interleaved_with_buffered_adds():
    table: Table[dict] = Table("t")
    table.add({"id": "a", "v": 1})
    table.add({"id": "b", "v": 2})

    assert table.remove("a") is True  # removes a still-buffered row
    table.add({"id": "c", "v": 3})
    table.update("b", v=20)

    assert [(r["id"], r["v"]) for r in table.all()] == [("b", 20), ("c", 3)]
    assert table.count == 2

    table.add({"id": "a", "v": 1})  # id is free again after removal
    assert table.exists("a")


def test_upsert_replaces_a_buffered_row():
    table: Table[dict] = Table("t")
    table.add({"id": "a", "v": 1})

    assert table.upsert({"id": "a", "v": 2}) is False
    assert table.get("a") == {"id": "a", "v": 2}
    assert table.count == 1


def test_get_persistable_df_flushes_pending_rows():
    table: Table[dict] = Table("t", runtime_fields={"tmp"})
    table.add({"id": "a", "keep": 1, "tmp": 9})

    persistable = table.get_persistable_df()
    assert persistable.columns == ["id", "keep"]
    assert persistable.height == 1


def test_save_flushes_buffered_rows_without_a_prior_read(tmp_path: Path):
    class ItemDB(Jsonjsdb):
        item: Table[dict]

    db = ItemDB()
    db.item.add_all([{"id": f"i{i}", "n": i} for i in range(3)])
    db.save(tmp_path)

    reloaded = ItemDB(tmp_path)
    assert [r["id"] for r in reloaded.item.all()] == ["i0", "i1", "i2"]


def test_typed_integer_field_rejects_non_integral_float_in_add_all():
    """Eager scalar validation: the offending call raises, not a later read."""

    class Metric(TypedDict):
        id: str
        count: Optional[int]

    class MetricDB(Jsonjsdb):
        metric: Table[Metric]

    db = MetricDB()
    with pytest.raises(ValueError, match="non-integer values"):
        db.metric.add_all(
            [
                {"id": "row-1", "count": 1},
                {"id": "row-2", "count": 392.5},  # type: ignore[typeddict-item]
            ]
        )

    assert db.metric.count == 0  # the whole batch is rejected, nothing buffered
    assert not db.metric.exists("row-1")


def test_typed_integer_field_accepts_integral_float_and_null():
    class Metric(TypedDict):
        id: str
        count: Optional[int]

    class MetricDB(Jsonjsdb):
        metric: Table[Metric]

    db = MetricDB()
    db.metric.add({"id": "row-1", "count": 3.0})  # type: ignore[typeddict-item]
    db.metric.add({"id": "row-2", "count": None})

    assert db.metric.df.schema["count"] == pl.Int64
    assert db.metric.get("row-1") == {"id": "row-1", "count": 3}
    assert db.metric.get("row-2") == {"id": "row-2", "count": None}


def test_count_matches_row_count_when_df_has_duplicate_ids():
    """count must not be derived from the id set: loaded ids need not be unique."""
    table: Table[dict] = Table("t", df=pl.DataFrame({"id": ["a", "a", "b"]}))

    assert table.count == 3
    assert len(table.all()) == 3


def test_exists_and_get_work_on_a_non_utf8_id_column():
    table: Table[dict] = Table("t", df=pl.DataFrame({"id": [1, 2], "v": ["x", "y"]}))
    numeric_id = cast(str, 1)  # ID is str, but a loaded df may hold integer ids

    assert table.exists(numeric_id) is True
    assert table.get(numeric_id) == {"id": 1, "v": "x"}


def test_update_cannot_rename_an_id():
    """The `id` positional shadows an `id=` kwarg, so update() never renames."""
    table: Table[dict] = Table("t")
    table.add({"id": "a", "v": 1})

    with pytest.raises(TypeError, match="multiple values for argument 'id'"):
        table.update("a", id="b")  # type: ignore[misc]


def test_update_many_that_renames_ids_keeps_uniqueness_tracking_in_sync():
    table: Table[dict] = Table("t")
    table.add_all([{"id": "a", "v": 1}, {"id": "b", "v": 2}])

    assert table.update_many(["a"], id="z") == 1
    assert not table.exists("a")
    assert table.exists("z")

    table.add({"id": "a", "v": 3})  # the old id is free again
    with pytest.raises(ValueError, match="already exists"):
        table.add({"id": "z", "v": 4})


def test_irreconcilable_row_raises_on_add_and_leaves_the_table_usable():
    """The buffer must not turn a rejected row into an unreadable table."""
    table: Table[dict] = Table("t")
    table.add({"id": "a", "x": 1})

    with pytest.raises(pl.exceptions.SchemaError):
        table.add({"id": "b", "x": [1, 2]})  # int vs list[int]: no supertype

    assert [r["id"] for r in table.all()] == ["a"]
    assert table.count == 1
    assert not table.exists("b")
    table.add({"id": "b", "x": 2})  # the id was never taken


def test_irreconcilable_row_raises_against_an_already_stored_column():
    table: Table[dict] = Table("t")
    table.add({"id": "a", "x": 1})
    table.all()  # flush: x is now an Int64 column of _df

    with pytest.raises(pl.exceptions.SchemaError):
        table.add({"id": "b", "x": [1, 2]})
    assert table.count == 1


def test_rejected_row_leaves_no_sample_behind_for_later_appends():
    """A row rejected on its 2nd column must not poison the 1st column's type."""

    class Metric(TypedDict):
        id: str
        tags: list[str]
        count: Optional[int]

    class MetricDB(Jsonjsdb):
        metric: Table[Metric]

    db = MetricDB()
    with pytest.raises(ValueError, match="non-integer values"):
        db.metric.add({"id": "a", "tags": ["x"], "count": 392.5})  # type: ignore[typeddict-item]

    # 'tags' saw a list before the row was rejected; a str must still be accepted
    db.metric.add({"id": "b", "tags": "x", "count": 1})  # type: ignore[typeddict-item]
    assert db.metric.count == 1


def test_column_type_widening_is_still_accepted():
    table: Table[dict] = Table("t")
    table.add_all([{"id": "a", "x": 1}, {"id": "b", "x": 1.5}, {"id": "c", "x": "s"}])

    assert table.df.schema["x"] == pl.String
    assert [r["x"] for r in table.all()] == ["1", "1.5", "s"]


def test_insert_only_upsert_loop_stays_buffered():
    table: Table[dict] = Table("t")
    for i in range(500):
        assert table.upsert({"id": f"i{i}", "v": i}) is True

    assert table.count == 500
    assert table.df.n_chunks() == 1


def test_upsert_updates_a_row_that_is_still_buffered():
    table: Table[dict] = Table("t")
    table.add({"id": "a", "v": 1})

    assert table.upsert({"id": "a", "v": 2}) is False
    assert table.get("a") == {"id": "a", "v": 2}
    assert table.count == 1


def test_upsert_all_appends_new_ids_in_batch_order_deterministically():
    """Row order reaches save() and its hashes, so it must not depend on a join."""
    orders = set()
    for _ in range(20):
        table: Table[dict] = Table("t")
        table.add_all(
            [{"id": "i4", "v": 1}, {"id": "i5", "v": 2}, {"id": "i2", "v": 3}]
        )
        table.all()  # flush, so upsert_all replaces against a stored frame
        table.upsert_all(
            [{"id": "i2", "v": 7}, {"id": "i3", "v": 99}, {"id": "i1", "v": 98}]
        )
        orders.add(tuple(r["id"] for r in table.all()))

    assert orders == {("i4", "i5", "i2", "i3", "i1")}


def test_upsert_all_preserves_runtime_fields_on_replaced_rows():
    table: Table[dict] = Table("t", runtime_fields={"rt"})
    table.add_all([{"id": "a", "v": 1, "rt": 10}, {"id": "b", "v": 2, "rt": 20}])

    table.upsert_all([{"id": "b", "v": 9}, {"id": "c", "v": 3}])

    assert table.get("b") == {"id": "b", "v": 9, "rt": 20}
    assert table.get("c") == {"id": "c", "v": 3, "rt": None}


# --- Buffering must be invisible: differential test against an eagerly flushed table ---

_DIFF_READS = [
    ("count", lambda t: t.count),
    ("is_empty", lambda t: t.is_empty),
    ("all", lambda t: t.all()),
    ("shape", lambda t: t.df.shape),
    ("schema", lambda t: str(t.df.schema)),
    ("persistable", lambda t: t.get_persistable_df().shape),
]


def _outcome(fn):
    """Return the value, or the exception type, so both are compared."""
    try:
        return ("ok", fn())
    except Exception as exc:
        return ("raised", type(exc).__name__)


def _random_row(rng, ids):
    row: dict = {"id": rng.choice(ids)}
    if rng.random() < 0.85:
        row["n"] = rng.choice([1, 2, 1.5, None])
    if rng.random() < 0.5:
        row["s"] = rng.choice(["a", "b", None])
    if rng.random() < 0.1:
        row["n"] = [1, 2]  # irreconcilable with an int column
    if rng.random() < 0.3:
        row["extra"] = rng.randint(0, 5)
    return row


def _random_op(rng, ids):
    """Build the op eagerly: both tables must receive the very same rows."""
    r = rng.random()
    if r < 0.30:
        row = _random_row(rng, ids)
        return lambda t: t.add(dict(row))
    if r < 0.42:
        rows = [_random_row(rng, ids) for _ in range(rng.randint(1, 3))]
        return lambda t: t.add_all([dict(row) for row in rows])
    if r < 0.54:
        row = _random_row(rng, ids)
        return lambda t: t.upsert(dict(row))
    if r < 0.62:
        rows = [_random_row(rng, ids) for _ in range(rng.randint(1, 2))]
        return lambda t: t.upsert_all([dict(row) for row in rows])
    if r < 0.70:
        i, n = rng.choice(ids), rng.randint(0, 9)
        return lambda t: t.update(i, n=n)
    if r < 0.76:
        sample = rng.sample(ids, 2)
        return lambda t: t.update_many(sample, s="z")
    if r < 0.82:
        i = rng.choice(ids)
        return lambda t: t.remove(i)
    if r < 0.86:
        sample = rng.sample(ids, 2)
        return lambda t: t.remove_all(sample)
    if r < 0.88:
        return lambda t: t.remove_where("n", "==", 1)
    if r < 0.91:  # whole-frame replacement through the public setter
        i = rng.choice(ids)
        return lambda t: setattr(t, "df", t.df.filter(pl.col("id") != i))
    if r < 0.94:
        k = rng.randint(0, 9)
        return lambda t: setattr(t, "df", t.df.with_columns(extra=pl.lit(k)))
    if r < 0.96:
        i = rng.choice(ids)
        return lambda t: t.get(i)
    if r < 0.98:
        return lambda t: t.where("n", ">", 0)
    return lambda t: t.ids_where("s", "==", "a")


@pytest.mark.parametrize("seed", range(60))
def test_buffering_is_invisible_on_random_operation_sequences(seed: int):
    """A buffered table must be indistinguishable from one flushed after every write.

    At most one read runs per step, rotating, and only sometimes: a read that
    forgets to flush must be the first to touch the table after a write, and
    writes must be able to pile up so the next op meets a non-empty buffer.
    """
    import random

    rng = random.Random(seed)
    ids = [f"i{k}" for k in range(6)]
    buffered: Table[dict] = Table("buffered")
    reference: Table[dict] = Table("reference")

    for step in range(40):
        op = _random_op(rng, ids)
        assert _outcome(lambda: op(buffered)) == _outcome(lambda: op(reference))
        reference._flush()  # the reference never carries a buffer

        if rng.random() < 0.5:  # otherwise let the buffer accumulate
            name, read = _DIFF_READS[step % len(_DIFF_READS)]
            got = _outcome(lambda: read(buffered))
            expected = _outcome(lambda: read(reference))
            assert got == expected, f"{name} diverged at step {step}"

    assert _outcome(lambda: buffered.df.to_dicts()) == _outcome(
        lambda: reference.df.to_dicts()
    )


def test_probe_ignores_the_schema_of_an_emptied_table():
    """_flush() replaces an empty _df wholesale, so its dtypes must not constrain add()."""
    table: Table[dict] = Table("t")
    table.add({"id": "a", "n": [1, 2]})
    assert table.remove("a") is True
    assert table.is_empty
    assert table.df.schema["n"] == pl.List(pl.Int64)  # zero rows, dtype retained

    table.add({"id": "b", "n": 2})  # would clash with List(Int64) if _df constrained it

    assert table.get("b") == {"id": "b", "n": 2}
    assert table.df.schema["n"] == pl.Int64


# --- save -> reload -> save must be deterministic, idempotent, buffer-agnostic ---

_SAVE_TIMESTAMP = 1_700_000_000


class _ItemDB(Jsonjsdb):
    item: Table[dict]


def _snapshot(path: Path) -> dict[str, str]:
    """Every written file -> its content hash."""
    return {
        str(p.relative_to(path)): file_hash(p)
        for p in sorted(path.rglob("*"))
        if p.is_file()
    }


def _saved_snapshot(rows: list[dict], path: Path, *, buffered: bool) -> dict[str, str]:
    db = _ItemDB()
    db.item.add_all([dict(r) for r in rows])
    if not buffered:
        db.item.all()  # materialize before saving
    db.save(path, timestamp=_SAVE_TIMESTAMP)
    return _snapshot(path)


_ROUNDTRIP_CASES = [
    ("scalars", [{"id": "a", "n": 1, "s": "x", "f": 1.5, "b": True}]),
    ("nulls", [{"id": "a", "n": None, "s": None}, {"id": "b", "n": 2, "s": "y"}]),
    ("ids_list", [{"id": "a", "tag_ids": ["t1", "t2"]}, {"id": "b", "tag_ids": []}]),
    ("ids_null", [{"id": "a", "tag_ids": None}]),
    ("float_list", [{"id": "a", "bbox": [1.5, 2.5]}]),
    ("int_list", [{"id": "a", "dims": [1, 2]}]),
    ("empty_string", [{"id": "a", "s": ""}]),
    ("unicode", [{"id": "a", "s": "héllo → ✓"}]),
    ("numeric_looking_string", [{"id": "a", "s": "0123"}]),
]

_roundtrip = pytest.mark.parametrize(
    "rows", [c[1] for c in _ROUNDTRIP_CASES], ids=[c[0] for c in _ROUNDTRIP_CASES]
)


@_roundtrip
def test_save_is_deterministic(rows: list[dict], tmp_path: Path):
    assert _saved_snapshot(rows, tmp_path / "a", buffered=True) == _saved_snapshot(
        rows, tmp_path / "b", buffered=True
    )


@_roundtrip
def test_save_does_not_depend_on_whether_rows_were_buffered(
    rows: list[dict], tmp_path: Path
):
    assert _saved_snapshot(rows, tmp_path / "a", buffered=True) == _saved_snapshot(
        rows, tmp_path / "b", buffered=False
    )


@_roundtrip
def test_reloading_and_saving_again_rewrites_the_same_files(
    rows: list[dict], tmp_path: Path
):
    """A scan that produced the same data must not churn the exported files."""
    first = _saved_snapshot(rows, tmp_path / "a", buffered=True)

    reloaded = _ItemDB(tmp_path / "a")
    reloaded.save(tmp_path / "b", timestamp=_SAVE_TIMESTAMP)

    assert _snapshot(tmp_path / "b") == first


@_roundtrip
def test_reloading_preserves_row_content(rows: list[dict], tmp_path: Path):
    original = _ItemDB()
    original.item.add_all([dict(r) for r in rows])
    expected = original.item.all()
    original.save(tmp_path, timestamp=_SAVE_TIMESTAMP)

    assert _ItemDB(tmp_path).item.all() == expected


def test_saving_an_id_containing_a_comma_raises(tmp_path: Path):
    """The CSV encoding cannot represent it; refuse rather than split it on read."""
    db = _ItemDB()
    db.item.add_all([{"id": "a", "tag_ids": ["x,y", "z"]}])

    with pytest.raises(ValueError, match="tag_ids.*comma"):
        db.save(tmp_path, timestamp=_SAVE_TIMESTAMP)


def test_saving_string_lists_without_commas_is_unaffected(tmp_path: Path):
    db = _ItemDB()
    db.item.add_all(
        [
            {"id": "a", "tag_ids": ["t1", "t2"]},
            {"id": "b", "tag_ids": []},
            {"id": "c", "tag_ids": None},
        ]
    )
    db.save(tmp_path, timestamp=_SAVE_TIMESTAMP)

    reloaded = _ItemDB(tmp_path).item
    assert reloaded.get("a") == {"id": "a", "tag_ids": ["t1", "t2"]}
    assert reloaded.get("b") == {"id": "b", "tag_ids": []}
    assert reloaded.get("c") == {"id": "c", "tag_ids": []}


def test_comma_rejection_reports_the_column_and_the_offending_value(tmp_path: Path):
    from jsonjsdb.writer import write_table_json

    df = pl.DataFrame(
        {
            "id": ["r1", "r2"],
            "tag_ids": pl.Series([["ok"], ["x,y", None]], dtype=pl.List(pl.Utf8)),
        }
    )

    with pytest.raises(ValueError) as excinfo:
        write_table_json(df, tmp_path / "data.json")

    message = str(excinfo.value)
    assert "tag_ids" in message
    assert "x,y" in message  # the offending row, not the clean one
    assert not (tmp_path / "data.json").exists()


@pytest.mark.xfail(
    strict=True,
    reason="every string list is written comma-separated but only *_ids is split "
    "back, so a plain list column reloads as a string. Stable and idempotent, but "
    "only a change to the output format could fix it. Delete this marker if it does.",
)
def test_reloading_preserves_a_string_list_column_not_named_ids(tmp_path: Path):
    db = _ItemDB()
    db.item.add_all([{"id": "a", "tags": ["p", "q"]}])
    db.save(tmp_path, timestamp=_SAVE_TIMESTAMP)

    assert _ItemDB(tmp_path).item.get("a") == {"id": "a", "tags": ["p", "q"]}


def test_an_unwritable_table_aborts_the_save_before_any_file_is_written(
    tmp_path: Path,
):
    class TwoTableDB(Jsonjsdb):
        aaa: Table[dict]
        zzz: Table[dict]

    db = TwoTableDB()
    db.aaa.add({"id": "a", "v": 1})
    db.zzz.add({"id": "b", "tag_ids": ["x,y"]})
    target = tmp_path / "export"

    with pytest.raises(ValueError, match="comma"):
        db.save(target, timestamp=_SAVE_TIMESTAMP)

    assert not target.exists()  # aaa.json must not survive zzz's rejection


# --- Whole-frame replacement: table.df = f(table.df) ---


def test_df_setter_transform_sees_buffered_rows():
    """The getter flushes, so f() is handed the rows added just before."""
    table: Table[dict] = Table("t")
    table.add({"id": "a", "v": 1})
    table.add({"id": "b", "v": 2})

    table.df = table.df.with_columns(doubled=pl.col("v") * 2)

    assert table.all() == [
        {"id": "a", "v": 1, "doubled": 2},
        {"id": "b", "v": 2, "doubled": 4},
    ]


def test_df_setter_rebuilds_the_id_index():
    table: Table[dict] = Table("t")
    table.add_all([{"id": "a"}, {"id": "b"}])

    table.df = table.df.filter(pl.col("id") != "a")

    assert not table.exists("a")
    assert table.count == 1
    table.add({"id": "a"})  # the dropped id is free again
    with pytest.raises(ValueError, match="already exists"):
        table.add({"id": "b"})  # a surviving id is still taken


def test_df_setter_picks_up_ids_introduced_by_the_transform():
    table: Table[dict] = Table("t")
    table.add({"id": "a"})

    table.df = pl.concat([table.df, pl.DataFrame({"id": ["grafted"]})])

    assert table.exists("grafted")
    with pytest.raises(ValueError, match="already exists"):
        table.add({"id": "grafted"})


def test_df_setter_clears_the_buffered_type_samples():
    """A sample left over from a dropped column would falsely reject a later add."""
    table: Table[dict] = Table("t")
    table.add({"id": "a", "x": 1})  # 'x' sampled as int, still buffered

    table.df = pl.DataFrame({"id": ["z"]})  # 'x' no longer exists anywhere

    table.add({"id": "b", "x": [1, 2]})  # must not clash with the stale int sample
    assert table.get("b") == {"id": "b", "x": [1, 2]}


def test_df_setter_replaces_rows_that_were_still_buffered():
    table: Table[dict] = Table("t")
    table.add({"id": "buffered"})

    table.df = pl.DataFrame({"id": ["fresh"]})

    assert [r["id"] for r in table.all()] == ["fresh"]
    assert table.count == 1


def test_df_setter_applies_the_storage_schema():
    class Metric(TypedDict):
        id: str
        count: Optional[int]

    class MetricDB(Jsonjsdb):
        metric: Table[Metric]

    db = MetricDB()
    db.metric.df = pl.DataFrame({"id": ["a"], "count": [3.0]})

    assert db.metric.df.schema["count"] == pl.Int64
    assert db.metric.get("a") == {"id": "a", "count": 3}


def test_df_setter_rejects_an_unstorable_frame_and_leaves_the_table_intact():
    class Metric(TypedDict):
        id: str
        count: Optional[int]

    class MetricDB(Jsonjsdb):
        metric: Table[Metric]

    db = MetricDB()
    db.metric.add({"id": "a", "count": 1})
    before = db.metric.all()

    with pytest.raises(ValueError, match="non-integer values"):
        db.metric.df = pl.DataFrame({"id": ["b"], "count": [2.5]})

    assert db.metric.all() == before


def test_df_setter_output_is_saved(tmp_path: Path):
    db = _ItemDB()
    db.item.add_all([{"id": "a", "v": 1}, {"id": "b", "v": 2}])
    db.item.df = db.item.df.with_columns(v=pl.col("v") * 10)
    db.save(tmp_path, timestamp=_SAVE_TIMESTAMP)

    assert _ItemDB(tmp_path).item.all() == [
        {"id": "a", "v": 10},
        {"id": "b", "v": 20},
    ]
