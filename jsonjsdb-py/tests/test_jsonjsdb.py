"""Tests for jsonjsdb package - Phase 1: Reading."""

import json
from pathlib import Path
from typing import Optional, TypedDict

import pytest

import jsonjsdb
from jsonjsdb import Jsonjsdb, Table

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
    assert (new_path / "user.json").exists()
    assert (new_path / "user.json.js").exists()


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


def test_runtime_fields_empty_set():
    """Should keep all columns when runtime_fields is empty."""
    table: Table[dict] = Table("test")
    assert table.runtime_fields == set()

    import polars as pl

    table._df = pl.DataFrame([{"id": "1", "a": 1, "b": 2}])
    persistable = table.get_persistable_df()
    assert persistable.columns == ["id", "a", "b"]


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
