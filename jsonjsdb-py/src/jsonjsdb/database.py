"""Main Jsonjsdb database class."""

from __future__ import annotations

from pathlib import Path
from typing import Any, get_type_hints

from .loader import load_table, load_table_index
from .table import Table
from .types import TableRow
from .writer import write_table_index, write_table_json, write_table_jsonjs


class Jsonjsdb:
    """JSONJS database with typed table access.

    Usage (untyped):
        db = Jsonjsdb("path/to/db")
        user = db["user"].get("user_1")

    Usage (typed):
        class MyDB(Jsonjsdb):
            user: Table[User]
            tag: Table[Tag]

        db = MyDB("path/to/db")
        user = db.user.get("user_1")  # Typed as User | None
    """

    def __init__(self, path: str | Path | None = None) -> None:
        self._path: Path | None = Path(path) if path else None
        self._tables: dict[str, Table[Any]] = {}

        if self._path:
            self._load_from_path(self._path)

        self._init_typed_tables()

    def _load_from_path(self, path: Path) -> None:
        """Load all tables from disk."""
        if not path.exists():
            raise FileNotFoundError(f"Database path does not exist: {path}")

        table_index_path = path / "__table__.json"
        if not table_index_path.exists():
            raise FileNotFoundError(f"Missing __table__.json in {path}")

        table_index = load_table_index(table_index_path)

        for entry in table_index:
            name = str(entry["name"])
            json_path = path / f"{name}.json"

            if json_path.exists():
                df = load_table(json_path)
                self._tables[name] = Table(name, self, df)

    def _init_typed_tables(self) -> None:
        """Initialize Table attributes from type annotations on subclasses."""
        hints = get_type_hints(self.__class__)

        for attr_name, hint in hints.items():
            origin = getattr(hint, "__origin__", None)
            if origin is Table:
                if attr_name in self._tables:
                    setattr(self, attr_name, self._tables[attr_name])
                else:
                    table = Table(attr_name, self)
                    self._tables[attr_name] = table
                    setattr(self, attr_name, table)

    def __getitem__(self, table_name: str) -> Table[TableRow]:
        """Access a table by name (untyped)."""
        if table_name not in self._tables:
            raise KeyError(f"Table '{table_name}' not found")
        return self._tables[table_name]

    @property
    def path(self) -> Path | None:
        return self._path

    @property
    def tables(self) -> list[str]:
        """List of loaded table names."""
        return list(self._tables.keys())

    def save(self, path: str | Path | None = None) -> None:
        """Save all tables to disk.

        If path is provided, saves to that location and updates self._path.
        If path is None, saves to the original path (must exist).
        """
        save_path = Path(path) if path else self._path

        if save_path is None:
            raise ValueError(
                "No path specified. Provide a path or load from an existing database first."
            )

        save_path.mkdir(parents=True, exist_ok=True)

        table_names = []
        for name, table in self._tables.items():
            persistable_df = table.get_persistable_df()
            if not persistable_df.is_empty():
                write_table_json(persistable_df, save_path / f"{name}.json")
                write_table_jsonjs(persistable_df, name, save_path / f"{name}.json.js")
                table_names.append(name)

        write_table_index(table_names, save_path / "__table__.json")

        self._path = save_path
