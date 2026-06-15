"""Main Jsonjsdb database class."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, get_args, get_type_hints

import polars as pl

from .evolution import (
    EvolutionEntry,
    compare_datasets,
    filter_cascade_entries,
    get_timestamp,
    load_evolution,
    save_evolution,
)
from .loader import load_table, load_table_index
from .table import Table
from .types import TableRow
from .writer import (
    file_hash,
    load_json_hashes,
    save_json_hashes,
    table_index_df,
    write_table_json_pair,
)

# Internal tables that should not be loaded as user data
INTERNAL_TABLES = {"evolution", "__table__"}


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
        self._original_snapshots: dict[str, pl.DataFrame] = {}

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
            if name in INTERNAL_TABLES:
                continue
            json_path = path / f"{name}.json"

            if json_path.exists():
                df = load_table(json_path)
                self._tables[name] = Table(name, self, df)
                self._original_snapshots[name] = df.clone()

    def _init_typed_tables(self) -> None:
        """Initialize Table attributes from type annotations on subclasses."""
        hints = get_type_hints(self.__class__)

        for attr_name, hint in hints.items():
            origin = getattr(hint, "__origin__", None)
            if origin is Table:
                entity_type = next(iter(get_args(hint)), None)
                if attr_name in self._tables:
                    table = self._tables[attr_name]
                    table.set_entity_type(entity_type)
                    setattr(self, attr_name, table)
                else:
                    table = Table(attr_name, self, entity_type=entity_type)
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

    def save(
        self,
        path: str | Path | None = None,
        *,
        track_evolution: bool = True,
        evolution_xlsx: Path | str | None = None,
        timestamp: int | None = None,
        write_js: bool = True,
        parent_relations: dict[str, str] | None = None,
    ) -> None:
        """Save all tables to disk with optional evolution tracking.

        If path is provided, saves to that location and updates self._path.
        If path is None, saves to the original path (must exist).

        Args:
            path: Target directory path (optional if already loaded from path)
            track_evolution: Enable change tracking (default: True)
            evolution_xlsx: Optional path for evolution.xlsx output
            timestamp: Optional timestamp override for deterministic outputs
            write_js: If True, write both .json and .json.js (default: True)
            parent_relations: Child->parent table mapping for cascade filtering
                Example: {"variable": "dataset", "freq": "variable"}
        """
        save_path = Path(path) if path else self._path

        if save_path is None:
            raise ValueError(
                "No path specified. Provide a path or load from an existing database first."
            )

        save_path.mkdir(parents=True, exist_ok=True)

        # Check if saving to same path (can use in-memory snapshots)
        same_path = (
            self._path is not None and save_path.resolve() == self._path.resolve()
        )

        ts = timestamp if timestamp is not None else get_timestamp()
        new_entries: list[EvolutionEntry] = []
        old_hashes = load_json_hashes(save_path)
        old_last_modifs = _load_last_modifs(save_path / "__table__.json")
        new_hashes: dict[str, str] = {}
        last_modifs: dict[str, int] = {}

        table_names = []
        for name, table in self._tables.items():
            persistable_df = table.get_persistable_df()
            if persistable_df.is_empty():
                continue

            json_rel_path = f"{name}.json"

            def track_table_evolution(data_changed: bool) -> None:
                if not track_evolution or not data_changed:
                    return

                old_df = self._get_old_table(save_path, name, same_path)
                entries = compare_datasets(
                    old_df, persistable_df, ts, name, parent_relations
                )
                new_entries.extend(entries)

            write_result = write_table_json_pair(
                persistable_df,
                name,
                save_path,
                write_js=write_js,
                export_root=save_path,
                previous_hashes=old_hashes,
                update_hash_metadata=False,
                before_write=track_table_evolution,
            )

            new_hashes[json_rel_path] = write_result.json_hash
            last_modifs[name] = (
                ts if write_result.data_changed else old_last_modifs.get(name, ts)
            )
            table_names.append(name)

            # Update snapshot for next comparison
            self._original_snapshots[name] = persistable_df.clone()

        # Save evolution if there are new entries
        if track_evolution and new_entries:
            # Filter cascade entries (child add/delete when parent has same operation)
            new_entries = filter_cascade_entries(new_entries)

        evolution_json_path = save_path / "evolution.json"
        evolution_hash_key = "evolution.json"
        old_evolution_hash = old_hashes.get(evolution_hash_key)
        current_evolution_hash = (
            file_hash(evolution_json_path) if evolution_json_path.exists() else None
        )
        evolution_json_changed = (
            old_evolution_hash is not None
            and current_evolution_hash is not None
            and current_evolution_hash != old_evolution_hash
        )
        evolution_jsonjs_missing = (
            write_js and not (save_path / "evolution.json.js").exists()
        )

        if track_evolution and new_entries:
            xlsx_path = Path(evolution_xlsx) if evolution_xlsx else None
            existing_entries = load_evolution(save_path, xlsx_path)
            all_entries = existing_entries + new_entries
            save_evolution(all_entries, save_path, xlsx_path)
            if "evolution" not in table_names:  # pragma: no branch
                table_names.append("evolution")
            last_modifs["evolution"] = ts
            new_hashes[evolution_hash_key] = file_hash(evolution_json_path)
        elif evolution_json_path.exists():
            if evolution_json_changed or evolution_jsonjs_missing:
                xlsx_path = Path(evolution_xlsx) if evolution_xlsx else None
                existing_entries = load_evolution(save_path, xlsx_path)
                save_evolution(
                    existing_entries,
                    save_path,
                    xlsx_path,
                    allow_empty=True,
                )
            table_names.append("evolution")
            last_modifs["evolution"] = (
                ts if evolution_json_changed else old_last_modifs.get("evolution", ts)
            )
            new_hashes[evolution_hash_key] = file_hash(evolution_json_path)

        last_modifs["__table__"] = (
            ts
            if _last_modifs_changed(last_modifs, old_last_modifs)
            else old_last_modifs.get("__table__", ts)
        )
        table_index = table_index_df(table_names, ts, last_modifs=last_modifs)
        table_index_result = write_table_json_pair(
            table_index,
            "__table__",
            save_path,
            write_js=write_js,
            export_root=save_path,
            previous_hashes=old_hashes,
            update_hash_metadata=False,
        )
        new_hashes["__table__.json"] = table_index_result.json_hash
        save_json_hashes(save_path, _merge_json_hashes(old_hashes, new_hashes))

        self._path = save_path

    def _get_old_table(self, path: Path, name: str, use_snapshot: bool) -> pl.DataFrame:
        """Get old table data for comparison.

        Uses in-memory snapshot if available and saving to same path,
        otherwise loads from disk.
        """
        if use_snapshot and name in self._original_snapshots:
            return self._original_snapshots[name]

        json_path = path / f"{name}.json"
        if not json_path.exists():
            return pl.DataFrame()
        return load_table(json_path)


def _merge_json_hashes(
    old_hashes: dict[str, str], new_hashes: dict[str, str]
) -> dict[str, str]:
    managed_hashes = {
        key: value
        for key, value in old_hashes.items()
        if key.startswith("_") or "/" in key or not key.endswith(".json")
    }
    managed_hashes.update(new_hashes)
    return managed_hashes


def _load_last_modifs(path: Path) -> dict[str, int]:
    if not path.exists():
        return {}

    try:
        entries = load_table_index(path)
    except (json.JSONDecodeError, OSError):
        return {}

    result: dict[str, int] = {}
    for entry in entries:
        name = entry.get("name")
        last_modif = entry.get("last_modif")
        if isinstance(name, str) and isinstance(last_modif, int):
            result[name] = last_modif
    return result


def _last_modifs_changed(
    new_values: dict[str, int], old_values: dict[str, int]
) -> bool:
    public_new_values = {
        key: value for key, value in new_values.items() if key != "__table__"
    }
    public_old_values = {
        key: value for key, value in old_values.items() if key != "__table__"
    }
    return public_new_values != public_old_values
