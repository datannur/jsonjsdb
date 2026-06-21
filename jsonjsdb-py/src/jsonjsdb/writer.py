"""Write Polars DataFrames to JSON and JSON.js files."""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

import polars as pl

HASHES_PATH = Path("_meta") / "json-hashes.json"


@dataclass(frozen=True)
class TableWriteResult:
    """Result of writing a logical table export."""

    data_changed: bool
    json_written: bool
    jsonjs_written: bool
    json_hash: str


def write_table_json(
    df: pl.DataFrame, path: Path, *, export_root: Path | None = None
) -> None:
    """Write a DataFrame to a JSON file (array of objects)."""
    write_table_json_pair(
        df,
        path.stem,
        path.parent,
        write_js=False,
        export_root=export_root,
        json_path=path,
    )


def write_table_jsonjs(
    df: pl.DataFrame,
    table_name: str,
    path: Path,
    *,
    export_root: Path | None = None,
) -> None:
    """Write a DataFrame to a JSON.js file (array of arrays format)."""
    prepared_df = prepare_table_for_export(df)
    json_content = table_json_content_from_prepared(prepared_df)
    content_hash = table_json_hash_from_content(json_content)
    if (
        _export_hash_matches(
            path.with_suffix(""), content_hash, export_root=export_root
        )
        and path.exists()
    ):
        return

    write_text_if_changed(
        path, table_jsonjs_content_from_prepared(prepared_df, table_name)
    )
    json_path = path.with_suffix("")
    if json_path.exists() and file_hash(json_path) == content_hash:
        _update_export_hash(json_path, content_hash, export_root=export_root)


def write_table_json_pair(
    df: pl.DataFrame,
    table_name: str,
    output_dir: Path,
    *,
    write_js: bool = True,
    export_root: Path | None = None,
    previous_hashes: dict[str, str] | None = None,
    update_hash_metadata: bool = True,
    before_write: Callable[[bool], None] | None = None,
    json_path: Path | None = None,
    hash_session: dict[str, str] | None = None,
) -> TableWriteResult:
    """Write .json and optional .json.js files for one logical table export."""
    json_path = json_path or output_dir / f"{table_name}.json"
    jsonjs_path = output_dir / f"{table_name}.json.js"
    active_hashes = hash_session if hash_session is not None else previous_hashes
    root, hashes, hash_key = _hash_context(json_path, export_root, active_hashes)

    prepared_df = prepare_table_for_export(df)
    json_content = table_json_content_from_prepared(prepared_df)
    json_hash = table_json_hash_from_content(json_content)
    old_hash = hashes.get(hash_key) if hash_key else None
    if old_hash is None and json_path.exists():
        old_hash = file_hash(json_path)

    data_changed = old_hash != json_hash
    json_missing = not json_path.exists()
    jsonjs_missing = write_js and not jsonjs_path.exists()

    if before_write:
        before_write(data_changed)

    json_written = False
    jsonjs_written = False
    if data_changed or json_missing:
        json_written = write_text_if_changed(json_path, json_content)
    if write_js and (data_changed or jsonjs_missing):
        jsonjs_written = write_text_if_changed(
            jsonjs_path,
            table_jsonjs_content_from_prepared(prepared_df, table_name),
        )
    if hash_session is not None and hash_key:
        hash_session[hash_key] = json_hash
    elif update_hash_metadata and root and hash_key:
        _update_export_hash(json_path, json_hash, export_root=root)

    return TableWriteResult(data_changed, json_written, jsonjs_written, json_hash)


@contextmanager
def export_hash_session(export_root: Path) -> Iterator[dict[str, str]]:
    """Batch hash metadata updates for multiple exports under one root."""
    hashes = load_json_hashes(export_root)
    original_hashes = hashes.copy()
    yield hashes
    if hashes != original_hashes:
        save_json_hashes(export_root, hashes)


def table_json_content(df: pl.DataFrame) -> str:
    """Return canonical JSON export content for a DataFrame."""
    return table_json_content_from_prepared(prepare_table_for_export(df))


def table_json_hash_from_content(content: str) -> str:
    """Return the canonical JSON export hash for pre-rendered content."""
    return _content_hash(content)


def table_json_hash(df: pl.DataFrame) -> str:
    """Return the canonical JSON export hash for a DataFrame."""
    return table_json_hash_from_content(table_json_content(df))


def table_jsonjs_content(df: pl.DataFrame, table_name: str) -> str:
    """Return JSON.js export content for a DataFrame."""
    return table_jsonjs_content_from_prepared(prepare_table_for_export(df), table_name)


def prepare_table_for_export(df: pl.DataFrame) -> pl.DataFrame:
    """Return a DataFrame normalized for JSON and JSON.js export."""
    return _prepare_df_for_write(df)


def table_json_content_from_prepared(prepared_df: pl.DataFrame) -> str:
    """Return canonical JSON export content from a prepared DataFrame."""
    rows = _df_to_json_rows(prepared_df)
    return json.dumps(rows, indent=2, ensure_ascii=False, allow_nan=False) + "\n"


def table_jsonjs_content_from_prepared(
    prepared_df: pl.DataFrame, table_name: str
) -> str:
    """Return JSON.js export content from a prepared DataFrame."""
    columns = prepared_df.columns
    rows: list[list[Any]] = [columns]

    for row in prepared_df.iter_rows():
        rows.append(list(row))

    json_array = json.dumps(
        rows, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    )
    return f"jsonjs.data['{table_name}'] = {json_array}\n"


def write_table_index(
    tables: list[str],
    path: Path,
    timestamp: Optional[int] = None,
    *,
    write_js: bool = True,
    last_modifs: dict[str, int] | None = None,
) -> None:
    """Write __table__.json and optionally __table__.json.js with table metadata.

    Args:
        tables: List of table names to include
        path: Path to write __table__.json
        timestamp: Optional timestamp override (uses current time if None)
        write_js: If True, also write __table__.json.js (default: True)
        last_modifs: Optional per-table last_modif values to preserve
    """
    write_table_json_pair(
        table_index_df(tables, timestamp, last_modifs=last_modifs),
        path.stem,
        path.parent,
        write_js=write_js,
        json_path=path,
    )


def table_index_df(
    tables: list[str],
    timestamp: Optional[int] = None,
    *,
    last_modifs: dict[str, int] | None = None,
) -> pl.DataFrame:
    """Build the __table__ DataFrame."""
    now = timestamp if timestamp is not None else int(time.time())
    all_tables = sorted(tables) + ["__table__"]
    table_rows = [
        {"name": name, "last_modif": last_modifs.get(name, now) if last_modifs else now}
        for name in all_tables
    ]
    return pl.DataFrame(table_rows)


def write_text_if_changed(path: Path, content: str) -> bool:
    """Write text only when content differs. Return True when a write occurred."""
    if path.exists() and path.read_text(encoding="utf-8") == content:
        return False

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return True


def _export_hash_matches(
    path: Path, content_hash: str, *, export_root: Path | None = None
) -> bool:
    """Return whether hash metadata says an export is unchanged and present."""
    root = export_root or find_export_root(path)
    if root is None or not path.exists():
        return False

    return load_json_hashes(root).get(export_hash_key(root, path)) == content_hash


def _update_export_hash(
    path: Path, content_hash: str, *, export_root: Path | None = None
) -> None:
    """Update hash metadata for an exported JSON file when a DB root is known."""
    root = export_root or find_export_root(path)
    if root is None:
        return

    hashes = load_json_hashes(root)
    hashes[export_hash_key(root, path)] = content_hash
    save_json_hashes(root, hashes)


def _hash_context(
    path: Path,
    export_root: Path | None,
    previous_hashes: dict[str, str] | None,
) -> tuple[Path | None, dict[str, str], str | None]:
    root = export_root or find_export_root(path)
    if previous_hashes is not None:
        hashes = previous_hashes
    elif root:
        hashes = load_json_hashes(root)
    else:
        hashes = {}
    hash_key = export_hash_key(root, path) if root else None
    return root, hashes, hash_key


def load_json_hashes(root: Path) -> dict[str, str]:
    hashes_path = root / HASHES_PATH
    if not hashes_path.exists():
        return {}

    try:
        with open(hashes_path, encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}

    return {str(key): str(value) for key, value in data.items()}


def save_json_hashes(root: Path, hashes: dict[str, str]) -> None:
    content = json.dumps(hashes, indent=2, ensure_ascii=False, sort_keys=True) + "\n"
    write_text_if_changed(root / HASHES_PATH, content)


def find_export_root(path: Path) -> Path | None:
    """Find the nearest parent directory that looks like a jsonjsdb export root."""
    current = path.parent
    for candidate in (current, *current.parents):
        if (candidate / "__table__.json").exists():
            return candidate
    return None


def export_hash_key(root: Path, path: Path) -> str:
    return path.relative_to(root).as_posix()


def file_hash(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _content_hash(content: str) -> str:
    return "sha256:" + hashlib.sha256(content.encode("utf-8")).hexdigest()


def _prepare_df_for_write(df: pl.DataFrame) -> pl.DataFrame:
    """Prepare DataFrame for writing for valid JSON output:

    - List(Utf8) columns (e.g. ``*_ids``) -> comma-separated string;
    - other List columns (numeric, e.g. ``bbox``) -> native JSON array, with NaN
      inside float lists replaced by null;
    - float columns -> NaN replaced by null.
    """
    transforms: list[pl.Expr] = []

    for col_name in df.columns:
        col_type = df.schema[col_name]
        if isinstance(col_type, pl.List):
            if col_type.inner == pl.Utf8:  # string lists (e.g. *_ids) -> CSV
                transforms.append(
                    pl.col(col_name)
                    .cast(pl.List(pl.Utf8))
                    .list.join(",")
                    .fill_null("")
                    .alias(col_name)
                )
            elif col_type.inner is not None and col_type.inner.is_float():
                # numeric float lists (e.g. bbox) -> JSON array, NaN -> null
                transforms.append(
                    pl.col(col_name)
                    .list.eval(pl.element().fill_nan(None))
                    .alias(col_name)
                )
            else:  # numeric (integer) lists -> native JSON array
                transforms.append(pl.col(col_name))
        elif col_type.is_float():
            transforms.append(pl.col(col_name).fill_nan(None))
        else:
            transforms.append(pl.col(col_name))

    return df.select(transforms)


def _df_to_json_rows(df: pl.DataFrame) -> list[dict[str, Any]]:
    """Convert DataFrame to list of dicts for JSON serialization."""
    rows = []
    for row in df.iter_rows(named=True):
        rows.append(row)
    return rows
