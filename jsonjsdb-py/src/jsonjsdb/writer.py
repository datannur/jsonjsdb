"""Write Polars DataFrames to JSON and JSON.js files."""

import json
import time
from pathlib import Path
from typing import Any, Optional

import polars as pl


def write_table_json(df: pl.DataFrame, path: Path) -> None:
    """Write a DataFrame to a JSON file (array of objects)."""
    prepared_df = _prepare_df_for_write(df)
    rows = _df_to_json_rows(prepared_df)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)
        f.write("\n")


def write_table_jsonjs(df: pl.DataFrame, table_name: str, path: Path) -> None:
    """Write a DataFrame to a JSON.js file (array of arrays format)."""
    prepared_df = _prepare_df_for_write(df)
    columns = prepared_df.columns
    rows: list[list[Any]] = [columns]

    for row in prepared_df.iter_rows():
        rows.append(list(row))

    json_array = json.dumps(rows, ensure_ascii=False, separators=(",", ":"))
    content = f"jsonjs.data['{table_name}'] = {json_array}\n"

    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def write_table_index(
    tables: list[str],
    path: Path,
    timestamp: Optional[int] = None,
    *,
    write_js: bool = True,
) -> None:
    """Write __table__.json and optionally __table__.json.js with table metadata.

    Args:
        tables: List of table names to include
        path: Path to write __table__.json
        timestamp: Optional timestamp override (uses current time if None)
        write_js: If True, also write __table__.json.js (default: True)
    """
    now = timestamp if timestamp is not None else int(time.time())
    all_tables = sorted(tables) + ["__table__"]
    df = pl.DataFrame([{"name": name, "last_modif": now} for name in all_tables])

    write_table_json(df, path)
    if write_js:
        write_table_jsonjs(df, "__table__", path.with_suffix(".json.js"))


def _prepare_df_for_write(df: pl.DataFrame) -> pl.DataFrame:
    """Prepare DataFrame for writing: convert List columns to comma-separated strings."""
    transforms: list[pl.Expr] = []

    for col_name in df.columns:
        col_type = df.schema[col_name]
        if isinstance(col_type, pl.List):
            transforms.append(
                pl.col(col_name)
                .cast(pl.List(pl.Utf8))
                .list.join(",")
                .fill_null("")
                .alias(col_name)
            )
        else:
            transforms.append(pl.col(col_name))

    return df.select(transforms)


def _df_to_json_rows(df: pl.DataFrame) -> list[dict[str, Any]]:
    """Convert DataFrame to list of dicts for JSON serialization."""
    rows = []
    for row in df.iter_rows(named=True):
        rows.append(row)
    return rows
