"""Load JSON files into Polars DataFrames."""

import json
from pathlib import Path
from typing import Any

import polars as pl


def load_table(path: Path) -> pl.DataFrame:
    """Load a JSON file into a Polars DataFrame.

    - Converts 'id' column to string type
    - Converts '*_ids' columns from comma-separated strings to list[str]
    """
    df = pl.read_json(path)

    if df.is_empty():
        return df

    transforms: list[pl.Expr] = []

    for col_name in df.columns:
        col_type = df.schema[col_name]

        if col_name == "id":
            transforms.append(pl.col("id").cast(pl.Utf8).alias("id"))
        elif col_name.endswith("_ids"):
            transforms.append(_convert_ids_column(col_name, col_type))
        else:
            transforms.append(pl.col(col_name))

    return df.select(transforms)


def _convert_ids_column(col_name: str, col_type: pl.DataType) -> pl.Expr:
    """Convert a *_ids column from comma-separated string to list[str]."""
    col = pl.col(col_name)

    if col_type == pl.Utf8 or col_type == pl.String:
        return (
            pl.when(col.is_null() | (col == ""))
            .then(pl.lit([]).cast(pl.List(pl.Utf8)))
            .otherwise(col.str.split(","))
            .alias(col_name)
        )

    return col


def load_table_index(path: Path) -> list[dict[str, Any]]:
    """Load __table__.json which contains table metadata."""
    with open(path, encoding="utf-8") as f:
        return json.load(f)
