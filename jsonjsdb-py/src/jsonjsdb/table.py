"""Table class wrapping a Polars DataFrame."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Generic, TypeVar, overload

import polars as pl

from .types import ID, Operator

if TYPE_CHECKING:
    from .database import Jsonjsdb

T = TypeVar("T")


class Table(Generic[T]):
    """A table backed by a Polars DataFrame.

    Use runtime_fields to specify columns that should exist in memory
    but never be persisted to JSON files:

        class UserTable(Table[User]):
            runtime_fields = {"_seen", "_processed"}
    """

    runtime_fields: ClassVar[set[str]] = set()

    def __init__(
        self,
        name: str,
        db: Jsonjsdb | None = None,
        df: pl.DataFrame | None = None,
    ) -> None:
        self._name = name
        self._db = db
        self._df = df if df is not None else pl.DataFrame()

    @property
    def name(self) -> str:
        return self._name

    @property
    def df(self) -> pl.DataFrame:
        return self._df

    def get_persistable_df(self) -> pl.DataFrame:
        """Return DataFrame with runtime_fields columns excluded."""
        if not self.runtime_fields:
            return self._df

        cols_to_keep = [
            col for col in self._df.columns if col not in self.runtime_fields
        ]
        return self._df.select(cols_to_keep)

    @property
    def having(self) -> HavingProxy[T]:
        """Access relational queries via having.{table}(id)."""
        if self._db is None:
            raise RuntimeError("Cannot use 'having' without a database context")
        return HavingProxy(self, self._db)

    def get(self, id: ID) -> T | None:
        """Get a single row by ID, or None if not found."""
        result = self._df.filter(pl.col("id") == id)
        if result.is_empty():
            return None
        return self._row_to_dict(result.row(0, named=True))

    def all(self) -> list[T]:
        """Get all rows as a list of dicts."""
        return [self._row_to_dict(row) for row in self._df.iter_rows(named=True)]

    @overload
    def where(self, column: str, op: Operator, value: Any) -> list[T]: ...

    @overload
    def where(self, column: str, op: Operator) -> list[T]: ...

    def where(self, column: str, op: Operator, value: Any = None) -> list[T]:
        """Filter rows by a condition.

        Operators: ==, !=, >, >=, <, <=, in, is_null, is_not_null
        """
        col = pl.col(column)
        expr: pl.Expr

        if op == "==":
            expr = col == value
        elif op == "!=":
            expr = col != value
        elif op == ">":
            expr = col > value
        elif op == ">=":
            expr = col >= value
        elif op == "<":
            expr = col < value
        elif op == "<=":
            expr = col <= value
        elif op == "in":
            expr = col.is_in(value)
        elif op == "is_null":
            expr = col.is_null()
        elif op == "is_not_null":
            expr = col.is_not_null()
        else:
            raise ValueError(f"Unknown operator: {op}")

        result = self._df.filter(expr)
        return [self._row_to_dict(row) for row in result.iter_rows(named=True)]

    def _row_to_dict(self, row: dict[str, Any]) -> T:
        """Convert a Polars row dict to API dict (T)."""
        return row  # type: ignore[return-value]

    # --- CRUD operations ---

    def add(self, row: T) -> None:
        """Add a single row. Raises ValueError if id missing or already exists."""
        row_dict: dict[str, Any] = row  # type: ignore[assignment]

        if "id" not in row_dict:
            raise ValueError("Row must have an 'id' field")

        row_id = str(row_dict["id"])
        if (
            not self._df.is_empty()
            and not self._df.filter(pl.col("id") == row_id).is_empty()
        ):
            raise ValueError(f"Row with id '{row_id}' already exists")

        prepared = self._prepare_row_for_storage(row_dict)
        new_df = pl.DataFrame([prepared])

        if self._df.is_empty():
            self._df = new_df
        else:
            self._df = pl.concat([self._df, new_df], how="diagonal")

    def add_all(self, rows: list[T]) -> None:
        """Add multiple rows."""
        for row in rows:
            self.add(row)

    def update(self, id: ID, **kwargs: Any) -> None:
        """Update a row by ID. Raises KeyError if not found."""
        if self._df.is_empty() or self._df.filter(pl.col("id") == id).is_empty():
            raise KeyError(f"Row with id '{id}' not found")

        updates: list[pl.Expr] = []
        for col_name in self._df.columns:
            if col_name in kwargs:
                value = kwargs[col_name]
                updates.append(
                    pl.when(pl.col("id") == id)
                    .then(pl.lit(value, allow_object=True))
                    .otherwise(pl.col(col_name))
                    .alias(col_name)
                )
            else:
                updates.append(pl.col(col_name))

        self._df = self._df.select(updates)

    def remove(self, id: ID) -> bool:
        """Remove a row by ID. Returns True if removed, False if not found."""
        if self._df.is_empty():
            return False

        original_len = len(self._df)
        self._df = self._df.filter(pl.col("id") != id)
        return len(self._df) < original_len

    def remove_all(self, ids: list[ID]) -> int:
        """Remove multiple rows by ID. Returns count of removed rows."""
        if self._df.is_empty():
            return 0

        original_len = len(self._df)
        self._df = self._df.filter(~pl.col("id").is_in(ids))
        return original_len - len(self._df)

    def _prepare_row_for_storage(self, row: dict[str, Any]) -> dict[str, Any]:
        """Prepare row for internal DataFrame storage.

        - Converts id to string
        - Keeps *_ids as lists (DataFrame stores them as List(String))
        """
        prepared = {}
        for key, value in row.items():
            if key == "id":
                prepared[key] = str(value)
            else:
                prepared[key] = value
        return prepared


class HavingProxy(Generic[T]):
    """Proxy for relational queries: table.having.{target}(id).

    Detects relation type automatically:
    - If {target}_id exists → one-to-many (filter by foreign key)
    - If {target}_ids exists → many-to-many (filter by list contains)
    - Special case: 'parent' → looks for parent_id (self-reference)
    """

    def __init__(self, table: Table[T], db: Jsonjsdb) -> None:
        self._table = table
        self._db = db

    def __getattr__(self, target: str) -> _RelationQuery[T]:
        return _RelationQuery(self._table, self._db, target)


class _RelationQuery(Generic[T]):
    """Callable that executes the relation query."""

    def __init__(self, table: Table[T], db: Jsonjsdb, target: str) -> None:
        self._table = table
        self._db = db
        self._target = target

    def __call__(self, id: ID) -> list[T]:
        columns = self._table.df.columns

        # Special case: 'parent' → parent_id (self-reference)
        lookup_target = "parent" if self._target == "parent" else self._target

        # Check for {target}_id (one-to-many)
        fk_col = f"{lookup_target}_id"
        if fk_col in columns:
            return self._table.where(fk_col, "==", id)

        # Check for {target}_ids (many-to-many)
        fk_ids_col = f"{lookup_target}_ids"
        if fk_ids_col in columns:
            result = self._table.df.filter(pl.col(fk_ids_col).list.contains(id))
            return [
                self._table._row_to_dict(row) for row in result.iter_rows(named=True)
            ]

        raise AttributeError(
            f"No relation '{self._target}' found in table '{self._table.name}'. "
            f"Expected column '{fk_col}' or '{fk_ids_col}'."
        )
