"""Table class wrapping a Polars DataFrame."""

from __future__ import annotations

import dataclasses
import types
from typing import (
    TYPE_CHECKING,
    Any,
    ForwardRef,
    Generic,
    TypeVar,
    Union,
    get_args,
    get_origin,
    get_type_hints,
    overload,
)

import polars as pl

from .types import ID, Operator

if TYPE_CHECKING:
    from .database import Jsonjsdb

T = TypeVar("T")


class Table(Generic[T]):
    """A table backed by a Polars DataFrame.

    Supports dataclass entities for ergonomic access (entity.name vs entity["name"]):

        @dataclass
        class User:
            id: str
            name: str

        table = Table("user", entity_type=User)
        user = table.get("u1")  # → User dataclass
        print(user.name)

    Use runtime_fields to specify columns that should exist in memory
    but never be persisted to JSON files.
    """

    runtime_fields: set[str] = set()

    def __init__(
        self,
        name: str,
        db: Jsonjsdb | None = None,
        df: pl.DataFrame | None = None,
        runtime_fields: set[str] | None = None,
        entity_type: type[T] | None = None,
    ) -> None:
        self._name = name
        self._db = db
        self._df = df if df is not None else pl.DataFrame()
        self._entity_type = entity_type
        self._storage_schema = _storage_schema_from_entity_type(entity_type)
        if self._storage_schema:
            self._df = self._apply_storage_schema(self._df)
        if runtime_fields is not None:
            self.runtime_fields = runtime_fields
        elif type(self).runtime_fields is not Table.runtime_fields:
            self.runtime_fields = type(self).runtime_fields.copy()
        else:
            self.runtime_fields = set()

    @property
    def name(self) -> str:
        return self._name

    @property
    def df(self) -> pl.DataFrame:
        return self._df

    @property
    def count(self) -> int:
        """Number of rows in the table."""
        return self._df.height

    @property
    def is_empty(self) -> bool:
        """Whether the table has no rows."""
        return self._df.is_empty()

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

    @property
    def ids_having(self) -> IdsHavingProxy:
        """Access relational queries returning IDs only via ids_having.{table}(id)."""
        if self._db is None:
            raise RuntimeError("Cannot use 'ids_having' without a database context")
        return IdsHavingProxy(self, self._db)

    def get(self, id: ID) -> T | None:
        """Get a single row by ID, or None if not found."""
        if self._df.is_empty() or "id" not in self._df.columns:
            return None
        result = self._df.filter(pl.col("id") == id)
        if result.is_empty():
            return None
        return self._row_to_entity(result.row(0, named=True))

    def exists(self, id: ID) -> bool:
        """Check if a row with the given ID exists."""
        if self._df.is_empty() or "id" not in self._df.columns:
            return False
        return not self._df.filter(pl.col("id") == id).is_empty()

    def get_by(self, column: str, value: Any) -> T | None:
        """Get single row by column value, or None if not found."""
        results = self.where(column, "==", value)
        return results[0] if results else None

    def all(self) -> list[T]:
        """Get all rows as a list of entities."""
        return [self._row_to_entity(row) for row in self._df.iter_rows(named=True)]

    @overload
    def where(self, column: str, op: Operator, value: Any) -> list[T]: ...

    @overload
    def where(self, column: str, op: Operator) -> list[T]: ...

    def where(self, column: str, op: Operator, value: Any = None) -> list[T]:
        """Filter rows by a condition.

        Operators: ==, !=, >, >=, <, <=, in, is_null, is_not_null
        """
        if self._df.is_empty() or column not in self._df.columns:
            return []
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
        return [self._row_to_entity(row) for row in result.iter_rows(named=True)]

    def ids_where(self, column: str, op: Operator, value: Any = None) -> list[ID]:
        """Return IDs of rows matching condition (no entity conversion).

        More efficient than [x.id for x in table.where(...)] when only IDs are needed.
        """
        if self._df.is_empty() or column not in self._df.columns:
            return []
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

        return self._df.filter(expr)["id"].to_list()

    def _row_to_entity(self, row: dict[str, Any]) -> T:
        """Convert a Polars row dict to entity (dataclass or dict)."""
        if self._entity_type is not None and dataclasses.is_dataclass(
            self._entity_type
        ):
            return self._entity_type(**row)
        return row  # type: ignore[return-value]

    # --- CRUD operations ---

    def _entity_to_dict(self, entity: T) -> dict[str, Any]:
        """Convert entity (dataclass or dict) to dict."""
        if dataclasses.is_dataclass(entity) and not isinstance(entity, type):
            return dataclasses.asdict(entity)
        return entity  # type: ignore[return-value]

    def add(self, row: T) -> None:
        """Add a single row. Raises ValueError if id missing or already exists."""
        row_dict = self._entity_to_dict(row)

        if "id" not in row_dict:
            raise ValueError("Row must have an 'id' field")

        row_id = str(row_dict["id"])
        if (
            not self._df.is_empty()
            and not self._df.filter(pl.col("id") == row_id).is_empty()
        ):
            raise ValueError(f"Row with id '{row_id}' already exists")

        prepared = self._prepare_row_for_storage(row_dict)
        new_df = self._apply_storage_schema(pl.DataFrame([prepared]))

        if self._df.is_empty():
            self._df = new_df
        else:
            self._df = pl.concat([self._df, new_df], how="diagonal_relaxed")

    def upsert(self, row: T) -> bool:
        """Add or update a row. Returns True if added, False if updated."""
        row_dict = self._entity_to_dict(row)
        if "id" not in row_dict:
            raise ValueError("Row must have an 'id' field")

        row_id = str(row_dict["id"])
        if self.exists(row_id):
            self.update(row_id, **{k: v for k, v in row_dict.items() if k != "id"})
            return False
        else:
            self.add(row)
            return True

    def add_all(self, rows: list[T]) -> None:
        """Add multiple rows in a single batch operation."""
        if not rows:
            return

        dicts = [self._entity_to_dict(r) for r in rows]

        for d in dicts:
            if "id" not in d:
                raise ValueError("Row must have an 'id' field")

        new_ids = {str(d["id"]) for d in dicts}
        if len(new_ids) != len(dicts):
            raise ValueError("Duplicate IDs in rows to add")

        if not self._df.is_empty():
            existing = set(self._df["id"].to_list())
            conflicts = new_ids & existing
            if conflicts:
                raise ValueError(f"IDs already exist: {conflicts}")

        prepared = [self._prepare_row_for_storage(d) for d in dicts]
        new_df = self._apply_storage_schema(
            pl.DataFrame(prepared, infer_schema_length=None)
        )

        if self._df.is_empty():
            self._df = new_df
        else:
            self._df = pl.concat([self._df, new_df], how="diagonal_relaxed")

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

        self._df = self._apply_storage_schema(self._df.select(updates), set(kwargs))

    def update_many(self, ids: list[ID], **kwargs: Any) -> int:
        """Update multiple rows by ID. Returns count of updated rows."""
        if self._df.is_empty():
            return 0

        mask = pl.col("id").is_in(ids)
        count = self._df.filter(mask).height
        if count == 0:
            return 0

        updates: list[pl.Expr] = []
        for col_name in self._df.columns:
            if col_name in kwargs:
                value = kwargs[col_name]
                updates.append(
                    pl.when(mask)
                    .then(pl.lit(value, allow_object=True))
                    .otherwise(pl.col(col_name))
                    .alias(col_name)
                )
            else:
                updates.append(pl.col(col_name))

        self._df = self._apply_storage_schema(self._df.select(updates), set(kwargs))
        return count

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

    def remove_where(self, column: str, op: Operator, value: Any = None) -> int:
        """Remove rows matching condition. Returns count of removed rows."""
        ids = self.ids_where(column, op, value)
        return self.remove_all(ids)

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

    def set_entity_type(self, entity_type: type[T] | None) -> None:
        self._entity_type = entity_type
        self._storage_schema = _storage_schema_from_entity_type(entity_type)
        if self._storage_schema:
            self._df = self._apply_storage_schema(self._df)

    def _apply_storage_schema(
        self, df: pl.DataFrame, column_names: set[str] | None = None
    ) -> pl.DataFrame:
        if not self._storage_schema or df.is_empty():
            return df

        schema_columns = (
            set(self._storage_schema) if column_names is None else column_names
        )
        transforms: list[pl.Expr] = []
        for col_name in df.columns:
            col = pl.col(col_name)
            if col_name in schema_columns and col_name in self._storage_schema:
                target_dtype = self._storage_schema[col_name]
                _validate_storage_cast(df, col_name, target_dtype)
                col = col.cast(target_dtype)
            transforms.append(col.alias(col_name))
        return df.select(transforms)


def _storage_schema_from_entity_type(
    entity_type: type[Any] | None,
) -> dict[str, Any]:
    if entity_type is None or entity_type is dict:
        return {}

    annotations = _entity_annotations(entity_type)
    schema: dict[str, Any] = {}
    for field_name, annotation in annotations.items():
        dtype = _annotation_to_polars_dtype(annotation)
        if dtype is not None:
            schema[field_name] = dtype
    return schema


def _validate_storage_cast(df: pl.DataFrame, col_name: str, target_dtype: Any) -> None:
    source_dtype = df.schema[col_name]
    if not (_dtype_is_integer(target_dtype) and _dtype_is_float(source_dtype)):
        return

    col = pl.col(col_name)
    invalid = df.filter(col.is_not_null() & (col.is_nan() | (col != col.floor())))
    if not invalid.is_empty():
        raise ValueError(
            f"Column '{col_name}' contains non-integer values and cannot be stored "
            f"as {target_dtype}"
        )


def _dtype_is_integer(dtype: Any) -> bool:
    is_integer = getattr(dtype, "is_integer", None)
    return bool(is_integer()) if callable(is_integer) else False


def _dtype_is_float(dtype: Any) -> bool:
    is_float = getattr(dtype, "is_float", None)
    return bool(is_float()) if callable(is_float) else False


def _entity_annotations(entity_type: type[Any]) -> dict[str, Any]:
    if dataclasses.is_dataclass(entity_type):
        return {field.name: field.type for field in dataclasses.fields(entity_type)}

    try:
        return get_type_hints(entity_type)
    except TypeError:
        return dict(getattr(entity_type, "__annotations__", {}))


def _annotation_to_polars_dtype(annotation: Any) -> Any | None:
    annotation = _unwrap_optional(annotation)

    if isinstance(annotation, str):
        return _string_annotation_to_polars_dtype(annotation)

    origin = get_origin(annotation)
    if origin is list:
        item_dtype = _annotation_to_polars_dtype(get_args(annotation)[0])
        return pl.List(item_dtype or pl.Utf8)

    if annotation is bool:
        return pl.Boolean
    if annotation is int:
        return pl.Int64
    if annotation is float:
        return pl.Float64
    if annotation is str:
        return pl.Utf8
    return None


def _unwrap_optional(annotation: Any) -> Any:
    if isinstance(annotation, ForwardRef):
        annotation = annotation.__forward_arg__

    origin = get_origin(annotation)
    union_type = getattr(types, "UnionType", None)
    if origin is Union or (union_type is not None and origin is union_type):
        args = [arg for arg in get_args(annotation) if arg is not type(None)]
        if len(args) == 1:
            return args[0]

    return annotation


def _string_annotation_to_polars_dtype(annotation: str) -> Any | None:
    normalized = annotation.strip().replace("typing.", "")

    optional_inner = _bracket_inner(normalized, "Optional")
    if optional_inner is not None:
        return _string_annotation_to_polars_dtype(optional_inner)

    union_inner = _bracket_inner(normalized, "Union")
    if union_inner is not None:
        union_parts = [part.strip() for part in union_inner.split(",")]
        non_null_parts = [
            part for part in union_parts if part not in {"None", "NoneType"}
        ]
        if len(non_null_parts) == 1:
            return _string_annotation_to_polars_dtype(non_null_parts[0])

    pipe_parts = [part.strip() for part in normalized.split("|")]
    if len(pipe_parts) > 1:
        non_null_parts = [
            part for part in pipe_parts if part not in {"None", "NoneType"}
        ]
        if len(non_null_parts) == 1:
            return _string_annotation_to_polars_dtype(non_null_parts[0])

    list_inner = _bracket_inner(normalized, "list") or _bracket_inner(
        normalized, "List"
    )
    if list_inner is not None:
        item_dtype = _string_annotation_to_polars_dtype(list_inner)
        return pl.List(item_dtype or pl.Utf8)

    if normalized == "bool":
        return pl.Boolean
    if normalized == "int":
        return pl.Int64
    if normalized == "float":
        return pl.Float64
    if normalized == "str":
        return pl.Utf8
    return None


def _bracket_inner(annotation: str, name: str) -> str | None:
    prefix = f"{name}["
    if annotation.startswith(prefix) and annotation.endswith("]"):
        return annotation[len(prefix) : -1].strip()
    return None


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


class IdsHavingProxy:
    """Proxy for relational queries returning IDs: table.ids_having.{target}(id)."""

    def __init__(self, table: Table[Any], db: Jsonjsdb) -> None:
        self._table = table
        self._db = db

    def __getattr__(self, target: str) -> _IdsRelationQuery:
        return _IdsRelationQuery(self._table, self._db, target)


class _RelationQuery(Generic[T]):
    """Callable that executes the relation query."""

    def __init__(self, table: Table[T], db: Jsonjsdb, target: str) -> None:
        self._table = table
        self._db = db
        self._target = target

    def __call__(self, id: ID) -> list[T]:
        if self._table.df.is_empty():
            return []
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
                self._table._row_to_entity(row) for row in result.iter_rows(named=True)
            ]

        raise AttributeError(
            f"No relation '{self._target}' found in table '{self._table.name}'. "
            f"Expected column '{fk_col}' or '{fk_ids_col}'."
        )


class _IdsRelationQuery:
    """Callable that executes the relation query and returns IDs."""

    def __init__(self, table: Table[Any], db: Jsonjsdb, target: str) -> None:
        self._table = table
        self._db = db
        self._target = target

    def __call__(self, id: ID) -> list[ID]:
        if self._table.df.is_empty():
            return []
        columns = self._table.df.columns

        lookup_target = "parent" if self._target == "parent" else self._target

        fk_col = f"{lookup_target}_id"
        if fk_col in columns:
            return self._table.ids_where(fk_col, "==", id)

        fk_ids_col = f"{lookup_target}_ids"
        if fk_ids_col in columns:
            result = self._table.df.filter(pl.col(fk_ids_col).list.contains(id))
            return result["id"].to_list()

        raise AttributeError(
            f"No relation '{self._target}' found in table '{self._table.name}'. "
            f"Expected column '{fk_col}' or '{fk_ids_col}'."
        )
