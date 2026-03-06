# Changelog

## 0.7.4

add: `write_js` parameter on `save()` - skip `.json.js` generation when `False`
fix: `write_table_index()` now writes `__table__.json.js` (was missing)

## 0.7.3

add: Optional `timestamp` parameter on `save()` for deterministic outputs

## 0.7.2

fix: Skip tracking for initial creation (no previous data to compare)

## 0.7.1

fix: Skip internal tables (`evolution`, `__table__`) when loading database

## 0.7.0

add: Evolution tracking - automatic change detection (add/delete/update) on `save()`
add: `evolution_xlsx` parameter - use Excel as source for easy log editing
add: `track_evolution` parameter - opt-out of tracking (enabled by default)
add: `EvolutionEntry` dataclass and `compare_datasets()` exported from package
dep: `openpyxl>=3.1.0` now a required dependency

## 0.6.0

add: `Table.is_empty` property - check if table has no rows
add: `Table.exists(id)` - check if row exists without fetching it
add: `Table.upsert(entity)` - add or update in single call (returns True if added, False if updated)

## 0.5.0

add: `Table.get_by(column, value)` - lookup by column value (returns single entity or None)
add: `Table.ids_having.{relation}(id)` - relational query returning IDs only
perf: `Table.add_all()` now batch inserts in single Polars concat (was N individual inserts)

## 0.4.0

add: `Table.count` property - returns number of rows
add: `Table.update_many(ids, **kwargs)` - batch update multiple rows by ID
add: `Table.ids_where(col, op, value)` - returns IDs matching condition without entity conversion
add: `Table.remove_where(col, op, value)` - removes rows matching condition

## 0.3.2

fix: `Table.add()` now uses `how="diagonal_relaxed"` for `pl.concat` to handle type coercion when adding rows to tables with Null-typed columns

## 0.3.1

fix: `get()`, `where()`, and `having` now handle empty tables gracefully (return `None`/`[]` instead of `ColumnNotFoundError`)

## 0.3.0

add: `entity_type` parameter - Table returns dataclasses instead of dicts

## 0.2.1

fix: `Table.runtime_fields` typing - now an instance attribute instead of `ClassVar`

## 0.2.0

add: `runtime_fields` - exclude fields from persistence (in-memory only)

## 0.1.0

add: initial release
