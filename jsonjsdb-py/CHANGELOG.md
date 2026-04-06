# Changelog

## 0.8.6

fix: `load_table()` fails with `ComputeError` when a nullable column is all `null` in the first inferred rows and contains a numeric value later

## 0.8.5

fix: `add_all()` fails with `ComputeError` when leading rows have `None` and later rows have `str` values

## 0.8.4

fix: Convert NaN to null in JSON output for valid JSON (RFC 8259) and TypeScript compatibility

## 0.8.3

fix: Add explicit `encoding="utf-8"` to all file operations (Windows compatibility)
ref: Move inline `import json` to module top in loader.py

## 0.8.2

fix: `parent_entity_id` now correct for tables with composite ID (was using first segment instead of FK value)

## 0.8.1

fix: `__table__.json` now includes itself in the list (align with JS behavior)

## 0.8.0

add: `parent_relations` parameter on `save()` - define child→parent table mappings
add: Cascade filtering - child add/delete entries removed when parent has same operation
add: `parent_entity` field in `EvolutionEntry` - tracks parent table name for FK relationships
add: `filter_cascade_entries()` function exported from evolution module
ref: `_get_first_parent_id()` → `_get_parent_info()` - now returns (parent_entity, parent_entity_id)
BREAKING: Evolution schema changed - new `parent_entity` column in evolution.json/xlsx

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
