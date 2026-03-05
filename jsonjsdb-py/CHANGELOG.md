# Changelog

## unreleased

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
