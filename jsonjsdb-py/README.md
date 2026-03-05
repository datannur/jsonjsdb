# jsonjsdb

[![PyPI version](https://img.shields.io/pypi/v/jsonjsdb.svg)](https://pypi.org/project/jsonjsdb/)
[![Python](https://img.shields.io/badge/python-≥3.9-blue.svg)](https://pypi.org/project/jsonjsdb/)
[![CI](https://github.com/datannur/jsonjsdb/actions/workflows/ci.yml/badge.svg)](https://github.com/datannur/jsonjsdb/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/datannur/jsonjsdb/branch/main/graph/badge.svg?flag=python)](https://codecov.io/gh/datannur/jsonjsdb)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Python library for JSONJS databases with full CRUD support and relational queries.

## Features

- **Read & Write**: Full CRUD operations
- **Typed API**: Optional TypedDict support with autocompletion
- **Relational queries**: `having.{table}(id)` for one-to-many and many-to-many
- **Filtering**: `where()` with operators (`==`, `!=`, `>`, `in`, `is_null`, etc.)
- **TypeScript compatible**: Same file format as the TypeScript jsonjsdb library

## Installation

```bash
pip install jsonjsdb
```

## Quick Start

```python
from jsonjsdb import Jsonjsdb

db = Jsonjsdb("path/to/db")

# Read
user = db["user"].get("user_1")
active = db["user"].where("status", "==", "active")

# Write
db["user"].add({"id": "u1", "name": "Alice", "tag_ids": []})
db["user"].update("u1", name="Alice Updated")
db.save()
```

## Typed Access

### With TypedDict (dict-style)

```python
from typing import TypedDict
from jsonjsdb import Jsonjsdb, Table

class User(TypedDict):
    id: str
    name: str
    tag_ids: list[str]

class MyDB(Jsonjsdb):
    user: Table[User]

db = MyDB("path/to/db")
user = db.user.get("user_1")  # Returns User | None
print(user["name"])           # Dict-style access
```

### With Dataclass (attribute-style)

```python
from dataclasses import dataclass
from jsonjsdb import Jsonjsdb, Table

@dataclass
class User:
    id: str
    name: str
    tag_ids: list[str]

# Pass entity_type to get dataclass instances
table: Table[User] = Table("user", entity_type=User)

user = table.get("user_1")    # Returns User dataclass
print(user.name)              # Attribute-style access

table.add(User(id="u2", name="Bob", tag_ids=[]))
```

## API Reference

### CRUD

```python
db.user.add({"id": "u1", "name": "Alice", ...})  # Add row (id required)
db.user.add_all([...])                           # Add multiple rows

db.user.get("u1")                                # → User | None
db.user.all()                                    # → list[User]
db.user.count                                    # → int (number of rows)

db.user.update("u1", name="New Name")            # Update fields
db.user.update_many(["u1", "u2"], status="x")   # Batch update → int (count)
db.user.remove("u1")                             # → bool
db.user.remove_all(["u1", "u2"])                 # → int (count)
db.user.remove_where("status", "==", "inactive") # → int (count)
```

### Filtering

```python
db.user.where("status", "==", "active")          # Equality
db.user.where("age", ">", 18)                    # Comparison (>, >=, <, <=)
db.user.where("status", "in", ["a", "b"])        # In list
db.user.where("email", "is_null")                # Null check (is_not_null)

db.user.ids_where("status", "==", "active")      # → list[str] (IDs only, faster)
```

### Relations

```python
db.email.having.user("user_1")      # One-to-many: where user_id == "user_1"
db.user.having.tag("tag_1")         # Many-to-many: where tag_ids contains "tag_1"
db.folder.having.parent("folder_1") # Hierarchy: where parent_id == "folder_1"
```

### Save / New Database

```python
db.save()                # Save to original path
db.save("new/path")      # Save to new location

db = MyDB()              # Create empty in-memory DB
db.user.add({...})
db.save("path/to/db")    # Path required on first save
```

### Runtime Fields

Exclude fields from persistence (in-memory only):

```python
from jsonjsdb import Table

# Option 1: Via constructor
table: Table[dict] = Table("user", runtime_fields={"_seen", "_processed"})

# Option 2: Via subclass
class UserTable(Table[User]):
    runtime_fields = {"_seen", "_processed"}

table.add({"id": "1", "name": "Alice", "_seen": True})

table.get("1")["_seen"]           # → True (in memory)
table.get_persistable_df()        # → DataFrame without _seen
# On save(), runtime_fields are automatically excluded
```

## File Format

- `__table__.json` — Index of tables with metadata
- `{table}.json` — Data as array of objects
- `{table}.json.js` — Same data for browser (JavaScript)

### Column Conventions

| Column | Description |
|--------|-------------|
| `id` | Primary key (always string) |
| `xxx_id` | Foreign key to table `xxx` |
| `xxx_ids` | Many-to-many (comma-separated in file, `list[str]` in API) |
| `parent_id` | Self-reference for hierarchies |

## License

MIT
