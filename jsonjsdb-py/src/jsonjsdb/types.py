"""Type definitions for jsonjsdb."""

from typing import Any, Literal

ID = str

TableRow = dict[str, Any]

Operator = Literal["==", "!=", ">", ">=", "<", "<=", "in", "is_null", "is_not_null"]
