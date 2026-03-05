"""JSONJS database library for Python."""

from importlib.metadata import version

from .database import Jsonjsdb
from .table import Table
from .types import ID, Operator, TableRow

__version__ = version("jsonjsdb")
__all__ = ["Jsonjsdb", "Table", "ID", "Operator", "TableRow"]
