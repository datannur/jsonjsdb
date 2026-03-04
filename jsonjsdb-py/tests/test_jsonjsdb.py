"""Tests for jsonjsdb package."""

import jsonjsdb


def test_version_exists():
    """Package should expose a version string."""
    assert hasattr(jsonjsdb, "__version__")
    assert isinstance(jsonjsdb.__version__, str)
    assert len(jsonjsdb.__version__) > 0
