"""Auto-mark tests: anything not in tests/integration/ gets the 'unit' marker."""

import pytest


def pytest_collection_modifyitems(config, items):
    for item in items:
        if "integration" not in str(item.fspath):
            item.add_marker(pytest.mark.unit)
