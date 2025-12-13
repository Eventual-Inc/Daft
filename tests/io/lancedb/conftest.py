from __future__ import annotations

import pytest
from packaging.version import Version

from daft.dependencies import pa

# Try to import lance; if it fails, all tests in this directory will be skipped.
lance = pytest.importorskip("lance")


def pytest_collection_modifyitems(config, items):
    pyarrow_version = Version(pa.__version__)
    if pyarrow_version < Version("9.0.0"):
        skip_marker = pytest.mark.skip(reason=f"lance integration requires pyarrow>=9.0.0, but found {pyarrow_version}")
        for item in items:
            item.add_marker(skip_marker)
