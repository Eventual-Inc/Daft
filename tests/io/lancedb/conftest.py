from __future__ import annotations

import pytest
from packaging.version import Version

from daft.dependencies import pa

# Try to import lance; if it fails, all tests in this directory will be skipped.
lance = pytest.importorskip("lance")


@pytest.fixture(scope="session", autouse=True)
def check_pyarrow_version():
    """Check pyarrow version and skip all tests in this directory if version is too old."""
    pyarrow_version = Version(pa.__version__)
    if pyarrow_version < Version("9.0.0"):
        pytest.skip(f"lance integration requires pyarrow>=9.0.0, but found {pyarrow_version}")
