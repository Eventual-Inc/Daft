from __future__ import annotations

import importlib

import pytest

from daft.dependencies import pa

# Skip if pyarrow < 9 or lance not installed
PYARROW_LOWER_BOUND_SKIP = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) < (9, 0, 0)
LANCE_INSTALLED = importlib.util.find_spec("lance") is not None
pytestmark = pytest.mark.skipif(
    PYARROW_LOWER_BOUND_SKIP or not LANCE_INSTALLED,
    reason="lance requires pyarrow>=9 and lance installed",
)
