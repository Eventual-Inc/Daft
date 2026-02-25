"""Example Daft extension: increment(expr) adds 1 to an int64 column."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from daft.expressions import Expression

# Import triggers _native loader, but doesn't require a session
from daft_ext_example import _native


def increment(expr: Expression) -> Expression:
    """Adds 1 to each value in an int64 column."""
    from daft.session import get_function

    return get_function("increment", expr)
