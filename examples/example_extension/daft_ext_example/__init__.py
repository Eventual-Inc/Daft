"""Example Daft extension: increment(expr) adds 1 to an int32 column."""

from __future__ import annotations

import platform
from pathlib import Path
from typing import TYPE_CHECKING

from daft.session import get_function

if TYPE_CHECKING:
    from daft.expressions import Expression


def increment(expr: Expression) -> Expression:
    """Adds 1 to each value in an int32 column."""
    return get_function("increment", expr)
