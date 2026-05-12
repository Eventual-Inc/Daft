from __future__ import annotations

from typing import TYPE_CHECKING

import daft

if TYPE_CHECKING:
    from daft.expressions import Expression


def greet(name: Expression) -> Expression:
    """Greet someone by name."""
    return daft.get_function("greet", name)


def string_count(name: Expression) -> Expression:
    """Count non-null strings."""
    return daft.get_aggregate_function("string_count", name)
