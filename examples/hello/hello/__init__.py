from __future__ import annotations

from typing import TYPE_CHECKING

import daft

if TYPE_CHECKING:
    from daft.expressions import Expression


def greet(name: Expression) -> Expression:
    """Greet someone by name."""
    return daft.get_function("greet", name)


def byte_length(input: Expression) -> Expression:
    """Return the byte length of a string or binary column."""
    return daft.get_function("byte_length", input)


def string_count(name: Expression) -> Expression:
    """Count non-null strings."""
    return daft.get_aggregate_function("string_count", name)
