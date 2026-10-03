from __future__ import annotations

from typing import TYPE_CHECKING

import daft

if TYPE_CHECKING:
    from daft.expressions import Expression


def greet(name: Expression) -> Expression:
    """Greet someone by name."""
    return daft.get_function("greet", name)


def splat(value: Expression, count: int) -> Expression:
    """Repeat each value into a fixed-size list of length `count`.

    `count` must be a literal: the output type's width is resolved during
    planning from the literal's value.
    """
    return daft.get_function("splat", value, daft.lit(count))


def string_count(name: Expression) -> Expression:
    """Count non-null strings."""
    return daft.get_aggregate_function("string_count", name)
