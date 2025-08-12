from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from daft.expressions import Expression

from .window import (
    row_number,
    rank,
    dense_rank,
)
from .misc import monotonically_increasing_id, format
from .columnar import (
    columns_sum,
    columns_mean,
    columns_avg,
    columns_min,
    columns_max,
)
from .llm import llm_generate


def file(expr: Expression) -> Expression:
    """Converts either a string containing a file reference, or a binary column to a `daft.File` reference.

    If the input is a string, it is assumed to be a file path and is converted to a `daft.File`.
    If the input is a binary column, it is converted to a `daft.File` where the entire contents are buffered in memory.
    """
    return expr._eval_expressions("file")


__all__ = [
    "columns_avg",
    "columns_max",
    "columns_mean",
    "columns_min",
    "columns_sum",
    "dense_rank",
    "file",
    "format",
    "llm_generate",
    "monotonically_increasing_id",
    "rank",
    "row_number",
]
