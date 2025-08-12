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


def to_file(expr: Expression) -> Expression:
    return expr._eval_expressions("to_file")


__all__ = [
    "columns_avg",
    "columns_max",
    "columns_mean",
    "columns_min",
    "columns_sum",
    "dense_rank",
    "format",
    "llm_generate",
    "monotonically_increasing_id",
    "rank",
    "row_number",
    "to_file",
]
