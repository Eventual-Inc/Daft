from __future__ import annotations

from dataclasses import dataclass

from daft.types import ExpressionType


@dataclass(frozen=True)
class Field:
    name: str
    dtype: ExpressionType
