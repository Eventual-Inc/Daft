from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from daft.types import ExpressionType

if TYPE_CHECKING:
    from daft.expressions import Expression


@dataclass(frozen=True)
class Field:
    name: str
    dtype: ExpressionType

    def to_column_expression(self) -> Expression:
        from daft.expressions import col

        return col(self.name)
