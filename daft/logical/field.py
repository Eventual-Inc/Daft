from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from daft.datatype import DataType

if TYPE_CHECKING:
    from daft.expressions import Expression


@dataclass(frozen=True)
class Field:
    name: str
    dtype: DataType

    def to_column_expression(self) -> Expression:
        from daft.expressions import col

        return col(self.name)
