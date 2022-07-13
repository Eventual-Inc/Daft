from __future__ import annotations

from typing import Union, Optional, ForwardRef

from dataclasses import dataclass
from enum import Enum

Operation = ForwardRef('Operation')


class ColumnType(Enum):
    RESULT = 'result'
    BACKED = 'backed'


@dataclass
class Column:
    name: str
    column_type: Optional[ColumnType] = None
    operation: Optional[Operation] = None

    def alias(self, name:str) -> Column:
        from daft.operations import Operation
        return Column(
            name=name,
            column_type=ColumnType.RESULT,
            operation=Operation("alias_pl", [self])
        )

    @classmethod
    def from_arg(cls, c: "ColumnArgType") -> Column:
        if isinstance(c, Column):
            return c
        elif isinstance(c, str):
            return Column(c)
        raise ValueError(f"unknown type for column {type(c)}")


class ColumnExpression:
    ...

ColumnArgType = Union[str, Column]

