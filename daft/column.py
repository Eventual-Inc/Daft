from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Union


class ColumnType(Enum):
    RESULT = "result"
    BACKED = "backed"


@dataclass
class Column:
    ...
    # name: str
    # column_type: Optional[ColumnType] = None
    # operation: Optional[Operation] = None

    # @classmethod
    # def from_arg(cls, c: "ColumnArgType") -> Column:
    #     if isinstance(c, Column):
    #         return c
    #     elif isinstance(c, str):
    #         return Column(c)
    #     raise ValueError(f"unknown type for column {type(c)}")


class ColumnExpression:
    ...


ColumnArgType = Union[str, Column]
