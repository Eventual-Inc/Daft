from daft.dataclasses import dataclass

from typing import Any, Dict, Callable, Literal, cast, Union

from icebridge.client import IceBridgeClient, IcebergExpression

# Node IDs in the NetworkX graph are uuid strings
NodeId = str

# Columns are just aliases for strings
QueryColumn = str

Comparator = Union[
    Literal[">"],
    Literal[">="],
    Literal["<"],
    Literal["<="],
    Literal["="],
]
COMPARATOR_MAP: Dict[Comparator, str] = {
    ">": "__gt__",
    ">=": "__ge__",
    "<": "__lt__",
    "<=": "__le__",
    "=": "__eq__",
}
ICEBRIDGE_COMPARATOR_MAP: Dict[Comparator, Callable[[IceBridgeClient, str, Any], IcebergExpression]] = {
    ">": IcebergExpression.gt,
    ">=": IcebergExpression.gte,
    "<": IcebergExpression.lt,
    "<=": IcebergExpression.lte,
    "=": IcebergExpression.equal,
}


@dataclass
class WriteDatarepoStageOutput:
    filepath: str
