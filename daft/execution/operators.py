from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from functools import lru_cache, partial
from types import MappingProxyType
from typing import Dict, FrozenSet, Optional, Tuple, Type

import pyarrow as pa


class ExpressionType:
    @staticmethod
    def unknown() -> ExpressionType:
        return _TYPE_REGISTRY["unknown"]

    @staticmethod
    def from_py_type(obj_type: Type) -> ExpressionType:
        """Gets the appropriate ExpressionType from a Python object, or _TYPE_REGISTRY["unknown"]
        if unable to find the appropriate type. ExpressionTypes.Python is never returned.
        """
        global _TYPE_REGISTRY
        if hasattr(obj_type, "__origin__") and hasattr(obj_type, "__args__") and obj_type.__origin__ is tuple:
            type_registry_key = f"Tuple[{', '.join(arg.__name__ for arg in obj_type.__args__)}]"
            if type_registry_key in _TYPE_REGISTRY:
                return _TYPE_REGISTRY[type_registry_key]
            _TYPE_REGISTRY[type_registry_key] = CompositeExpressionType(
                tuple(ExpressionType.from_py_type(arg) for arg in obj_type.__args__)
            )
            return _TYPE_REGISTRY[type_registry_key]
        if obj_type not in _PY_TYPE_TO_EXPRESSION_TYPE:
            return _TYPE_REGISTRY["unknown"]
        return _PY_TYPE_TO_EXPRESSION_TYPE[obj_type]

    @staticmethod
    def from_arrow_type(datatype: pa.DataType) -> ExpressionType:
        if datatype not in _PYARROW_TYPE_TO_EXPRESSION_TYPE:
            return _TYPE_REGISTRY["unknown"]
        return _PYARROW_TYPE_TO_EXPRESSION_TYPE[datatype]


@dataclass(frozen=True)
class PrimitiveExpressionType(ExpressionType):
    class TypeEnum(Enum):

        UNKNOWN = 1
        NUMBER = 2
        LOGICAL = 3
        STRING = 4
        PYTHON = 5

    enum: PrimitiveExpressionType.TypeEnum


@dataclass(frozen=True)
class CompositeExpressionType(ExpressionType):
    args: Tuple[ExpressionType, ...]


_TYPE_REGISTRY: Dict[str, ExpressionType] = {
    "unknown": PrimitiveExpressionType(PrimitiveExpressionType.TypeEnum.UNKNOWN),
    "number": PrimitiveExpressionType(PrimitiveExpressionType.TypeEnum.NUMBER),
    "logical": PrimitiveExpressionType(PrimitiveExpressionType.TypeEnum.LOGICAL),
    "string": PrimitiveExpressionType(PrimitiveExpressionType.TypeEnum.STRING),
}


_PYARROW_TYPE_TO_EXPRESSION_TYPE = {
    pa.null(): _TYPE_REGISTRY["unknown"],
    pa.bool_(): _TYPE_REGISTRY["logical"],
    pa.int8(): _TYPE_REGISTRY["unknown"],
    pa.int16(): _TYPE_REGISTRY["number"],
    pa.int32(): _TYPE_REGISTRY["number"],
    pa.int64(): _TYPE_REGISTRY["number"],
    pa.uint8(): _TYPE_REGISTRY["number"],
    pa.uint16(): _TYPE_REGISTRY["number"],
    pa.uint32(): _TYPE_REGISTRY["number"],
    pa.uint64(): _TYPE_REGISTRY["number"],
    pa.float16(): _TYPE_REGISTRY["number"],
    pa.float32(): _TYPE_REGISTRY["number"],
    pa.float64(): _TYPE_REGISTRY["number"],
    pa.date32(): _TYPE_REGISTRY["unknown"],
    pa.date64(): _TYPE_REGISTRY["unknown"],
    pa.string(): _TYPE_REGISTRY["string"],
    pa.utf8(): _TYPE_REGISTRY["string"],
    pa.large_binary(): _TYPE_REGISTRY["unknown"],
    pa.large_string(): _TYPE_REGISTRY["string"],
    pa.large_utf8(): _TYPE_REGISTRY["string"],
}

_PY_TYPE_TO_EXPRESSION_TYPE = {
    int: _TYPE_REGISTRY["number"],
    float: _TYPE_REGISTRY["number"],
    str: _TYPE_REGISTRY["string"],
    bool: _TYPE_REGISTRY["logical"],
}


TypeMatrix = FrozenSet[Tuple[Tuple[ExpressionType, ...], ExpressionType]]


@dataclass(frozen=True)
class ExpressionOperator:
    name: str
    nargs: int
    type_matrix: TypeMatrix
    accepts_kwargs: bool = False
    symbol: Optional[str] = None

    def __post_init__(self) -> None:
        for k, v in self.type_matrix:
            assert len(k) == self.nargs, f"all keys in type matrix must have {self.nargs}"
            for sub_k in k:
                assert isinstance(sub_k, ExpressionType)
                assert sub_k != _TYPE_REGISTRY["unknown"]

            assert isinstance(v, ExpressionType), f"{v} is not an ExpressionType"
            assert v != _TYPE_REGISTRY["unknown"]

    @lru_cache
    def type_matrix_dict(self) -> MappingProxyType[Tuple[ExpressionType, ...], ExpressionType]:
        return MappingProxyType(dict(self.type_matrix))


_UnaryNumericalTM = frozenset({(_TYPE_REGISTRY["number"],): _TYPE_REGISTRY["number"]}.items())

_UnaryLogicalTM = frozenset({(_TYPE_REGISTRY["logical"],): _TYPE_REGISTRY["logical"]}.items())


_BinaryNumericalTM = frozenset({(_TYPE_REGISTRY["number"], _TYPE_REGISTRY["number"]): _TYPE_REGISTRY["number"]}.items())

_ComparisionTM = frozenset(
    {
        (_TYPE_REGISTRY["number"], _TYPE_REGISTRY["number"]): _TYPE_REGISTRY["logical"],
        (_TYPE_REGISTRY["string"], _TYPE_REGISTRY["string"]): _TYPE_REGISTRY["logical"],
    }.items()
)

_BinaryLogicalTM = frozenset(
    {
        (_TYPE_REGISTRY["logical"], _TYPE_REGISTRY["logical"]): _TYPE_REGISTRY["logical"],
    }.items()
)

_CountLogicalTM = frozenset(
    {
        (_TYPE_REGISTRY["number"],): _TYPE_REGISTRY["number"],
        (_TYPE_REGISTRY["logical"],): _TYPE_REGISTRY["number"],
        (_TYPE_REGISTRY["string"],): _TYPE_REGISTRY["number"],
    }.items()
)


_UOp = partial(ExpressionOperator, nargs=1, accepts_kwargs=False)
# Numerical Unary Ops
_NUop = partial(_UOp, type_matrix=_UnaryNumericalTM)

_BOp = partial(ExpressionOperator, nargs=2, accepts_kwargs=False)

# Numerical Binary Ops
_NBop = partial(_BOp, type_matrix=_BinaryNumericalTM)

# Comparison Binary Ops
_CBop = partial(_BOp, type_matrix=_ComparisionTM)

# _TYPE_REGISTRY["logical"] Binary Ops
_LBop = partial(_BOp, type_matrix=_BinaryLogicalTM)


class Operators(Enum):
    # UnaryOps
    # Arithmetic
    NEGATE = _NUop(name="negate", symbol="-")
    POSITIVE = _NUop(name="positive", symbol="+")
    ABS = _NUop(name="abs", symbol="abs")

    # Reductions
    SUM = _NUop(name="sum", symbol="sum")
    MEAN = _NUop(name="mean", symbol="mean")
    MIN = _NUop(name="min", symbol="min")
    MAX = _NUop(name="max", symbol="max")

    COUNT = _UOp(name="count", symbol="count", type_matrix=_CountLogicalTM)

    # _TYPE_REGISTRY["logical"]
    INVERT = _UOp(name="invert", symbol="~", type_matrix=_UnaryLogicalTM)

    # BinaryOps

    # Arithmetic
    ADD = _NBop(name="add", symbol="+")
    SUB = _NBop(name="subtract", symbol="-")
    MUL = _NBop(name="multiply", symbol="*")
    FLOORDIV = _NBop(name="floor_divide", symbol="//")
    TRUEDIV = _NBop(name="true_divide", symbol="/")
    POW = _NBop(name="power", symbol="**")
    MOD = _NBop(name="mod", symbol="%")

    # _TYPE_REGISTRY["logical"]
    AND = _LBop(name="and", symbol="&")
    OR = _LBop(name="or", symbol="|")
    LT = _CBop(name="less_than", symbol="<")
    LE = _CBop(name="less_than_equal", symbol="<=")
    EQ = _CBop(name="equal", symbol="=")
    NEQ = _CBop(name="not_equal", symbol="!=")
    GT = _CBop(name="greater_than", symbol=">")
    GE = _CBop(name="greater_than_equal", symbol=">=")
