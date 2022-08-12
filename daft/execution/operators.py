from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from functools import partial
from typing import Dict, Optional, Tuple

import pyarrow as pa


class ExpressionType(Enum):
    UNKNOWN = 0
    NUMBER = 1
    LOGICAL = 2
    STRING = 3
    PYTHON = 4

    def __str__(self) -> str:
        return self.name

    @staticmethod
    def from_py_obj(obj: object) -> ExpressionType:
        """Gets the appropriate ExpressionType from a Python object, or ExpressionType.UNKNOWN
        if unable to find the appropriate type. ExpressionType.PYTHON is never returned.
        """
        obj_type = type(obj)
        if obj_type not in _PY_TYPE_TO_EXPRESSION_TYPE:
            return ExpressionType.UNKNOWN
        return _PY_TYPE_TO_EXPRESSION_TYPE[obj_type]

    @staticmethod
    def from_arrow_type(datatype: pa.DataType) -> ExpressionType:
        if datatype not in _PYARROW_TYPE_TO_EXPRESSION_TYPE:
            return ExpressionType.UNKNOWN
        return _PYARROW_TYPE_TO_EXPRESSION_TYPE[datatype]


_PYARROW_TYPE_TO_EXPRESSION_TYPE = {
    pa.null(): ExpressionType.UNKNOWN,
    pa.bool_(): ExpressionType.LOGICAL,
    pa.int8(): ExpressionType.UNKNOWN,
    pa.int16(): ExpressionType.NUMBER,
    pa.int32(): ExpressionType.NUMBER,
    pa.int64(): ExpressionType.NUMBER,
    pa.uint8(): ExpressionType.NUMBER,
    pa.uint16(): ExpressionType.NUMBER,
    pa.uint32(): ExpressionType.NUMBER,
    pa.uint64(): ExpressionType.NUMBER,
    pa.float16(): ExpressionType.NUMBER,
    pa.float32(): ExpressionType.NUMBER,
    pa.float64(): ExpressionType.NUMBER,
    pa.date32(): ExpressionType.UNKNOWN,
    pa.date64(): ExpressionType.UNKNOWN,
    pa.string(): ExpressionType.STRING,
    pa.utf8(): ExpressionType.STRING,
    pa.large_binary(): ExpressionType.UNKNOWN,
    pa.large_string(): ExpressionType.STRING,
    pa.large_utf8(): ExpressionType.STRING,
}

_PY_TYPE_TO_EXPRESSION_TYPE = {
    int: ExpressionType.NUMBER,
    float: ExpressionType.NUMBER,
    str: ExpressionType.STRING,
    bool: ExpressionType.LOGICAL,
}

TypeMatrix = Dict[Tuple[ExpressionType, ...], ExpressionType]


@dataclass(frozen=True)
class ExpressionOperator:
    name: str
    nargs: int
    type_matrix: TypeMatrix
    accepts_kwargs: bool = False
    symbol: Optional[str] = None

    def __post_init__(self) -> None:
        for k, v in self.type_matrix.items():
            assert len(k) == self.nargs, f"all keys in type matrix must have {self.nargs}"
            for sub_k in k:
                assert isinstance(sub_k, ExpressionType)
                assert sub_k != ExpressionType.UNKNOWN

            assert isinstance(v, ExpressionType)
            assert v != ExpressionType.UNKNOWN


_UnaryNumericalTM = {(ExpressionType.NUMBER,): ExpressionType.NUMBER}

_UnaryLogicalTM = {(ExpressionType.LOGICAL,): ExpressionType.LOGICAL}


_BinaryNumericalTM = {(ExpressionType.NUMBER, ExpressionType.NUMBER): ExpressionType.NUMBER}

_ComparisionTM = {
    (ExpressionType.NUMBER, ExpressionType.NUMBER): ExpressionType.LOGICAL,
    (ExpressionType.STRING, ExpressionType.STRING): ExpressionType.LOGICAL,
}

_BinaryLogicalTM = {
    (ExpressionType.LOGICAL, ExpressionType.LOGICAL): ExpressionType.LOGICAL,
}

_CountLogicalTM = {
    (ExpressionType.NUMBER,): ExpressionType.NUMBER,
    (ExpressionType.LOGICAL,): ExpressionType.NUMBER,
    (ExpressionType.STRING,): ExpressionType.NUMBER,
}


_UOp = partial(ExpressionOperator, nargs=1, accepts_kwargs=False)
# Numerical Unary Ops
_NUop = partial(_UOp, type_matrix=_UnaryNumericalTM)

_BOp = partial(ExpressionOperator, nargs=2, accepts_kwargs=False)

# Numerical Binary Ops
_NBop = partial(_BOp, type_matrix=_BinaryNumericalTM)

# Comparison Binary Ops
_CBop = partial(_BOp, type_matrix=_ComparisionTM)

# Logical Binary Ops
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

    # Logical
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

    # Logical
    AND = _LBop(name="and", symbol="&")
    OR = _LBop(name="or", symbol="|")
    LT = _CBop(name="less_than", symbol="<")
    LE = _CBop(name="less_than_equal", symbol="<=")
    EQ = _CBop(name="equal", symbol="=")
    NEQ = _CBop(name="not_equal", symbol="!=")
    GT = _CBop(name="greater_than", symbol=">")
    GE = _CBop(name="greater_than_equal", symbol=">=")
