from __future__ import annotations

import datetime
import sys
from dataclasses import dataclass
from enum import Enum
from functools import lru_cache, partial
from types import MappingProxyType
from typing import Any, Callable, Dict, FrozenSet, Optional, Tuple, Type, TypeVar

if sys.version_info < (3, 8):
    from typing_extensions import Protocol
else:
    from typing import Protocol

import pyarrow as pa


class ExpressionType:
    @staticmethod
    def is_logical(t: ExpressionType) -> bool:
        return t == _TYPE_REGISTRY["logical"]

    @staticmethod
    def unknown() -> ExpressionType:
        return _TYPE_REGISTRY["unknown"]

    @staticmethod
    def python_object() -> ExpressionType:
        return _TYPE_REGISTRY["pyobj"]

    @staticmethod
    def from_py_type(obj_type: Type) -> ExpressionType:
        """Gets the appropriate ExpressionType from a Python object, or _TYPE_REGISTRY["unknown"]
        if unable to find the appropriate type. ExpressionTypes.Python is never returned.
        """
        if obj_type not in _PY_TYPE_TO_EXPRESSION_TYPE:
            return PythonExpressionType(obj_type)
        return _PY_TYPE_TO_EXPRESSION_TYPE[obj_type]

    @staticmethod
    def from_arrow_type(datatype: pa.DataType) -> ExpressionType:
        if datatype not in _PYARROW_TYPE_TO_EXPRESSION_TYPE:
            return ExpressionType.python_object()
        return _PYARROW_TYPE_TO_EXPRESSION_TYPE[datatype]


@dataclass(frozen=True)
class PrimitiveExpressionType(ExpressionType):
    class TypeEnum(Enum):

        UNKNOWN = 1
        INTEGER = 2
        FLOAT = 3
        LOGICAL = 4
        STRING = 5
        DATE = 6
        BYTES = 7

    enum: PrimitiveExpressionType.TypeEnum

    def __repr__(self) -> str:
        return self.enum.name

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, PrimitiveExpressionType):
            return False
        return self.enum == other.enum


@dataclass(frozen=True)
class CompositeExpressionType(ExpressionType):
    args: Tuple[ExpressionType, ...]

    def __repr__(self) -> str:
        return f"({', '.join([str(arg) for arg in self.args])})"

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, CompositeExpressionType):
            return False
        return self.args == other.args


@dataclass(frozen=True)
class PythonExpressionType(ExpressionType):
    python_cls: Type

    def __repr__(self) -> str:
        return f"PY[{self.python_cls.__name__}]"

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, PythonExpressionType):
            return False
        return self.python_cls == other.python_cls


_TYPE_REGISTRY: Dict[str, ExpressionType] = {
    "unknown": PrimitiveExpressionType(PrimitiveExpressionType.TypeEnum.UNKNOWN),
    "integer": PrimitiveExpressionType(PrimitiveExpressionType.TypeEnum.INTEGER),
    "float": PrimitiveExpressionType(PrimitiveExpressionType.TypeEnum.FLOAT),
    "logical": PrimitiveExpressionType(PrimitiveExpressionType.TypeEnum.LOGICAL),
    "string": PrimitiveExpressionType(PrimitiveExpressionType.TypeEnum.STRING),
    "date": PrimitiveExpressionType(PrimitiveExpressionType.TypeEnum.DATE),
    "bytes": PrimitiveExpressionType(PrimitiveExpressionType.TypeEnum.BYTES),
    # Represents a generic Python object that we have no further information about
    "pyobj": PythonExpressionType(object),
}


EXPRESSION_TYPE_TO_PYARROW_TYPE = {
    _TYPE_REGISTRY["logical"]: pa.bool_(),
    _TYPE_REGISTRY["integer"]: pa.int64(),
    _TYPE_REGISTRY["float"]: pa.float64(),
    _TYPE_REGISTRY["string"]: pa.string(),
    _TYPE_REGISTRY["date"]: pa.date32(),
    _TYPE_REGISTRY["bytes"]: pa.large_binary(),
}


_PYARROW_TYPE_TO_EXPRESSION_TYPE = {
    # pa.null(): _TYPE_REGISTRY["unknown"],
    pa.bool_(): _TYPE_REGISTRY["logical"],
    pa.int8(): _TYPE_REGISTRY["integer"],
    pa.int16(): _TYPE_REGISTRY["integer"],
    pa.int32(): _TYPE_REGISTRY["integer"],
    pa.int64(): _TYPE_REGISTRY["integer"],
    pa.uint8(): _TYPE_REGISTRY["integer"],
    pa.uint16(): _TYPE_REGISTRY["integer"],
    pa.uint32(): _TYPE_REGISTRY["integer"],
    pa.uint64(): _TYPE_REGISTRY["integer"],
    pa.float16(): _TYPE_REGISTRY["float"],
    pa.float32(): _TYPE_REGISTRY["float"],
    pa.float64(): _TYPE_REGISTRY["float"],
    pa.date32(): _TYPE_REGISTRY["date"],
    # pa.date64(): _TYPE_REGISTRY["unknown"],
    pa.string(): _TYPE_REGISTRY["string"],
    pa.utf8(): _TYPE_REGISTRY["string"],
    pa.binary(): _TYPE_REGISTRY["bytes"],
    pa.large_binary(): _TYPE_REGISTRY["bytes"],
    pa.large_string(): _TYPE_REGISTRY["string"],
    pa.large_utf8(): _TYPE_REGISTRY["string"],
}

_PY_TYPE_TO_EXPRESSION_TYPE = {
    int: _TYPE_REGISTRY["integer"],
    float: _TYPE_REGISTRY["float"],
    str: _TYPE_REGISTRY["string"],
    bool: _TYPE_REGISTRY["logical"],
    datetime.date: _TYPE_REGISTRY["date"],
    bytes: _TYPE_REGISTRY["bytes"],
}


TypeMatrix = FrozenSet[Tuple[Tuple[ExpressionType, ...], ExpressionType]]


@dataclass(frozen=True)
class ExpressionOperator:
    name: str
    nargs: int
    type_matrix: TypeMatrix
    symbol: Optional[str] = None

    def __post_init__(self) -> None:
        for k, v in self.type_matrix:
            assert len(k) == self.nargs, f"all keys in type matrix must have {self.nargs}"
            for sub_k in k:
                assert isinstance(sub_k, ExpressionType)
                assert sub_k != _TYPE_REGISTRY["unknown"]
            assert isinstance(v, ExpressionType), f"{v} is not an ExpressionType"

    @lru_cache(maxsize=1)
    def _type_matrix_dict(self) -> MappingProxyType[Tuple[ExpressionType, ...], ExpressionType]:
        return MappingProxyType(dict(self.type_matrix))

    def get_return_type(self, args: Tuple[ExpressionType, ...]) -> Optional[ExpressionType]:
        # Any operation on a Python type will just return a generic Python type
        if any([isinstance(arg, PythonExpressionType) for arg in args]):
            return ExpressionType.python_object()
        return self._type_matrix_dict().get(args, ExpressionType.unknown())


_UnaryNumericalTM = frozenset(
    {
        (_TYPE_REGISTRY["integer"],): _TYPE_REGISTRY["integer"],
        (_TYPE_REGISTRY["float"],): _TYPE_REGISTRY["float"],
    }.items()
)

_UnaryLogicalTM = frozenset(
    {
        (_TYPE_REGISTRY["logical"],): _TYPE_REGISTRY["logical"],
    }.items()
)


_BinaryNumericalTM = frozenset(
    {
        (_TYPE_REGISTRY["integer"], _TYPE_REGISTRY["integer"]): _TYPE_REGISTRY["integer"],
        (_TYPE_REGISTRY["float"], _TYPE_REGISTRY["float"]): _TYPE_REGISTRY["float"],
        (_TYPE_REGISTRY["float"], _TYPE_REGISTRY["integer"]): _TYPE_REGISTRY["float"],
        (_TYPE_REGISTRY["integer"], _TYPE_REGISTRY["float"]): _TYPE_REGISTRY["float"],
    }.items()
)

_ComparisionTM = frozenset(
    {
        (_TYPE_REGISTRY["integer"], _TYPE_REGISTRY["integer"]): _TYPE_REGISTRY["logical"],
        (_TYPE_REGISTRY["float"], _TYPE_REGISTRY["float"]): _TYPE_REGISTRY["logical"],
        (_TYPE_REGISTRY["integer"], _TYPE_REGISTRY["float"]): _TYPE_REGISTRY["logical"],
        (_TYPE_REGISTRY["float"], _TYPE_REGISTRY["integer"]): _TYPE_REGISTRY["logical"],
        (_TYPE_REGISTRY["string"], _TYPE_REGISTRY["string"]): _TYPE_REGISTRY["logical"],
        (_TYPE_REGISTRY["date"], _TYPE_REGISTRY["date"]): _TYPE_REGISTRY["logical"],
        (_TYPE_REGISTRY["bytes"], _TYPE_REGISTRY["bytes"]): _TYPE_REGISTRY["logical"],
    }.items()
)

_BinaryLogicalTM = frozenset(
    {
        (_TYPE_REGISTRY["logical"], _TYPE_REGISTRY["logical"]): _TYPE_REGISTRY["logical"],
    }.items()
)

_CountLogicalTM = frozenset(
    {
        (_TYPE_REGISTRY["integer"],): _TYPE_REGISTRY["integer"],
        (_TYPE_REGISTRY["float"],): _TYPE_REGISTRY["integer"],
        (_TYPE_REGISTRY["logical"],): _TYPE_REGISTRY["integer"],
        (_TYPE_REGISTRY["string"],): _TYPE_REGISTRY["integer"],
        (_TYPE_REGISTRY["date"],): _TYPE_REGISTRY["integer"],
        (_TYPE_REGISTRY["bytes"],): _TYPE_REGISTRY["integer"],
    }.items()
)

_AllLogicalTM = frozenset(
    {
        (_TYPE_REGISTRY["integer"],): _TYPE_REGISTRY["logical"],
        (_TYPE_REGISTRY["float"],): _TYPE_REGISTRY["logical"],
        (_TYPE_REGISTRY["logical"],): _TYPE_REGISTRY["logical"],
        (_TYPE_REGISTRY["string"],): _TYPE_REGISTRY["logical"],
        (_TYPE_REGISTRY["date"],): _TYPE_REGISTRY["logical"],
        (_TYPE_REGISTRY["bytes"],): _TYPE_REGISTRY["logical"],
    }.items()
)

_IfElseTM = frozenset(
    {
        (_TYPE_REGISTRY["logical"], _TYPE_REGISTRY["integer"], _TYPE_REGISTRY["integer"]): _TYPE_REGISTRY["integer"],
        (_TYPE_REGISTRY["logical"], _TYPE_REGISTRY["float"], _TYPE_REGISTRY["float"]): _TYPE_REGISTRY["float"],
        (_TYPE_REGISTRY["logical"], _TYPE_REGISTRY["logical"], _TYPE_REGISTRY["logical"]): _TYPE_REGISTRY["logical"],
        (_TYPE_REGISTRY["logical"], _TYPE_REGISTRY["string"], _TYPE_REGISTRY["string"]): _TYPE_REGISTRY["string"],
        (_TYPE_REGISTRY["logical"], _TYPE_REGISTRY["date"], _TYPE_REGISTRY["date"]): _TYPE_REGISTRY["date"],
        (_TYPE_REGISTRY["logical"], _TYPE_REGISTRY["bytes"], _TYPE_REGISTRY["bytes"]): _TYPE_REGISTRY["bytes"],
    }.items()
)

_DatetimeExtractionTM = frozenset(
    {
        (_TYPE_REGISTRY["date"],): _TYPE_REGISTRY["integer"],
    }.items()
)

_LengthTM = frozenset(
    {
        (_TYPE_REGISTRY["string"],): _TYPE_REGISTRY["integer"],
    }.items()
)

_UOp = partial(ExpressionOperator, nargs=1)
# Numerical Unary Ops
_NUop = partial(_UOp, type_matrix=_UnaryNumericalTM)

_BOp = partial(ExpressionOperator, nargs=2)

# Numerical Binary Ops
_NBop = partial(_BOp, type_matrix=_BinaryNumericalTM)

# Comparison Binary Ops
_CBop = partial(_BOp, type_matrix=_ComparisionTM)

# Logical Binary Ops
_LBop = partial(_BOp, type_matrix=_BinaryLogicalTM)

# Logical String Ops
_LSop = partial(
    _BOp,
    type_matrix=frozenset({(_TYPE_REGISTRY["string"], _TYPE_REGISTRY["string"]): _TYPE_REGISTRY["logical"]}.items()),
)


class OperatorEnum(Enum):
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

    # String
    STR_CONTAINS = _LSop(name="str_contains", symbol="contains")
    STR_ENDSWITH = _LSop(name="str_endswith", symbol="endswith")
    STR_STARTSWITH = _LSop(name="str_startswith", symbol="startswith")
    STR_LENGTH = _UOp(name="str_length", symbol="len", type_matrix=_LengthTM)

    # Null
    IS_NULL = _UOp(name="is_null", symbol="is_null", type_matrix=_AllLogicalTM)
    IS_NAN = _UOp(name="is_nan", symbol="is_nan", type_matrix=_AllLogicalTM)

    # Date
    DT_DAY = _UOp(name="day", symbol="day", type_matrix=_DatetimeExtractionTM)
    DT_MONTH = _UOp(name="month", symbol="month", type_matrix=_DatetimeExtractionTM)
    DT_YEAR = _UOp(name="year", symbol="year", type_matrix=_DatetimeExtractionTM)
    DT_DAY_OF_WEEK = _UOp(name="day_of_week", symbol="day_of_week", type_matrix=_DatetimeExtractionTM)

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

    # TernaryOps

    IF_ELSE = ExpressionOperator(nargs=3, name="if_else", type_matrix=_IfElseTM)


ValueType = TypeVar("ValueType", covariant=True)
UnaryFunction = Callable[[ValueType], ValueType]
BinaryFunction = Callable[[ValueType, ValueType], ValueType]
TernaryFunction = Callable[[ValueType, ValueType, ValueType], ValueType]


class OperatorEvaluator(Protocol[ValueType]):
    def __new__(cls):
        raise TypeError("Evaluator classes cannot be instantiated")

    NEGATE: UnaryFunction
    POSITIVE: UnaryFunction
    ABS: UnaryFunction
    SUM: UnaryFunction
    MEAN: UnaryFunction
    MIN: UnaryFunction
    MAX: UnaryFunction
    COUNT: UnaryFunction
    INVERT: UnaryFunction

    ADD: BinaryFunction
    SUB: BinaryFunction
    MUL: BinaryFunction
    FLOORDIV: BinaryFunction
    TRUEDIV: BinaryFunction
    POW: BinaryFunction
    MOD: BinaryFunction
    AND: BinaryFunction
    OR: BinaryFunction
    LT: BinaryFunction
    LE: BinaryFunction
    EQ: BinaryFunction
    NEQ: BinaryFunction
    GT: BinaryFunction
    GE: BinaryFunction

    STR_CONTAINS: BinaryFunction
    STR_ENDSWITH: BinaryFunction
    STR_STARTSWITH: BinaryFunction
    STR_LENGTH: UnaryFunction

    IS_NULL: UnaryFunction
    IS_NAN: UnaryFunction

    DT_DAY: UnaryFunction
    DT_MONTH: UnaryFunction
    DT_YEAR: UnaryFunction
    DT_DAY_OF_WEEK: UnaryFunction

    IF_ELSE: TernaryFunction
