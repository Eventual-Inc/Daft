from __future__ import annotations

import datetime
from dataclasses import dataclass
from enum import Enum
from functools import lru_cache, partial
from types import MappingProxyType
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import pyarrow as pa


class ExpressionType:
    @staticmethod
    def unknown() -> ExpressionType:
        return _TYPE_REGISTRY["unknown"]

    @staticmethod
    def from_py_type(obj_type: Union[Type, Sequence[Type]]) -> ExpressionType:
        """Gets the appropriate ExpressionType from a Python object, or _TYPE_REGISTRY["unknown"]
        if unable to find the appropriate type. ExpressionTypes.Python is never returned.
        """
        print(obj_type, obj_type in _PY_TYPE_TO_EXPRESSION_TYPE)
        global _TYPE_REGISTRY
        if isinstance(obj_type, Sequence):
            type_registry_key = f"Tuple[{', '.join(arg.__name__ for arg in obj_type)}]"
            if type_registry_key in _TYPE_REGISTRY:
                return _TYPE_REGISTRY[type_registry_key]
            _TYPE_REGISTRY[type_registry_key] = CompositeExpressionType(
                tuple(ExpressionType.from_py_type(arg) for arg in obj_type)
            )
            return _TYPE_REGISTRY[type_registry_key]
        elif obj_type not in _PY_TYPE_TO_EXPRESSION_TYPE:
            type_registry_key = f"PyObj[{obj_type.__name__}]"
            if type_registry_key in _TYPE_REGISTRY:
                return _TYPE_REGISTRY[type_registry_key]
            _TYPE_REGISTRY[type_registry_key] = PythonExpressionType(obj_type)
            return _TYPE_REGISTRY[type_registry_key]
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
        INTEGER = 2
        FLOAT = 3
        LOGICAL = 4
        STRING = 5
        DATE = 6

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
        return f"PyObj[{self.python_cls.__name__}]"

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
}


EXPRESSION_TYPE_TO_PYARROW_TYPE = {
    _TYPE_REGISTRY["logical"]: pa.bool_(),
    _TYPE_REGISTRY["integer"]: pa.int64(),
    _TYPE_REGISTRY["float"]: pa.float64(),
    _TYPE_REGISTRY["string"]: pa.string(),
    _TYPE_REGISTRY["date"]: pa.date32(),
}


_PYARROW_TYPE_TO_EXPRESSION_TYPE = {
    pa.null(): _TYPE_REGISTRY["unknown"],
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
    pa.date64(): _TYPE_REGISTRY["unknown"],
    pa.string(): _TYPE_REGISTRY["string"],
    pa.utf8(): _TYPE_REGISTRY["string"],
    pa.large_binary(): _TYPE_REGISTRY["unknown"],
    pa.large_string(): _TYPE_REGISTRY["string"],
    pa.large_utf8(): _TYPE_REGISTRY["string"],
}

_PY_TYPE_TO_EXPRESSION_TYPE = {
    int: _TYPE_REGISTRY["integer"],
    float: _TYPE_REGISTRY["float"],
    str: _TYPE_REGISTRY["string"],
    bool: _TYPE_REGISTRY["logical"],
    datetime.date: _TYPE_REGISTRY["date"],
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


_UnaryNumericalTM = frozenset(
    {
        (_TYPE_REGISTRY["integer"],): _TYPE_REGISTRY["integer"],
        (_TYPE_REGISTRY["float"],): _TYPE_REGISTRY["float"],
    }.items()
)

_UnaryLogicalTM = frozenset({(_TYPE_REGISTRY["logical"],): _TYPE_REGISTRY["logical"]}.items())


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

    # _TYPE_REGISTRY["logical"]
    INVERT = _UOp(name="invert", symbol="~", type_matrix=_UnaryLogicalTM)

    # BinaryOps

    # Arithmetic
    ADD = _NBop(name="add", symbol="+")
    SUB = _NBop(name="subtract", symbol="-")
    MUL = _NBop(name="multiply", symbol="*")
    FLOORDIV = partial(
        _BOp,
        type_matrix=frozenset(
            {
                (_TYPE_REGISTRY["integer"], _TYPE_REGISTRY["integer"]): _TYPE_REGISTRY["integer"],
                (_TYPE_REGISTRY["float"], _TYPE_REGISTRY["float"]): _TYPE_REGISTRY["integer"],
                (_TYPE_REGISTRY["float"], _TYPE_REGISTRY["integer"]): _TYPE_REGISTRY["integer"],
                (_TYPE_REGISTRY["integer"], _TYPE_REGISTRY["float"]): _TYPE_REGISTRY["integer"],
            }.items()
        ),
    )(name="floor_divide", symbol="//")
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


ValueType = TypeVar("ValueType", covariant=True)
UnaryFunction = Callable[[ValueType], ValueType]
BinaryFunction = Callable[[ValueType, ValueType], ValueType]


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
