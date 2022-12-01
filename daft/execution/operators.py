from __future__ import annotations

import sys
from dataclasses import dataclass
from enum import Enum
from functools import lru_cache, partial
from types import MappingProxyType
from typing import Callable, FrozenSet, Tuple, TypeVar

from daft.types import ExpressionType

if sys.version_info < (3, 8):
    from typing_extensions import Protocol
else:
    from typing import Protocol


TypeMatrix = FrozenSet[Tuple[Tuple[ExpressionType, ...], ExpressionType]]


@dataclass(frozen=True)
class ExpressionOperator:
    name: str
    nargs: int
    type_matrix: TypeMatrix
    symbol: str | None = None

    def __post_init__(self) -> None:
        for k, v in self.type_matrix:
            assert len(k) == self.nargs, f"all keys in type matrix must have {self.nargs}"
            for sub_k in k:
                assert isinstance(sub_k, ExpressionType)
                assert sub_k != ExpressionType.unknown()
            assert isinstance(v, ExpressionType), f"{v} is not an ExpressionType"

    @lru_cache(maxsize=1)
    def _type_matrix_dict(self) -> MappingProxyType[tuple[ExpressionType, ...], ExpressionType]:
        return MappingProxyType(dict(self.type_matrix))

    def get_return_type(self, args: tuple[ExpressionType, ...]) -> ExpressionType | None:
        # Treat all Python types as a PY[object] for the purposes of typing
        args = tuple([ExpressionType.python_object() if ExpressionType.is_py(a) else a for a in args])

        res = self._type_matrix_dict().get(args, ExpressionType.unknown())

        # For Python types, we return another Python type instead of unknown
        if res == ExpressionType.unknown() and any([ExpressionType.is_py(arg) for arg in args]):
            return ExpressionType.python_object()

        return res


_UnaryNumericalTM = frozenset(
    {
        (ExpressionType.integer(),): ExpressionType.integer(),
        (ExpressionType.float(),): ExpressionType.float(),
    }.items()
)

_UnaryLogicalTM = frozenset(
    {
        (ExpressionType.logical(),): ExpressionType.logical(),
    }.items()
)


_BinaryNumericalTM = frozenset(
    {
        (ExpressionType.integer(), ExpressionType.integer()): ExpressionType.integer(),
        (ExpressionType.float(), ExpressionType.float()): ExpressionType.float(),
        (ExpressionType.float(), ExpressionType.integer()): ExpressionType.float(),
        (ExpressionType.integer(), ExpressionType.float()): ExpressionType.float(),
    }.items()
)

_ComparableTM = frozenset(
    {
        (ExpressionType.integer(),): ExpressionType.integer(),
        (ExpressionType.logical(),): ExpressionType.logical(),
        (ExpressionType.float(),): ExpressionType.float(),
        (ExpressionType.string(),): ExpressionType.string(),
        (ExpressionType.date(),): ExpressionType.date(),
        (ExpressionType.bytes(),): ExpressionType.bytes(),
    }.items()
)

_ComparisionTM = frozenset(
    {
        (ExpressionType.integer(), ExpressionType.integer()): ExpressionType.logical(),
        (ExpressionType.float(), ExpressionType.float()): ExpressionType.logical(),
        (ExpressionType.integer(), ExpressionType.float()): ExpressionType.logical(),
        (ExpressionType.float(), ExpressionType.integer()): ExpressionType.logical(),
        (ExpressionType.string(), ExpressionType.string()): ExpressionType.logical(),
        (ExpressionType.date(), ExpressionType.date()): ExpressionType.logical(),
        (ExpressionType.bytes(), ExpressionType.bytes()): ExpressionType.logical(),
    }.items()
)


_BinaryLogicalTM = frozenset(
    {
        (ExpressionType.logical(), ExpressionType.logical()): ExpressionType.logical(),
    }.items()
)

_CountLogicalTM = frozenset(
    {
        (ExpressionType.integer(),): ExpressionType.integer(),
        (ExpressionType.float(),): ExpressionType.integer(),
        (ExpressionType.logical(),): ExpressionType.integer(),
        (ExpressionType.string(),): ExpressionType.integer(),
        (ExpressionType.date(),): ExpressionType.integer(),
        (ExpressionType.bytes(),): ExpressionType.integer(),
        (ExpressionType.null(),): ExpressionType.integer(),
    }.items()
)

_AllLogicalTM = frozenset(
    {
        (ExpressionType.integer(),): ExpressionType.logical(),
        (ExpressionType.float(),): ExpressionType.logical(),
        (ExpressionType.logical(),): ExpressionType.logical(),
        (ExpressionType.string(),): ExpressionType.logical(),
        (ExpressionType.date(),): ExpressionType.logical(),
        (ExpressionType.bytes(),): ExpressionType.logical(),
        (ExpressionType.null(),): ExpressionType.logical(),
        (ExpressionType.python_object(),): ExpressionType.logical(),
    }.items()
)

_FloatLogicalTM = frozenset(
    {
        (ExpressionType.float(),): ExpressionType.logical(),
    }.items()
)

_IfElseTM = frozenset(
    {
        (ExpressionType.logical(), ExpressionType.integer(), ExpressionType.integer()): ExpressionType.integer(),
        (ExpressionType.logical(), ExpressionType.float(), ExpressionType.float()): ExpressionType.float(),
        (ExpressionType.logical(), ExpressionType.logical(), ExpressionType.logical()): ExpressionType.logical(),
        (ExpressionType.logical(), ExpressionType.string(), ExpressionType.string()): ExpressionType.string(),
        (ExpressionType.logical(), ExpressionType.date(), ExpressionType.date()): ExpressionType.date(),
        (ExpressionType.logical(), ExpressionType.bytes(), ExpressionType.bytes()): ExpressionType.bytes(),
        (
            ExpressionType.logical(),
            ExpressionType.python_object(),
            ExpressionType.python_object(),
        ): ExpressionType.python_object(),
    }.items()
)

_DatetimeExtractionTM = frozenset(
    {
        (ExpressionType.date(),): ExpressionType.integer(),
    }.items()
)

_StrConcatTM = frozenset(
    {
        (ExpressionType.string(), ExpressionType.string()): ExpressionType.string(),
    }.items()
)

_StrLengthTM = frozenset(
    {
        (ExpressionType.string(),): ExpressionType.integer(),
    }.items()
)

_ExplodeTM = frozenset(
    {
        (ExpressionType.python_object(),): ExpressionType.python_object(),
    }.items()
)

_ListTM = frozenset(
    {
        (ExpressionType.integer(),): ExpressionType.python_object(),
        (ExpressionType.float(),): ExpressionType.python_object(),
        (ExpressionType.logical(),): ExpressionType.python_object(),
        (ExpressionType.string(),): ExpressionType.python_object(),
        (ExpressionType.date(),): ExpressionType.python_object(),
        (ExpressionType.bytes(),): ExpressionType.python_object(),
        (ExpressionType.null(),): ExpressionType.python_object(),
        (ExpressionType.python_object(),): ExpressionType.python_object(),
    }.items()
)

_ConcatTM = frozenset(
    {
        (ExpressionType.python_object(),): ExpressionType.python_object(),
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

_ComparibleUop = partial(_UOp, type_matrix=_ComparableTM)

# Logical Binary Ops
_LBop = partial(_BOp, type_matrix=_BinaryLogicalTM)

# Logical String Ops
_LSop = partial(
    _BOp,
    type_matrix=frozenset({(ExpressionType.string(), ExpressionType.string()): ExpressionType.logical()}.items()),
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
    LIST = _UOp(name="list", symbol="list", type_matrix=_ListTM)
    CONCAT = _UOp(name="concat", symbol="concat", type_matrix=_ConcatTM)
    MIN = _ComparibleUop(name="min", symbol="min")
    MAX = _ComparibleUop(name="max", symbol="max")

    COUNT = _UOp(name="count", symbol="count", type_matrix=_CountLogicalTM)

    # Logical
    INVERT = _UOp(name="invert", symbol="~", type_matrix=_UnaryLogicalTM)

    # Cast
    CAST_INT = _UOp(
        name="cast_int",
        type_matrix=frozenset(
            {
                (ExpressionType.integer(),): ExpressionType.integer(),
                (ExpressionType.float(),): ExpressionType.integer(),
                (ExpressionType.logical(),): ExpressionType.integer(),
                (ExpressionType.python_object(),): ExpressionType.integer(),
            }.items()
        ),
    )
    CAST_FLOAT = _UOp(
        name="cast_float",
        type_matrix=frozenset(
            {
                (ExpressionType.float(),): ExpressionType.float(),
                (ExpressionType.integer(),): ExpressionType.float(),
                (ExpressionType.python_object(),): ExpressionType.float(),
            }.items()
        ),
    )
    CAST_STRING = _UOp(
        name="cast_string",
        type_matrix=frozenset(
            {
                (ExpressionType.string(),): ExpressionType.string(),
                (ExpressionType.integer(),): ExpressionType.string(),
                (ExpressionType.logical(),): ExpressionType.string(),
                (ExpressionType.float(),): ExpressionType.string(),
                (ExpressionType.python_object(),): ExpressionType.string(),
            }.items()
        ),
    )
    CAST_LOGICAL = _UOp(
        name="cast_logical",
        type_matrix=frozenset(
            {
                (ExpressionType.logical(),): ExpressionType.logical(),
                (ExpressionType.python_object(),): ExpressionType.logical(),
            }.items()
        ),
    )
    CAST_DATE = _UOp(
        name="cast_date",
        type_matrix=frozenset(
            {
                (ExpressionType.date(),): ExpressionType.date(),
                (ExpressionType.python_object(),): ExpressionType.date(),
            }.items()
        ),
    )
    CAST_BYTES = _UOp(
        name="cast_bytes",
        type_matrix=frozenset(
            {
                (ExpressionType.bytes(),): ExpressionType.bytes(),
                (ExpressionType.python_object(),): ExpressionType.bytes(),
            }.items()
        ),
    )

    # String
    STR_CONCAT = _BOp(name="str_concat", symbol="concat", type_matrix=_StrConcatTM)
    STR_CONTAINS = _LSop(name="str_contains", symbol="contains")
    STR_ENDSWITH = _LSop(name="str_endswith", symbol="endswith")
    STR_STARTSWITH = _LSop(name="str_startswith", symbol="startswith")
    STR_LENGTH = _UOp(name="str_length", symbol="len", type_matrix=_StrLengthTM)

    # Null
    IS_NULL = _UOp(name="is_null", symbol="is_null", type_matrix=_AllLogicalTM)
    IS_NAN = _UOp(name="is_nan", symbol="is_nan", type_matrix=_FloatLogicalTM)

    # Date
    DT_DAY = _UOp(name="day", symbol="day", type_matrix=_DatetimeExtractionTM)
    DT_MONTH = _UOp(name="month", symbol="month", type_matrix=_DatetimeExtractionTM)
    DT_YEAR = _UOp(name="year", symbol="year", type_matrix=_DatetimeExtractionTM)
    DT_DAY_OF_WEEK = _UOp(name="day_of_week", symbol="day_of_week", type_matrix=_DatetimeExtractionTM)

    # Lists
    EXPLODE = _UOp(name="explode", symbol="explode", type_matrix=_ExplodeTM)

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
    LIST: UnaryFunction
    CONCAT: UnaryFunction
    MIN: UnaryFunction
    MAX: UnaryFunction
    COUNT: UnaryFunction
    INVERT: UnaryFunction

    CAST_INT: UnaryFunction
    CAST_FLOAT: UnaryFunction
    CAST_STRING: UnaryFunction
    CAST_LOGICAL: UnaryFunction
    CAST_DATE: UnaryFunction
    CAST_BYTES: UnaryFunction

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

    STR_CONCAT: BinaryFunction
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

    EXPLODE: UnaryFunction

    IF_ELSE: TernaryFunction
