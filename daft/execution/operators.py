from dataclasses import dataclass
from enum import Enum
from functools import partial
from typing import Optional, Tuple


class ExpressionType(Enum):
    NUMBER = 1
    LOGICAL = 2
    STRING = 3
    PYTHON = 4


@dataclass(frozen=True)
class ExpressionOperator:
    name: str
    nargs: int
    output_type: ExpressionType
    input_types: Tuple[ExpressionType, ...]
    accepts_kwargs: bool = False
    symbol: Optional[str] = None


_ET = ExpressionType

_UOp = partial(ExpressionOperator, nargs=1, accepts_kwargs=False)
# Numerical Unary Ops
_NUop = partial(_UOp, input_types=(_ET.NUMBER,), output_type=_ET.NUMBER)

_BOp = partial(ExpressionOperator, nargs=2, accepts_kwargs=False)

# Numerical Binary Ops
_NBop = partial(_BOp, input_types=(_ET.NUMBER,), output_type=_ET.NUMBER)

# Comparison Binary Ops
_CBop = partial(_BOp, input_types=(_ET.NUMBER, _ET.STRING), output_type=_ET.LOGICAL)

# Logical Binary Ops
_LBop = partial(_BOp, input_types=(_ET.LOGICAL,), output_type=_ET.LOGICAL)


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

    COUNT = _UOp(
        name="count", symbol="count", input_types=(_ET.NUMBER, _ET.LOGICAL, _ET.STRING), output_type=_ET.NUMBER
    )

    # Logical
    INVERT = _UOp(name="invert", symbol="~", input_types=(_ET.LOGICAL,), output_type=_ET.LOGICAL)

    # BinaryOps

    # Arithmetic
    ADD = _NBop(name="add", symbol="+")
    SUB = _NBop(name="subtract", symbol="-")
    MUL = _NBop(name="multiply", symbol="*")
    TRUEDIV = _NBop(name="true_divide", symbol="//")
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
