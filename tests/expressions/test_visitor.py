from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import pytest

from daft.expressions import (
    Expression,
    col,
    list_,
    lit,
)
from daft.expressions.visitor import PredicateVisitor
from daft.logical.schema import DataType
from daft.series import Series


@dataclass
class Trace:
    """Trace tells us which method was called."""

    attr: str
    args: list[Any]

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Trace):
            return False
        if self.attr != other.attr:
            return False

        # recursive equals using the PyExpr._eq method
        def eq(obj1: object, obj2: object) -> bool:
            if isinstance(obj1, Expression) and isinstance(obj2, Expression):
                return obj1._expr._eq(obj2._expr)
            elif isinstance(obj1, list) and isinstance(obj2, list):
                if len(obj1) != len(obj2):
                    return False
                for lc, rc in zip(obj1, obj2):
                    if not eq(lc, rc):
                        return False
                return True
            else:
                return obj1 == obj2

        return eq(self.args, other.args)


class TracingVisitor(PredicateVisitor[list[Trace]]):
    """TracingVisitor accumulates a callstack so we can verify how the rust PyVisitor dispatches.

    This is factored intentionally to exemplify a stateless tree fold, and I've made the
    accumulator explicit. You could implement this much more concisely by making the visitor
    stateful with a `_traces: List[Trace]` field, but again this example is showing off
    functional style folding with dispatch on the rust side bridging into this visitor.

    I've also left the scoped context parameter out of the ExpressionVisitor interface because
    it's unlikely anyone would actually care for it, and anyone who does is already capable of
    managing their own state stack, this visitor is for the people.
    """

    def visit_col(self, name: str) -> list[Trace]:
        trace = Trace("visit_col", [name])
        acc = []
        acc += [trace]
        return acc

    def visit_lit(self, value: Any) -> list[Trace]:
        trace = Trace("visit_lit", [value])
        acc = []
        acc += [trace]
        return acc

    def visit_alias(self, expr: Expression, alias: str) -> list[Trace]:
        trace = Trace("visit_alias", [expr, alias])
        acc = []
        acc += self.visit(expr)
        acc += [trace]
        return acc

    def visit_cast(self, expr: Expression, dtype: DataType) -> list[Trace]:
        trace = Trace("visit_cast", [expr, dtype])
        acc = []
        acc += self.visit(expr)
        acc += [trace]
        return acc

    def visit_list(self, items: list[Expression]) -> list[Trace]:
        trace = Trace("visit_list", [items])
        acc = []
        for item in items:
            acc += self.visit(item)
        acc += [trace]
        return acc

    def visit_and(self, left: Expression, right: Expression) -> list[Trace]:
        trace = Trace("visit_and", [left, right])
        acc = []
        acc += self.visit(left)
        acc += self.visit(right)
        acc += [trace]
        return acc

    def visit_or(self, left: Expression, right: Expression) -> list[Trace]:
        trace = Trace("visit_or", [left, right])
        acc = []
        acc += self.visit(left)
        acc += self.visit(right)
        acc += [trace]
        return acc

    def visit_not(self, expr: Expression) -> list[Trace]:
        trace = Trace("visit_not", [expr])
        acc = []
        acc += self.visit(expr)
        acc += [trace]
        return acc

    def visit_equal(self, left: Expression, right: Expression) -> list[Trace]:
        trace = Trace("visit_equal", [left, right])
        acc = []
        acc += self.visit(left)
        acc += self.visit(right)
        acc += [trace]
        return acc

    def visit_not_equal(self, left: Expression, right: Expression) -> list[Trace]:
        trace = Trace("visit_not_equal", [left, right])
        acc = []
        acc += self.visit(left)
        acc += self.visit(right)
        acc += [trace]
        return acc

    def visit_less_than(self, left: Expression, right: Expression) -> list[Trace]:
        trace = Trace("visit_less_than", [left, right])
        acc = []
        acc += self.visit(left)
        acc += self.visit(right)
        acc += [trace]
        return acc

    def visit_less_than_or_equal(self, left: Expression, right: Expression) -> list[Trace]:
        trace = Trace("visit_less_than_or_equal", [left, right])
        acc = []
        acc += self.visit(left)
        acc += self.visit(right)
        acc += [trace]
        return acc

    def visit_greater_than(self, left: Expression, right: Expression) -> list[Trace]:
        trace = Trace("visit_greater_than", [left, right])
        acc = []
        acc += self.visit(left)
        acc += self.visit(right)
        acc += [trace]
        return acc

    def visit_greater_than_or_equal(self, left: Expression, right: Expression) -> list[Trace]:
        trace = Trace("visit_greater_than_or_equal", [left, right])
        acc = []
        acc += self.visit(left)
        acc += self.visit(right)
        acc += [trace]
        return acc

    def visit_between(self, expr: Expression, lower: Expression, upper: Expression) -> list[Trace]:
        trace = Trace("visit_between", [expr, lower, upper])
        acc = []
        acc += self.visit(expr)
        acc += self.visit(lower)
        acc += self.visit(upper)
        acc += [trace]
        return acc

    def visit_is_in(self, expr: Expression, items: list[Expression]) -> list[Trace]:
        trace = Trace("visit_is_in", [expr, items])
        acc = []
        acc += self.visit(expr)
        for item in items:
            acc += self.visit(item)
        acc += [trace]
        return acc

    def visit_is_null(self, expr: Expression) -> list[Trace]:
        trace = Trace("visit_is_null", [expr])
        acc = []
        acc += self.visit(expr)
        acc += [trace]
        return acc

    def visit_not_null(self, expr: Expression) -> list[Trace]:
        trace = Trace("visit_not_null", [expr])
        acc = []
        acc += self.visit(expr)
        acc += [trace]
        return acc

    def visit_function(self, name: str, args: list[Expression]) -> list[Trace]:
        trace = Trace("visit_function", [name, args])
        acc = []
        for arg in args:
            acc += self.visit(arg)
        acc += [trace]
        return acc


# tracing is stateless and therefore effectively just a function module, hence the singleton
TRACING = TracingVisitor()


def trace(expr: Expression) -> list[Trace]:
    return TRACING.visit(expr)


def test_visit_col():
    exp_1 = col("abc")
    assert trace(exp_1) == [Trace("visit_col", ["abc"])]


def test_visit_lit():
    # Test null literal
    lit_1 = lit(None)
    assert trace(lit_1) == [Trace("visit_lit", [None])]

    # Test boolean literals
    lit_2 = lit(True)
    lit_3 = lit(False)
    assert trace(lit_2) == [Trace("visit_lit", [True])]
    assert trace(lit_3) == [Trace("visit_lit", [False])]

    # Test string literal
    lit_4 = lit("hello")
    assert trace(lit_4) == [Trace("visit_lit", ["hello"])]

    # integer literal (always int64, casts are tested elsewhere)
    lit_5 = lit(1)
    assert trace(lit_5) == [Trace("visit_lit", [1])]

    # bytes literal
    lit_6 = lit(b"hello")
    assert trace(lit_6) == [Trace("visit_lit", [b"hello"])]

    # float literal
    lit_7 = lit(1.5)
    assert trace(lit_7) == [Trace("visit_lit", [1.5])]

    # decimal literal
    lit_8 = lit(Decimal("1.5"))
    assert trace(lit_8) == [Trace("visit_lit", [Decimal("1.5")])]

    # series literal (can't compare series directly, so manual compare..)
    lit_9 = lit(Series.from_pylist([1, 2, 3]))
    lit_9_trace = trace(lit_9)
    assert len(lit_9_trace) == 1
    assert len(lit_9_trace[0].args) == 1
    assert lit_9_trace[0].attr == "visit_lit"
    assert lit_9_trace[0].args[0].to_pylist() == [[1, 2, 3]]

    # struct literal
    lit_10 = lit({"a": 1, "b": 2})
    assert trace(lit_10) == [Trace("visit_lit", [{"a": 1, "b": 2}])]


@pytest.mark.parametrize(
    "data_type",
    [
        DataType.null(),
        DataType.bool(),
        # numbers
        DataType.int8(),
        DataType.int16(),
        DataType.int32(),
        DataType.int64(),
        DataType.uint8(),
        DataType.uint16(),
        DataType.uint32(),
        DataType.uint64(),
        DataType.decimal128(10, 2),
        DataType.float32(),
        DataType.float64(),
        # string
        DataType.string(),
        # datetime
        DataType.timestamp("ns"),
        DataType.date(),
        DataType.time("ns"),
        DataType.duration("ns"),
        DataType.interval(),
        # binary
        DataType.binary(),
        DataType.embedding(DataType.int64(), 512),
        # collections
        DataType.list(DataType.int64()),
        DataType.fixed_size_list(DataType.int64(), size=10),
        DataType.map(DataType.string(), DataType.int64()),
        DataType.struct({"a": DataType.int64(), "b": DataType.int64()}),
        # tensor
        DataType.tensor(DataType.int64()),
        DataType.tensor(DataType.int64(), shape=(2, 3)),
        DataType.sparse_tensor(DataType.int64()),
        DataType.sparse_tensor(DataType.int64(), shape=(2, 3)),
    ],
)
def test_visit_cast(data_type):
    exp = col("x").cast(data_type)
    assert trace(exp) == [Trace("visit_col", ["x"]), Trace("visit_cast", [col("x"), data_type])]


def test_visit_list():
    exp = list_(lit(1), lit(2), lit(3))
    assert trace(exp) == [
        Trace("visit_lit", [1]),
        Trace("visit_lit", [2]),
        Trace("visit_lit", [3]),
        Trace("visit_list", [[lit(1), lit(2), lit(3)]]),
    ]


def test_visit_equal():
    exp_0 = col("x")
    exp_1 = exp_0 == 1
    assert trace(exp_1) == [Trace("visit_col", ["x"]), Trace("visit_lit", [1]), Trace("visit_equal", [exp_0, lit(1)])]


def test_visit_not_equal():
    exp_0 = col("x")
    exp_1 = exp_0 != 1
    assert trace(exp_1) == [
        Trace("visit_col", ["x"]),
        Trace("visit_lit", [1]),
        Trace("visit_not_equal", [exp_0, lit(1)]),
    ]


def test_visit_less_than():
    exp_0 = col("x")
    exp_1 = exp_0 < 1
    assert trace(exp_1) == [
        Trace("visit_col", ["x"]),
        Trace("visit_lit", [1]),
        Trace("visit_less_than", [exp_0, lit(1)]),
    ]


def test_visit_less_than_or_equal():
    exp_0 = col("x")
    exp_1 = exp_0 <= 1
    assert trace(exp_1) == [
        Trace("visit_col", ["x"]),
        Trace("visit_lit", [1]),
        Trace("visit_less_than_or_equal", [exp_0, lit(1)]),
    ]


def test_visit_greater_than():
    exp_0 = col("x")
    exp_1 = exp_0 > 1
    assert trace(exp_1) == [
        Trace("visit_col", ["x"]),
        Trace("visit_lit", [1]),
        Trace("visit_greater_than", [exp_0, lit(1)]),
    ]


def test_visit_greater_than_or_equal():
    exp_0 = col("x")
    exp_1 = exp_0 >= 1
    assert trace(exp_1) == [
        Trace("visit_col", ["x"]),
        Trace("visit_lit", [1]),
        Trace("visit_greater_than_or_equal", [exp_0, lit(1)]),
    ]


def test_visit_between():
    exp_0 = col("x")
    exp_1 = exp_0.between(1, 2)
    assert trace(exp_1) == [
        Trace("visit_col", ["x"]),
        Trace("visit_lit", [1]),
        Trace("visit_lit", [2]),
        Trace("visit_between", [exp_0, lit(1), lit(2)]),
    ]


def test_visit_is_in():
    exp_0 = col("x")
    exp_1 = exp_0.is_in([1, 2, 3])
    assert trace(exp_1) == [
        Trace("visit_col", ["x"]),
        Trace("visit_lit", [1]),
        Trace("visit_lit", [2]),
        Trace("visit_lit", [3]),
        Trace("visit_is_in", [exp_0, [lit(1), lit(2), lit(3)]]),
    ]


def test_visit_is_null():
    exp_0 = col("x")
    exp_1 = exp_0.is_null()
    assert trace(exp_1) == [Trace("visit_col", ["x"]), Trace("visit_is_null", [exp_0])]


def test_visit_not_null():
    exp_0 = col("x")
    exp_1 = exp_0.not_null()
    assert trace(exp_1) == [Trace("visit_col", ["x"]), Trace("visit_not_null", [exp_0])]


@pytest.mark.parametrize(
    "attr",
    [
        "abs",
        "ceil",
        "floor",
        "sqrt",
        "exp",
        "log10",
        "sin",
        "cos",
        "tan",
        "arcsin",
        "arccos",
        "arctan",
        "sinh",
        "cosh",
        "tanh",
        "degrees",
        "radians",
        "sign",
    ],
)
def test_visit_functions0(attr):
    exp_0 = col("x")
    assert trace(getattr(exp_0, attr)()) == [
        Trace("visit_col", ["x"]),
        Trace("visit_function", [attr, [exp_0]]),
    ]
