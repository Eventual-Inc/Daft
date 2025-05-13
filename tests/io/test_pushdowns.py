from __future__ import annotations

from typing import TYPE_CHECKING

from daft.daft import Pushdowns as PyPushdowns
from daft.expressions import col, lit
from daft.io.pushdowns import Pushdowns

if TYPE_CHECKING:
    from daft.expressions import Expression


###
# Pushdowns Translations Tests
###


def assert_eq(e1: Expression, e2: Expression):
    assert e1._expr._eq(e2._expr)


def test_projection_pushdowns():
    py_pushdowns = PyPushdowns(columns=["a", "b", "c"])
    pushdowns = Pushdowns._from_pypushdowns(py_pushdowns)

    assert pushdowns.projections
    assert len(pushdowns.projections) == 3
    assert_eq(pushdowns.projections[0], col("a"))
    assert_eq(pushdowns.projections[1], col("b"))
    assert_eq(pushdowns.projections[2], col("c"))

    assert pushdowns.limit is None
    assert pushdowns.predicate is None


def test_simple_predicate_pushdown():
    predicate = col("a") == lit(1)  # (= a 1)
    py_pushdowns = PyPushdowns(filters=predicate._expr)
    pushdowns = Pushdowns._from_pypushdowns(py_pushdowns)

    assert_eq(pushdowns.predicate, predicate)  # <- roundtrip assert

    assert pushdowns.projections is None
    assert pushdowns.limit is None


def test_complex_predicate_pushdown():
    predicate = col("a") == (col("b") + col("c"))  # (= a (+ b c))
    py_pushdowns = PyPushdowns(filters=predicate._expr)
    pushdowns = Pushdowns._from_pypushdowns(py_pushdowns)

    assert_eq(pushdowns.predicate, predicate)  # <- roundtrip assert

    assert pushdowns.projections is None
    assert pushdowns.limit is None


def test_limit_pushdown():
    py_pushdowns = PyPushdowns(limit=1738)
    pushdowns = Pushdowns._from_pypushdowns(py_pushdowns)

    assert pushdowns.projections is None
    assert pushdowns.predicate is None
    assert pushdowns.limit == 1738


def test_simple_partition_pushdown():
    predicate = col("a") == lit(1)  # (= a 1)
    py_pushdowns = PyPushdowns(partition_filters=predicate._expr)
    pushdowns = Pushdowns._from_pypushdowns(py_pushdowns)

    assert_eq(pushdowns.predicate, predicate)  # <- roundtrip assert

    assert pushdowns.projections is None
    assert pushdowns.limit is None


def test_composite_partition_pushdown():
    filters = col("a") == lit(1)  # (= a 1)
    partition_filters = col("b") > lit(2)  # (> b 2)
    py_pushdowns = PyPushdowns(filters=filters._expr, partition_filters=partition_filters._expr)
    pushdowns = Pushdowns._from_pypushdowns(py_pushdowns)

    # translation should combine them like (p1 AND p2)
    p1 = filters
    p2 = partition_filters
    assert_eq(pushdowns.predicate, p1 & p2)

    assert pushdowns.projections is None
    assert pushdowns.limit is None
