from __future__ import annotations

from typing import TYPE_CHECKING

from daft.daft import PyPushdowns
from daft.expressions import col, lit
from daft.io.pushdowns import Pushdowns
from daft.sql import sql_expr

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

    assert pushdowns.columns
    assert len(pushdowns.columns) == 3
    assert pushdowns.columns[0] == "a"
    assert pushdowns.columns[1] == "b"
    assert pushdowns.columns[2] == "c"

    assert pushdowns.limit is None
    assert pushdowns.filters is None


def test_simple_filters_pushdown():
    filters = col("a") == lit(1)  # (= a 1)
    py_pushdowns = PyPushdowns(filters=filters._expr)
    pushdowns = Pushdowns._from_pypushdowns(py_pushdowns)

    assert_eq(pushdowns.filters, filters)  # <- roundtrip assert

    assert pushdowns.columns is None
    assert pushdowns.limit is None


def test_complex_filters_pushdown():
    filters = col("a") == (col("b") + col("c"))  # (= a (+ b c))
    py_pushdowns = PyPushdowns(filters=filters._expr)
    pushdowns = Pushdowns._from_pypushdowns(py_pushdowns)

    assert_eq(pushdowns.filters, filters)  # <- roundtrip assert

    assert pushdowns.columns is None
    assert pushdowns.limit is None


def test_limit_pushdown():
    py_pushdowns = PyPushdowns(limit=1738)
    pushdowns = Pushdowns._from_pypushdowns(py_pushdowns)

    assert pushdowns.columns is None
    assert pushdowns.filters is None
    assert pushdowns.limit == 1738


def test_simple_partition_pushdown():
    partition_filters = col("a") == lit(1)  # (= a 1)
    py_pushdowns = PyPushdowns(partition_filters=partition_filters._expr)
    pushdowns = Pushdowns._from_pypushdowns(py_pushdowns)

    # roundtrip assert
    assert_eq(pushdowns.partition_filters, partition_filters)

    assert pushdowns.filters is None
    assert pushdowns.columns is None
    assert pushdowns.limit is None


def test_composite_predicate_pushdown():
    filters = col("a") == lit(1)
    partition_filters = col("b") > lit(2)
    py_pushdowns = PyPushdowns(filters=filters._expr, partition_filters=partition_filters._expr)
    pushdowns = Pushdowns._from_pypushdowns(py_pushdowns)

    # roundtrip assert
    assert_eq(pushdowns.filters, filters)
    assert_eq(pushdowns.partition_filters, partition_filters)

    assert pushdowns.columns is None
    assert pushdowns.limit is None


def test_filter_required_column_names():
    # multiple columns with duplicates
    filters = sql_expr("a > 1 AND b < 2 OR c > 3 AND a < 10")
    pushdowns = Pushdowns(filters)
    assert {"a", "b", "c"} == pushdowns.filter_required_column_names()

    # single column
    filters = sql_expr("foo = 42")
    pushdowns = Pushdowns(filters)
    assert {"foo"} == pushdowns.filter_required_column_names()

    # no columns (literal only) is empty set
    filters = sql_expr("1 = 1")
    pushdowns = Pushdowns(filters)
    assert set() == pushdowns.filter_required_column_names()

    # nested expressions and repeated columns
    filters = sql_expr("(x > 0 AND y < 5) OR (x < 10 AND z = 3)")
    pushdowns = Pushdowns(filters)
    assert {"x", "y", "z"} == pushdowns.filter_required_column_names()

    # no filter is empty set
    pushdowns = Pushdowns(filters=None)
    assert set() == pushdowns.filter_required_column_names()
