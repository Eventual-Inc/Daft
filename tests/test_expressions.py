from __future__ import annotations

import pytest

from daft.expressions import Expression, col


def test_column_expression_creation() -> None:
    c = col("name")
    assert isinstance(c, Expression)


def test_col_expr_add() -> None:
    id = col("id")
    assert id._is_column()
    new_id = id + 1
    assert new_id.has_call()


def test_name() -> None:
    expr = col("a") + col("b")
    assert expr.name() == "a"
    assert expr._required_columns() == {"a", "b"}

    new_expr = col("c") + expr
    new_expr.name() == "c"
    assert new_expr._required_columns() == {"c", "a", "b"}


def test_alias() -> None:
    expr = col("a") + col("b")
    assert expr.name() == "a"

    alias_expr = expr.alias("ab")
    assert alias_expr.name() == "ab"
    assert alias_expr._required_columns() == {"a", "b"}
    assert (alias_expr + col("c")).name() == "ab"
    assert (col("c") + alias_expr).name() == "c"

    assert (col("c") + alias_expr)._required_columns() == {"c", "a", "b"}


def test_column_expr_eq() -> None:
    assert col("a")._is_eq(col("a"))

    assert not col("a")._is_eq(col("b"))


def test_unary_op_eq() -> None:
    neg_col = -col("a")
    assert not neg_col._is_eq(col("a"))
    assert neg_col._is_eq(-col("a"))
    assert not (-neg_col)._is_eq(neg_col)
    assert not (-neg_col)._is_eq(abs(neg_col))


def test_binary_op_eq() -> None:
    assert col("a")._is_eq(col("a"))
    assert (col("a") + col("b"))._is_eq(col("a") + col("b"))

    assert not (col("a") + col("b"))._is_eq(col("a") + col("c"))

    assert not (col("a") + col("b"))._is_eq(col("b") + col("a"))

    assert not (col("a") + col("b"))._is_eq(col("b") + col("a"))

    assert not (col("a") + col("b"))._is_eq((col("a") + col("b")).alias("c"))

    assert (col("a") + col("b")).alias("c")._is_eq((col("a") + col("b")).alias("c"))

    assert not col("c")._is_eq((col("a") + col("b")).alias("c"))


def test_expression_bool() -> None:
    c = col("c")
    with pytest.raises(ValueError):
        if c:
            pass


def test_expression_bool_or() -> None:
    a = col("a")
    b = col("b")
    with pytest.raises(ValueError):
        a or b


def test_expression_bool_or_value() -> None:
    a = col("a")
    with pytest.raises(ValueError):
        a or 1


def test_expression_bool_and() -> None:
    a = col("a")
    b = col("b")
    with pytest.raises(ValueError):
        a and b


def test_expression_bool_and_value() -> None:
    a = col("a")
    with pytest.raises(ValueError):
        a and 1


def test_expression_fill_nan() -> None:
    a = col("a")
    act_expr = a.fillnan("5.1").__repr__()
    assert act_expr == "if_else(is_nan(col(a)), lit(5.1), col(a))"


def test_expression_fill_null() -> None:
    a = col("a")
    act_expr = a.fillnull("5").__repr__()
    assert act_expr == "if_else(is_null(col(a)), lit(5), col(a))"
