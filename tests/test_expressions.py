from daft.expressions import ColumnExpression, Expression, col


def test_column_expression_creation() -> None:
    c = col("name")
    assert isinstance(c, Expression)


def test_col_expr_add() -> None:
    id = col("id")
    assert isinstance(id, ColumnExpression)
    new_id = id + 1
    assert new_id.has_call()


def test_name() -> None:
    expr = col("a") + col("b")
    assert expr.name() == "a"
    assert expr.required_columns() == [col("a"), col("b")]

    new_expr = col("c") + expr
    new_expr.name() == "c"
    assert new_expr.required_columns() == [col("c"), col("a"), col("b")]


def test_alias() -> None:
    expr = col("a") + col("b")
    assert expr.name() == "a"

    alias_expr = expr.alias("ab")
    assert alias_expr.name() == "ab"

    assert alias_expr.required_columns() == [col("a"), col("b")]
    assert (alias_expr + col("c")) == "ab"
    assert (col("c") + alias_expr) == "c"

    assert (col("c") + alias_expr).required_columns() == [col("c"), col("a"), col("b")]


def test_column_expr_eq() -> None:
    assert col("a").is_eq(col("a"))

    assert not col("a").is_eq(col("b"))


def test_unary_op_eq() -> None:
    neg_col = -col("a")
    assert not neg_col.is_eq(col("a"))
    assert neg_col.is_eq(-col("a"))
    assert not (-neg_col).is_eq(neg_col)
    assert not (-neg_col).is_eq(abs(neg_col))


def test_binary_op_eq() -> None:
    assert col("a").is_eq(col("a"))
    assert (col("a") + col("b")).is_eq(col("a") + col("b"))

    assert not (col("a") + col("b")).is_eq(col("a") + col("c"))

    assert not (col("a") + col("b")).is_eq(col("b") + col("a"))

    assert not (col("a") + col("b")).is_eq(col("b") + col("a"))

    assert not (col("a") + col("b")).is_eq((col("a") + col("b")).alias("c"))

    assert (col("a") + col("b")).alias("c").is_eq((col("a") + col("b")).alias("c"))

    assert not col("c").is_eq((col("a") + col("b")).alias("c"))
