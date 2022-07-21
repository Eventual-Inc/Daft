from daft.expressions import ColumnExpression, Expression, col, udf


def test_column_expression_creation() -> None:
    c = col("name")
    assert isinstance(c, Expression)


def test_col_expr_add() -> None:
    id = col("id")
    assert isinstance(id, ColumnExpression)
    new_id = id + 1
    assert new_id.has_call()


def test_udf_single_return() -> None:
    @udf(num_returns=1)
    def f(x, y):
        return x + y

    assert f(10, 20) == 30

    output = f(10, col("y"))
    assert isinstance(output, Expression)
    assert output.has_call()


def test_udf_multiple_return() -> None:
    @udf(num_returns=2)
    def f(x, y):
        return y, x

    assert f(10, 20) == (20, 10)

    output = f(10, col("y"))
    assert isinstance(output, tuple)
    assert len(output) == 2

    out1, out2 = output

    assert out1.has_call()
    assert out2.has_call()


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
