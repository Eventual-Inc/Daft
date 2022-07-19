from daft.expressions import ColumnExpression, Expression, col, udf


def test_column_expression_creation() -> None:
    c = col("name")
    assert isinstance(c, Expression)


def test_col_expr_add() -> None:
    id = col("id")
    assert isinstance(id, ColumnExpression)
    new_id = id + 1
    assert new_id.is_operation()


def test_udf_single_return() -> None:
    @udf(num_returns=1)
    def f(x, y):
        return x + y

    assert f(10, 20) == 30

    output = f(10, col("y"))
    assert isinstance(output, Expression)
    assert output.is_operation()


def test_udf_multiple_return() -> None:
    @udf(num_returns=2)
    def f(x, y):
        return y, x

    assert f(10, 20) == (20, 10)

    output = f(10, col("y"))
    assert isinstance(output, tuple)
    assert len(output) == 2

    out1, out2 = output

    assert out1.is_operation()
    assert out2.is_operation()


def test_name() -> None:
    expr = col("a") + col("b")
    assert expr.name() == "a"
    assert set(expr.required_columns()) == {"a", "b"}

    new_expr = col("c") + expr
    new_expr.name() == "c"
    assert set(new_expr.required_columns()) == {"c", "a", "b"}


def test_alias() -> None:
    expr = col("a") + col("b")
    assert expr.name() == "a"

    alias_expr = expr.alias("ab")
    assert alias_expr.name() == "ab"

    assert set(alias_expr.required_columns()) == {"a", "b"}
    assert (alias_expr + col("c")) == "ab"
    assert (col("c") + alias_expr) == "c"

    assert set((col("c") + alias_expr).required_columns()) == {"c", "a", "b"}
