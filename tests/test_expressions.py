from daft.expression import ColumnExpression, Expression, col


def test_column_expression_creation() -> None:
    c = col("name")
    assert isinstance(c, Expression)


def test_col_expr_add() -> None:
    id = col("id")
    assert isinstance(id, ColumnExpression)
    new_id = id + 1
    print(new_id > 10)
    assert new_id.is_operation()


def test_udf() -> None:
    col("id")

    def f(x, y):
        return y, x
