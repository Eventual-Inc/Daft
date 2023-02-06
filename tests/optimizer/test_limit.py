from __future__ import annotations

from daft.dataframe import DataFrame
from daft.expressions import col
from daft.logical import logical_plan


def test_limit(valid_data: list[dict[str, float]]) -> None:
    predicate_expr = col("sepal_length") > 4.8
    df = DataFrame.from_pylist(valid_data)

    df.schema()
    df = df.where(predicate_expr)
    df = df.limit(10)

    assert isinstance(df.plan(), logical_plan.GlobalLimit)
