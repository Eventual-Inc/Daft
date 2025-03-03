from __future__ import annotations

from daft import Window, col

# from daft.expressions import count, max, mean, min, sum


def test_single_partition_sum(make_df):
    """Stage: PARTITION BY-Only Window Aggregations.

    Test sum over a single partition column.
    """
    df = make_df({"category": ["B", "A", "C", "A", "B", "C", "A", "B"], "value": [10, 5, 15, 8, 12, 6, 9, 7]})

    window = Window.partition_by("category")
    result = df.select(
        col("category"),
        col("value"),
        # sum("value").over(window).alias("sum"),
        col("value").sum().over(window).alias("sum"),
    ).collect()

    expected = {
        "category": ["A", "A", "A", "B", "B", "B", "C", "C"],
        "value": [5, 8, 9, 10, 12, 7, 15, 6],
        "sum": [22, 22, 22, 29, 29, 29, 21, 21],
    }
    assert result.to_pydict() == expected
