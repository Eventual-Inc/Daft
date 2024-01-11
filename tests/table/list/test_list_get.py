from __future__ import annotations

from daft.expressions import col
from daft.table import MicroPartition


def test_list_get():
    table = MicroPartition.from_pydict(
        {
            "col1": [["a"], ["ab", "a"], [None, "a", ""], None, [""]],
            "col2": [[1, 2], [3, 4, 5], [], [6], [7, 8, 9]],
            "idx": [-1, 1, 0, 2, -4],
        }
    )

    result = table.eval_expression_list(
        [
            col("col1").list.get(0).alias("col1"),
            col("col1").list.get(col("idx")).alias("col1-idx"),
            col("col2").list.get(0).alias("col2"),
            col("col2").list.get(col("idx")).alias("col2-idx"),
            col("col1").list.get(0, default="z").alias("col1-default"),
            col("col1").list.get(col("idx"), default="z").alias("col1-idx-default"),
            col("col2").list.get(0, default=-1).alias("col2-default"),
            col("col2").list.get(col("idx"), default=-1).alias("col2-idx-default"),
        ]
    )

    assert result.to_pydict() == {
        "col1": ["a", "ab", None, None, ""],
        "col1-idx": ["a", "a", None, None, None],
        "col2": [1, 3, None, 6, 7],
        "col2-idx": [2, 4, None, None, None],
        "col1-default": ["a", "ab", None, "z", ""],
        "col1-idx-default": ["a", "a", None, "z", "z"],
        "col2-default": [1, 3, -1, 6, 7],
        "col2-idx-default": [2, 4, -1, -1, -1],
    }
