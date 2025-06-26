from __future__ import annotations

import pytest

from daft.expressions import col
from daft.expressions.expressions import lit
from daft.recordbatch.micropartition import MicroPartition


@pytest.mark.parametrize(
    ["predicate", "if_true", "if_false", "expected"],
    [
        # Single row
        ([True], [1], [2], [1]),
        ([False], [1], [2], [2]),
        # Multiple rows
        ([True, False, True], [1, 2, 3], [4, 5, 6], [1, 5, 3]),
        ([False, False, False], [1, 2, 3], [4, 5, 6], [4, 5, 6]),
    ],
)
def test_table_expr_if_else(predicate, if_true, if_false, expected) -> None:
    daft_recordbatch = MicroPartition.from_pydict({"predicate": predicate, "if_true": if_true, "if_false": if_false})
    daft_recordbatch = daft_recordbatch.eval_expression_list(
        [col("predicate").if_else(col("if_true"), col("if_false"))]
    )
    pydict = daft_recordbatch.to_pydict()

    assert pydict["if_true"] == expected


@pytest.mark.skip(reason="missing_key does not get evaluated but it errors on binding")
@pytest.mark.parametrize(
    "if_else_expr",
    [
        lit(True).if_else(col("struct").struct.get("key"), col("struct").struct.get("missing_key")),
        lit(False).if_else(col("struct").struct.get("missing_key"), col("struct").struct.get("key")),
    ],
)
def test_table_expr_if_else_literal_predicate(if_else_expr) -> None:
    daft_recordbatch = MicroPartition.from_pydict({"struct": [{"key": "value"}]})
    daft_recordbatch = daft_recordbatch.eval_expression_list([if_else_expr])
    pydict = daft_recordbatch.to_pydict()

    assert pydict == {if_else_expr.name(): ["value"]}
