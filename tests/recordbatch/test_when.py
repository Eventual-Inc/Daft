from __future__ import annotations

import pytest

from daft.expressions import col
from daft.expressions.expressions import lit
from daft.functions import when
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
def test_table_expr_when(predicate, if_true, if_false, expected) -> None:
    daft_recordbatch = MicroPartition.from_pydict({"predicate": predicate, "if_true": if_true, "if_false": if_false})
    daft_recordbatch = daft_recordbatch.eval_expression_list(
        [when(col("predicate"), col("if_true")).otherwise(col("if_false"))]
    )
    pydict = daft_recordbatch.to_pydict()

    assert pydict["if_true"] == expected


@pytest.mark.skip(reason="missing_key does not get evaluated but it errors on binding")
@pytest.mark.parametrize(
    "when_expr",
    [
        when(lit(True), col("struct").get("key")).otherwise(col("struct").get("missing_key")),
        when(lit(False), col("struct").get("missing_key")).otherwise(col("struct").get("key")),
    ],
)
def test_table_expr_when_literal_predicate(when_expr) -> None:
    daft_recordbatch = MicroPartition.from_pydict({"struct": [{"key": "value"}]})
    daft_recordbatch = daft_recordbatch.eval_expression_list([when_expr])
    pydict = daft_recordbatch.to_pydict()

    assert pydict == {when_expr.name(): ["value"]}
