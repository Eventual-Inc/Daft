from __future__ import annotations

from daft.expressions import col
from daft.table import MicroPartition


def test_struct_field():
    table = MicroPartition.from_pydict(
        {
            "col": [
                {"foo": 1, "bar": "a"},
                {"foo": None, "bar": "b"},
                None,
                {"foo": 4, "bar": None},
            ]
        }
    )

    result = table.eval_expression_list([col("col").struct.field("foo"), col("col").struct.field("bar")])

    assert result.to_pydict() == {"foo": [1, None, None, 4], "bar": ["a", "b", None, None]}
