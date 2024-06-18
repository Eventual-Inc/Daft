from __future__ import annotations

import datetime

from daft.expressions import col
from daft.table import MicroPartition


def test_utf8_to_date():
    table = MicroPartition.from_pydict({"col": ["2021-01-01", None, "2021-01-02", "2021-01-03", "2021-01-04"]})
    result = table.eval_expression_list([col("col").str.to_date("%Y-%m-%d")])
    assert result.to_pydict() == {
        "col": [
            datetime.date(2021, 1, 1),
            None,
            datetime.date(2021, 1, 2),
            datetime.date(2021, 1, 3),
            datetime.date(2021, 1, 4),
        ]
    }
