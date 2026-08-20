from __future__ import annotations

import pyarrow as pa

from daft.expressions import col
from daft.recordbatch import MicroPartition


def test_map_keys():
    data = pa.array([[("a", 1), ("b", 2)], [("c", 3)], [], None], type=pa.map_(pa.string(), pa.int64()))
    table = MicroPartition.from_arrow(pa.table({"map_col": data}))

    result = table.eval_expression_list([col("map_col").map_keys()])

    assert result.to_pydict() == {"keys": [["a", "b"], ["c"], None, None]}
