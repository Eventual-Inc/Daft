from __future__ import annotations

import pytest

from daft import DataType, col
from daft.table import Table


def test_table_size_bytes() -> None:

    data = Table.from_pydict({"a": [1, 2, 3, 4, None], "b": [False, True, False, True, None]}).eval_expression_list(
        [col("a").cast(DataType.int64()), col("b")]
    )
    assert data.size_bytes() == (5 * 8 + 1) + (1 + 1)


@pytest.mark.parametrize("length", [10, 200])
def test_table_size_bytes_pyobj(length) -> None:
    import pickle

    size_per_obj = len(pickle.dumps(object()))
    data = Table.from_pydict({"a": [object()] * length})
    assert data.size_bytes() == size_per_obj * length
