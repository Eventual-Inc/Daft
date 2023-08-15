from __future__ import annotations

import daft


def test_into_partitions_some_empty() -> None:
    data = {"foo": [1, 2, 3]}
    df = daft.from_pydict(data).into_partitions(32).collect()
    assert df.to_pydict() == data
