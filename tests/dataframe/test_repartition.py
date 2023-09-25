from __future__ import annotations

import daft


def test_into_partitions_some_empty() -> None:
    data = {"foo": [1, 2, 3]}
    df = daft.from_pydict(data).into_partitions(32).collect()
    assert df.to_pydict() == data


def test_into_partitions_coalesce() -> None:
    data = {"foo": list(range(100))}
    df = daft.from_pydict(data).into_partitions(20).into_partitions(1).collect()
    assert df.to_pydict() == data
