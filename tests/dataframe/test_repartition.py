from __future__ import annotations


def test_into_partitions_some_empty(make_df) -> None:
    data = {"foo": [1, 2, 3]}
    df = make_df(data).into_partitions(32).collect()
    assert df.to_pydict() == data


def test_into_partitions_coalesce(make_df) -> None:
    data = {"foo": list(range(100))}
    df = make_df(data).into_partitions(20).into_partitions(1).collect()
    assert df.to_pydict() == data


def test_into_partitions_some_no_split(make_df) -> None:
    data = {"foo": [1, 2, 3]}

    # Materialize as 3 partitions
    df = make_df(data).into_partitions(3).collect()

    # Attempt to split into 4 partitions, so only 1 split occurs
    df = df.into_partitions(4).collect()

    assert df.to_pydict() == data
