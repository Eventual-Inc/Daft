from __future__ import annotations

import daft
from tests.conftest import get_tests_daft_runner_name


def test_into_partitions_some_empty(make_df) -> None:
    data = {"foo": [1, 2, 3]}
    df = make_df(data).into_partitions(32).collect()
    assert df.to_pydict() == data


def test_into_partitions_split(make_df) -> None:
    data = {"foo": list(range(100))}
    parts = list(make_df(data).into_partitions(20).iter_partitions())
    if get_tests_daft_runner_name() == "ray":
        import ray

        parts = ray.get(parts)
    values = set(v for p in parts for v in p.to_pydict()["foo"])
    assert values == set(range(100))


def test_into_partitions_coalesce() -> None:
    df = daft.range(100, partitions=10).into_partitions(2)
    parts = list(df.iter_partitions())
    assert len(parts) == 2
    if get_tests_daft_runner_name() == "ray":
        import ray

        parts = ray.get(parts)
    values = set(v for p in parts for v in p.to_pydict()["id"])
    assert values == set(range(100))


def test_into_partitions_split_and_coalesce(make_df) -> None:
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
