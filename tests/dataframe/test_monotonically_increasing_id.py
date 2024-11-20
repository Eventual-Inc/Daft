from __future__ import annotations

import pytest

from daft.datatype import DataType
from daft.io._generator import read_generator
from daft.table.table import Table
from tests.conftest import get_tests_daft_runner_name


def test_monotonically_increasing_id_single_partition(make_df) -> None:
    data = {"a": [1, 2, 3, 4, 5]}
    df = make_df(data)._add_monotonically_increasing_id().collect()

    assert len(df) == 5
    assert set(df.column_names) == {"id", "a"}
    assert df.schema()["id"].dtype == DataType.uint64()
    assert df.to_pydict() == {"id": [0, 1, 2, 3, 4], "a": [1, 2, 3, 4, 5]}


def test_monotonically_increasing_id_empty_table(make_df) -> None:
    data = {"a": []}
    df = make_df(data)._add_monotonically_increasing_id().collect()

    assert len(df) == 0
    assert set(df.column_names) == {"id", "a"}
    assert df.schema()["id"].dtype == DataType.uint64()
    assert df.to_pydict() == {"id": [], "a": []}


@pytest.mark.skipif(
    get_tests_daft_runner_name() == "native",
    reason="Native runner does not support repartitioning",
)
@pytest.mark.parametrize("repartition_nparts", [1, 2, 20, 50, 100])
def test_monotonically_increasing_id_multiple_partitions_with_into_partition(make_df, repartition_nparts) -> None:
    ITEMS = [i for i in range(100)]

    data = {"a": ITEMS}
    df = make_df(data).into_partitions(repartition_nparts)._add_monotonically_increasing_id().collect()

    assert len(df) == 100
    assert set(df.column_names) == {"id", "a"}
    assert df.schema()["id"].dtype == DataType.uint64()

    # we can predict the ids because into_partitions evenly distributes without shuffling the data,
    # and the chosen repartition_nparts is a multiple of the number of items, so each partition will have the same number of items
    items_per_partition = len(ITEMS) // repartition_nparts
    ids = []
    for index, _ in enumerate(ITEMS):
        partition_num = index // items_per_partition
        counter = index % items_per_partition
        ids.append(partition_num << 36 | counter)

    assert df.to_pydict() == {"id": ids, "a": ITEMS}


def test_monotonically_increasing_id_from_generator() -> None:
    ITEMS = list(range(10))
    table = Table.from_pydict({"a": ITEMS})

    num_tables = 3
    num_generators = 3

    def generator():
        for _ in range(num_tables):
            yield table

    def generators():
        for _ in range(num_generators):
            yield generator

    df = read_generator(generators(), schema=table.schema())._add_monotonically_increasing_id().collect()

    assert len(df) == 90
    assert set(df.column_names) == {"id", "a"}
    assert df.schema()["id"].dtype == DataType.uint64()

    if get_tests_daft_runner_name() == "native":
        # On the native runner, there are no partitions, so the ids are just the row numbers.
        assert df.to_pydict() == {"id": list(range(90)), "a": ITEMS * 9}
    else:
        # On the ray / py runner, the ids are generated based on the partition number and the row number within the partition.
        # The partition number is put in the upper 28 bits and the row number is put in the lower 36 bits.
        # There are num_generators partitions, and each partition has num_tables * len(ITEMS) rows.
        ids = [(p << 36) | c for p in range(num_generators) for c in range(num_tables * len(ITEMS))]
        assert df.to_pydict() == {"id": ids, "a": ITEMS * 9}


@pytest.mark.parametrize("repartition_nparts", [1, 2, 20, 50, 100])
def test_monotonically_increasing_id_multiple_partitions_with_repartition(make_df, repartition_nparts) -> None:
    ITEMS = [i for i in range(100)]

    data = {"a": ITEMS}
    df = make_df(data, repartition=repartition_nparts)._add_monotonically_increasing_id().collect()

    assert len(df) == 100
    assert set(df.column_names) == {"id", "a"}
    assert df.schema()["id"].dtype == DataType.uint64()

    py_dict = df.to_pydict()
    assert set(py_dict["a"]) == set(ITEMS)

    # cannot predict the ids because repartition shuffles the data, so we just check that they are unique
    assert len(set(py_dict["id"])) == 100


def test_monotonically_increasing_id_custom_col_name(make_df) -> None:
    data = {"a": [1, 2, 3, 4, 5]}
    df = make_df(data)._add_monotonically_increasing_id("custom_id").collect()

    assert len(df) == 5
    assert set(df.column_names) == {"custom_id", "a"}
    assert df.schema()["custom_id"].dtype == DataType.uint64()
    assert df.to_pydict() == {"custom_id": [0, 1, 2, 3, 4], "a": [1, 2, 3, 4, 5]}
