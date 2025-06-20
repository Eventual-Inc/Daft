from __future__ import annotations

import pytest
import ray

import daft


def test_range():
    df = daft.range(5)
    assert df.to_pydict() == {"id": [0, 1, 2, 3, 4]}


def test_range_with_start():
    df = daft.range(2, 5)
    assert df.to_pydict() == {"id": [2, 3, 4]}


def test_range_with_step():
    df = daft.range(2, 10, 2)
    assert df.to_pydict() == {"id": [2, 4, 6, 8]}


def test_range_with_step_kwargs():
    df = daft.range(2, 10, step=2)
    assert df.to_pydict() == {"id": [2, 4, 6, 8]}


def test_with_start_end_and_step_kwargs():
    df = daft.range(start=2, end=10, step=2)
    assert df.to_pydict() == {"id": [2, 4, 6, 8]}


def test_with_no_args_raises_error():
    with pytest.raises(TypeError):
        daft.range()


def test_range_called_multiple_times():
    df = daft.range(10)
    assert df.count_rows() == 10
    assert df.count_rows() == 10
    assert len(df.collect()) == 10
    assert len(df.collect()) == 10

    # test with op
    df_with_filter = df.filter(daft.col("id") >= 5)
    assert df_with_filter.count_rows() == 5
    assert df_with_filter.count_rows() == 5
    assert len(df_with_filter.collect()) == 5
    assert len(df_with_filter.collect()) == 5


def test_range_partitioning_even():
    df = daft.range(0, 10, 1, 2)
    assert df.num_partitions() == 2

    partitions = list(df.iter_partitions())
    partitions = ray.get(partitions) if isinstance(partitions[0], ray.ObjectRef) else partitions
    assert len(partitions) == 2

    assert partitions[0].to_pydict() == {"id": [0, 1, 2, 3, 4]}
    assert partitions[1].to_pydict() == {"id": [5, 6, 7, 8, 9]}


def test_range_partitioning_uneven():
    df = daft.range(0, 10, 1, 3)
    assert df.num_partitions() == 3

    partitions = list(df.iter_partitions())
    partitions = ray.get(partitions) if isinstance(partitions[0], ray.ObjectRef) else partitions
    assert len(partitions) == 3

    assert partitions[0].to_pydict() == {"id": [0, 1, 2, 3]}
    assert partitions[1].to_pydict() == {"id": [4, 5, 6]}
    assert partitions[2].to_pydict() == {"id": [7, 8, 9]}


def test_range_partitioning_with_step_even():
    df = daft.range(0, 24, 2, 4)
    assert df.num_partitions() == 4

    partitions = list(df.iter_partitions())
    partitions = ray.get(partitions) if isinstance(partitions[0], ray.ObjectRef) else partitions
    assert len(partitions) == 4

    assert partitions[0].to_pydict() == {"id": [0, 2, 4]}
    assert partitions[1].to_pydict() == {"id": [6, 8, 10]}
    assert partitions[2].to_pydict() == {"id": [12, 14, 16]}
    assert partitions[3].to_pydict() == {"id": [18, 20, 22]}


def test_range_partitioning_with_step_uneven():
    df = daft.range(0, 15, 2, 3)
    assert df.num_partitions() == 3

    partitions = list(df.iter_partitions())
    partitions = ray.get(partitions) if isinstance(partitions[0], ray.ObjectRef) else partitions
    assert len(partitions) == 3

    assert partitions[0].to_pydict() == {"id": [0, 2, 4]}
    assert partitions[1].to_pydict() == {"id": [6, 8, 10]}
    assert partitions[2].to_pydict() == {"id": [12, 14]}


def test_range_partitioning_with_negative_step_even():
    df = daft.range(10, -2, -2, 2)
    assert df.num_partitions() == 2

    partitions = list(df.iter_partitions())
    partitions = ray.get(partitions) if isinstance(partitions[0], ray.ObjectRef) else partitions
    assert len(partitions) == 2

    assert partitions[0].to_pydict() == {"id": [10, 8, 6]}
    assert partitions[1].to_pydict() == {"id": [4, 2, 0]}


def test_range_partitioning_with_negative_step_uneven():
    df = daft.range(15, 0, -2, 3)
    assert df.num_partitions() == 3

    partitions = list(df.iter_partitions())
    partitions = ray.get(partitions) if isinstance(partitions[0], ray.ObjectRef) else partitions
    assert len(partitions) == 3

    assert partitions[0].to_pydict() == {"id": [15, 13, 11]}
    assert partitions[1].to_pydict() == {"id": [9, 7, 5]}
    assert partitions[2].to_pydict() == {"id": [3, 1]}


def test_range_negative_step_validation():
    with pytest.raises(
        ValueError,
        match="daft.range\\(\\) with negative step -2 requires start \\(5\\) to be greater than end \\(10\\)",
    ):
        daft.range(5, 10, -2)

    # Should raise error when start == end for negative step
    with pytest.raises(
        ValueError,
        match="daft.range\\(\\) with negative step -1 requires start \\(5\\) to be greater than end \\(5\\)",
    ):
        daft.range(5, 5, -1)


def test_range_positive_step_validation():
    # Should raise error when start >= end for positive step
    with pytest.raises(
        ValueError,
        match="daft.range\\(\\) with positive step 2 requires start \\(10\\) to be less than end \\(5\\)",
    ):
        daft.range(10, 5, 2)

    # Should raise error when start == end for positive step
    with pytest.raises(
        ValueError,
        match="daft.range\\(\\) with positive step 1 requires start \\(5\\) to be less than end \\(5\\)",
    ):
        daft.range(5, 5, 1)


def test_range_zero_step_validation():
    with pytest.raises(
        ValueError,
        match="daft.range\\(\\) step parameter cannot be zero - use a positive or negative integer",
    ):
        daft.range(0, 10, 0)
