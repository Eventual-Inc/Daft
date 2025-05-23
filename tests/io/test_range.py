from __future__ import annotations

import pytest

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


@pytest.mark.skip("determine usage of 'num_partitions' in sources.")
def test_range_with_partitions():
    df = daft.range(2, 10, 2, 2)
    assert df.num_partitions() == 2


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
