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
