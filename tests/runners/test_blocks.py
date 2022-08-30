import datetime

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from daft.runners import blocks


class MyObj:
    def __init__(self, x):
        self._x = x

    def __eq__(self, other):
        if not isinstance(other, MyObj):
            return False
        return self._x == other._x


def _get_block_in_variations(l, expected_block_out, arrow_variations: bool = False):
    blocks_in = [
        l,
        pd.Series(l),
        np.array(l),
    ]
    if arrow_variations:
        blocks_in.extend([pa.array(l), pa.chunked_array([l])])
    return [(block_in, expected_block_out) for block_in in blocks_in]


@pytest.mark.parametrize(
    ["block_in", "block_out"],
    [
        # Scalars that fit into Arrow
        ("test", blocks.ArrowDataBlock(pa.scalar("test"))),
        (1, blocks.ArrowDataBlock(pa.scalar(1))),
        (True, blocks.ArrowDataBlock(pa.scalar(True))),
        (0.1, blocks.ArrowDataBlock(pa.scalar(0.1))),
        (datetime.date(1994, 1, 1), blocks.ArrowDataBlock(pa.scalar(datetime.date(1994, 1, 1)))),
        # TODO(jay): These fail right now but should be made to work
        # Scalars that don't fit into Arrow (nested data types, Python objects etc)
        # (MyObj(1), blocks.PyListDataBlock(MyObj(1))),
        # ({"foo": 1}, blocks.PyListDataBlock({"foo": 1})),
        # (np.array([1, 2, 3]), blocks.PyListDataBlock(np.array([1, 2, 3])),  # How to differentiate this as a "lit" as opposed to a normal seq block?
        # Sequences that fit into Arrow
        *_get_block_in_variations(
            [1, 2, 3], blocks.ArrowDataBlock(pa.chunked_array([[1, 2, 3]])), arrow_variations=True
        ),
        *_get_block_in_variations(
            ["foo", "bar", "baz"],
            blocks.ArrowDataBlock(pa.chunked_array([["foo", "bar", "baz"]])),
            arrow_variations=True,
        ),
        # Sequences with Nested data types
        *_get_block_in_variations(
            [{"foo": 1}, {"bar": 1}, {"baz": 1}], blocks.PyListDataBlock([{"foo": 1}, {"bar": 1}, {"baz": 1}])
        ),
        *_get_block_in_variations(
            [[1, 2, 3], [1, 2, 3], [1, 2, 3]], blocks.PyListDataBlock([[1, 2, 3], [1, 2, 3], [1, 2, 3]])
        ),
        # Sequences with Python objects
        *_get_block_in_variations([[1], [1, 2], [1, 2, 3]], blocks.PyListDataBlock([[1], [1, 2], [1, 2, 3]])),
        *_get_block_in_variations(
            [np.array([1, 2, 3]), np.array([1, 2, 3]), np.array([1, 2, 3])],
            blocks.PyListDataBlock([np.array([1, 2, 3]), np.array([1, 2, 3]), np.array([1, 2, 3])]),
        ),
        *_get_block_in_variations(
            [np.array([1]), np.array([1, 2]), np.array([1, 2, 3])],
            blocks.PyListDataBlock([np.array([1]), np.array([1, 2]), np.array([1, 2, 3])]),
        ),
        *_get_block_in_variations(
            [MyObj(1), MyObj(2), MyObj(3)], blocks.PyListDataBlock([MyObj(1), MyObj(2), MyObj(3)])
        ),
    ],
)
def test_make_block(block_in, block_out):
    b = blocks.DataBlock.make_block(block_in)
    assert isinstance(b, type(block_out))

    # DataBlock's equality operator doesn't work for np.ndarray, so we special-case it
    if isinstance(b.data, np.ndarray):
        assert isinstance(block_out.data, np.ndarray)
        np.testing.assert_almost_equal(b.data, block_out.data)
    elif (isinstance(b.data, list) or isinstance(b.data, np.ndarray)) and isinstance(b.data[0], np.ndarray):
        assert len(b.data) == len(block_out.data)
        for actual, expected in zip(b.data, block_out.data):
            np.testing.assert_almost_equal(actual, expected)
    else:
        assert b.data == block_out.data
