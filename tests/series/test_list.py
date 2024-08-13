from __future__ import annotations

import pyarrow as pa
import pytest

from daft.series import Series
from tests.series import ARROW_FLOAT_TYPES, ARROW_INT_TYPES, ARROW_STRING_TYPES


@pytest.mark.parametrize("inner_dtype", ARROW_FLOAT_TYPES + ARROW_INT_TYPES)
@pytest.mark.parametrize("fixed", [False, True])
@pytest.mark.parametrize("desc", [False, True])
def test_list_list_sort(inner_dtype, fixed, desc):
    dtype = pa.list_(inner_dtype, list_size=2 if fixed else -1)
    data_list = [
        [20, 10],
        [None, 50],
        [45, 43],
        None,
        [43, 45],
        [50, 50],
        [80, None],
        [50, 52],
        None,
    ]
    data = pa.array(data_list, type=dtype)

    s = Series.from_arrow(data)
    res = s.list.sort(desc)
    assert s.datatype() == res.datatype()

    sorted_data_list = [
        (sorted(x, key=lambda v: (v is None, v), reverse=desc) if x is not None else x) for x in data_list
    ]
    assert res.to_pylist() == sorted_data_list


@pytest.mark.parametrize("inner_dtype", ARROW_STRING_TYPES)
@pytest.mark.parametrize("fixed", [False, True])
@pytest.mark.parametrize("desc", [False, True])
def test_list_list_sort_strings(inner_dtype, fixed, desc):
    dtype = pa.list_(inner_dtype, list_size=2 if fixed else -1)
    data_list = [
        ["hello", "world"],
        ["aaaa", "aaa"],
        ["aa", "aaaa"],
        [None, "hi"],
        None,
        ["", "blahblah"],
        ["None", None],
        None,
    ]
    data = pa.array(data_list, type=dtype)

    s = Series.from_arrow(data)
    res = s.list.sort(desc)
    assert s.datatype() == res.datatype()

    sorted_data_list = [
        (sorted(x, key=lambda v: (v is None, v), reverse=desc) if x is not None else None) for x in data_list
    ]
    assert res.to_pylist() == sorted_data_list


@pytest.mark.parametrize("fixed", [False, True])
def test_list_list_sort_multi_desc(fixed):
    dtype = pa.list_(pa.int64(), list_size=2 if fixed else -1)
    data_list = [
        [10, 20],
        [10, 20],
        [40, 30],
        [40, 30],
    ]
    data = pa.array(data_list, type=dtype)

    desc = Series.from_pylist([False, True, False, True])

    s = Series.from_arrow(data)
    res = s.list.sort(desc)
    assert s.datatype() == res.datatype()

    expected = [
        [10, 20],
        [20, 10],
        [30, 40],
        [40, 30],
    ]
    assert res.to_pylist() == expected
