import random
import string

import numpy as np
import pyarrow as pa
import pytest

from daft.internal.kernels.search_sorted import search_sorted_chunked_array

int_types = [pa.int8(), pa.uint8(), pa.int16(), pa.uint16(), pa.int32(), pa.uint32(), pa.int64(), pa.uint64()]
float_types = [pa.float32(), pa.float64()]
number_types = int_types + float_types


@pytest.mark.parametrize("num_chunks", range(1, 4))
@pytest.mark.parametrize("dtype", number_types, ids=[repr(it) for it in number_types])
def test_number_array(num_chunks, dtype) -> None:
    pa_keys = pa.chunked_array([np.array([4, 2, 1, 3], dtype=dtype.to_pandas_dtype()) for _ in range(num_chunks)])
    np_keys = pa_keys.to_numpy()
    np_sorted_arr = np.arange(100, dtype=dtype.to_pandas_dtype()) * 10
    result = np.searchsorted(np_sorted_arr, np_keys)
    pa_sorted_arr = pa.chunked_array([np_sorted_arr], type=dtype)
    pa_result = search_sorted_chunked_array(pa_sorted_arr, pa_keys)
    assert np.all(result == pa_result.to_numpy())


def test_number_array_with_nulls() -> None:
    keys = np.random.randint(0, 100, 1000)
    data = np.arange(100)
    result = np.searchsorted(data, keys)
    pa_data = pa.chunked_array([data])
    pa_keys = pa.chunked_array([pa.chunked_array([keys] + [[None] * 10] + [keys]).combine_chunks()])
    pa_result = search_sorted_chunked_array(pa_data, pa_keys)
    assert np.all(result == pa_result[:1000].to_numpy())
    assert np.all(result == pa_result[1000 + 10 :].to_numpy())
    assert pa_result[1000 : 1000 + 10].null_count == 10


def gen_random_str(k: int):
    return "".join(random.choices(string.ascii_uppercase + string.ascii_lowercase, k=k))


@pytest.mark.parametrize("str_len", range(0, 10))
@pytest.mark.parametrize("num_chunks", range(1, 4))
def test_string_array(str_len, num_chunks) -> None:

    pa_keys = pa.chunked_array(
        [[gen_random_str(str_len) for _ in range(10)] for _ in range(num_chunks)],
    )
    keys = pa_keys.to_numpy()

    data = np.array([gen_random_str(str_len + 1) for i in range(10)])
    data.sort()
    result = np.searchsorted(data, keys)
    pa_data = pa.chunked_array([data])
    pa_result = search_sorted_chunked_array(pa_data, pa_keys)
    assert np.all(result == pa_result.to_numpy())


def test_string_array_with_nulls() -> None:
    py_keys = [str(i) for i in range(100)]
    random.shuffle(py_keys)
    keys = np.array(py_keys)
    data = np.array([gen_random_str(10 + 1) for i in range(10)])
    data.sort()
    result = np.searchsorted(data, keys)
    pa_data = pa.chunked_array([data])
    pa_keys = pa.chunked_array([py_keys + [None] * 10 + py_keys])
    pa_result = search_sorted_chunked_array(pa_data, pa_keys)
    assert np.all(result == pa_result[:100].to_numpy())
    assert np.all(result == pa_result[100 + 10 :].to_numpy())
    assert pa_result[100 : 100 + 10].null_count == 10


@pytest.mark.parametrize("str_len", range(0, 10))
@pytest.mark.parametrize("num_chunks", range(1, 4))
def test_large_string_array(str_len, num_chunks) -> None:
    pa_keys = pa.chunked_array(
        [[gen_random_str(str_len) for _ in range(10)] for _ in range(num_chunks)], type=pa.large_string()
    )

    keys = pa_keys.cast(pa.string()).to_numpy()
    data = np.array([gen_random_str(10 + 1) for i in range(10)])
    data.sort()
    result = np.searchsorted(data, keys)
    pa_data = pa.chunked_array([data]).cast(pa.large_string())
    pa_result = search_sorted_chunked_array(pa_data, pa_keys)
    assert np.all(result == pa_result.to_numpy())


def test_large_string_array_with_nulls() -> None:
    py_keys = [str(i) for i in range(100)]
    random.shuffle(py_keys)
    keys = np.array(py_keys)
    data = np.array([gen_random_str(10 + 1) for i in range(10)])
    data.sort()
    result = np.searchsorted(data, keys)
    pa_data = pa.chunked_array([data]).cast(pa.large_string())
    pa_keys = pa.chunked_array([py_keys + [None] * 10 + py_keys], type=pa.large_string())
    pa_result = search_sorted_chunked_array(pa_data, pa_keys)
    assert np.all(result == pa_result[:100].to_numpy())
    assert np.all(result == pa_result[100 + 10 :].to_numpy())
    assert pa_result[100 : 100 + 10].null_count == 10
