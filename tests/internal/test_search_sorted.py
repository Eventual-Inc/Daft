import numpy as np
import pyarrow as pa

from daft.internal.search_sorted import search_sorted_chunked_array


def test_int_array() -> None:
    keys = np.random.randint(0, 100, 1000)
    data = np.arange(100)
    result = np.searchsorted(data, keys)
    pa_data = pa.chunked_array([data])
    pa_keys = pa.chunked_array([keys])
    pa_result = search_sorted_chunked_array(pa_data, pa_keys)
    assert np.all(result == pa_result.to_numpy())
