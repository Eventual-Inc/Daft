from __future__ import annotations

import random

import pyarrow as pa
import pyarrow.parquet as papq
import pytest

import daft

PYARROW_GE_8_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) >= (8, 0, 0)


@pytest.mark.parametrize("has_none", [True, False])
def test_roundtrip_boolean_rle(tmp_path, has_none):
    file_path = f"{tmp_path}/test.parquet"
    if has_none:
        # Create an array of random True/False values that are None 10% of the time.
        random_bools = random.choices([True, False, None], weights=[45, 45, 10], k=1000_000)
    else:
        # Create an array of random True/False values.
        random_bools = random.choices([True, False], k=1000_000)
    pa_original = pa.table({"bools": pa.array(random_bools, type=pa.bool_())})
    # Use data page version 2.0 which uses RLE encoding for booleans.
    papq.write_table(pa_original, file_path, data_page_version="2.0")
    df_roundtrip = daft.read_parquet(file_path)
    assert pa_original == df_roundtrip.to_arrow()


# TODO: reading/writing:

# 2. Image type
# 3. Extension type?
