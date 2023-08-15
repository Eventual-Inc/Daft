from __future__ import annotations

import datetime

import pandas as pd
import pyarrow as pa
import pytest

import daft
from tests.utils import sort_arrow_table

TEST_DATA_LEN = 16
FIXED_SIZE_LIST_ARRAY_LENGTH = 4
TEST_DATA = {
    "dates": [datetime.date(2020, 1, 1) + datetime.timedelta(days=i) for i in range(TEST_DATA_LEN - 1)] + [None],
    "strings": [f"foo_{i}" for i in range(TEST_DATA_LEN - 1)] + [None],
    "integers": [i for i in range(TEST_DATA_LEN - 1)] + [None],
    "floats": [float(i) for i in range(TEST_DATA_LEN - 1)] + [None],
    "bools": [True for i in range(TEST_DATA_LEN - 1)] + [None],
    "var_sized_arrays": [[i for _ in range(i)] for i in range(TEST_DATA_LEN - 1)] + [None],
    # We manually created a fixed-size list array since Arrow's inference isn't aggressive enough to detect that
    # this is a fixed-size list array.
    "fixed_sized_arrays": pa.array(
        [[i for _ in range(FIXED_SIZE_LIST_ARRAY_LENGTH)] for i in range(TEST_DATA_LEN - 1)] + [None],
        type=pa.list_(pa.int64(), FIXED_SIZE_LIST_ARRAY_LENGTH),
    ),
    "structs": [{"foo": i} for i in range(TEST_DATA_LEN - 1)] + [None],
}
TEST_DATA_SCHEMA = pa.schema(
    [
        ("dates", pa.date32()),
        ("strings", pa.large_string()),
        ("integers", pa.int64()),
        ("floats", pa.float64()),
        ("bools", pa.bool_()),
        ("var_sized_arrays", pa.large_list(pa.int64())),
        ("fixed_sized_arrays", pa.list_(pa.int64(), FIXED_SIZE_LIST_ARRAY_LENGTH)),
        ("structs", pa.struct({"foo": pa.int64()})),
    ]
)


@pytest.mark.parametrize("n_partitions", [1, 2])
def test_to_arrow(n_partitions: int) -> None:
    df = daft.from_pydict(TEST_DATA).repartition(n_partitions)
    table = df.to_arrow()
    # String column is converted to large_string column in Daft.
    assert table.schema == TEST_DATA_SCHEMA
    expected_table = sort_arrow_table(pa.table(TEST_DATA, schema=table.schema), "integers")
    assert sort_arrow_table(table, "integers").equals(expected_table)


@pytest.mark.parametrize("n_partitions", [1, 2])
def test_to_pandas(n_partitions: int) -> None:
    df = daft.from_pydict(TEST_DATA).repartition(n_partitions)
    pd_df = df.to_pandas().sort_values("integers").reset_index(drop=True)
    expected_df = pd.DataFrame(TEST_DATA).sort_values("integers").reset_index(drop=True)
    pd.testing.assert_frame_equal(pd_df, expected_df)
