from __future__ import annotations

import pytest

import daft
from daft import DataType


@pytest.fixture
def input_df():
    df = daft.range(start=0, end=1024, partitions=100)
    df = df.with_columns(
        {
            "name": df["id"].apply(func=lambda x: f"user_{x}", return_dtype=DataType.string()),
            "email": df["id"].apply(func=lambda x: f"user_{x}@getdaft.io", return_dtype=DataType.string()),
        }
    )
    return df


def test_lancedb_read_and_write(input_df):
    # input_df.write_lance(
    #     uri="/tmp/daft/lance",
    #     use_native_writer=True,
    #     # mode="overwrite",
    # )
    # df.explain(show_all=True)

    df = daft.read_lance(
        url="/opt/workspace/lance/my_table.lance",
        use_native_reader=True,
    )

    print(df.schema())

    df.show()
