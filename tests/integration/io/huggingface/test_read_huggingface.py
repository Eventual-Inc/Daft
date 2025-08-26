from __future__ import annotations

import pytest
from datasets import load_dataset

import daft
from tests.conftest import assert_df_equals


@pytest.mark.skip("Failing due to parquet read bug. See: https://github.com/Eventual-Inc/Daft/issues/5003")
@pytest.mark.integration()
def test_read_huggingface_datasets_doesnt_fail():
    from daft import DataType as dt

    # run it multiple times to ensure it doesn't fail
    for _ in range(10):
        df = daft.read_parquet("hf://datasets/huggingface/documentation-images")
        schema = df.schema()
        expected = daft.Schema.from_pydict({"image": dt.struct({"bytes": dt.binary(), "path": dt.string()})})
        assert schema == expected


@pytest.mark.integration()
def test_read_huggingface():
    ds = load_dataset("Eventual-Inc/sample-parquet", split="train")
    ds = ds.with_format("arrow")
    expected = ds.to_pandas()

    df = daft.read_huggingface("Eventual-Inc/sample-parquet")
    actual = df.to_pandas()

    assert_df_equals(actual, expected, "foo")
