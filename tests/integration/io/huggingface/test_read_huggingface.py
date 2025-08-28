from __future__ import annotations

import pytest
from datasets import load_dataset

import daft
from daft import DataType as dt
from tests.conftest import assert_df_equals


@pytest.mark.integration()
def test_read_huggingface_datasets_doesnt_fail():
    # run it multiple times to ensure it doesn't fail
    for _ in range(10):
        df = daft.read_parquet("hf://datasets/huggingface/documentation-images")
        schema = df.schema()
        expected = daft.Schema.from_pydict({"image": dt.struct({"bytes": dt.binary(), "path": dt.string()})})
        assert schema == expected


@pytest.mark.integration()
@pytest.mark.parametrize(
    "path, split, sort_key",
    [
        ("Eventual-Inc/sample-parquet", "train", "foo"),
        ("fka/awesome-chatgpt-prompts", "train", "act"),
        ("nebius/SWE-rebench", "test", "instance_id"),
        ("SWE-Gym/SWE-Gym", "train", "instance_id"),
        # ("HuggingFaceFW/fineweb", "train", "id")
    ],
)
def test_read_huggingface(path, split, sort_key):
    ds = load_dataset(path, split=split)
    ds = ds.with_format("arrow")
    expected = ds.to_pandas()

    df = daft.read_huggingface(path)
    actual = df.to_pandas()

    assert_df_equals(actual, expected, sort_key)


@pytest.mark.integration()
@pytest.mark.parametrize(
    "path, schema",
    [
        (
            "https://huggingface.co/api/datasets/huggingface/documentation-images/parquet/default/train/0.parquet",
            daft.Schema.from_pydict({"image": dt.struct({"bytes": dt.binary(), "path": dt.string()})}),
        ),
        (
            "https://huggingface.co/api/datasets/Anthropic/hh-rlhf/parquet/default/train/0.parquet",
            daft.Schema.from_pydict({"chosen": dt.string(), "rejected": dt.string()}),
        ),
    ],
)
def test_read_huggingface_http_urls(path, schema):
    df = daft.read_parquet(path)
    assert df.schema() == schema
