from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest
from datasets import load_dataset

import daft
from daft import DataType as dt
from daft.exceptions import DaftCoreException
from tests.conftest import assert_df_equals


@pytest.mark.integration()
def test_read_huggingface_datasets_doesnt_fail():
    # run it multiple times to ensure it doesn't fail
    for _ in range(10):
        df = daft.read_huggingface("huggingface/documentation-images")
        schema = df.schema()
        expected = daft.Schema.from_pydict({"image": dt.struct({"bytes": dt.binary(), "path": dt.string()})})
        assert schema == expected


@pytest.mark.integration()
@pytest.mark.parametrize(
    "path, sort_key",
    [
        ("Eventual-Inc/sample-parquet", "foo"),
        ("fka/awesome-chatgpt-prompts", "act"),
        ("nebius/SWE-rebench", "instance_id"),
        ("SWE-Gym/SWE-Gym", "instance_id"),
        # ("HuggingFaceFW/fineweb", "id")
    ],
)
def test_read_huggingface(path, sort_key):
    # Load all splits and concatenate them to match what daft.read_huggingface() does
    ds = load_dataset(path)
    expected = pd.concat([ds[s].with_format("arrow").to_pandas() for s in ds.keys()], ignore_index=True)

    df = daft.read_huggingface(path)
    actual = df.to_pandas()

    assert_df_equals(actual, expected, sort_key)


@pytest.mark.integration()
def test_read_huggingface_fallback_on_400_error():
    """Test that read_huggingface falls back to datasets library when parquet files return 400 error."""
    repo = "Eventual-Inc/sample-parquet"

    # Mock read_parquet to raise a DaftCoreException with Status(400
    # This matches the actual error format from HuggingFace when parquet files aren't ready
    with patch("daft.io.huggingface.read_parquet") as mock_read_parquet:
        mock_read_parquet.side_effect = DaftCoreException(
            f"DaftError::External Unable to open file https://huggingface.co/api/datasets/{repo}/parquet: "
            f'reqwest::Error {{ kind: Status(400, None), url: "https://huggingface.co/api/datasets/{repo}/parquet" }}'
        )

        # This should trigger the fallback to datasets library
        df = daft.read_huggingface(repo)

        # Verify read_parquet was called with the correct HF path
        mock_read_parquet.assert_called_once_with(f"hf://datasets/{repo}", io_config=None)

        # Load expected data using datasets library (all splits)
        ds = load_dataset(repo)
        expected = pd.concat([ds[s].with_format("arrow").to_pandas() for s in ds.keys()], ignore_index=True)

        # Compare the results
        actual = df.to_pandas()
        assert_df_equals(actual, expected, "foo")


@pytest.mark.integration()
def test_read_huggingface_multi_split_dataset():
    """Test that read_huggingface works with datasets that have multiple splits.

    This tests both the main path (read_parquet) and fallback path (datasets library)
    to ensure they return the same data for a multi-split dataset.
    """
    repo = "stanfordnlp/imdb"

    # Main path: read_huggingface uses read_parquet internally
    df_main = daft.read_huggingface(repo)
    main_result = df_main.to_pandas()

    # Fallback path: mock read_parquet to force fallback to datasets library
    with patch("daft.io.huggingface.read_parquet") as mock_read_parquet:
        mock_read_parquet.side_effect = DaftCoreException(
            f"DaftError::External Unable to open file https://huggingface.co/api/datasets/{repo}/parquet: "
            f'reqwest::Error {{ kind: Status(400, None), url: "https://huggingface.co/api/datasets/{repo}/parquet" }}'
        )

        df_fallback = daft.read_huggingface(repo)
        fallback_result = df_fallback.to_pandas()

    # Both paths should return the same data
    assert_df_equals(main_result, fallback_result, sort_key=["text", "label"])
