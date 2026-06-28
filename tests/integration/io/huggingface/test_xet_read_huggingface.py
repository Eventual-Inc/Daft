from __future__ import annotations

import pytest

import daft
from daft.io import HuggingFaceConfig, IOConfig
from tests._hf_retry import call_with_hf_retry


@pytest.mark.integration()
def test_read_xet_backed_parquet_file():
    """Read a public dataset file known to be stored on the Xet backend."""
    test_file_path = (
        "hf://datasets/google-research-datasets/mbpp/full/train-00000-of-00001.parquet"
    )
    io_config = IOConfig(hf=HuggingFaceConfig(use_xet=True))

    with call_with_hf_retry(
        lambda: daft.read_parquet(test_file_path, io_config=io_config).collect()
    ) as df:
        assert len(df) > 0


@pytest.mark.integration()
def test_read_small_file_with_xet_enabled():
    """Small non-Xet files should still read successfully when use_xet=True."""
    test_file_path = "hf://datasets/Eventual-Inc/sample-files/README.md"
    io_config = IOConfig(hf=HuggingFaceConfig(use_xet=True))

    file = daft.File(test_file_path, io_config=io_config)
    with call_with_hf_retry(file.open) as f:
        assert len(f.read()) > 0
