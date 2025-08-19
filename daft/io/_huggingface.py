from __future__ import annotations

from typing import TYPE_CHECKING

from daft.api_annotations import PublicAPI

from ._parquet import read_parquet

if TYPE_CHECKING:
    from daft.daft import IOConfig
    from daft.dataframe import DataFrame


@PublicAPI
def read_huggingface(repo: str, io_config: IOConfig | None = None) -> DataFrame:
    """Create a DataFrame from a Hugging Face dataset.

    Currently only supports Parquet datasets.

    Args:
        repo (str): repository to read in the form `username/dataset_name`
        io_config (IOConfig): Config to use when reading data
    """
    return read_parquet(f"hf://datasets/{repo}", io_config=io_config)
