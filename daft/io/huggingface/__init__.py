from __future__ import annotations

from typing import TYPE_CHECKING

from daft.api_annotations import PublicAPI
from daft.exceptions import DaftCoreException
from daft.io._parquet import read_parquet

if TYPE_CHECKING:
    from daft.daft import IOConfig
    from daft.dataframe import DataFrame


@PublicAPI
def read_huggingface(repo: str, io_config: IOConfig | None = None) -> DataFrame:
    """Create a DataFrame from a Hugging Face dataset.

    Currently supports all public datasets and all private Parquet datasets. See [the Hugging Face docs](https://huggingface.co/docs/dataset-viewer/en/parquet) for more details.

    Args:
        repo (str): repository to read in the form `username/dataset_name`
        io_config (IOConfig): Config to use when reading data
    """
    try:
        # Try the fast path: read parquet files directly
        return read_parquet(f"hf://datasets/{repo}", io_config=io_config)
    except DaftCoreException as e:
        # Check if this is a 400 error (parquet files not yet available)
        if "Status(400" in str(e):
            # Fall back to using the datasets library
            try:
                from datasets import load_dataset
            except ImportError:
                raise ImportError(
                    "Parquet files are not yet available for this dataset. "
                    "Please install the datasets library for fallback support: pip install 'daft[huggingface]'"
                ) from e

            # Load dataset using datasets library and convert to Daft
            import daft
            from datasets import concatenate_datasets

            # Load all splits and concatenate them to match the main path behavior
            ds = load_dataset(repo)
            all_data = concatenate_datasets([ds[split] for split in ds.keys()])
            # Convert to arrow format for better compatibility
            all_data = all_data.with_format("arrow")
            arrow_table = all_data.data.table
            return daft.from_arrow(arrow_table)
        else:
            # Re-raise other errors
            raise
