from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from daft.api_annotations import PublicAPI
from daft.exceptions import DaftCoreException
from daft.io._parquet import read_parquet
from daft.io.webdataset import read_webdataset

if TYPE_CHECKING:
    from daft.daft import IOConfig
    from daft.dataframe import DataFrame


def _fallback_to_datasets_library(repo: str, original_error: Exception) -> DataFrame:
    """Fall back to using the datasets library when parquet files are not available."""
    try:
        from datasets import load_dataset
    except ImportError:
        raise ImportError(
            "Parquet files are not available for this dataset. "
            "Please install the datasets library for fallback support: pip install 'daft[huggingface]'"
        ) from original_error

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


@PublicAPI
def read_huggingface(
    repo: str,
    io_config: IOConfig | None = None,
    format: Literal["parquet", "webdataset"] | None = None,
) -> DataFrame:
    """Create a DataFrame from a Hugging Face dataset.

    Currently supports all public datasets, private Parquet datasets, and
    repositories stored as WebDataset TAR shards. See the
    [Hugging Face dataset docs](https://huggingface.co/docs/hub/en/datasets-overview)
    for more details.

    Args:
        repo (str): repository to read in the form `username/dataset_name`
        io_config (IOConfig): Config to use when reading data
        format: Dataset storage format. If ``None``, currently defaults to
            ``"parquet"``. Use ``"webdataset"`` to read uncompressed TAR shards.
    """
    if format == "webdataset":
        return read_webdataset(f"hf://datasets/{repo}/**/*.tar", io_config=io_config)
    if format not in (None, "parquet"):
        raise ValueError(f"Unsupported Hugging Face dataset format: {format!r}")

    try:
        # Try the fast path: read parquet files directly
        return read_parquet(f"hf://datasets/{repo}", io_config=io_config)
    except FileNotFoundError as e:
        # No parquet files found (glob returned no matches)
        # Fall back to using the datasets library
        return _fallback_to_datasets_library(repo, e)
    except DaftCoreException as e:
        # Check if this is a 400 error (parquet files not yet available)
        e_msg = str(e)
        if "Status(400" in e_msg or "400 Bad Request" in e_msg:
            # Fall back to using the datasets library
            return _fallback_to_datasets_library(repo, e)
        else:
            # Re-raise other errors
            raise
