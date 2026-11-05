from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Literal

from daft.api_annotations import PublicAPI
from daft.context import get_context
from daft.exceptions import DaftCoreException
from daft.io._parquet import read_parquet
from daft.io.huggingface._metadata import parquet_files, repo_root, source_files
from daft.io.webdataset import read_webdataset

if TYPE_CHECKING:
    from daft.daft import IOConfig
    from daft.dataframe import DataFrame


def _fallback_to_datasets_library(
    repo: str,
    original_error: Exception | None,
    io_config: IOConfig,
    config_name: str | None,
    split: str | None,
    data_dir: str | None,
    revision: str | None,
) -> DataFrame:
    """Fall back to using the datasets library when parquet files are not available."""
    try:
        from datasets import load_dataset
    except ImportError:
        raise ImportError(
            "Parquet files are not available for this dataset. "
            "Please install the datasets library for fallback support: pip install 'daft[huggingface]'"
        ) from original_error

    warnings.warn(
        f"Reading {repo!r} with the Hugging Face datasets library instead of Daft's native reader. "
        "This may download and materialize the entire selected dataset before returning; "
        "a subsequent limit() does not bound that work.",
        UserWarning,
        stacklevel=3,
    )

    # Load dataset using datasets library and convert to Daft.
    import daft
    from datasets import concatenate_datasets

    ds = load_dataset(
        repo,
        name=config_name,
        split=split,
        data_dir=data_dir,
        revision=revision,
        token=False if io_config.hf.anonymous else io_config.hf.token,
    )
    # Preserve the legacy all-splits behavior only when no split was requested.
    all_data = ds if split is not None else concatenate_datasets(list(ds.values()))
    # Convert to arrow format for better compatibility
    all_data = all_data.with_format("arrow")
    arrow_table = all_data.data.table
    return daft.from_arrow(arrow_table)


@PublicAPI
def read_huggingface(
    repo: str,
    io_config: IOConfig | None = None,
    format: Literal["parquet", "webdataset", "datasets"] | None = None,
    *,
    config_name: str | None = None,
    split: str | None = None,
    data_dir: str | None = None,
    revision: str | None = None,
    allow_partial: bool = False,
) -> DataFrame:
    """Create a DataFrame from a Hugging Face dataset.

    Reads native or HF-converted Parquet, or uncompressed WebDataset TAR shards.
    When conversion is unavailable, optionally installed ``datasets`` provides
    a fallback for source formats supported by that library. See the
    [Hugging Face dataset docs](https://huggingface.co/docs/hub/en/datasets-overview)
    for more details.

    Args:
        repo (str): repository to read in the form `username/dataset_name`
        io_config (IOConfig): Config to use when reading data
        format: Dataset storage format. If ``None``, currently defaults to
            ``"parquet"``. Use ``"webdataset"`` for uncompressed TAR shards,
            or ``"datasets"`` to explicitly load the original source with the
            Hugging Face datasets library.
        config_name: HF configuration (also called a subset). Omitted selections
            preserve the existing all-configs/all-splits Parquet behavior.
        split: A single named split. Slice expressions are not supported.
        data_dir: Select original files under a repository directory. This bypasses
            the viewer conversion, which does not describe source directories.
        revision: Repository branch, tag, or commit. Explicit non-main revisions
            read original files, never the viewer's conversion of current main.
        allow_partial: Explicitly allow incomplete converted Parquet, with a warning.

    Note:
        The datasets fallback can download/materialize the selection before this
        function returns. It is not a native lazy scan. Known partial conversions
        are rejected by default rather than returned as a complete dataset.
    """
    if format not in (None, "parquet", "webdataset", "datasets"):
        raise ValueError(f"Unsupported Hugging Face dataset format: {format!r}")
    for name, value in (("config_name", config_name), ("split", split), ("revision", revision), ("data_dir", data_dir)):
        if value is not None and (not isinstance(value, str) or not value.strip()):
            raise ValueError(f"{name} must be a nonempty string")
    if split is not None and any(character in split for character in "[]+"):
        raise ValueError("split must be a single named split, not a slice or combination")
    if data_dir is not None and (data_dir.startswith("/") or ".." in data_dir.split("/")):
        raise ValueError("data_dir must be a relative repository directory without '..'")
    io_config = get_context().daft_planning_config.default_io_config if io_config is None else io_config

    if format == "webdataset":
        if config_name is None and split is None and data_dir is None:
            paths: str | list[str] = f"{repo_root(repo, revision)}/**/*.tar"
        else:
            paths = source_files(repo, ".tar", config_name, split, data_dir, revision, io_config)
        return read_webdataset(paths, io_config=io_config)

    def fallback(error: Exception | None) -> DataFrame:
        return _fallback_to_datasets_library(repo, error, io_config, config_name, split, data_dir, revision)

    if format == "datasets":
        return fallback(None)

    def original_parquet_or_fallback(error: Exception) -> DataFrame:
        try:
            original_files = source_files(repo, ".parquet", config_name, split, data_dir, revision, io_config)
        except FileNotFoundError:
            return fallback(error)
        return read_parquet(original_files, io_config=io_config)

    try:
        if data_dir is not None or revision not in (None, "main"):
            files = source_files(repo, ".parquet", config_name, split, data_dir, revision, io_config)
        else:
            files = parquet_files(repo, config_name, split, allow_partial, io_config)
    except FileNotFoundError as e:
        if data_dir is not None or revision not in (None, "main"):
            return fallback(e)
        return original_parquet_or_fallback(e)
    except DaftCoreException as e:
        # Only unavailable-conversion discovery errors trigger fallback. Data-read,
        # authentication, transient, and format errors must propagate unchanged.
        e_msg = str(e)
        if any(marker in e_msg for marker in ("Status(400", "400 Bad Request", "Status(404", "404 Not Found")):
            return original_parquet_or_fallback(e)
        raise
    return read_parquet(files, io_config=io_config)
