# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations
from typing import Optional, Union

from daft import context
from daft.api_annotations import PublicAPI
from daft.daft import IOConfig, ScanOperatorHandle
from daft.dataframe import DataFrame
from daft.io.lance.lance_scan import LanceDBScanOperator
from daft.io.object_store_options import io_config_to_storage_options
from daft.logical.builder import LogicalPlanBuilder


@PublicAPI
def read_lance(
    url: str,
    io_config: Optional[IOConfig] = None,
    version: Optional[Union[str, int]] = None,
    asof: Optional[str] = None,
    block_size: Optional[int] = None,
    commit_lock: Optional[object] = None,
    index_cache_size: Optional[int] = None,
    default_scan_options: Optional[dict[str, str]] = None,
    metadata_cache_size_bytes: Optional[int] = None,
) -> DataFrame:
    """Create a DataFrame from a LanceDB table.

    Args:
        url: URL to the LanceDB table (supports remote URLs to object stores such as `s3://` or `gs://`)
        io_config: A custom IOConfig to use when accessing LanceDB data. Defaults to None.

    Returns:
        DataFrame: a DataFrame with the schema converted from the specified LanceDB table

    Note:
        This function requires the use of [LanceDB](https://lancedb.github.io/lancedb/), which is the Python library for the LanceDB project.
        To ensure that this is installed with Daft, you may install: `pip install daft[lance]`

    Examples:
        Read a local LanceDB table:
        >>> df = daft.read_lance("s3://my-lancedb-bucket/data/")
        >>> df.show()

        Read a LanceDB table from a public S3 bucket:
        >>> from daft.io import S3Config
        >>> s3_config = S3Config(region="us-west-2", anonymous=True)
        >>> df = daft.read_lance("s3://daft-public-data/lance/words-test-dataset", io_config=s3_config)
        >>> df.show()

        Read a local LanceDB table and specify a version:
        >>> df = daft.read_lance("s3://my-lancedb-bucket/data/", version=1)
        >>> df.show()
    """
    try:
        import lance
    except ImportError as e:
        raise ImportError(
            "Unable to import the `lance` package, please ensure that Daft is installed with the lance extra dependency: `pip install daft[lance]`"
        ) from e

    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config
    storage_options = io_config_to_storage_options(io_config, url)

    ds = lance.dataset(
        url,
        storage_options=storage_options,
        version=version,
        asof=asof,
        block_size=block_size,
        commit_lock=commit_lock,
        index_cache_size=index_cache_size,
        default_scan_options=default_scan_options,
        metadata_cache_size_bytes=metadata_cache_size_bytes,
    )
    lance_operator = LanceDBScanOperator(ds)

    handle = ScanOperatorHandle.from_python_scan_operator(lance_operator)
    builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
    return DataFrame(builder)
