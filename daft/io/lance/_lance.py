# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations
from typing import TYPE_CHECKING, Any, Callable, Optional, Union

from daft import context
from daft.api_annotations import PublicAPI
from daft.daft import IOConfig, ScanOperatorHandle
from daft.dataframe import DataFrame
from daft.io.lance.lance_merge_column import merge_columns_internal
from daft.io.lance.lance_scan import LanceDBScanOperator
from daft.io.object_store_options import io_config_to_storage_options
from daft.logical.builder import LogicalPlanBuilder

if TYPE_CHECKING:
    from daft.dependencies import pa

    try:
        from lance.udf import BatchUDF
    except ImportError:
        BatchUDF = None


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
    fragment_group_size: Optional[int] = None,
) -> DataFrame:
    """Create a DataFrame from a LanceDB table.

    Args:
        url: URL to the LanceDB table (supports remote URLs to object stores such as `s3://` or `gs://`)
        io_config: A custom IOConfig to use when accessing LanceDB data. Defaults to None.
        version : optional, int | str
            If specified, load a specific version of the Lance dataset. Else, loads the
            latest version. A version number (`int`) or a tag (`str`) can be provided.
        asof : optional, datetime or str
            If specified, find the latest version created on or earlier than the given
            argument value. If a version is already specified, this arg is ignored.
        block_size : optional, int
            Block size in bytes. Provide a hint for the size of the minimal I/O request.
        commit_lock : optional, lance.commit.CommitLock
            A custom commit lock.  Only needed if your object store does not support
            atomic commits.  See the user guide for more details.
        index_cache_size : optional, int
            Index cache size. Index cache is a LRU cache with TTL. This number specifies the
            number of index pages, for example, IVF partitions, to be cached in
            the host memory. Default value is ``256``.

            Roughly, for an ``IVF_PQ`` partition with ``n`` rows, the size of each index
            page equals the combination of the pq code (``np.array([n,pq], dtype=uint8))``
            Approximately, ``n = Total Rows / number of IVF partitions``.
            ``pq = number of PQ sub-vectors``.
        default_scan_options : optional, dict
            Default scan options that are used when scanning the dataset.  This accepts
            the same arguments described in :py:meth:`lance.LanceDataset.scanner`.  The
            arguments will be applied to any scan operation.

            This can be useful to supply defaults for common parameters such as
            ``batch_size``.

            It can also be used to create a view of the dataset that includes meta
            fields such as ``_rowid`` or ``_rowaddr``.  If ``default_scan_options`` is
            provided then the schema returned by :py:meth:`lance.LanceDataset.schema` will
            include these fields if the appropriate scan options are set.
        metadata_cache_size_bytes : optional, int
            Size of the metadata cache in bytes. This cache is used to store metadata
            information about the dataset, such as schema and statistics. If not specified,
            a default size will be used.
        fragment_group_size : optional, int
            Number of fragments to group together in a single scan task. If None or <= 1,
            each fragment will be processed individually (default behavior).

    Returns:
        DataFrame: a DataFrame with the schema converted from the specified LanceDB table

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

        Read a local LanceDB table with fragment grouping:
        >>> df = daft.read_lance("s3://my-lancedb-bucket/data/", fragment_group_size=5)
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
    lance_operator = LanceDBScanOperator(ds, fragment_group_size=fragment_group_size)

    handle = ScanOperatorHandle.from_python_scan_operator(lance_operator)
    builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
    return DataFrame(builder)


@PublicAPI
def merge_columns(
    url: str,
    io_config: Optional[IOConfig] = None,
    *,
    transform: Union[dict[str, str], "BatchUDF", Callable[["pa.lib.RecordBatch"], "pa.lib.RecordBatch"]] = None,
    read_columns: Optional[list[str]] = None,
    reader_schema: Optional["pa.Schema"] = None,
    storage_options: Optional[dict[str, Any]] = None,
    daft_remote_args: Optional[dict[str, Any]] = None,
    concurrency: Optional[int] = None,
    version: Optional[Union[int, str]] = None,
    asof: Optional[str] = None,
    block_size: Optional[int] = None,
    commit_lock: Optional[Any] = None,
    index_cache_size: Optional[int] = None,
    default_scan_options: Optional[dict[str, Any]] = None,
    metadata_cache_size_bytes: Optional[int] = None,
) -> None:
    """Merge new columns into a LanceDB table using a transformation function.

    This function modifies the LanceDB table in-place by adding new columns computed
    from existing data using a transformation function. It does not return a DataFrame.

    Args:
        url: URL to the LanceDB table (supports remote URLs to object stores such as `s3://` or `gs://`)
        io_config: A custom IOConfig to use when accessing LanceDB data. Defaults to None.
        transform: A transformation function or UDF to apply to the data.
        read_columns: List of column names to read for the transformation.
        reader_schema: Schema for the reader.
        storage_options: Extra options for storage connection.
        daft_remote_args: Optional arguments for remote execution.
        concurrency: Optional concurrency level for processing.
        version: If specified, load a specific version of the Lance dataset.
        asof: If specified, find the latest version created on or earlier than the given argument value.
        block_size: Block size in bytes. Provide a hint for the size of the minimal I/O request.
        commit_lock: A custom commit lock.
        index_cache_size: Index cache size.
        default_scan_options: Default scan options.
        metadata_cache_size_bytes: Size of the metadata cache in bytes.

    Returns:
        None: This function modifies the table in-place and does not return a value.

    Note:
        This function requires the use of [LanceDB](https://lancedb.github.io/lancedb/), which is the Python library for the LanceDB project.
        To ensure that this is installed with Daft, you may install: `pip install daft[lance]`

    Examples:
        Merge new columns into a local LanceDB table:
        >>> def double_score(batch):
        ...     # Example transformation function
        ...     import pyarrow.compute as pc
        ...
        ...     return batch.append_column("new_column", pc.multiply(batch["c"], 2))
        >>> daft.io.lance.merge_columns("s3://my-lancedb-bucket/data/", transform=double_score)
    """
    try:
        import lance
    except ImportError as e:
        raise ImportError(
            "Unable to import the `lance` package, please ensure that Daft is installed with the lance extra dependency: `pip install daft[lance]`"
        ) from e
    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config
    storage_options = storage_options or io_config_to_storage_options(io_config, url)

    lance_ds = lance.dataset(
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

    merge_columns_internal(
        lance_ds,
        url,
        transform=transform,
        read_columns=read_columns,
        reader_schema=reader_schema,
        storage_options=storage_options,
        daft_remote_args=daft_remote_args,
        concurrency=concurrency,
    )
