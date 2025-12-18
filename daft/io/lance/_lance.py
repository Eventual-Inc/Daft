# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations
import pathlib
import warnings
from typing import TYPE_CHECKING, Any, Callable, Optional, Union

from daft import context
from daft.api_annotations import PublicAPI
from daft.daft import IOConfig, ScanOperatorHandle
from daft.dataframe import DataFrame
from daft.io.lance.lance_merge_column import merge_columns_internal
from daft.io.lance.lance_scan import LanceDBScanOperator
from daft.io.lance.utils import construct_lance_dataset
from daft.io.object_store_options import io_config_to_storage_options
from daft.logical.builder import LogicalPlanBuilder

if TYPE_CHECKING:
    from daft.dependencies import pa

    try:
        from lance.dataset import LanceDataset
        from lance.udf import BatchUDF
    except ImportError:
        BatchUDF = None
        LanceDataset = None

LanceDataset = Any


@PublicAPI
def read_lance(
    uri: Union[str, pathlib.Path],
    io_config: Optional[IOConfig] = None,
    version: Optional[Union[str, int]] = None,
    asof: Optional[str] = None,
    block_size: Optional[int] = None,
    commit_lock: Optional[object] = None,
    index_cache_size: Optional[int] = None,
    default_scan_options: Optional[dict[str, str]] = None,
    metadata_cache_size_bytes: Optional[int] = None,
    fragment_group_size: Optional[int] = None,
    include_fragment_id: Optional[bool] = None,
) -> DataFrame:
    """Create a DataFrame from a LanceDB table.

    Args:
        uri: The URI of the Lance table to read from (supports remote URLs to object stores such as `s3://` or `gs://`)
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
            like this:
            default_scan_options = {"with_row_address": True, "with_row_id" : True,  "batch_size": 1024}
            more see: https://lance-format.github.io/lance-python-doc/dataset.html
        metadata_cache_size_bytes : optional, int
            Size of the metadata cache in bytes. This cache is used to store metadata
            information about the dataset, such as schema and statistics. If not specified,
            a default size will be used.
        fragment_group_size : optional, int
            Number of fragments to group together in a single scan task. If None or <= 1,
            each fragment will be processed individually (default behavior).
        include_fragment_id : Optional, bool
            Whether to display fragment_id.
            if you have the behavior of 'merge_columns_df' or 'write_lance(mode = 'merge')', the `include_fragment_id` must be set to True

    Returns:
        DataFrame: a DataFrame with the schema converted from the specified LanceDB table

        This function requires the use of [LanceDB](https://lancedb.github.io/lancedb/), which is the Python library for the LanceDB project.
        To ensure that this is installed with Daft, you may install: `pip install daft[lance]`

    Examples:
        Read a local LanceDB table:
        >>> df = daft.read_lance("/path/to/lance/data/")
        >>> df.show()

        Read a LanceDB table and specify a version:
        >>> df = daft.read_lance("/path/to/lance/data/", version=1)
        >>> df.show()

        Read a LanceDB table with fragment grouping:
        >>> df = daft.read_lance("/path/to/lance/data/", fragment_group_size=5)
        >>> df.show()

        Read a LanceDB table from a public S3 bucket:
        >>> from daft.io import S3Config, IOConfig
        >>> io_config = IOConfig(s3=S3Config(region="us-west-2", anonymous=True))
        >>> df = daft.read_lance("s3://daft-public-data/lance/words-test-dataset", io_config=io_config)
        >>> df.show()
    """
    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config
    storage_options = io_config_to_storage_options(io_config, uri)

    ds = construct_lance_dataset(
        uri,
        storage_options=storage_options,
        version=version,
        asof=asof,
        block_size=block_size,
        commit_lock=commit_lock,
        index_cache_size=index_cache_size,
        default_scan_options=default_scan_options,
        metadata_cache_size_bytes=metadata_cache_size_bytes,
    )

    lance_operator = LanceDBScanOperator(
        ds, fragment_group_size=fragment_group_size, include_fragment_id=include_fragment_id
    )

    handle = ScanOperatorHandle.from_python_scan_operator(lance_operator)
    builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
    return DataFrame(builder)


@PublicAPI
def merge_columns(
    uri: Union[str, pathlib.Path],
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
) -> LanceDataset:
    """Merge new columns into a LanceDB table using a transformation function.

    This function modifies the LanceDB table in-place by adding new columns computed
    from existing data using a transformation function. It does not return a DataFrame.

    Args:
        uri: The URI of the Lance table (supports remote URLs to object stores such as `s3://` or `gs://`)
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
    warnings.warn(
        "daft.io.lance.merge_columns is deprecated and will be removed in a future release. "
        "Please use daft.io.lance.merge_columns_df instead.",
        category=DeprecationWarning,
        stacklevel=2,
    )

    if transform is None:
        raise ValueError(
            "merge_columns requires a `transform` function; prefer using merge_columns_df with a prepared DataFrame if no transform is needed."
        )

    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config
    storage_options = storage_options or io_config_to_storage_options(io_config, uri)

    # Build Lance dataset handle for committing
    lance_ds = construct_lance_dataset(
        uri,
        storage_options=storage_options,
        version=version,
        asof=asof,
        block_size=block_size,
        commit_lock=commit_lock,
        index_cache_size=index_cache_size,
        default_scan_options=default_scan_options,
        metadata_cache_size_bytes=metadata_cache_size_bytes,
    )

    return merge_columns_internal(
        lance_ds,
        uri,
        transform=transform,
        read_columns=read_columns,
        reader_schema=reader_schema,
        storage_options=storage_options,
        daft_remote_args=daft_remote_args,
        concurrency=concurrency,
    )


@PublicAPI
def merge_columns_df(
    df: DataFrame,
    uri: Union[str, pathlib.Path],
    io_config: Optional[IOConfig] = None,
    *,
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
    batch_size: Optional[int] = None,
    left_on: Optional[str] = "_rowaddr",
    right_on: Optional[str] = "_rowaddr",
) -> None:
    """Row-level merge columns entrypoint using a DataFrame.

    This function modifies the LanceDB table in-place by merging new columns from a DataFrame
    into existing fragments using a row-level join. It does not return a DataFrame.

    Args:
        df: DataFrame containing the new columns to merge along with fragment_id and join key columns
        uri: URL to the LanceDB table (supports remote URLs to object stores such as `s3://` or `gs://`)
        io_config: A custom IOConfig to use when accessing LanceDB data. Defaults to None.
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
        batch_size: Optional batch size when building RecordBatchReader from the provided data.
        left_on: Key column on the Lance fragment. Defaults to "_rowaddr".
        right_on: Key column name present in the provided DataFrame. Defaults to the value of left_on.

    Returns:
        None: This function modifies the table in-place and does not return a value.

    Note:
        This function requires the use of [LanceDB](https://lancedb.github.io/lancedb/), which is the Python library for the LanceDB project.
        To ensure that this is installed with Daft, you may install: `pip install daft[lance]`

    Examples:
        Merge new columns into a local LanceDB table:
        >>> import daft
        >>> # Read the existing table with row addresses
        >>> df = daft.read_lance(
        ...     "s3://my-lancedb-bucket/data/",
        ...     default_scan_options={"with_row_address": True},
        ...     include_fragment_id=True,
        ... )
        >>> # Add new columns based on existing data
        >>> df = df.with_column("doubled_c", df["c"] * 2)
        >>> # Merge the new columns back to the table
        >>> daft.io.lance.merge_columns_df(df, "s3://my-lancedb-bucket/data/")
    """
    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config
    storage_options = storage_options or io_config_to_storage_options(io_config, uri)

    # Build Lance dataset handle for committing
    lance_ds = construct_lance_dataset(
        uri,
        storage_options=storage_options,
        version=version,
        asof=asof,
        block_size=block_size,
        commit_lock=commit_lock,
        index_cache_size=index_cache_size,
        default_scan_options=default_scan_options,
        metadata_cache_size_bytes=metadata_cache_size_bytes,
    )

    # Set default value for right_on if not provided
    effective_right_on = right_on or left_on
    effective_batch_size = (
        batch_size if batch_size is not None else daft_remote_args.get("batch_size", None) if daft_remote_args else None
    )

    # Import here to avoid circular imports
    from daft.io.lance.lance_merge_column import merge_columns_from_df

    return merge_columns_from_df(
        df,
        lance_ds=lance_ds,
        uri=uri,
        read_columns=read_columns,
        reader_schema=reader_schema,
        storage_options=storage_options,
        daft_remote_args=daft_remote_args,
        concurrency=concurrency,
        left_on=left_on,
        right_on=effective_right_on,
        batch_size=effective_batch_size,
    )


@PublicAPI
def create_scalar_index(
    uri: Union[str, pathlib.Path],
    io_config: Optional[IOConfig] = None,
    *,
    column: str,
    index_type: str = "INVERTED",
    name: Optional[str] = None,
    replace: bool = True,
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
    **kwargs: Any,
) -> None:
    """Build a distributed full-text search index using Daft's distributed computing.

    This function distributes the index building process across multiple Daft workers,
    with each worker building indices for a subset of fragments. The indices are then
    merged and committed as a single index.

    Args:
        uri: The URI of the Lance table (supports remote URLs to object stores such as `s3://` or `gs://`)
        io_config: A custom IOConfig to use when accessing LanceDB data. Defaults to None.
        column: Column name to index
        index_type: Type of index to build ("INVERTED" or "FTS")
        name: Name of the index (generated if None)
        replace: Whether to replace an existing index with the same name. Defaults to True.
        storage_options: Storage options for the dataset
        daft_remote_args: Options for Daft remote execution (e.g., num_cpus, num_gpus, memory_bytes)
        concurrency: Number of Daft workers to use
        version: Version of the dataset to use
        asof: Timestamp to use for time travel queries
        block_size: Block size for the index
        commit_lock: Commit lock for the dataset
        index_cache_size: Size of the index cache
        default_scan_options: Default scan options for the dataset
        metadata_cache_size_bytes: Size of the metadata cache in bytes
        **kwargs: Additional arguments to pass to create_scalar_index

    Returns:
        None

    Raises:
        ValueError: If input parameters are invalid
        TypeError: If column type is not string
        RuntimeError: If index building fails
        ImportError: If lance package is not available

    Note:
        This function requires the use of [LanceDB](https://lancedb.github.io/lancedb/), which is the Python library for the LanceDB project.
        To ensure that this is installed with Daft, you may install: `pip install daft[lance]`

    Examples:
        Create a distributed inverted index:
        >>> import daft
        >>> daft.io.lance.create_scalar_index(
        ...     "s3://my-bucket/dataset/", column="text_content", index_type="INVERTED", concurrency=8
        ... )

        Create an index with custom Daft remote arguments:
        >>> daft.io.lance.create_scalar_index(
        ...     "s3://my-bucket/dataset/",
        ...     column="description",
        ...     daft_remote_args={"num_cpus": 2},
        ... )
    """
    try:
        import lance
        from packaging import version as packaging_version

        from daft.io.lance.lance_scalar_index import create_scalar_index_internal

        lance_version = packaging_version.parse(lance.__version__)
        min_required_version = packaging_version.parse("0.37.0")
        if lance_version < min_required_version:
            raise RuntimeError(
                f"Distributed indexing requires pylance >= 0.37.0, but found {lance.__version__}. "
                "The distribute-related interfaces are not available in older versions. "
                "Please upgrade lance by running: pip install --upgrade pylance"
            )
    except ImportError as e:
        raise ImportError(
            "Unable to import the `lance` package, please ensure that Daft is installed with the lance extra dependency: `pip install daft[lance]`"
        ) from e

    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config
    storage_options = storage_options or io_config_to_storage_options(io_config, str(uri))

    lance_ds = construct_lance_dataset(
        uri,
        storage_options=storage_options,
        version=version,
        asof=asof,
        block_size=block_size,
        commit_lock=commit_lock,
        index_cache_size=index_cache_size,
        default_scan_options=default_scan_options,
        metadata_cache_size_bytes=metadata_cache_size_bytes,
    )

    create_scalar_index_internal(
        lance_ds=lance_ds,
        uri=uri,
        column=column,
        index_type=index_type,
        name=name,
        replace=replace,
        storage_options=storage_options,
        daft_remote_args=daft_remote_args,
        concurrency=concurrency,
        **kwargs,
    )


@PublicAPI
def compact_files(
    uri: Union[str, pathlib.Path],
    io_config: Optional[IOConfig] = None,
    *,
    storage_options: Optional[dict[str, Any]] = None,
    version: Optional[Union[int, str]] = None,
    asof: Optional[str] = None,
    block_size: Optional[int] = None,
    commit_lock: Optional[Any] = None,
    index_cache_size: Optional[int] = None,
    default_scan_options: Optional[dict[str, Any]] = None,
    metadata_cache_size_bytes: Optional[int] = None,
    compaction_options: Optional[dict[str, Any]] = None,
    partition_num: Optional[int] = None,
    concurrency: Optional[int] = None,
) -> Any:
    """Compact Lance dataset files using Daft UDF-style distributed execution.

    This function maintains compatibility with the original interface but uses Daft's UDF
    mechanism for distributed execution instead of Ray directly.

    Args:
        uri: The URI of the Lance table (supports remote URLs to object stores such as `s3://` or `gs://`)
        io_config: A custom IOConfig to use when accessing LanceDB data. Defaults to None.
        storage_options: Extra options for storage connection.
        version: If specified, load a specific version of the Lance dataset.
        asof: If specified, find the latest version created on or earlier than the given argument value.
        block_size: Block size in bytes. Provide a hint for the size of the minimal I/O request.
        commit_lock: A custom commit lock.
        index_cache_size: Index cache size.
        default_scan_options: Default scan options.
        metadata_cache_size_bytes: Size of the metadata cache in bytes.
        compaction_options: A dictionary of options to control compaction behavior. See `lance.optimize.CompactionOptions` for details. e.g.
            target_rows_per_fragment: Target number of rows per fragment.
            max_rows_per_group: Max number of rows per group.(default: 1024).
            max_bytes_per_file: Max number of bytes in a single file.
            materialize_deletions: Whether to compact fragments with soft deleted rows so they are no
                longer present in the file.(default: True).
            materialize_deletions_threadhold: The fraction of original rows that are soft deleted in a fragment
                before the fragment is a candidate for compaction.(default: 0.1 = 10%).
        partition_num: Number of partitions to use for compaction. Defaults to None.
        concurrency: Number of concurrent compaction tasks to run. Defaults to None.

    Returns:
        CompactionMetrics: Compaction statistics; None if no compaction needed. The `CompactionMetrics` object contains the following fields:
            - `fragments_removed`: Number of fragments removed during compaction.
            - `fragments_added`: Number of fragments added during compaction.
            - `files_removed`: Number of files removed during compaction.
            - `files_added`: Number of files added during compaction.

    Raises:
        RuntimeError: When compaction fails or no successful results
    """
    try:
        import lance

        from daft.io.lance.lance_compaction import compact_files_internal
    except ImportError as e:
        raise ImportError(
            "Unable to import the `lance` package, please ensure that Daft is installed with the lance extra dependency: `pip install daft[lance]`"
        ) from e
    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config
    storage_options = storage_options or io_config_to_storage_options(
        io_config, str(uri) if isinstance(uri, pathlib.Path) else uri
    )

    lance_ds = lance.dataset(
        uri,
        storage_options=storage_options,
        version=version,
        asof=asof,
        block_size=block_size,
        commit_lock=commit_lock,
        index_cache_size=index_cache_size,
        default_scan_options=default_scan_options,
        metadata_cache_size_bytes=metadata_cache_size_bytes,
    )

    return compact_files_internal(
        lance_ds=lance_ds,
        compaction_options=compaction_options,
        partition_num=partition_num,
        concurrency=concurrency,
    )
