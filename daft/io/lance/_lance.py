# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations
import pathlib
import warnings
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Optional, Union

from daft import context
from daft.api_annotations import PublicAPI
from daft.daft import IOConfig, ScanOperatorHandle
from daft.dataframe import DataFrame
from daft.io.lance.lance_merge_column import merge_columns_internal
from daft.io.lance.lance_scan import LanceDBScanOperator
from daft.io.lance.rest_config import LanceRestConfig, parse_lance_uri
from daft.io.lance.rest_scan import LanceRestScanOperator
from daft.io.lance.utils import check_pylance_version, construct_lance_dataset
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
    uri: str | pathlib.Path,
    io_config: IOConfig | None = None,
    rest_config: LanceRestConfig | None = None,
    version: str | int | None = None,
    asof: str | None = None,
    block_size: int | None = None,
    commit_lock: object | None = None,
    index_cache_size: int | None = None,
    default_scan_options: dict[str, str] | None = None,
    metadata_cache_size_bytes: int | None = None,
    fragment_group_size: int | None = None,
    include_fragment_id: bool | None = None,
) -> DataFrame:
    """Create a DataFrame from a LanceDB table or Lance REST service.

    Args:
        uri: The URI of the Lance table to read from. Supports:
            - File paths: "/path/to/lance/data/" or cloud URIs like "s3://bucket/path"
            - REST URIs: "rest://namespace/table_name" (requires rest_config)
        io_config: A custom IOConfig to use when accessing LanceDB data. Defaults to None.
        rest_config: Configuration for REST-based Lance services. Required when using REST URIs.
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

        Read a Lance table via REST API:
        >>> from daft.io.lance import LanceRestConfig
        >>> rest_config = LanceRestConfig(base_url="https://api.lancedb.com", api_key="your-api-key")
        >>> df = daft.read_lance("rest://my_namespace/my_table", rest_config=rest_config)
        >>> df.show()
    """
    # Parse URI to determine if it's REST-based or file-based
    uri_str = str(uri)
    uri_type, uri_info = parse_lance_uri(uri_str)

    if uri_type == "rest":
        # REST-based Lance table
        if rest_config is None:
            raise ValueError("rest_config is required when using REST URIs (rest://namespace/table_name)")

        lance_operator: LanceDBScanOperator | LanceRestScanOperator = LanceRestScanOperator(
            rest_config=rest_config,
            namespace=uri_info["namespace"],
            table_name=uri_info["table_name"],
        )
    else:
        # File-based Lance table (existing logic)
        io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config
        storage_options = io_config_to_storage_options(io_config, uri_str)

        ds = construct_lance_dataset(
            uri_str,
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
    uri: str | pathlib.Path,
    io_config: IOConfig | None = None,
    *,
    transform: Union[dict[str, str], "BatchUDF", Callable[["pa.lib.RecordBatch"], "pa.lib.RecordBatch"]] = None,
    read_columns: list[str] | None = None,
    reader_schema: Optional["pa.Schema"] = None,
    storage_options: dict[str, Any] | None = None,
    daft_remote_args: dict[str, Any] | None = None,
    concurrency: int | None = None,
    version: int | str | None = None,
    asof: str | None = None,
    block_size: int | None = None,
    commit_lock: Any | None = None,
    index_cache_size: int | None = None,
    default_scan_options: dict[str, Any] | None = None,
    metadata_cache_size_bytes: int | None = None,
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
    uri: str | pathlib.Path,
    io_config: IOConfig | None = None,
    *,
    read_columns: list[str] | None = None,
    reader_schema: Optional["pa.Schema"] = None,
    storage_options: dict[str, Any] | None = None,
    daft_remote_args: dict[str, Any] | None = None,
    concurrency: int | None = None,
    version: int | str | None = None,
    asof: str | None = None,
    block_size: int | None = None,
    commit_lock: Any | None = None,
    index_cache_size: int | None = None,
    default_scan_options: dict[str, Any] | None = None,
    metadata_cache_size_bytes: int | None = None,
    batch_size: int | None = None,
    left_on: str | None = "_rowaddr",
    right_on: str | None = "_rowaddr",
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
    uri: str | pathlib.Path,
    io_config: IOConfig | None = None,
    *,
    column: str,
    index_type: str = "INVERTED",
    name: str | None = None,
    replace: bool = True,
    storage_options: dict[str, Any] | None = None,
    version: int | str | None = None,
    asof: str | None = None,
    block_size: int | None = None,
    commit_lock: Any | None = None,
    index_cache_size: int | None = None,
    default_scan_options: dict[str, Any] | None = None,
    metadata_cache_size_bytes: int | None = None,
    fragment_group_size: int | None = None,
    num_partitions: int | None = None,
    max_concurrency: int | None = None,
    **kwargs: Any,
) -> None:
    """Build a distributed scalar index using Daft's distributed execution.

    This function distributes the index building process across multiple Daft workers,
    with each worker building indices for a subset of fragments. The indices are then
    merged and committed as a single index.

    Args:
        uri: The URI of the Lance table (supports remote URLs to object stores such as `s3://` or `gs://`)
        io_config: A custom IOConfig to use when accessing LanceDB data. Defaults to None.
        column: Column name to index
        index_type: Type of index to build.
            For distributed execution this supports "INVERTED", "FTS", and "BTREE".
            Other scalar index types supported by Lance (for example "BITMAP", "NGRAM", "ZONEMAP",
            "LABEL_LIST", "BLOOMFILTER") are passed through to Lance's scalar index implementation.
        name: Name of the index (generated if None).
        replace: Whether to replace an existing index with the same name. Defaults to True.
        storage_options: Storage options for the dataset.
        version: Version of the dataset to use.
        asof: Timestamp to use for time travel queries.
        block_size: Block size for the index.
        commit_lock: Commit lock for the dataset.
        index_cache_size: Size of the index cache.
        default_scan_options: Default scan options for the dataset.
        metadata_cache_size_bytes: Size of the metadata cache in bytes.
        fragment_group_size: Number of fragments to group together in each fragment batch. If None,
            defaults to 10. Must be a positive integer.
        num_partitions: Optional number of partitions to use when repartitioning fragment batches before execution. Only values
            greater than 1 enable additional parallelism on distributed runners; values <= 1 or None will use the default partitioning.
        max_concurrency: Maximum number of concurrent tasks to use for processing fragment batches.
            If None, Daft will use its default concurrency setting. Must be a positive integer.
        **kwargs: Additional keyword arguments forwarded to ``lance.LanceDataset.create_scalar_index``.

    Returns:
        None

    Raises:
        ValueError: If input parameters are invalid (e.g., empty column name, non-existent column, invalid index type, etc.)
        TypeError: If column type is incompatible with the chosen ``index_type``
        RuntimeError: If index building fails (e.g., version compatibility issues, commit failures)
        ImportError: If lance package is not available

    Note:
        This function requires the use of [LanceDB](https://lancedb.github.io/lancedb/), which is the Python library for the LanceDB project.
        To ensure that this is installed with Daft, you may install: `pip install daft[lance]`

    Examples:
        Create a distributed inverted index with custom concurrency:
        >>> import daft
        >>> daft.io.lance.create_scalar_index(
        ...     "s3://my-bucket/dataset/", column="text_content", index_type="INVERTED", max_concurrency=8
        ... )

        Create a FTS (Full-Text Search) index:
        >>> daft.io.lance.create_scalar_index("s3://my-bucket/dataset/", column="document", index_type="FTS")

        Create a BTREE index for numeric or string columns:
        >>> daft.io.lance.create_scalar_index(
        ...     "s3://my-bucket/dataset/", column="price", index_type="BTREE", name="price_idx"
        ... )

        Create an index with custom fragment grouping and partitioning:
        >>> daft.io.lance.create_scalar_index(
        ...     "s3://my-bucket/dataset/", column="description", fragment_group_size=8, num_partitions=16
        ... )

        Create an index without replacing existing ones:
        >>> daft.io.lance.create_scalar_index("s3://my-bucket/dataset/", column="title", replace=False)
    """
    check_pylance_version("0.37.0")

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

    from daft.io.lance.lance_scalar_index import create_scalar_index_internal

    create_scalar_index_internal(
        lance_ds=lance_ds,
        uri=uri,
        column=column,
        index_type=index_type,
        name=name,
        replace=replace,
        storage_options=storage_options,
        fragment_group_size=fragment_group_size,
        num_partitions=num_partitions,
        max_concurrency=max_concurrency,
        **kwargs,
    )


@PublicAPI
def create_index(
    uri: str | pathlib.Path,
    io_config: IOConfig | None = None,
    *,
    column: str,
    index_type: str = "IVF_PQ",
    name: str | None = None,
    replace: bool = True,
    metric: str = "l2",
    storage_options: dict[str, Any] | None = None,
    daft_remote_args: dict[str, Any] | None = None,
    concurrency: int | None = None,
    version: int | str | None = None,
    asof: str | None = None,
    block_size: int | None = None,
    commit_lock: Any | None = None,
    index_cache_size: int | None = None,
    default_scan_options: dict[str, Any] | None = None,
    metadata_cache_size_bytes: int | None = None,
    ivf_num_partitions: int | None = None,
    num_sub_vectors: int | None = None,
    pq_codebook: Any | None = None,
    **kwargs: Any,
) -> None:
    """Build a distributed IVF-based vector index using Daft's distributed computing.

    This is the single public entrypoint for vector index creation in Daft.
    It builds IVF / PQ-based indices on a LanceDB table by first training
    models on the driver and then delegating fragment-level index building
    to Daft workers via UDFs.

    Args:
        uri: The URI of the Lance table (supports remote URLs to object stores such as `s3://` or `gs://`)
        io_config: A custom IOConfig to use when accessing LanceDB data. Defaults to None.
        column: Column name containing the vector data to index.
        index_type: Type of vector index to build. Supported types include "IVF_PQ", "IVF_HNSW_PQ",
            "IVF_FLAT", "IVF_SQ", "IVF_HNSW_SQ", and "IVF_HNSW_FLAT". Defaults to "IVF_PQ".
        name: Name of the index (generated if None).
        replace: Whether to replace an existing index with the same name. Defaults to True.
        metric: Distance metric to use for the index. Supported values include "l2", "cosine", "euclidean", and "dot".
            Defaults to "l2".
        storage_options: Storage options for the dataset.
        daft_remote_args: Remote execution arguments for Daft, including GPU configuration.
            For example: {"num_gpus": 2} to use 2 GPUs.
        concurrency: Number of concurrent workers to use for distributed index building.
            If None, defaults to min(4, number of fragments).
        version: Version of the dataset to use.
        asof: Timestamp to use for time travel queries.
        block_size: Block size for the index.
        commit_lock: Commit lock for the dataset.
        index_cache_size: Size of the index cache.
        default_scan_options: Default scan options for the dataset.
        metadata_cache_size_bytes: Size of the metadata cache in bytes.
        ivf_num_partitions: Number of IVF partitions to use. If None, the system will
            automatically determine an appropriate number based on the dataset size.
        num_sub_vectors: Number of sub-vectors for PQ (Product Quantization) compression.
            Only applicable for PQ-based index types. If None, the system will
            automatically determine an appropriate number based on the vector dimension.
        pq_codebook: Pre-trained PQ codebook to use. If provided, this will be used
            instead of training a new codebook. Only applicable for PQ-based index types.
        **kwargs: Additional keyword arguments forwarded to ``lance.indices.builder.IndicesBuilder``.

    Returns:
        None

    Raises:
        ValueError
            If ``index_type`` is not a supported IVF-based vector index type.
        RuntimeError
            If the installed pylance / lance version is too old.
        ImportError
            If the ``lance`` package is not available.

    Notes:
    -----
    * This API is **vector-only**. If you want to build scalar indices
      (for example ``INVERTED`` / ``FTS``), please use
      :func:`create_scalar_index` instead.
    * The underlying vector pipeline requires pylance / lance
      ``>= 2.0.1``. If an older version is installed, a ``RuntimeError``
      with a clear upgrade hint will be raised.

    Examples:
        Create a basic IVF_PQ vector index:
        >>> import daft
        >>> daft.io.lance.create_index("s3://my-bucket/dataset/", column="vector", index_type="IVF_PQ")

        Create a vector index with custom parameters:
        >>> daft.io.lance.create_index(
        ...     "s3://my-bucket/dataset/",
        ...     column="vector",
        ...     index_type="IVF_PQ",
        ...     metric="cosine",
        ...     ivf_num_partitions=256,
        ...     num_sub_vectors=16,
        ...     concurrency=4,
        ... )

        Create a vector index with GPU acceleration:
        >>> daft.io.lance.create_index(
        ...     "s3://my-bucket/dataset/", column="vector", index_type="IVF_PQ", daft_remote_args={"num_gpus": 2}
        ... )
    """
    from daft.io.lance.lance_vector_index import _normalize_index_type, create_vector_index_internal

    check_pylance_version("2.0.1")

    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config
    storage_options = storage_options or io_config_to_storage_options(io_config, uri)

    index_type_name = _normalize_index_type(index_type)

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
    create_vector_index_internal(
        lance_ds=lance_ds,
        uri=uri,
        column=column,
        index_type=index_type_name,
        name=name,
        replace=replace,
        metric=metric,
        storage_options=storage_options,
        daft_remote_args=daft_remote_args,
        concurrency=concurrency,
        ivf_num_partitions=ivf_num_partitions,
        num_sub_vectors=num_sub_vectors,
        pq_codebook=pq_codebook,
        **kwargs,
    )


@PublicAPI
def compact_files(
    uri: str | pathlib.Path,
    io_config: IOConfig | None = None,
    *,
    storage_options: dict[str, Any] | None = None,
    version: int | str | None = None,
    asof: str | None = None,
    block_size: int | None = None,
    commit_lock: Any | None = None,
    index_cache_size: int | None = None,
    default_scan_options: dict[str, Any] | None = None,
    metadata_cache_size_bytes: int | None = None,
    compaction_options: dict[str, Any] | None = None,
    partition_num: int | None = None,
    concurrency: int | None = None,
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
