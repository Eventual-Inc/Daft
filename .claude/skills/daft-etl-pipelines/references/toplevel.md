# toplevel

## attach

```python
attach(object: Catalog | Provider | Table | UDF | DataFrame, alias: str | None=None) -> None
```

Attaches a known attachable object like a Catalog or Table.

## attach_catalog

```python
attach_catalog(catalog: object | Catalog, alias: str | None=None) -> Catalog
```

Attaches an external catalog to the current session.

## attach_function

```python
attach_function(function: UDF, alias: str | None=None) -> None
```

Attaches a Python function as a UDF in the current session.

## attach_provider

```python
attach_provider(provider: Provider, alias: str | None=None) -> Provider
```

Attaches a provider instance to the current session.

## attach_subscriber

```python
attach_subscriber(alias: str, subscriber: Subscriber) -> DaftContext
```

Attaches a subscriber to the current context.

Args:
    alias (str): Name-based alias for the subscriber
    subscriber (Subscriber): Subscriber instance that will receive events

## attach_table

```python
attach_table(table: object | Table, alias: str | None=None) -> Table
```

Attaches an external table to the current session.

## attach_view

```python
attach_view(view: DataFrame, alias: str) -> Table
```

Attaches a DataFrame as a non-materialized temporary view to the current session.

## AudioFile

```python
AudioFile(url: str, io_config: IOConfig | None=None) -> None
```

An audio-specific file interface that provides audio operations.

## Catalog

```python
Catalog()
```

Interface for Python catalog implementations.

A Catalog is a service for discovering, accessing, and querying
tabular and non-tabular data. You can instantiate a Catalog using
one of the static `from_` methods.

## CheckpointConfig

```python
CheckpointConfig(store: CheckpointStore, on: str, settings: KeyFilteringSettings | None=None) -> None
```

Per-source checkpoint configuration.

Bundles the store, the key column, and strategy-specific tuning into a
single object. Pass via ``checkpoint=`` on source readers (e.g.
:func:`daft.read_parquet`).

On re-run, rows whose key already exists in the store are skipped.

Args:
    store: The :class:`CheckpointStore` holding sealed keys.
    on: Name of the source column that uniquely identifies inputs (e.g.
        a file hash or document ID). Must exist in the source schema.
    settings: Optional tuning for the key-filtering anti-join. Defaults
        to engine-chosen values when omitted.

## CheckpointStore

```python
CheckpointStore(path: str, io_config: IOConfig | None=None) -> None
```

A checkpoint store for tracking which source keys have been processed.

Identifies *where* checkpoint state lives. Pair with
:class:`CheckpointConfig` to attach the store to a specific source.

Args:
    path: URI for the store root (e.g. ``s3://bucket/checkpoints``).
    io_config: Optional IO configuration for the object store backend.

## cls

```python
cls(class_: type | None=None, *, cpus: float | None=None, gpus: float=0, use_process: bool | None=None, max_concurrency: int | None=None, max_retries: int | None=None, on_error: Literal['raise', 'log', 'ignore'] | None=None, name_override: str | None=None, ray_options: dict[str, Any] | None=None) -> type | Callable[[type], type]
```

Decorator to convert a Python class into a Daft user-defined class.

Args:
    cpus: The number of CPUs each instance of the class requires. Defaults to None (let the engine decide).
    gpus: The number of GPUs each instance of the class requires. Defaults to 0.
          Fractional values between 0 and 1.0, such as 0.5, are supported. This can be useful when running multiple small models on the same GPU.
          However, fractional values greater than 1.0, such as 1.5 or 2.5, are not supported.
    use_process: Whether to run each instance of the class in a separate process. If unset, Daft will automatically choose based on runtime performance.
    max_concurrency: The maximum number of concurrent invocations. For sync methods, this controls the number of actor pool processes. For async methods, this controls the number of concurrent coroutines.
    name_override: The name to display for the UDF class in the plan and progress bars.
    ray_options: Options to pass to the Ray executor (e.g. {"num_cpus": 1, "num_gpus": 1}).

Daft classes allow you to initialize a class instance once, and then reuse it for multiple rows of data.
This is useful for expensive initializations that need to be amortized across multiple rows of data, such as loading a model or establishing a network connection.

Daft classes are initialized lazily. This means that when you create a Daft class, the arguments are saved and only passed into the `__init__` method of each instance once a query is executed.
Methods can also be called with scalar arguments to run locally, in which case `__init__` will be called locally first.

Methods in a Daft class can be used as Daft functions. Use the `@daft.method` decorator to override default arguments.

## col

```python
col(name: str) -> Expression
```

Creates an Expression referring to the column with the provided name.

Args:
    name: Name of column

Returns:
    Expression: Expression representing the selected column

## concat

```python
concat(dfs: Iterable['DataFrame']) -> DataFrame
```

Concatenates multiple DataFrames into a single DataFrame.

All DataFrames must have exactly the same schema.

Args:
    dfs: DataFrames to concatenate.

Returns:
    DataFrame: DataFrame with rows from each input DataFrame in order.

Raises:
    ValueError: If ``dfs`` is empty or if schemas do not match.

## context

_(submodule)_

_(no docstring)_

## create_namespace

```python
create_namespace(identifier: Identifier | str) -> None
```

Creates a namespace in the current session's active catalog.

## create_namespace_if_not_exists

```python
create_namespace_if_not_exists(identifier: Identifier | str) -> None
```

Creates a namespace in the current session's active catalog if it does not already exist.

## create_table

```python
create_table(identifier: Identifier | str, source: Schema | DataFrame, **properties: Any) -> Table
```

Creates a table in the current session's active catalog and namespace.

## create_table_if_not_exists

```python
create_table_if_not_exists(identifier: Identifier | str, source: Schema | DataFrame, **properties: Any) -> Table
```

Creates a table in the current session's active catalog and namespace if it does not already exist.

## create_temp_table

```python
create_temp_table(identifier: str, source: Schema | DataFrame) -> Table
```

Creates a temp table scoped to current session's lifetime.

## create_temp_view

```python
create_temp_view(identifier: str, view: DataFrame) -> Table
```

Creates or replaces a non-materialized temporary view in the current session.

## current_catalog

```python
current_catalog() -> Catalog | None
```

Returns the active session's current catalog or None.

## current_model

```python
current_model() -> str | None
```

Returns the active session's current model or None.

## current_namespace

```python
current_namespace() -> Identifier | None
```

Returns the active session's current namespace or None.

## current_provider

```python
current_provider() -> Provider | None
```

Returns the active session's current provider or None.

## current_session

```python
current_session() -> Session
```

Returns the active session's current session.

## DataFrame

```python
DataFrame(builder: LogicalPlanBuilder) -> None
```

A Daft DataFrame is a table of data.

It has columns, where each column has a type and the same number of items (rows) as all other columns.

## datasets

_(submodule)_

_(no docstring)_

## DataType

```python
DataType() -> None
```

A Daft DataType defines the type of all the values in an Expression or DataFrame column.

## delete_deltalake

```python
delete_deltalake(table: Union[str, 'UnityCatalogTable', 'deltalake.DeltaTable'], predicate: str | None=None, io_config: IOConfig | None=None, custom_metadata: dict[str, str] | None=None) -> dict[str, Any]
```

Delete rows from a Delta Lake table.

Args:
    table: Destination Delta table URI, ``deltalake.DeltaTable``, or ``UnityCatalogTable``.
    predicate: SQL predicate that selects rows to delete. If ``None``, deletes all rows.
    io_config: Optional :class:`~daft.daft.IOConfig` used for object storage access.
    custom_metadata: Optional key-value metadata to attach to the Delta commit.

Returns:
    dict[str, Any]: Delta-rs metrics from the delete operation.

## detach_catalog

```python
detach_catalog(alias: str) -> None
```

Detaches the catalog from the current session.

## detach_function

```python
detach_function(alias: str) -> None
```

Detaches a Python function as a UDF in the current session.

## detach_provider

```python
detach_provider(alias: str) -> None
```

Detaches the provider from the current session.

## detach_subscriber

```python
detach_subscriber(alias: str) -> None
```

Detaches a subscriber from the current context.

Args:
    alias (str): Alias of subscriber to detach

## detach_table

```python
detach_table(alias: str) -> None
```

Detaches the table from the current session.

## distributed_merge_deltalake

```python
distributed_merge_deltalake(table: Union[str, 'UnityCatalogTable', 'deltalake.DeltaTable'], source: DataFrame, predicate: str, on: str | list[str] | None=None, io_config: IOConfig | None=None, source_alias: str='source', target_alias: str='target', custom_metadata: dict[str, str] | None=None, validate_unique_keys: bool=True, broadcast_join: bool | None=None, materialize_source: bool=True, materialize_join: bool=False, compression: str='snappy') -> DistributedDeltaMergeBuilder
```

Create a distributed Delta Lake MERGE builder that uses Daft's distributed join.

Unlike :func:`merge_deltalake` which runs the entire merge on a single process
via delta-rs/DataFusion, this function distributes the join across all workers
in your cluster. The merged result is written back via streaming overwrite.

Returns a builder for chaining merge clauses, then call ``.execute()`` to run.

Args:
    table: Destination Delta table URI, ``deltalake.DeltaTable``, or ``UnityCatalogTable``.
    source: Source Daft DataFrame to merge from.
    predicate: SQL merge predicate (e.g. ``"target.id = source.id"``).
        Only equality (equi-join) conditions of the form
        ``target.col = source.col`` are supported; the join keys are
        extracted from them. Non-equality ("residual") conditions raise a
        ``ValueError`` — pre-filter or pre-join the source instead.
    on: Explicit join key column name(s). If ``None``, keys are parsed from
        the ``predicate``. Providing ``on`` overrides predicate-derived keys.
    io_config: Optional :class:`~daft.daft.IOConfig` for object storage access.
    source_alias: Alias for the source side (used in update expressions).
    target_alias: Alias for the target side (used in update expressions).
    custom_metadata: Optional metadata to attach to the Delta commit.
    validate_unique_keys: If ``True`` (default), verify the source has unique
        join keys before merging and raise ``ValueError`` on duplicates. Set
        to ``False`` to skip the check when uniqueness is guaranteed upstream.
    broadcast_join: Join strategy hint. ``True`` decomposes the full outer
        join into a broadcast-friendly LEFT join plus a keys-only ANTI join
        (avoids shuffling the target when the source is small); ``False``
        uses a plain full outer join; ``None`` (default) decomposes only on
        the Ray runner **and** only when the materialized source is small
        enough to broadcast safely. Never pass ``True`` for a large source —
        it is copied to every worker.
    materialize_source: If ``True`` (default), collect the source once and
        reuse it across the guard and both execution passes. Set to
        ``False`` for very large sources (e.g. 100M+ rows): the source plan
        is then re-executed per pass (with column pruning) instead of being
        pinned in cluster memory.
    materialize_join: If ``True``, execute the join once and pin the merged
        result in cluster memory (spillable on the Ray runner) so the
        statistics pass and the write pass both read from it. If ``False``
        (default), the joined plan is re-executed per pass and never held
        in memory; on partitioned tables the write pass then re-reads only
        the affected partitions of the target.
    compression: Compression codec for the parquet data files this merge writes.
        Defaults to "snappy". See :meth:`daft.DataFrame.write_deltalake`.

Returns:
    DistributedDeltaMergeBuilder: A builder for chaining merge clauses.

Note:
    The join is distributed across all workers and the result is written
    back via Daft's native Delta writer, so data files are produced on
    workers and never funneled through the driver. Execution is two-pass
    streaming: a narrow statistics pass (metrics + affected partitions)
    followed by a write pass — the joined table is never materialized in
    memory (unless ``materialize_join=True``). On partitioned tables the
    write pass re-reads only the affected partitions of the target. Best
    for cases where the source is large relative to the target, or when
    single-node merge causes OOM.

    Writes are **incremental on partitioned tables**: only partitions
    containing an insert, update, or delete are rewritten (including the
    pre-image partition when an update moves a row across partitions), and
    a merge that modifies nothing skips the commit entirely. With multiple
    partition columns the rewrite covers the per-column cartesian closure
    of the touched partitions (delta-rs's commit filter is
    conjunction-only). Unpartitioned tables are rewritten in full.

    Clause predicates and update expressions given as strings are parsed
    with Daft SQL (full expression grammar); unparseable input raises
    ``ValueError``. A clause predicate that evaluates to NULL is treated
    as not-satisfied, per SQL semantics. Clauses follow SQL MERGE
    **first-match-wins** ordering: per row, the first clause (in
    declaration order) whose predicate holds is the only one applied.

    A commit by a concurrent writer between the start of the merge and
    its commit is detected and raises ``RuntimeError`` (re-run the
    merge). Tables with CHECK constraints, Change Data Feed, or generated
    columns are rejected with ``ValueError`` — this write path bypasses
    delta-rs's write-time enforcement; use :func:`merge_deltalake` for
    those tables.

    The source must have **unique join keys** (multiple source rows matching
    one target row is rejected with a ``ValueError`` unless
    ``validate_unique_keys=False``).

## drop_namespace

```python
drop_namespace(identifier: Identifier | str) -> None
```

Drops the namespace in the current session's active catalog.

## drop_table

```python
drop_table(identifier: Identifier | str) -> None
```

Drops the table in the current session's active catalog.

## element

```python
element() -> Expression
```

Creates an expression referring to an elementwise list operation.

This is used to create an expression that operates on each element of a list column.

If used outside of a list column, it will raise an error.

## execution_config_ctx

```python
execution_config_ctx(**kwargs: Any) -> Generator[None, None, None]
```

Context manager that wraps set_execution_config to reset the config to its original setting afternwards.

## Expression

```python
Expression() -> None
```

_(no docstring)_

## File

```python
File(url: str, io_config: IOConfig | None=None, media_type: MediaType=MediaType.unknown(), position: int | None=None, size: int | None=None, offset: int | None=None, length: int | None=None) -> None
```

A file-like object for working with file contents in Daft.

This is an abstract base class that provides a standard file interface compatible
with Python's file protocol.

The File object can be used with most Python libraries that accept file-like objects,
and implements the standard read/seek/tell interface. Files are read-only in the
current implementation.

## from_arrow

```python
from_arrow(data: Union['pa.Table', list['pa.Table'], Iterable['pa.Table'], ArrowStreamExportable]) -> DataFrame
```

Creates a DataFrame from Arrow data.

Accepts pyarrow Tables, lists/iterables of pyarrow Tables, or any object
implementing the `Arrow PyCapsule Interface <https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface>`
(i.e. has an ``__arrow_c_stream__`` method). This includes pyarrow RecordBatchReaders,
pandas DataFrames (2.2+), nanoarrow arrays, and other Arrow-compatible libraries.

Args:
    data: Arrow data to convert into a Daft DataFrame.

Returns:
    DataFrame: DataFrame created from the provided Arrow data.

## from_dask_dataframe

```python
from_dask_dataframe(ddf: 'dask.DataFrame') -> DataFrame
```

Creates a Daft DataFrame from a Dask DataFrame.

The provided Dask DataFrame must have been created using [Dask-on-Ray](https://docs.ray.io/en/latest/ray-more-libs/dask-on-ray.html).

Args:
    ddf: The Dask DataFrame to create a Daft DataFrame from.

Returns:
    DataFrame: Daft DataFrame created from the provided Dask DataFrame.

Note:
    This function can only work if Daft is running using the RayRunner

## from_files

```python
from_files(path: str | list[str], io_config: IOConfig | None=None) -> DataFrame
```

Creates a DataFrame of `daft.File` references from a glob path.

This method supports wildcards:

1. ``*`` matches any number of any characters including none
2. ``?`` matches any single character
3. ``[...]`` matches any single character in the brackets
4. ``**`` recursively matches any number of layers of directories

The returned DataFrame will have a single ``"file"`` column of type `daft.DataType.file`.
Files are not downloaded eagerly; the ``File`` type is a lazy reference that can be read on demand.

Args:
    path (str | list[str]): Path to files on disk (allows wildcards). Supports remote URLs such as ``s3://``, ``gs://``, or ``az://``.
    io_config (IOConfig | None): Configuration to use when running IO with remote services.

Returns:
    DataFrame: DataFrame with a single ``"file"`` column containing `daft.File` references.

Note:
    If no files match the glob pattern(s), an empty DataFrame is returned instead of raising an error.

## from_glob_path

```python
from_glob_path(path: str | list[str], io_config: IOConfig | None=None) -> DataFrame
```

Creates a DataFrame of file paths and other metadata from a glob path.

This method supports wildcards:

1. `*` matches any number of any characters including none
2. `?` matches any single character
3. `[...]` matches any single character in the brackets
4. `**` recursively matches any number of layers of directories

The returned DataFrame will have the following columns:

1. path: the path to the file/directory
2. size: size of the object in bytes
3. rows: the total rows of parquet object, it's None for other formats.

Args:
    path (str|list): Path to files on disk (allows wildcards).
    io_config (IOConfig): Configuration to use when running IO with remote services

Returns:
    DataFrame: DataFrame containing the path to each file as a row, along with other metadata parsed from the provided filesystem.

Note:
    If no files match the glob pattern(s), an empty DataFrame is returned instead of raising an error.

## from_pandas

```python
from_pandas(data: Union['pd.DataFrame', list['pd.DataFrame']]) -> DataFrame
```

Creates a Daft DataFrame from a pandas DataFrame.

Args:
    data: pandas DataFrame(s) that we wish to convert into a Daft DataFrame.

Returns:
    DataFrame: Daft DataFrame created from the provided pandas DataFrame.

## from_pydict

```python
from_pydict(data: dict[str, InputListType]) -> DataFrame
```

Creates a DataFrame from a Python dictionary.

Args:
    data: Key -> Sequence[item] of data. Each Key is created as a column, and must have a value that is
        a Python list, Numpy array or PyArrow array. Values must be equal in length across all keys.

Returns:
    DataFrame: DataFrame created from dictionary of columns

## from_pylist

```python
from_pylist(data: list[dict[str, Any]]) -> DataFrame
```

Creates a DataFrame from a list of dictionaries.

Args:
    data: List of dictionaries, where each key is a column name.

Returns:
    DataFrame: DataFrame created from list of dictionaries.

## from_ray_dataset

```python
from_ray_dataset(ds: 'RayDataset') -> DataFrame
```

Creates a DataFrame from a Ray Dataset.

Args:
    ds: The Ray Dataset to create a Daft DataFrame from.

Returns:
    DataFrame: Daft DataFrame created from the provided Ray dataset.

Note:
    This function can only work if Daft is running using the RayRunner

## func

_(exported object)_

_(no docstring)_

## functions

_(submodule)_

_(no docstring)_

## get_aggregate_function

```python
get_aggregate_function(name: str, *args: Expression) -> Expression
```

Returns the aggregate function from the current session or raises an exception if it does not exist.

## get_catalog

```python
get_catalog(identifier: str) -> Catalog
```

Returns the catalog from the current session or raises an exception if it does not exist.

## get_context

```python
get_context() -> DaftContext
```

Returns the global singleton daft context.

## get_function

```python
get_function(name: str, *args: Expression) -> Expression
```

Returns the function from the current session or raises an exception if it does not exist.

## get_loaded_extension_paths

```python
get_loaded_extension_paths() -> list[str]
```

_(no docstring)_

## get_or_create_runner

```python
get_or_create_runner() -> Runner[PartitionT]
```

Get or create the current runner instance.

If a runner has already been set, returns it. Otherwise, creates a new
runner using the default configuration (native) and locks it in.

Returns:
    Runner[PartitionT]: The current runner instance.

Note:
    After calling this function, the runner cannot be changed for the
    lifetime of the process. Use ``get_or_infer_runner_type`` to check the
    runner type without this side effect.

## get_or_infer_runner_type

```python
get_or_infer_runner_type() -> str
```

Get or infer the runner type.

This API will get or infer the currently used runner type according to the following strategies:
1. If the `runner` has been set, return its type directly;
2. Try to determine whether it's currently running on a ray cluster. If so, consider it to be a ray type;
3. Try to determine based on `DAFT_RUNNER` env variable.

Returns:
    str: The runner type ("native" or "ray").

## get_provider

```python
get_provider(identifier: str) -> Provider
```

Returns the provider from the current session or raises an exception if it does not exist.

## get_table

```python
get_table(identifier: Identifier | str) -> Table
```

Returns the table from the current session or raises an exception if it does not exist.

## has_catalog

```python
has_catalog(identifier: str) -> bool
```

Returns true if a catalog with the given identifier exists in the current session.

## has_namespace

```python
has_namespace(identifier: Identifier | str) -> bool
```

Returns true if a namespace with the given identifier exists in the current session.

## has_provider

```python
has_provider(identifier: str) -> bool
```

Returns true if a provider with the given identifier exists in the current session.

## has_table

```python
has_table(identifier: Identifier | str) -> bool
```

Returns true if a table with the given identifier exists in the current session.

## Hdf5File

```python
Hdf5File(url: str, io_config: IOConfig | None=None) -> None
```

Represents an HDF5 file backed by Daft file IO.

This class keeps ``File.open()`` as the inherited raw byte-stream API and
provides HDF5-specific helpers that mirror common h5py ``File`` and
``Group`` operations. HDF5 access uses a smaller default file buffer than
the generic ``File`` type because h5py performs frequent small reads after
seeks while traversing metadata and chunk indexes.

## history_deltalake

```python
history_deltalake(table: Union[str, 'UnityCatalogTable', 'deltalake.DeltaTable'], limit: int | None=None, io_config: IOConfig | None=None, parse_operation_metrics: bool=True) -> list[dict[str, Any]]
```

Return commit history for a Delta Lake table.

Args:
    table: Delta table URI, ``deltalake.DeltaTable``, or ``UnityCatalogTable``.
    limit: Maximum number of commits to return. ``None`` returns full history.
    io_config: Optional :class:`~daft.daft.IOConfig` used for object storage access.
    parse_operation_metrics: If ``True``, parse JSON-encoded ``operationMetrics`` into dictionaries.

Returns:
    list[dict[str, Any]]: Delta commit history entries.

## IdempotentCommit

```python
IdempotentCommit()
```

Per-call idempotent commit configuration for sink writes.

Bundles a :class:`CheckpointStore` with a stable ``idempotence_key`` that
identifies a single logical commit attempt. Pass via ``checkpoint=`` on
sinks that support idempotent commits (e.g. :meth:`DataFrame.write_iceberg`).

Args:
    store: The :class:`CheckpointStore` backing the commit's staging.
    idempotence_key: Stable identifier for this logical commit. Stamped
        on the resulting commit's metadata (e.g. an Iceberg snapshot
        summary's ``daft.idempotence-key``) so retries — after a crash,
        network blip, etc. — recognize the prior attempt and don't
        produce a duplicate. The user is responsible for keeping this
        stable across retries of the same logical commit and unique
        across distinct commits.

## Identifier

```python
Identifier(*parts: str)
```

A reference (path) to a catalog object.

## ImageFile

```python
ImageFile(url: str, io_config: IOConfig | None=None) -> None
```

An image-specific file interface that provides image operations.

## ImageFormat

```python
ImageFormat()
```

Supported image formats for Daft's image I/O.

## ImageMode

```python
ImageMode()
```

Supported image modes for Daft's image type.

Warning:
    Currently, only the 8-bit modes (L, LA, RGB, RGBA) can be stored in a DataFrame.
    If your binary image data includes other modes, use the `mode` argument
    in `image.decode` to convert the images to a supported mode.

## ImageProperty

```python
ImageProperty()
```

Supported image properties for Daft's image type.

## interval

```python
interval(years: int | None=None, months: int | None=None, days: int | None=None, hours: int | None=None, minutes: int | None=None, seconds: int | None=None, millis: int | None=None, nanos: int | None=None) -> Expression
```

Creates an Expression representing an interval.

## io

_(submodule)_

_(no docstring)_

## IOConfig

```python
IOConfig(s3: S3Config | None=None, azure: AzureConfig | None=None, gcs: GCSConfig | None=None, http: HTTPConfig | None=None, unity: UnityConfig | None=None, hf: HuggingFaceConfig | None=None, disable_suffix_range: bool | None=None, tos: TosConfig | None=None, gravitino: GravitinoConfig | None=None, cos: CosConfig | None=None, goosefs: GooseFSConfig | None=None, opendal_backends: dict[str, dict[str, str]] | None=None, protocol_aliases: dict[str, str] | None=None)
```

Configuration for the native I/O layer, e.g. credentials for accessing cloud storage systems.

## KeyFilteringSettings

```python
KeyFilteringSettings(num_workers: int | None=None, cpus_per_worker: float | None=None, keys_load_batch_size: int | None=None, max_concurrency_per_worker: int | None=None, filter_batch_size: int | None=None) -> None
```

_(no docstring)_

## list_catalogs

```python
list_catalogs(pattern: str | None=None) -> list[str]
```

Returns a list of available catalogs in the current session.

## list_tables

```python
list_tables(pattern: str | None=None) -> list[Identifier]
```

Returns a list of available tables in the current session.

## lit

```python
lit(value: object) -> Expression
```

Creates an Expression representing a column with every value set to the provided value.

Args:
    value: value of the literal

Returns:
    Expression: Expression representing the value provided

## load_extension

```python
load_extension(extension: str | types.ModuleType | Path) -> None
```

Load a native extension by module symbol or an explicit file path.

## MediaType

```python
MediaType() -> None
```

_(no docstring)_

## merge_deltalake

```python
merge_deltalake(table: Union[str, 'UnityCatalogTable', 'deltalake.DeltaTable'], source: Union[DataFrame, 'pa.Table'], predicate: str, io_config: IOConfig | None=None, source_alias: str='source', target_alias: str='target', custom_metadata: dict[str, str] | None=None, safe_cast: bool=True, merge_schema: bool=False, writer_properties: 'deltalake.WriterProperties | None'=None, streamed_exec: bool=True, max_spill_size: int | None=None, max_temp_directory_size: int | None=None, post_commithook_properties: 'deltalake.PostCommitHookProperties | None'=None, compression: str | None=None) -> DeltaMergeBuilder
```

Create a Delta Lake MERGE operation builder for composable merge clauses.

Returns a merge builder that mirrors the underlying ``deltalake`` merge API.
Call ``.execute()`` on the builder to perform the merge and return a DataFrame with operation metrics.

Args:
    table: Destination Delta table URI, ``deltalake.DeltaTable``, or ``UnityCatalogTable``.
    source: Source records to merge from, as a Daft DataFrame or PyArrow table.
    predicate: SQL merge predicate between ``target_alias`` and ``source_alias``.
    io_config: Optional :class:`~daft.daft.IOConfig` used for object storage access.
    source_alias: SQL alias for the source side of the merge predicate.
    target_alias: SQL alias for the target side of the merge predicate.
    custom_metadata: Optional key-value metadata to attach to the Delta commit.
    safe_cast: If ``True``, safely cast source expressions to target column types when needed.
    merge_schema: If ``True``, allow schema evolution during merge.
    writer_properties: Optional Arrow writer properties to use when writing files.
    streamed_exec: If ``True``, use the streamed execution path.
    max_spill_size: Maximum spill size in bytes for streamed execution.
    max_temp_directory_size: Maximum temporary directory size in bytes for streamed execution.
    post_commithook_properties: Optional post-commit hook properties.
    compression: Compression codec for the parquet data files this merge writes.
        Defaults to "snappy" when `writer_properties` is not supplied. Mutually
        exclusive with `writer_properties` (which already carries its own
        `compression` field) — passing both raises `ValueError`.

Returns:
    DeltaMergeBuilder: A builder object for chaining merge clauses with ``.execute()`` finalizer that returns a DataFrame.

Note:
    This runs a single atomic delta-rs merge: the source is streamed through
    one process and committed once, and only the files touched by the merge
    are rewritten. All clause types are supported, including
    ``when_not_matched_by_source_update`` / ``when_not_matched_by_source_delete``.
    For a source that is very large relative to the target, see
    :func:`distributed_merge_deltalake`, which distributes the join across workers.

    The returned DataFrame from ``.execute()`` contains merge metrics as columns and stores the raw metrics dict in ``_metadata["merge_metrics"]``.

## method

_(exported object)_

_(no docstring)_

## metrics

_(submodule)_

_(no docstring)_

## open_file

```python
open_file(url: str, mode: Literal['r', 'rt', 'rb']='r', buffering: int=-1, encoding: str | None=None, io_config: IOConfig | None=None) -> io.IOBase
```

Open a file from a URL, potentially from a remote filesystem using Daft's IO backend.

This is not intended for building expressions inside of DataFrames. Instead, this
is a convenience function for building custom DataSources and DataSinks without
another filesystem library like fsspec or pyarrow.fs.

Args:
    url (str): The URL of the file to open.
    mode (Literal["r", "rt", "rb"]): The mode to open the file in.
        - "r" / "rt": Text mode (default).
        - "rb": Binary mode.
    buffering (int): The buffering strategy to use. Note, if reading in text mode, we use the
    platform default buffer size defined in `io.DEFAULT_BUFFER_SIZE`.
        - -1: Use the default buffer size (default).
        - 0: No buffering (binary mode only).
        - 1: Line buffering (only applies to text mode).
        - >0: Fixed-sized buffer of size `buffering`.
    encoding (str | None): The encoding to use for text mode.
        - None: Use the default encoding (based on locale.getencoding()).
        - Any valid encoding supported by Python's codecs module.
    io_config (IOConfig | None): The IO configuration to use.

Returns:
    The opened file object to perform read/seek/tell operations on.

## planning_config_ctx

```python
planning_config_ctx(**kwargs: Any) -> Generator[None, None, None]
```

Context manager that wraps set_planning_config to reset the config to its original setting afternwards.

## range

```python
range(start: int, end: int | None=None, step: int=1, partitions: int=1) -> DataFrame
```

Creates a DataFrame with a range of values.

Args:
    start (int): The start of the range.
    end (int, optional): The end of the range. If not provided, the start is 0 and the end is `start`.
    step (int, optional): The step size of the range. Defaults to 1.
    partitions (int, optional): The number of partitions to split the range into. Defaults to 1.

## read_csv

```python
read_csv(path: str | list[str], infer_schema: bool=True, schema: dict[str, DataType] | None=None, has_headers: bool=True, delimiter: str | None=None, double_quote: bool=True, quote: str | None=None, escape_char: str | None=None, comment: str | None=None, allow_variable_columns: bool=False, io_config: IOConfig | None=None, file_path_column: str | None=None, hive_partitioning: bool=False, ignore_corrupt_files: bool=False, _buffer_size: int | None=None, _chunk_size: int | None=None, checkpoint: 'CheckpointConfig | None'=None) -> DataFrame
```

Creates a DataFrame from CSV file(s).

Args:
    path (str): Path to CSV (allows for wildcards; supports remote URLs to object stores such as ``s3://`` or ``gs://``)
    infer_schema (bool): Whether to infer the schema of the CSV, defaults to True.
    schema (dict[str, DataType]): A schema that is used as the definitive schema for the CSV if infer_schema is False, otherwise it is used as a schema hint that is applied after the schema is inferred (overriding the types of inferred columns, and appending any new columns not found during inference).
    has_headers (bool): Whether the CSV has a header or not, defaults to True
    delimiter (Str): Delimiter used in the CSV, defaults to ","
    double_quote (bool): Whether to support double quote escapes, defaults to True
    escape_char (str): Character to use as the escape character for double quotes, or defaults to `"`
    comment (str): Character to treat as the start of a comment line, or None to not support comments
    allow_variable_columns (bool): Whether to allow for variable number of columns in the CSV, defaults to False. If set to True, Daft will append nulls to rows with less columns than the schema, and ignore extra columns in rows with more columns
    io_config (IOConfig): Config to be used with the native downloader
    file_path_column: Include the source path(s) as a column with this name. Defaults to None.
    hive_partitioning: Whether to infer hive_style partitions from file paths and include them as columns in the Dataframe. Defaults to False.
    checkpoint: Optional :class:`daft.CheckpointConfig` for progress tracking across runs. Bundles the
        checkpoint store, the source key column (``on=``), and optional anti-join tuning. Rows whose key
        already exists in the store are skipped on re-run. Requires the Ray runner.
    ignore_corrupt_files: If True, corrupt or unreadable CSV files are silently skipped instead
        of raising an error. Skipped files are recorded in ``df.skipped_corrupt_files`` after collection.
        Defaults to False.

Returns:
    DataFrame: parsed DataFrame

## read_deltalake

```python
read_deltalake(table: Union[str, 'UnityCatalogTable'], version: Union[int, str, 'datetime'] | None=None, io_config: IOConfig | None=None, ignore_deletion_vectors: bool=False, _multithreaded_io: bool | None=None) -> DataFrame
```

Create a DataFrame from a Delta Lake table.

Args:
    table: Either a URI for the Delta Lake table (supports remote URLs to object stores such as ``s3://`` or ``gs://``)
        or a ``UnityCatalogTable`` instance from a Unity Catalog client.
    version (optional): If int is passed, read the table with specified version number. Otherwise if string or datetime,
        read the timestamp version of the table. Strings must be RFC 3339 and ISO 8601 date and time format.
        Datetimes are assumed to be UTC timezone unless specified. By default, read the latest version of the table.
    io_config (optional): A custom :class:`~daft.daft.IOConfig` to use when accessing Delta Lake object storage data. Defaults to None.
    ignore_deletion_vectors (optional): Whether to skip checking for deletion vectors when reading the table. Defaults to False.
    _multithreaded_io (optional): Whether to use multithreading for IO threads. Setting this to False can be helpful in reducing
        the amount of system resources (number of connections and thread contention) when running in the Ray runner.
        Defaults to None, which will let Daft decide based on the runner it is currently using.

Returns:
    DataFrame: A DataFrame with the schema converted from the specified Delta Lake table.

Note:
    This function requires the use of [deltalake](https://delta-io.github.io/delta-rs/), a Python library for interacting with Delta Lake.

## read_hudi

```python
read_hudi(table_uri: str, io_config: IOConfig | None=None, checkpoint: 'CheckpointConfig | None'=None) -> DataFrame
```

Create a DataFrame from a Hudi table.

Args:
    table_uri: URI to the Hudi table (supports remote URLs to object stores such as ``s3://`` or ``gs://``).
    io_config: A custom IOConfig to use when accessing Hudi table object storage data. Defaults to None.
    checkpoint: Optional :class:`daft.CheckpointConfig` for progress tracking across runs. Bundles the
        checkpoint store, the source key column (``on=``), and optional anti-join tuning. Rows whose key
        already exists in the store are skipped on re-run. Requires the Ray runner.

Returns:
    DataFrame: A DataFrame with the schema converted from the specified Hudi table.

Note:
    This function requires the use of Apache Hudi. To ensure that this is installed with Daft, you may install: ``pip install -U daft[hudi]``

## read_huggingface

```python
read_huggingface(repo: str, io_config: IOConfig | None=None) -> DataFrame
```

Create a DataFrame from a Hugging Face dataset.

Currently supports all public datasets and all private Parquet datasets. See [the Hugging Face docs](https://huggingface.co/docs/dataset-viewer/en/parquet) for more details.

Args:
    repo (str): repository to read in the form `username/dataset_name`
    io_config (IOConfig): Config to use when reading data

## read_iceberg

```python
read_iceberg(table: Union[str, os.PathLike[str], 'PyIcebergTable'], snapshot_id: int | None=None, branch: str | None=None, tag: str | None=None, io_config: IOConfig | None=None, checkpoint: 'CheckpointConfig | None'=None, ignore_corrupt_files: bool=False) -> DataFrame
```

Create a DataFrame from an Iceberg table.

Args:
    table (str, os.PathLike, or pyiceberg.table.Table): A path to an Iceberg metadata file (supports remote URLs
        to object stores such as ``s3://`` or ``gs://``) or a
        [PyIceberg Table](https://py.iceberg.apache.org/reference/pyiceberg/table/#pyiceberg.table.Table) created
        using the PyIceberg library.
    snapshot_id (int, optional): Snapshot ID of the table to query
    branch (str, optional): Iceberg branch name to query. Cannot be combined with ``snapshot_id`` or ``tag``.
    tag (str, optional): Iceberg tag name to query. Cannot be combined with ``snapshot_id`` or ``branch``.
    io_config (IOConfig, optional): A custom IOConfig to use when accessing Iceberg object storage data. If provided, configurations set in `table` are ignored.
    checkpoint: Optional :class:`daft.CheckpointConfig` for progress tracking across runs. Bundles the
        checkpoint store, the source key column (``on=``), and optional anti-join tuning. Rows whose key
        already exists in the store are skipped on re-run. Requires the Ray runner.
    ignore_corrupt_files (bool): If True, silently skip corrupt or unreadable data files
        instead of raising an error. Skipped files are recorded in ``df.skipped_corrupt_files``
        after collection. Defaults to False.

Returns:
    DataFrame: a DataFrame with the schema converted from the specified Iceberg table

Note:
    This function requires the use of [PyIceberg](https://py.iceberg.apache.org/), which is the Apache Iceberg's
    official project for Python.

## read_json

```python
read_json(path: str | list[str], infer_schema: bool=True, schema: dict[str, DataType] | None=None, io_config: IOConfig | None=None, file_path_column: str | None=None, hive_partitioning: bool=False, skip_empty_files: bool=False, _buffer_size: int | None=None, _chunk_size: int | None=None, checkpoint: 'CheckpointConfig | None'=None) -> DataFrame
```

Creates a DataFrame from line-delimited JSON file(s).

Args:
    path (str): Path to JSON files (allows for wildcards; supports remote URLs to object stores such as ``s3://`` or ``gs://``)
    infer_schema (bool): Whether to infer the schema of the JSON, defaults to True.
    schema (dict[str, DataType]): A schema that is used as the definitive schema for the JSON if infer_schema is False, otherwise it is used as a schema hint that is applied after the schema is inferred (overriding the types of inferred columns, and appending any new columns not found during inference).
    io_config (IOConfig): Config to be used with the native downloader
    file_path_column: Include the source path(s) as a column with this name. Defaults to None.
    hive_partitioning: Whether to infer hive_style partitions from file paths and include them as columns in the Dataframe. Defaults to False.
    skip_empty_files: Whether to skip empty files when reading. Defaults to False.
    checkpoint: Optional :class:`daft.CheckpointConfig` for progress tracking across runs. Bundles the
        checkpoint store, the source key column (``on=``), and optional anti-join tuning. Rows whose key
        already exists in the store are skipped on re-run. Requires the Ray runner.

Returns:
    DataFrame: parsed DataFrame

## read_kafka

```python
read_kafka(bootstrap_servers: str | Sequence[str], topics: str | Sequence[str], *, start: object=_KIND_EARLIEST, end: object=_KIND_LATEST, group_id: str='daft-bounded-kafka-reader', partitions: Sequence[int] | None=None, chunk_size: int=1024, kafka_client_config: Mapping[str, object] | None=None, timeout_ms: int=10000) -> DataFrame
```

Creates a DataFrame by reading messages from Kafka topic(s).

.. warning::

    This API is **experimental** and may change in future releases. Currently only bounded
    batch reads are supported — there is no streaming/unbounded mode and no offset commit
    management.

This function reads bounded ranges of messages from one or more Kafka topics. It supports
multiple ways to specify the start and end bounds: earliest/latest, timestamp, or explicit
partition offsets.

Args:
    bootstrap_servers (str | Sequence[str]): Kafka bootstrap server(s) to connect to.
        Can be a single server string (e.g., "localhost:9092") or a sequence of servers.
    topics (str | Sequence[str]): Kafka topic(s) to read from. Can be a single topic string
        or a sequence of topics.
    start (object): The start bound for reading messages. Defaults to "earliest".
        Supported values:
        - "earliest": Start from the earliest available offset for each partition.
        - "latest": Start from the latest offset for each partition.
        - int: Timestamp in milliseconds since epoch.
        - datetime: A timezone-aware or naive datetime (naive datetimes are assumed UTC).
        - str: An ISO-8601 timestamp string (e.g., "2024-01-01T00:00:00Z").
        - dict: For single topic: ``{partition: offset}``. For multiple topics: ``{topic: {partition: offset}}``.
    end (object): The end bound for reading messages. Defaults to "latest".
        Supports the same value types as ``start``. The end offset is exclusive.
    group_id (str): Consumer group ID used for the Kafka consumer. Defaults to
        "daft-bounded-kafka-reader".
    partitions (Sequence[int] | None): Optional sequence of partition IDs to read from.
        If None, reads from all partitions of the specified topic(s). Defaults to None.
    chunk_size (int): Maximum number of messages per RecordBatch. Defaults to 1024.
    kafka_client_config (Mapping[str, object] | None): Optional additional configuration
        options passed directly to the underlying Kafka consumer. These are merged with
        the default configuration. Defaults to None.
    timeout_ms (int): Timeout in milliseconds for Kafka operations (metadata queries,
        message consumption, etc.). Defaults to 10_000 (10 seconds).

Returns:
    DataFrame: A DataFrame with the following schema:
        - topic (string): The topic the message was read from.
        - partition (int32): The partition ID within the topic.
        - offset (int64): The offset of the message within the partition.
        - timestamp_ms (int64): The timestamp of the message in milliseconds since epoch,
          or null if not available.
        - key (binary): The message key as raw bytes, or null if not present.
        - value (binary): The message value as raw bytes.

## read_lance

```python
read_lance(uri: str | os.PathLike[str], io_config: Any=None, version: Any=None, asof: Any=None, block_size: Any=None, commit_lock: Any=None, index_cache_size: Any=None, default_scan_options: Any=None, metadata_cache_size_bytes: Any=None, fragment_group_size: Any=None, include_fragment_id: Any=None, checkpoint: Any=None) -> Any
```

Create a DataFrame from a LanceDB table.

Args:
    uri: The URI of the Lance table to read from. Accepts a local path or an
        object-store URI like "s3://bucket/path".
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
    checkpoint: Optional :class:`daft.CheckpointConfig` for progress tracking across runs. Bundles the
        checkpoint store, the source key column (``on=``), and optional anti-join tuning. Rows whose key
        already exists in the store are skipped on re-run. Requires the Ray runner.

Returns:
    DataFrame: a DataFrame with the schema converted from the specified LanceDB table

    This function requires the use of [LanceDB](https://lancedb.github.io/lancedb/), which is the Python library for the LanceDB project.
    To ensure that this is installed with Daft, you may install: `pip install daft[lance]`

## read_mcap

```python
read_mcap(path: str, io_config: IOConfig | None=None, start_time: int | None=None, end_time: int | None=None, topics: list[str] | None=None, batch_size: int=1000, topic_start_time_resolver: TopicStartTimeResolver | None=None) -> DataFrame
```

Read mcap file.

Args:
    path: mcap file path
    start_time: Start time to filter messages (same unit as MCAP message.log_time, typically nanoseconds).
    end_time: End time to filter messages (same unit as MCAP message.log_time, typically nanoseconds).
    topics: List of topics to filter messages.
    batch_size: Number of messages to read in each batch.
    topic_start_time_resolver: Optional callable to compute per-file, per-topic start times.
        The callable is invoked once per MCAP file with the resolved file path and must return
        a mapping where:
        - key: topic name (str)
        - value: start time (int, same unit as MCAP message.log_time)

        will create one scan task per (file, topic) and set the task's start_time to:
        max(start_time, topic_start_time_resolver(file)[topic]).

Returns:
    DataFrame: DataFrame with the schema converted from the specified MCAP file.

## read_paimon

```python
read_paimon(table: 'PaimonTable', io_config: IOConfig | None=None) -> DataFrame
```

Create a DataFrame from an Apache Paimon table.

Args:
    table (pypaimon.table.Table): A Paimon table object created using the pypaimon library.
        Use ``pypaimon.CatalogFactory.create(options).get_table(identifier)`` to obtain one.
    io_config (IOConfig, optional): A custom IOConfig to use when accessing Paimon object
        storage data. If provided, any credentials in the catalog options are ignored.

Returns:
    DataFrame: a DataFrame with the schema converted from the specified Paimon table.

Note:
    This function requires the use of `pypaimon <https://pypi.org/project/pypaimon/>`_,
    the Apache Paimon official Python API.

    For primary-key tables that require LSM-tree merge (i.e. splits with multiple files
    at overlapping levels), reads fall back to pypaimon's native reader. Append-only tables
    and single-file PK splits are read via Daft's native high-performance Parquet reader.

## read_parquet

```python
read_parquet(path: str | list[str], row_groups: list[list[int]] | None=None, infer_schema: bool=True, schema: dict[str, DataType] | None=None, io_config: IOConfig | None=None, file_path_column: str | None=None, hive_partitioning: bool=False, coerce_int96_timestamp_unit: str | TimeUnit | None=None, ignore_corrupt_files: bool=False, geometry: bool=True, _multithreaded_io: bool | None=None, _chunk_size: int | None=None, checkpoint: 'CheckpointConfig | None'=None) -> DataFrame
```

Creates a DataFrame from Parquet file(s).

Args:
    path (str): Path to Parquet file (allows for wildcards; supports remote URLs to object stores such as ``s3://`` or ``gs://``)
    row_groups (List[int] or List[List[int]]): List of row groups to read corresponding to each file.
    infer_schema (bool): Whether to infer the schema of the Parquet, defaults to True.
    schema (dict[str, DataType]): A schema that is used as the definitive schema for the Parquet file if infer_schema is False, otherwise it is used as a schema hint that is applied after the schema is inferred (overriding the types of inferred columns, and appending any new columns not found during inference).
    io_config (IOConfig): Config to be used with the native downloader
    file_path_column: Include the source path(s) as a column with this name. Defaults to None.
    hive_partitioning: Whether to infer hive_style partitions from file paths and include them as columns in the Dataframe. Defaults to False.
    coerce_int96_timestamp_unit: TimeUnit to coerce Int96 TimeStamps to. e.g.: [ns, us, ms], Defaults to None.
    ignore_corrupt_files: If True, corrupt or unreadable Parquet files are silently skipped
        instead of raising an error. Skipped files are recorded in ``df.skipped_corrupt_files`` after
        collection. Only genuine format errors (bad magic bytes, truncated footer, corrupt
        row-group data) are ignored; network errors and permission errors are still raised.
        Defaults to False.
    geometry: If True (default), WKB columns declared in GeoParquet ``"geo"`` footer metadata
        are automatically re-typed from ``Binary`` to ``Geometry`` on read.  Set to False to
        suppress geo detection and keep the raw ``Binary`` dtype.
    _multithreaded_io: Whether to use multithreading for IO threads. Setting this to False can be helpful in reducing
        the amount of system resources (number of connections and thread contention) when running in the Ray runner.
        Defaults to None, which will let Daft decide based on the runner it is currently using.
    checkpoint: Optional :class:`daft.CheckpointConfig` for progress tracking across runs. Bundles the
        checkpoint store, the source key column (``on=``), and optional anti-join tuning. Rows whose key
        already exists in the store are skipped on re-run. Requires the Ray runner.

Returns:
    DataFrame: parsed DataFrame

## read_sql

```python
read_sql(sql: str, conn: Callable[[], 'Connection'] | str, partition_col: str | None=None, num_partitions: int | None=None, partition_bound_strategy: str='min-max', disable_pushdowns_to_sql: bool=False, infer_schema: bool=True, infer_schema_length: int=10, schema: dict[str, DataType] | None=None) -> DataFrame
```

Create a DataFrame from the results of a SQL query.

Args:
    sql (str): SQL query to execute
    conn (Union[Callable[[], Connection], str]): SQLAlchemy connection factory or database URL
    partition_col (Optional[str]): Column to partition the data by, defaults to None
    num_partitions (Optional[int]): Number of partitions to read the data into,
        defaults to None, which will lets Daft determine the number of partitions.
        If specified, `partition_col` must also be specified.
    partition_bound_strategy (str): Strategy to determine partition bounds, either "min-max" or "percentile", defaults to "min-max"
    disable_pushdowns_to_sql (bool): Whether to disable pushdowns to the SQL query, defaults to False
    infer_schema (bool): Whether to turn on schema inference, defaults to True. If set to False, the schema parameter must be provided.
    infer_schema_length (int): The number of rows to scan when inferring the schema, defaults to 10. If infer_schema is False, this parameter is ignored. Note that if Daft is able to use ConnectorX to infer the schema, this parameter is ignored as ConnectorX is an Arrow backed driver.
    schema (Optional[Dict[str, DataType]]): A mapping of column names to datatypes. If infer_schema is False, this schema is used as the definitive schema for the data, otherwise it is used as a schema hint that is applied after the schema is inferred (overriding the types of inferred columns, and appending any new columns not found during inference).
        This can be useful if the types can be more precisely determined than what the inference can provide (e.g., if a column can be declared as a fixed-sized list rather than a list).

Returns:
    DataFrame: Dataframe containing the results of the query

Note:
    1. **Supported dialects**:
        Daft uses [SQLGlot](https://sqlglot.com/sqlglot.html) to build and translate SQL queries between dialects. For a list of supported dialects, see [SQLGlot's dialect documentation](https://sqlglot.com/sqlglot/dialects.html).

    2. **Partitioning**:
        When `partition_col` is specified, the function partitions the query based on that column.
        You can define `num_partitions` or leave it to Daft to decide.
        Daft uses the `partition_bound_strategy` parameter to determine the partitioning strategy:
        - `min_max`: Daft calculates the minimum and maximum values of the specified column, then partitions the query using equal ranges between the minimum and maximum values.
        - `percentile`: Daft calculates the specified column's percentiles via a `PERCENTILE_DISC` function to determine partitions (e.g., for `num_partitions=3`, it uses the 33rd and 66th percentiles).

    3. **Execution**:
        Daft executes SQL queries using using [ConnectorX](https://sfu-db.github.io/connector-x/intro.html) or [SQLAlchemy](https://docs.sqlalchemy.org/en/20/orm/quickstart.html#create-an-engine),
        preferring ConnectorX unless a SQLAlchemy connection factory is specified or the database dialect is unsupported by ConnectorX.

    4. **Pushdowns**:
        Daft pushes down operations such as filtering, projections, and limits into the SQL query when possible.
        You can disable pushdowns by setting `disable_pushdowns_to_sql=True`, which will execute the SQL query as is.

## read_table

```python
read_table(identifier: Identifier | str, **options: Any) -> DataFrame
```

Returns the table as a DataFrame or raises an exception if it does not exist.

## read_text

```python
read_text(path: str | list[str], *, encoding: str='utf-8', skip_blank_lines: bool=True, whole_text: bool=False, file_path_column: str | None=None, hive_partitioning: bool=False, io_config: IOConfig | None=None, _buffer_size: int | None=None, _chunk_size: int | None=None) -> DataFrame
```

Creates a DataFrame from line-oriented text file(s).

Args:
    path: Path to text file(s). Supports wildcards and remote URLs such as ``s3://`` or ``gs://``.
    encoding: Encoding of the input files, defaults to ``"utf-8"``.
    skip_blank_lines: Whether to skip empty lines (after stripping whitespace). Defaults to ``True``.
        When ``whole_text=True``, this skips files that are entirely blank.
    whole_text: Whether to read each file as a single row. Defaults to ``False``.
        When ``False``, each line in the file becomes a row in the DataFrame.
        When ``True``, the entire content of each file becomes a single row in the DataFrame.
    file_path_column: Include the source path(s) as a column with this name. Defaults to ``None``.
    hive_partitioning: Whether to infer hive-style partitions from file paths and include them as
        columns in the DataFrame. Defaults to ``False``.
    io_config: IO configuration for the native downloader.
    _buffer_size: Optional tuning parameter for the underlying streaming reader buffer size (bytes).
    _chunk_size: Optional tuning parameter for the underlying streaming reader chunk size (rows).
        Has no effect when ``whole_text=True``.

Returns:
    DataFrame: A DataFrame with a single ``"text"`` column containing lines from the input files
        (when ``whole_text=False``) or entire file contents (when ``whole_text=True``).

## read_video_frames

```python
read_video_frames(path: str | list[str], image_height: int, image_width: int, is_key_frame: bool | None=None, *, sample_interval_seconds: float | None=None, io_config: IOConfig | None=None) -> DataFrame
```

Creates a DataFrame by reading the frames of one or more video files.

This produces a DataFrame with the following fields:
    * path (string): path to the video file that produced this frame.
    * frame_index (int): frame index in the video.
    * frame_time (float): frame time in fractional seconds as a floating point.
    * frame_time_base (str): fractional unit of seconds in which timestamps are expressed.
    * frame_pts (int): frame presentation timestamp in time_base units.
    * frame_dts (int): frame decoding timestamp in time_base units.
    * frame_duration (int): frame duration in time_base units.
    * is_key_frame (bool): true iff this is a key frame.

Warning:
    This requires PyAV which can be installed with `pip install av`.

Note:
    This function will stream the frames from all videos as a DataFrame of images.
    If you wish to load an entire video into a single row, this can be done with
    DataFrame.from_glob_path and url_download.

Args:
    path (str|list[str]): Path(s) to the video file(s) which allows wildcards.
    image_height (int): Height to which each frame will be resized.
    image_width (int): Width to which each frame will be resized.
    is_key_frame (bool|None): If True, only include key frames; if False, only non-key frames; if None, include all frames.
    sample_interval_seconds (float|None): If provided and > 0, sample frames at approximately this time interval in seconds based on ``frame_time``.
        The algorithm selects the first frame whose timestamp is >= target time (0, interval, 2*interval, ...).
        This is an approximate sampling strategy; actual sampling times depend on the video's frame timestamps.
        Frames without valid timestamps (frame_time=None) are skipped.
    io_config (IOConfig|None): Optional IOConfig.

Returns:
    DataFrame: dataframe of images.

## read_warc

```python
read_warc(path: str | list[str], io_config: IOConfig | None=None, file_path_column: str | None=None, _multithreaded_io: bool | None=None, checkpoint: 'CheckpointConfig | None'=None) -> DataFrame
```

Creates a DataFrame from WARC or gzipped WARC file(s). This is an experimental feature and the API may change in the future.

Args:
    path (Union[str, List[str]]): Path to WARC file (allows for wildcards; supports remote URLs to object stores such as ``s3://`` or ``gs://``)
    io_config (Optional[IOConfig]): Config to be used with the native downloader
    file_path_column (Optional[str]): Include the source path(s) as a column with this name. Defaults to None.
    _multithreaded_io (Optional[bool]): Whether to use multithreading for IO threads. Setting this to False can be helpful in reducing
        the amount of system resources (number of connections and thread contention) when running in the Ray runner.
        Defaults to None, which will let Daft decide based on the runner it is currently using.
    checkpoint: Optional :class:`daft.CheckpointConfig` for progress tracking across runs. Bundles the
        checkpoint store, the source key column (``on=``), and optional anti-join tuning. Rows whose key
        already exists in the store are skipped on re-run. Requires the Ray runner.

Returns:
    DataFrame: parsed DataFrame with mandatory metadata columns ("WARC-Record-ID", "WARC-Type", "WARC-Date", "Content-Length"), one optional
        metadata column ("WARC-Identified-Payload-Type"), one column "warc_content" with the raw byte content of the WARC record,
        and one column "warc_headers" with the remaining headers of the WARC record stored as a JSON string.

## refresh_logger

```python
refresh_logger() -> None
```

Refreshes Daft's internal rust logging to the current python log level.

## register_viz_hook

```python
register_viz_hook(klass: type[HookClass], hook: Callable[[Any], str]) -> None
```

Registers a visualization hook that returns the appropriate HTML for visualizing a specific class in HTML.

## ResourceRequest

```python
ResourceRequest(num_cpus: float | None=None, num_gpus: float | None=None, memory_bytes: int | None=None)
```

Resource request for a query fragment task.

## runners

_(submodule)_

_(no docstring)_

## Schema

```python
Schema() -> None
```

_(no docstring)_

## Series

```python
Series() -> None
```

A Daft Series is an array of data of a single type, and is usually a column in a DataFrame.

## Session

```python
Session() -> None
```

Session holds a connection's state and orchestrates execution of DataFrame and SQL queries against catalogs.

## session

_(submodule)_

_(no docstring)_

## set_catalog

```python
set_catalog(identifier: str | None) -> None
```

Set the given catalog as current_catalog for the current session or raises an if it does not exist.

## set_execution_config

```python
set_execution_config(config: PyDaftExecutionConfig | None=None, enable_scan_task_split_and_merge: bool | None=None, scan_tasks_min_size_bytes: int | None=None, scan_tasks_max_size_bytes: int | None=None, max_sources_per_scan_task: int | None=None, broadcast_join_size_bytes_threshold: int | None=None, parquet_split_row_groups_max_files: int | None=None, hash_join_partition_size_leniency: float | None=None, sample_size_for_sort: int | None=None, num_preview_rows: int | None=None, parquet_target_filesize: int | None=None, parquet_target_row_group_size: int | None=None, parquet_inflation_factor: float | None=None, csv_target_filesize: int | None=None, csv_inflation_factor: float | None=None, json_target_filesize: int | None=None, json_inflation_factor: float | None=None, text_inflation_factor: float | None=None, shuffle_aggregation_default_partitions: int | None=None, partial_aggregation_threshold: int | None=None, high_cardinality_aggregation_threshold: float | None=None, read_sql_partition_size_bytes: int | None=None, default_morsel_size: int | None=None, shuffle_algorithm: str | None=None, pre_shuffle_merge_threshold: int | None=None, pre_shuffle_merge_partition_threshold: int | None=None, scantask_max_parallel: int | None=None, native_parquet_writer: bool | None=None, min_cpu_per_task: float | None=None, actor_udf_ready_timeout: int | None=None, maintain_order: bool | None=None, enable_dynamic_batching: bool | None=None, dynamic_batching_strategy: str | None=None, flight_shuffle_dirs: list[str] | None=None, flight_shuffle_compression: str | None=None, enable_multi_glob_path_tasks: bool | None=None) -> DaftContext
```

Globally sets various configuration parameters which control various aspects of Daft execution.

These configuration values
are used when a Dataframe is executed (e.g. calls to `DataFrame.write_*`, [DataFrame.collect()](https://docs.daft.ai/en/stable/api/dataframe/#daft.DataFrame.collect) or [DataFrame.show()](https://docs.daft.ai/en/stable/api/dataframe/#daft.DataFrame.select)).

Args:
    config: A PyDaftExecutionConfig object to set the config to, before applying other kwargs. Defaults to None which indicates
        that the old (current) config should be used.
    enable_scan_task_split_and_merge: Whether to enable scan task split and merge. Defaults to False.
    scan_tasks_min_size_bytes: Minimum size of scan tasks in bytes. Defaults to 96MB.
    scan_tasks_max_size_bytes: Maximum size of scan tasks in bytes. Defaults to 384MB.
    max_sources_per_scan_task: Maximum number of sources per scan task. Defaults to 10.
    parquet_split_row_groups_max_files: Maximum number of files to read in which the row group splitting should happen. (Defaults to 10)
    broadcast_join_size_bytes_threshold: If one side of a join is smaller than this threshold, a broadcast join will be used.
        Default is 10 MiB.
    hash_join_partition_size_leniency: If the left side of a hash join is already correctly partitioned and the right side isn't,
        and the ratio between the left and right size is at least this value, then the right side is repartitioned to have an equal
        number of partitions as the left. Defaults to 0.5.
    sample_size_for_sort: number of elements to sample from each partition when running sort,
        Default is 20.
    num_preview_rows: number of rows to when showing a dataframe preview,
        Default is 8.
    parquet_target_filesize: Target File Size when writing out Parquet Files. Defaults to 512MB
    parquet_target_row_group_size: Target Row Group Size when writing out Parquet Files. Defaults to 128MB
    parquet_inflation_factor: Inflation Factor of parquet files (In-Memory-Size / File-Size) ratio. Defaults to 3.0
    csv_target_filesize: Target File Size when writing out CSV Files. Defaults to 512MB
    csv_inflation_factor: Inflation Factor of CSV files (In-Memory-Size / File-Size) ratio. Defaults to 0.5
    json_target_filesize: Target File Size when writing out JSON Files. Defaults to 512MB
    json_inflation_factor: Inflation Factor of JSON files (In-Memory-Size / File-Size) ratio. Defaults to 0.25
    text_inflation_factor: Inflation Factor of Text files (In-Memory-Size / File-Size) ratio. Defaults to 1.0
    shuffle_aggregation_default_partitions: Maximum number of partitions to create when performing aggregations on the Ray Runner. Defaults to 200, unless the number of input partitions is less than 200.
    partial_aggregation_threshold: Threshold for performing partial aggregations on the Native Runner. Defaults to 10000 rows.
    high_cardinality_aggregation_threshold: Threshold selectivity for performing high cardinality aggregations on the Native Runner. Defaults to 0.8.
    read_sql_partition_size_bytes: Target size of partition when reading from SQL databases. Defaults to 512MB
    default_morsel_size: Default size of morsels used for the new local executor. Defaults to 131072 rows.
    shuffle_algorithm: The shuffle algorithm to use. Defaults to "auto", which will let Daft determine the algorithm. Options are "map_reduce", "pre_shuffle_merge", and "flight_shuffle".
    pre_shuffle_merge_threshold: Memory threshold in bytes for pre-shuffle merge. Defaults to 1GB
    pre_shuffle_merge_partition_threshold: Number of partitions threshold to enable pre-shuffle merge when shuffle_algorithm is "auto". Defaults to 200.
    scantask_max_parallel: Set the max parallelism for running scan tasks simultaneously. Currently, this only works for Native Runner. If set to 0, all available CPUs will be used. Defaults to 8.
    native_parquet_writer: Whether to use the native parquet writer vs the pyarrow parquet writer. Defaults to `True`.
    min_cpu_per_task: Deprecated. This was used by the old Ray runner and has no effect on
        distributed scheduling. It will be removed in v0.8.0.
    actor_udf_ready_timeout: Timeout for UDF actors to be ready. Defaults to 120 seconds.
    maintain_order: Whether to maintain order during execution. Defaults to True. Some blocking sink operators (e.g. write_parquet) won't respect this flag and will always keep maintain_order as false, and propagate to child operators. It's useful to set this to False for running df.collect() when no ordering is required.
    enable_dynamic_batching: Whether to enable dynamic batching. Defaults to False.
    dynamic_batching_strategy: The strategy to use for dynamic batching. Defaults to 'auto'.
    flight_shuffle_dirs: Directories to use for flight shuffle. Defaults to ["/tmp"]. Must not be empty.
    flight_shuffle_compression: Arrow IPC compression for flight shuffle spill files. One of "lz4", "zstd", or "none". Defaults to "lz4". Pass "none" to disable compression; passing Python None leaves the current config unchanged.
    enable_multi_glob_path_tasks: Whether to create multiple glob path tasks in Ray Runner to achieve parallel glob. Defaults to False.

## set_model

```python
set_model(identifier: str | None) -> None
```

Set the given model as current_model for the active session.

## set_namespace

```python
set_namespace(identifier: Identifier | str | None) -> None
```

Set the given namespace as current_namespace for the active session.

## set_planning_config

```python
set_planning_config(config: PyDaftPlanningConfig | None=None, default_io_config: IOConfig | None=None, enable_strict_filter_pushdown: bool | None=None) -> DaftContext
```

Globally sets various configuration parameters which control Daft plan construction behavior.

These configuration values are used when a Dataframe is being constructed (e.g. calls to create a Dataframe, or to build on an existing Dataframe).

Args:
    config: A PyDaftPlanningConfig object to set the config to, before applying other kwargs. Defaults to None which indicates
        that the old (current) config should be used.
    default_io_config: A default IOConfig to use in the absence of one being explicitly passed into any Expression (e.g. `.download()`)
        or Dataframe operation (e.g. `daft.read_parquet()`).

## set_provider

```python
set_provider(identifier: str | Provider | None, **options: Any) -> None
```

Set the given provider as current_provider for the active session.

## set_runner_native

```python
set_runner_native(num_threads: int | None=None) -> Runner[PartitionT]
```

Configure Daft to execute dataframes using native multi-threaded processing.

This is the default execution mode for Daft.

Returns:
    Runner[PartitionT]: A runner object with the native runner's configuration.

Note:
    Can also be configured via environment variable: DAFT_RUNNER=native

## set_runner_ray

```python
set_runner_ray(address: str | None=None, noop_if_initialized: bool=False, force_client_mode: bool=False, *, downscale_enabled: bool | None=None, downscale_idle_seconds: int | None=None, min_survivor_workers: int | None=None, pending_release_exclude_seconds: int | None=None, worker_startup_timeout: int | None=None) -> Runner[PartitionT]
```

Configure Daft to execute dataframes using the Ray distributed computing framework.

Args:
    address: Ray cluster address to connect to. If None, connects to or starts a local Ray instance.
    noop_if_initialized: If True, skip initialization if Ray is already running.
    force_client_mode: If True, forces Ray to run in client mode.
    downscale_enabled: Enable/disable retiring idle Ray workers (scale-in). If not provided,
        falls back to the ``DAFT_AUTOSCALING_DOWNSCALE_ENABLED`` environment variable (default: False).
    downscale_idle_seconds: Minimum number of seconds a worker must be idle before it becomes eligible
        for retirement. If not provided, falls back to ``DAFT_AUTOSCALING_DOWNSCALE_IDLE_SECONDS``
        (default: 60).
    min_survivor_workers: Minimum number of Ray workers to keep alive even if they are idle.
        If not provided, falls back to ``DAFT_AUTOSCALING_MIN_SURVIVOR_WORKERS`` (default: 1).
    pending_release_exclude_seconds: Grace period (TTL) for recently-released worker IDs during
        worker discovery, to prevent the autoscaler from immediately respawning them. If not
        provided, falls back to ``DAFT_AUTOSCALING_PENDING_RELEASE_EXCLUDE_SECONDS`` (default: 120).
    worker_startup_timeout: Timeout in seconds for Ray worker actors to report their addresses during startup.
        Can also be configured via the ``DAFT_RAY_WORKER_STARTUP_TIMEOUT`` environment variable.

Returns:
    Runner[PartitionT]: A runner object with the Ray runner's configurations.

Note:
    Can also be configured via environment variable: DAFT_RUNNER=ray

## set_session

```python
set_session(session: Session) -> None
```

Sets the global context's current session.

## sql

_(submodule)_

_(no docstring)_

## sql_expr

```python
sql_expr(sql: str) -> Expression
```

Parses a SQL string into a Daft Expression.

This function allows you to create Daft Expressions from SQL snippets, which can then be used
in Daft operations or combined with other Daft Expressions.

Args:
    sql (str): A SQL string to be parsed into a Daft Expression.

Returns:
    Expression: A Daft Expression representing the parsed SQL.

## Table

```python
Table()
```

Interface for python table implementations.

## TimeUnit

```python
TimeUnit() -> None
```

_(no docstring)_

## udaf

_(submodule)_

_(no docstring)_

## udf

_(submodule)_

_(no docstring)_

## UnionMode

```python
UnionMode()
```

Union mode for Arrow union types.

## update_deltalake

```python
update_deltalake(table: Union[str, 'UnityCatalogTable', 'deltalake.DeltaTable'], updates: 'Mapping[str, str]', predicate: str | None=None, io_config: IOConfig | None=None, custom_metadata: dict[str, str] | None=None, safe_cast: bool=True) -> dict[str, Any]
```

Update rows in a Delta Lake table.

Args:
    table: Destination Delta table URI, ``deltalake.DeltaTable``, or ``UnityCatalogTable``.
    updates: Mapping from column name to SQL update expression.
    predicate: SQL predicate that selects rows to update. If ``None``, updates all rows.
    io_config: Optional :class:`~daft.daft.IOConfig` used for object storage access.
    custom_metadata: Optional key-value metadata to attach to the Delta commit.
    safe_cast: If ``True``, safely cast update expressions to target column types when needed.

Returns:
    dict[str, Any]: Delta-rs metrics from the update operation.

## VideoFile

```python
VideoFile(url: str, io_config: IOConfig | None=None) -> None
```

A video-specific file interface that provides video operations.

## Window

```python
Window() -> None
```

Describes how to partition data and in what order to apply the window function.

This class provides a way to specify window definitions for window functions.
Window functions operate on a group of rows (called a window frame) and return
a result for each row based on the values in its window frame.

## with_subscriber

```python
with_subscriber(alias: str, subscriber: Subscriber) -> Generator[None, None, None]
```

Context manager that attaches a subscriber to the current context, and detaches it afterwards.

Args:
    alias (str): Alias of subscriber to attach
    subscriber (Subscriber): Subscriber instance that will receive events

## write_table

```python
write_table(identifier: Identifier | str, df: DataFrame, mode: Literal['append', 'overwrite']='append', **options: Any) -> None
```

Writes the DataFrame to the table specified with the identifier.
