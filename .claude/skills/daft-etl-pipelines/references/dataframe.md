# dataframe

## agg

```python
agg(*to_agg: Expression | Iterable[Expression]) -> DataFrame
```

Perform aggregations on this DataFrame.

Allows for mixed aggregations for multiple columns and will return a single row that aggregated the entire DataFrame.

Args:
    *to_agg (Expression): aggregation expressions

Returns:
    DataFrame: DataFrame with aggregated results

## agg_concat

```python
agg_concat(*cols: ColumnInputType, delimiter: str | None=None) -> DataFrame
```

Performs a global concatenation agg on the DataFrame.

Args:
    *cols (Union[str, Expression]): columns that are lists or strings to concatenate
    delimiter: Optional delimiter to insert between concatenated string values. Only supported for string
        columns.

Returns:
    DataFrame: Globally aggregated list or string. Should be a single row.

## agg_list

```python
agg_list(*cols: ColumnInputType) -> DataFrame
```

Performs a global list agg on the DataFrame.

Args:
    *cols (Union[str, Expression]): columns to form into a list
Returns:
    DataFrame: Globally aggregated list. Should be a single row.

## agg_set

```python
agg_set(*cols: ColumnInputType) -> DataFrame
```

Performs a global set agg on the DataFrame (ignoring nulls).

Args:
    *cols (Union[str, Expression]): columns to form into a set

Returns:
    DataFrame: Globally aggregated set. Should be a single row.

## any_value

```python
any_value(*cols: ColumnInputType) -> DataFrame
```

Returns an arbitrary value on this DataFrame.

Values for each column are not guaranteed to be from the same row.

Args:
    *cols (Union[str, Expression]): columns to get an arbitrary value from
Returns:
    DataFrame: DataFrame with any values.

## collect

```python
collect(num_preview_rows: int | None=8) -> DataFrame
```

Executes the entire DataFrame and materializes the results.

Args:
    num_preview_rows: Number of rows to preview. Defaults to 8.

Returns:
    DataFrame: DataFrame with materialized results.

Note:
    This call is **blocking** and will execute the DataFrame when called

## column_names

```python
column_names() -> list[str]
```

Returns column names of DataFrame as a list of strings.

Returns:
    List[str]: Column names of this DataFrame.

## columns

```python
columns() -> list[Expression]
```

Returns column of DataFrame as a list of Expressions.

Returns:
    List[Expression]: Columns of this DataFrame.

## concat

```python
concat(other: 'DataFrame') -> DataFrame
```

Concatenates two DataFrames together in a "vertical" concatenation.

The resulting DataFrame has number of rows equal to the sum of the number of rows of the input DataFrames.

Args:
    other (DataFrame): other DataFrame to concatenate

Returns:
    DataFrame: DataFrame with rows from `self` on top and rows from `other` at the bottom.

Note:
    DataFrames being concatenated **must have exactly the same schema**. You may wish to use the
    [df.select()][daft.DataFrame.select] and [expr.cast()][daft.expressions.Expression.cast] methods
    to ensure schema compatibility before concatenation.

## count

```python
count(*cols: ColumnInputType | int) -> DataFrame
```

Performs a global count on the DataFrame.

Args:
    *cols (Union[str, Expression, int]): columns to count
Returns:
    DataFrame: Globally aggregated count. Should be a single row.

## count_distinct

```python
count_distinct(*cols: ColumnInputType) -> DataFrame
```

Performs a global count of distinct values on the DataFrame.

Args:
    *cols (Union[str, Expression]): columns to count distinct values
Returns:
    DataFrame: Globally aggregated count of distinct values. Should be a single row.

## count_rows

```python
count_rows() -> int
```

Executes the Dataframe to count the number of rows.

Returns:
    int: count of the number of rows in this DataFrame.

## describe

```python
describe() -> DataFrame
```

Returns the Schema of the DataFrame, which provides information about each column, as a new DataFrame.

Returns:
    DataFrame: A dataframe where each row is a column name and its corresponding type.

## distinct

```python
distinct(*on: ColumnInputType) -> DataFrame
```

Computes distinct rows, dropping duplicates.

Optionally, specify a subset of columns to perform distinct on.

Args:
    *on (Union[str, Expression]): columns to perform distinct on. Defaults to all columns.

Returns:
    DataFrame: DataFrame that has only distinct rows.

## drop_deltalake

```python
drop_deltalake(table: Union[str, pathlib.Path, 'deltalake.DeltaTable', 'UnityCatalogTable'], io_config: IOConfig | None=None) -> None
```

Delete a Delta Lake table completely from the filesystem.

Removes the entire Delta Lake table including all data files and metadata.
This operation cannot be undone.

Args:
    table (Union[str, pathlib.Path, deltalake.DeltaTable, UnityCatalogTable]): 
        Destination Delta Lake table URI, path, or ``deltalake.DeltaTable`` reference to delete.
    io_config (IOConfig, optional): Configurations to use when interacting with remote storage.

Raises:
    FileNotFoundError: If the table does not exist.
    PermissionError: If insufficient permissions to delete the table.

Note:
    This operation is **blocking** and immediately deletes the table from storage.
    The operation removes the entire table directory including all parquet files and the .delta directory.

## drop_duplicates

```python
drop_duplicates(*subset: ColumnInputType) -> DataFrame
```

Computes distinct rows, dropping duplicates.

Alias for [DataFrame.distinct][daft.DataFrame.distinct].

Args:
    *subset (Union[str, Expression]): columns to perform distinct on. Defaults to all columns.

Returns:
    DataFrame: DataFrame that has only distinct rows.

## drop_nan

```python
drop_nan(*cols: ColumnInputType) -> DataFrame
```

Drops rows that contains NaNs. If cols is None it will drop rows with any NaN value.

If column names are supplied, it will drop only those rows that contains NaNs in one of these columns.

Args:
    *cols (str): column names by which rows containing nans/NULLs should be filtered

Returns:
    DataFrame: DataFrame without NaNs in specified/all columns

## drop_null

```python
drop_null(*cols: ColumnInputType) -> DataFrame
```

Drops rows that contains NaNs or NULLs. If cols is None it will drop rows with any NULL value.

If column names are supplied, it will drop only those rows that contains NULLs in one of these columns.

Args:
    *cols (str): column names by which rows containing nans should be filtered

Returns:
    DataFrame: DataFrame without missing values in specified/all columns

## drop_parquet

```python
drop_parquet(table: Union[str, pathlib.Path], io_config: IOConfig | None=None) -> None
```

Delete a Parquet table path from the filesystem.

Removes either a Parquet dataset directory or a single Parquet file.
This operation cannot be undone.

Args:
    table (Union[str, pathlib.Path]): Parquet file path or dataset directory URI/path.
    io_config (IOConfig, optional): Configurations to use when interacting with remote storage.

Raises:
    FileNotFoundError: If the target path does not exist.
    PermissionError: If insufficient permissions to delete the target.

Note:
    This operation is **blocking** and immediately deletes data from storage.

## except_all

```python
except_all(other: 'DataFrame') -> DataFrame
```

Returns the set difference of two DataFrames, considering duplicates.

Args:
    other (DataFrame): DataFrame to except with

Returns:
    DataFrame: DataFrame with the set difference of the two DataFrames, considering duplicates

## except_distinct

```python
except_distinct(other: 'DataFrame') -> DataFrame
```

Returns the set difference of two DataFrames.

Args:
    other (DataFrame): DataFrame to except with

Returns:
    DataFrame: DataFrame with the set difference of the two DataFrames

## exclude

```python
exclude(*names: str) -> DataFrame
```

Drops columns from the current DataFrame by name.

This is equivalent of performing a select with all the columns but the ones excluded.

Args:
    *names (str): names to exclude

Returns:
    DataFrame: DataFrame with some columns excluded.

## explain

```python
explain(show_all: bool=False, format: str='ascii', simple: bool=False, file: io.IOBase | None=None) -> Any
```

Prints the (logical and physical) plans that will be executed to produce this DataFrame.

Defaults to showing the unoptimized logical plan. Use `show_all=True` to show the unoptimized logical plan,
the optimized logical plan, and the physical plan.

Args:
    show_all (bool): Whether to show the optimized logical plan and the physical plan in addition to the
        unoptimized logical plan.
    format (str): The format to print the plan in. one of 'ascii' or 'mermaid'
    simple (bool): Whether to only show the type of op for each node in the plan, rather than showing details
        of how each op is configured.

    file (Optional[io.IOBase]): Location to print the output to, or defaults to None which defaults to the default location for
        print (in Python, that should be sys.stdout)

Returns:
    Union[None, str, MermaidFormatter]:
        - If `format="mermaid"` and running in a notebook, returns a `MermaidFormatter` instance for rich rendering.
        - If `format="mermaid"` and not in a notebook, returns a string representation of the plan.
        - Otherwise, prints the plan(s) to the specified file or stdout and returns `None`.

## explode

```python
explode(*columns: ColumnInputType, index_column: ColumnInputType | None=None, ignore_empty_and_null: bool=False) -> DataFrame
```

Explodes a List column, where every element in each row's List becomes its own row, and all other columns in the DataFrame are duplicated across rows.

If multiple columns are specified, each row must contain the same number of items in each specified column.

By default, exploding Null values or empty lists will create a single Null entry (see example below).
Set ``ignore_empty_and_null=True`` to drop these rows instead.

Args:
    *columns (ColumnInputType): columns to explode
    index_column (ColumnInputType | None): optional name for an index column that tracks the position of each element within its original list
    ignore_empty_and_null (bool): If True, drops rows where the list is empty or null.
        If False (default), empty lists and null values each produce a single row with a null value.

Returns:
    DataFrame: DataFrame with exploded column

## filter

```python
filter(predicate: Expression | str) -> DataFrame
```

Filters rows via a predicate expression, similar to SQL ``WHERE``.

Alias for [daft.DataFrame.where][daft.DataFrame.where].

Args:
    predicate (Expression): expression that keeps row if evaluates to True.

Returns:
    DataFrame: Filtered DataFrame.

Tip:
    See also [.where(predicate)][daft.DataFrame.where]

## groupby

```python
groupby(*group_by: ManyColumnsInputType) -> GroupedDataFrame
```

Performs a GroupBy on the DataFrame for aggregation.

Args:
    *group_by (Union[str, Expression]): columns to group by

Returns:
    GroupedDataFrame: DataFrame to Aggregate

## intersect

```python
intersect(other: 'DataFrame') -> DataFrame
```

Returns the intersection of two DataFrames.

Args:
    other (DataFrame): DataFrame to intersect with

Returns:
    DataFrame: DataFrame with the intersection of the two DataFrames

## intersect_all

```python
intersect_all(other: 'DataFrame') -> DataFrame
```

Returns the intersection of two DataFrames, including duplicates.

Args:
    other (DataFrame): DataFrame to intersect with

Returns:
    DataFrame: DataFrame with the intersection of the two DataFrames, including duplicates

## into_batches

```python
into_batches(batch_size: int) -> DataFrame
```

Splits or coalesces DataFrame to partitions of size ``batch_size``.

Note:
    Batch sizing is performed on a best-effort basis.
    The heuristic is to emit a batch when we have enough rows to fill `batch_size * 0.8` rows.
    This approach prioritizes processing efficiency over uniform batch sizes, especially when using the Ray Runner, as batches can be distributed over the cluster.
    The exception to this is that the last batch will be the remainder of the total number of rows in the DataFrame.

Args:
    batch_size (int): number of target rows per partition.

Returns:
    DataFrame: Dataframe with `batch_size` rows per partition.

## into_partitions

```python
into_partitions(num: int) -> DataFrame
```

Splits or coalesces DataFrame to ``num`` partitions. Order is preserved.

This will naively greedily split partitions in a round-robin fashion to hit the targeted number of partitions.
The number of rows/size in a given partition is not taken into account during the splitting.

Args:
    num (int): number of target partitions.

Returns:
    DataFrame: Dataframe with `num` partitions.

## iter_partitions

```python
iter_partitions(results_buffer_size: int | None | Literal['num_cpus']='num_cpus') -> Iterator[Union[MicroPartition, 'ray.ObjectRef']]
```

Begin executing this dataframe and return an iterator over the partitions.

Each partition will be returned as a daft.MicroPartition object (if using Python runner backend)
or a ray ObjectRef (if using Ray runner backend).

Args:
    results_buffer_size: how many partitions to allow in the results buffer (defaults to the total number of CPUs
        available on the machine).

Note: A quick note on configuring asynchronous/parallel execution using `results_buffer_size`.
    The `results_buffer_size` kwarg controls how many results Daft will allow to be in the buffer while iterating.
    Once this buffer is filled, Daft will not run any more work until some partition is consumed from the buffer.

    * Increasing this value means the iterator will consume more memory and CPU resources but have higher throughput
    * Decreasing this value means the iterator will consume lower memory and CPU resources, but have lower throughput
    * Setting this value to `None` means the iterator will consume as much resources as it deems appropriate per-iteration

    The default value is the total number of CPUs available on the current machine.

Returns:
    Iterator[Union[MicroPartition, ray.ObjectRef]]: An iterator over the partitions of the DataFrame.
    Each partition is a MicroPartition object (if using Python runner backend) or a ray ObjectRef
    (if using Ray runner backend).

## iter_rows

```python
iter_rows(results_buffer_size: int | None | Literal['num_cpus']='num_cpus', column_format: Literal['python', 'arrow']='python') -> Iterator[dict[str, Any]]
```

Return an iterator of rows for this dataframe.

Each row will be a Python dictionary of the form `{ "key" : value, ...}`. If you are instead looking to iterate over
entire partitions of data, see [`df.iter_partitions()`][daft.DataFrame.iter_partitions].

By default, Daft will convert the columns to Python lists for easy consumption. Datatypes with Python equivalents will be converted accordingly, e.g. timestamps to datetime, tensors to numpy arrays.
For nested data such as List or Struct arrays, however, this can be expensive. You may wish to set `column_format` to "arrow" such that the nested data is returned as Arrow scalars.

Args:
    results_buffer_size: how many partitions to allow in the results buffer (defaults to the total number of CPUs
        available on the machine).
    column_format: the format of the columns to iterate over. One of "python" or "arrow". Defaults to "python".

Note: A quick note on configuring asynchronous/parallel execution using `results_buffer_size`.
    The `results_buffer_size` kwarg controls how many results Daft will allow to be in the buffer while iterating.
    Once this buffer is filled, Daft will not run any more work until some partition is consumed from the buffer.

    * Increasing this value means the iterator will consume more memory and CPU resources but have higher throughput
    * Decreasing this value means the iterator will consume lower memory and CPU resources, but have lower throughput
    * Setting this value to `None` means the iterator will consume as much resources as it deems appropriate per-iteration

    The default value is the total number of CPUs available on the current machine.

Returns:
    Iterator[dict[str, Any]]: An iterator over the rows of the DataFrame, where each row is a dictionary
    mapping column names to values.

## join

```python
join(other: 'DataFrame', on: list[ColumnInputType] | ColumnInputType | None=None, left_on: list[ColumnInputType] | ColumnInputType | None=None, right_on: list[ColumnInputType] | ColumnInputType | None=None, how: Literal['inner', 'left', 'right', 'outer', 'anti', 'semi', 'cross']='inner', strategy: Literal['hash', 'sort_merge', 'broadcast'] | None=None, prefix: str | None=None, suffix: str | None=None) -> DataFrame
```

Column-wise join of the current DataFrame with an ``other`` DataFrame, similar to a SQL ``JOIN``.

If the two DataFrames have duplicate non-join key column names, "right." will be prepended to the conflicting right columns. You can change the behavior by passing either (or both) `prefix` or `suffix` to the function.
If `prefix` is passed, it will be prepended to the conflicting right columns. If `suffix` is passed, it will be appended to the conflicting right columns.

Args:
    other (DataFrame): the right DataFrame to join on.
    on (Optional[Union[List[ColumnInputType], ColumnInputType]]): key or keys to join on [use if the keys on the left and right side match.]. Defaults to None.
    left_on (Optional[Union[List[ColumnInputType], ColumnInputType]], optional): key or keys to join on left DataFrame. Defaults to None.
    right_on (Optional[Union[List[ColumnInputType], ColumnInputType]], optional): key or keys to join on right DataFrame. Defaults to None.
    how (str, optional): what type of join to perform; currently "inner", "left", "right", "outer", "anti", "semi", and "cross" are supported. Defaults to "inner".
    strategy (Optional[str]): The join strategy (algorithm) to use; currently "hash", "sort_merge", "broadcast", and None are supported, where None
        chooses the join strategy automatically during query optimization. The default is None.
    suffix (Optional[str], optional): Suffix to add to the column names in case of a name collision. Defaults to "".
    prefix (Optional[str], optional): Prefix to add to the column names in case of a name collision. Defaults to "right.".

Returns:
    DataFrame: Joined DataFrame.

Raises:
    ValueError: if `on` is passed in and `left_on` or `right_on` is not None.
    ValueError: if `on` is None but both `left_on` and `right_on` are not defined.

Note:
    Although self joins are supported, we currently duplicate the logical plan for the right side
    and recompute the entire tree. Caching for this is on the roadmap.

## join_asof

```python
join_asof(other: 'DataFrame', *, on: ColumnInputType | None=None, left_on: ColumnInputType | None=None, right_on: ColumnInputType | None=None, by: list[ColumnInputType] | ColumnInputType | None=None, left_by: list[ColumnInputType] | ColumnInputType | None=None, right_by: list[ColumnInputType] | ColumnInputType | None=None, strategy: Literal['backward', 'forward', 'nearest']='backward', prefix: str | None=None, suffix: str | None=None, _assume_sorted_and_aligned: bool=False) -> DataFrame
```

Point-in-time (asof) join: each left row matches the nearest right row according to the chosen strategy.

Args:
    other: Right-hand DataFrame (e.g. feature table).
    on: Asof key column when it has the same name on both sides. Exactly one column.
    left_on: Asof key on the left when names differ. Exactly one column; use with ``right_on``.
    right_on: Asof key on the right when names differ. Exactly one column; use with ``left_on``.
    by: Equality key column(s) with the same name on both sides (entity / group columns).
    left_by: Equality keys on the left when names differ; use with ``right_by``.
    right_by: Equality keys on the right when names differ; use with ``left_by``.
    strategy: Match strategy. ``"backward"`` finds the latest right row at or before the left timestamp. ``"forward"`` finds the earliest right row at or after the left timestamp. ``"nearest"`` finds the right row with the minimum absolute difference in on_key; For tie-breaking, prefer the larger/forward value.
    _assume_sorted_and_aligned: Asserts that both inputs have the same number of
        partitions with identical boundaries, and that rows within each partition are
        sorted ascending by the on-key. Also requires
        ``enable_scan_task_split_and_merge=False``. When these conditions hold, Daft
        skips the distributed range-repartition shuffle and zips partitions by index.
        Passing ``True`` when the conditions are not met produces incorrect results.

Returns:
    DataFrame: Left-join-shaped result (every left row kept; unmatched right columns are null).

Raises:
    ValueError: if ``on`` is set and ``left_on`` or ``right_on`` is not None.
    ValueError: if ``on`` is None but ``left_on`` or ``right_on`` is missing.
    ValueError: if both ``by`` and ``left_by`` / ``right_by`` are set.
    ValueError: if only one of ``left_by`` and ``right_by`` is set.
    ValueError: if ``left_by`` and ``right_by`` have different lengths.

## limit

```python
limit(num: int) -> DataFrame
```

Limits the rows in the DataFrame to the first ``N`` rows, similar to a SQL ``LIMIT``.

Args:
    num (int): maximum rows to allow.

Returns:
    DataFrame: Limited DataFrame

## max

```python
max(*cols: ColumnInputType) -> DataFrame
```

Performs a global max on the DataFrame.

Args:
    *cols (Union[str, Expression]): columns to max
Returns:
    DataFrame: Globally aggregated max. Should be a single row.

## mean

```python
mean(*cols: ColumnInputType) -> DataFrame
```

Performs a global mean on the DataFrame.

Args:
    *cols (Union[str, Expression]): columns to mean
Returns:
    DataFrame: Globally aggregated mean. Should be a single row.

## melt

```python
melt(ids: ManyColumnsInputType, values: ManyColumnsInputType=[], variable_name: str='variable', value_name: str='value') -> DataFrame
```

Alias for unpivot.

Args:
    ids (ManyColumnsInputType): Columns to keep as identifiers
    values (Optional[ManyColumnsInputType]): Columns to unpivot. If not specified, all columns except ids will be unpivoted.
    variable_name (Optional[str]): Name of the variable column. Defaults to "variable".
    value_name (Optional[str]): Name of the value column. Defaults to "value".

Returns:
    DataFrame: Unpivoted DataFrame

## merge_deltalake

```python
merge_deltalake(table: Union[str, 'UnityCatalogTable', 'deltalake.DeltaTable'], predicate: str, io_config: IOConfig | None=None, source_alias: str='source', target_alias: str='target', custom_metadata: dict[str, str] | None=None, safe_cast: bool=True, merge_schema: bool=False, writer_properties: 'deltalake.WriterProperties | None'=None, streamed_exec: bool=True, max_spill_size: int | None=None, max_temp_directory_size: int | None=None, post_commithook_properties: 'deltalake.PostCommitHookProperties | None'=None, compression: str | None=None) -> DeltaMergeBuilder
```

Create a Delta Lake MERGE operation builder using this DataFrame.

Returns a merge builder that mirrors the underlying ``deltalake`` merge API.
Call ``.execute()`` on the builder to perform the merge and return a DataFrame with operation metrics in metadata.

Args:
    table: Destination Delta table URI, ``deltalake.DeltaTable``, or ``UnityCatalogTable``.
    predicate: SQL merge predicate between ``target_alias`` and ``source_alias``.
    io_config: Optional :class:`~daft.daft.IOConfig` used for object storage access.
    source_alias: SQL alias for this DataFrame in merge predicate expressions.
    target_alias: SQL alias for the destination table in merge predicate expressions.
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
    The DataFrame is materialized to a PyArrow table when building the merge operation.
    The returned DataFrame from ``.execute()`` contains merge metrics as columns and stores the raw metrics dict in ``_metadata["merge_metrics"]``.

## metrics

```python
metrics() -> RecordBatch | None
```

_(no docstring)_

## min

```python
min(*cols: ColumnInputType) -> DataFrame
```

Performs a global min on the DataFrame.

Args:
    *cols (Union[str, Expression]): columns to min
Returns:
    DataFrame: Globally aggregated min. Should be a single row.

## num_partitions

```python
num_partitions() -> int | None
```

Returns the number of partitions that will be used to execute this DataFrame.

The query optimizer may change the partitioning strategy. This method runs the optimizer
and then inspects the resulting physical plan scheduler to determine how many partitions
the execution will use.

Returns:
    int: The number of partitions in the optimized physical execution plan.

## offset

```python
offset(num: int) -> DataFrame
```

Returns a new DataFrame by skipping the first ``N`` rows, similar to a SQL ``Offset``.

Args:
    num (int): the number of rows to skip

Returns:
    DataFrame: A new DataFrame by skipping the first ``N`` rows

## pipe

```python
pipe(function: Callable[Concatenate['DataFrame', P], T], *args: P.args, **kwargs: P.kwargs) -> T
```

Apply the function to this DataFrame.

Args:
    function (Callable[Concatenate["DataFrame", P], T]): Function to apply.
    *args (P.args): Positional arguments to pass to the function.
    **kwargs (P.kwargs): Keyword arguments to pass to the function.

Returns:
    Result of applying the function on this DataFrame.

## pivot

```python
pivot(group_by: ManyColumnsInputType, pivot_col: ColumnInputType, value_col: ColumnInputType, agg_fn: str, names: list[str] | None=None) -> DataFrame
```

Pivots a column of the DataFrame and performs an aggregation on the values.

Args:
    group_by (ManyColumnsInputType): columns to group by
    pivot_col (Union[str, Expression]): column to pivot
    value_col (Union[str, Expression]): column to aggregate
    agg_fn (str): aggregation function to apply
    names (Optional[List[str]]): names of the pivoted columns

Returns:
    DataFrame: DataFrame with pivoted columns

Note:
    You may wish to provide a list of distinct values to pivot on, which is more efficient as it avoids
    a distinct operation. Without this list, Daft will perform a distinct operation on the pivot column to
    determine the unique values to pivot on.

## product

```python
product(*cols: ColumnInputType) -> DataFrame
```

Performs a global product on the DataFrame.

Args:
    *cols (Union[str, Expression]): columns to product

Returns:
    DataFrame: Globally aggregated products. Should be a single row.

## repartition

```python
repartition(num: int | None, *partition_by: ColumnInputType) -> DataFrame
```

Repartitions DataFrame to ``num`` partitions.

If columns are passed in, then DataFrame will be repartitioned by those, otherwise
random repartitioning will occur.

Args:
    num (Optional[int]): Number of target partitions; if None, the number of partitions will not be changed.
    *partition_by (Union[str, Expression]): Optional columns to partition by.

Returns:
    DataFrame: Repartitioned DataFrame.

Note: This function will globally shuffle your data, which is potentially a very expensive operation.
    If instead you merely wish to "split" or "coalesce" partitions to obtain a target number of partitions,
    you mean instead wish to consider using [DataFrame.into_partitions][daft.DataFrame.into_partitions] which
    avoids shuffling of data in favor of splitting/coalescing adjacent partitions where appropriate.

## resolve_deltalake

```python
resolve_deltalake() -> tuple[str, 'deltalake.DeltaTable']
```

Resolve the Delta Lake table path and table object for this DataFrame.

This is an internal utility intended for DataFrames returned by
:meth:`write_deltalake`.

Returns:
    tuple[str, deltalake.DeltaTable]: ``(table_path, delta_table)``.

Raises:
    ValueError: If the DataFrame cannot be mapped to a Delta Lake table.

## resolve_parquet

```python
resolve_parquet() -> str
```

Resolve the parquet table path associated with this DataFrame.

This is an internal utility intended for DataFrames returned by
:meth:`write_parquet`.

Returns:
    str: Resolved parquet root path.

Raises:
    ValueError: If the DataFrame cannot be mapped to a parquet table path.

## sample

```python
sample(fraction: float | None=None, size: int | None=None, with_replacement: bool=False, seed: int | None=None) -> DataFrame
```

Samples rows from the DataFrame.

Args:
    fraction (Optional[float]): fraction of rows to sample (between 0.0 and 1.0).
        Must specify either `fraction` or `size`, but not both.
        For backward compatibility, can also be passed as a positional argument.
    size (Optional[int]): exact number of rows to sample.
        Must specify either `fraction` or `size`, but not both.
        If `size` exceeds the total number of rows:
        - When `with_replacement=False`: raises ValueError
        - When `with_replacement=True`: returns `size` rows (may contain duplicates)
        Note: Sample by size only works on the native runner right now.
    with_replacement (bool, optional): whether to sample with replacement. Defaults to False.
    seed (Optional[int], optional): random seed. Defaults to None.

Returns:
    DataFrame: DataFrame with sampled rows.

## schema

```python
schema() -> Schema
```

Returns the Schema of the DataFrame, which provides information about each column, as a Python object.

Returns:
    Schema: schema of the DataFrame

## select

```python
select(*columns: ColumnInputType, **projections: Expression) -> DataFrame
```

Creates a new DataFrame from the provided expressions, similar to a SQL ``SELECT``.

Args:
    *columns (Union[str, Expression]): columns to select from the current DataFrame
    **projections (Expression): additional projections in kwarg format.

Returns:
    DataFrame: new DataFrame that will select the passed in columns

## show

```python
show(n: int=8, format: PreviewFormat | None=None, verbose: bool | None=None, max_width: int | None=None, align: PreviewAlign | None=None, columns: list[PreviewColumn] | None=None) -> None
```

Executes enough of the DataFrame in order to display the first ``n`` rows.

If IPython is installed, this will use IPython's `display` utility to pretty-print in a
notebook/REPL environment. Otherwise, this will fall back onto a naive Python `print`.

If no format is given, then daft's truncating preview format is used.
    - The output is a 'fancy' table with rounded corners.
    - Headers contain the column's data type.
    - Columns are truncated to 30 characters.
    - The table's overall width is limited to 10 columns.
Default values can be overridden with environment variables:
    - ``DAFT_SHOW_FORMAT``
    - ``DAFT_SHOW_VERBOSE``
    - ``DAFT_SHOW_MAX_WIDTH``
    - ``DAFT_SHOW_ALIGN``

Args:
    n: number of rows to show. Defaults to 8.
    format (PreviewFormat): the box-drawing format e.g. "fancy" or "markdown".
    verbose (bool): if True, headers include the column's data type.
    max_width (int | None): global max column width
    align (PreviewAlign): global column align
    columns (list[PreviewColumn]): column overrides

Note:
    This call is **blocking** and will execute the DataFrame when called

## shuffle

```python
shuffle(seed: int | None=None) -> DataFrame
```

Randomly reorders rows of the DataFrame.

This is analogous to ``shuffle`` operation in the Hugging Face ``datasets`` library.

Note:
    This performs a global sort and is expensive. For randomly redistributing rows across
    partitions see :meth:`DataFrame.repartition` with no ``partition_by`` (random partition shuffle).

Args:
    seed: Optional RNG seed passed to ``random_int`` for best-effort reproducibility
        on a fixed partition layout; not guaranteed across runners or plan changes.

Returns:
    DataFrame: A new DataFrame with rows in random order.

## skew

```python
skew(*cols: ColumnInputType) -> DataFrame
```

Performs a global skew on the DataFrame.

Args:
    *cols (Union[str, Expression]): columns to compute skewness for

Returns:
    DataFrame: Globally aggregated skewness. Should be a single row.

Note:
    Daft uses the **biased (population) skewness** formula, which is equivalent to
    ``scipy.stats.skew(bias=True)``. This differs from pandas' default ``DataFrame.skew()``,
    which uses the adjusted Fisher-Pearson (sample) formula. For small samples, the two
    formulas can produce meaningfully different results.

## skip_existing

```python
skip_existing(existing_path: str | pathlib.Path | list[str | pathlib.Path], key_column: str | list[str], file_format: str | FileFormat, io_config: IOConfig | None=None, num_workers: int=4, cpus_per_worker: float=0.5, keys_load_batch_size: int=100000, max_concurrency_per_worker: int=1, filter_batch_size: int=10000, **reader_args: Any) -> DataFrame
```

Filter out rows whose key(s) already exist in existing data (i.e., already processed rows).

This method reads existing data from the given path(s), builds a Ray actor-backed
distributed key filter from the existing key columns, and filters the current
DataFrame to only include rows whose key(s) are not present in the existing data.
This is useful for incremental data processing pipelines where you want to avoid
re-processing data that has already been written.

Missing paths are treated permissively:
if none of the provided paths exist, the current DataFrame is returned unchanged;
if only some paths exist, Daft logs a warning and continues with the existing subset.

Args:
    existing_path: Path or list of paths to the existing data directory/file(s).
    key_column: Column name(s) to use as the key for matching. Can be a single column name
        or a list of column names for composite keys.
    file_format: Format of the existing data files. Supported formats are Parquet, CSV,
        and JSON/JSONL/NDJSON.
    io_config: IO configuration for reading the existing data.
    num_workers: Number of Ray actors to spawn for key filtering. Each actor holds a
        shard of existing keys and filters incoming partitions in parallel. Higher values
        increase parallelism and typically reduce per-actor memory usage.
    cpus_per_worker: Number of CPUs to allocate per Ray actor.
    keys_load_batch_size: Batch size when loading keys from existing data into actors.
    max_concurrency_per_worker: Maximum concurrency for per-actor operations.
    filter_batch_size: Batch size for the key filter operation. Controls how many rows
        are sent to the key filter actors per RPC call. Larger values reduce RPC
        overhead but increase memory usage proportionally across all concurrent tasks
        (total memory ≈ num_tasks × filter_batch_size × avg_key_size). For lightweight
        keys (int, short string), 10000-50000 works well. For large keys (URLs, long
        strings), keep this lower to avoid excessive memory usage. Defaults to 10000.
    **reader_args: Additional keyword arguments forwarded to the underlying reader for
        `file_format` when scanning `existing_path`.

Returns:
    DataFrame: A new DataFrame with rows filtered to exclude those whose keys exist
        in the existing data.

Raises:
    ValueError: If key columns are invalid, paths are empty, or parameters are out of range.
    RuntimeError: If the existing data cannot be read during execution or key filter
        resources cannot be allocated.

## skipped_corrupt_files

```python
skipped_corrupt_files() -> list[tuple[str, str, bool]]
```

Files skipped during the last execution due to ignore_corrupt_files=True.

Returns a list of ``(path, reason, partial)`` tuples. ``partial`` is ``True``
when some batches were already emitted before corruption was detected (the file
was not fully skipped). Only available after ``.collect()``.

Example::

    df = daft.read_parquet("s3://bucket/data/", ignore_corrupt_files=True)
    df.collect()
    for path, reason, partial in df.skipped_corrupt_files:
        tag = " (partial)" if partial else ""
        print(f"Skipped{tag} {path}: {reason}")

## sort

```python
sort(by: ColumnInputType | list[ColumnInputType], desc: bool | list[bool]=False, nulls_first: bool | list[bool] | None=None) -> DataFrame
```

Sorts DataFrame globally.

Args:
    by (Union[ColumnInputType, List[ColumnInputType]]): column to sort by. Can be `str` or expression as well as a list of either.
    desc (Union[bool, List[bool]), optional): Sort by descending order. Defaults to False.
    nulls_first (Union[bool, List[bool]), optional): Sort by nulls first. Defaults to nulls being treated as the greatest value.

Returns:
    DataFrame: Sorted DataFrame.

Note:
    * Since this a global sort, this requires an expensive repartition which can be quite slow.
    * Supports multicolumn sorts and can have unique `descending` and `nulls_first` flags per column.

## stddev

```python
stddev(*cols: ColumnInputType, ddof: int=1) -> DataFrame
```

Performs a global standard deviation on the DataFrame.

Args:
    *cols (Union[str, Expression]): columns to stddev
    ddof (int): Delta degrees of freedom used in the denominator `N - ddof`.
        Defaults to 1 (sample standard deviation).

Returns:
    DataFrame: Globally aggregated standard deviation. Should be a single row.

## sum

```python
sum(*cols: ManyColumnsInputType) -> DataFrame
```

Performs a global sum on the DataFrame.

Args:
    *cols (Union[str, Expression]): columns to sum
Returns:
    DataFrame: Globally aggregated sums. Should be a single row.

## summarize

```python
summarize() -> DataFrame
```

Returns column statistics for the DataFrame.

Returns:
    DataFrame: new DataFrame with the computed column statistics.

## to_arrow

```python
to_arrow() -> pyarrow.Table
```

Converts the current DataFrame to a [pyarrow Table](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html).

If results have not computed yet, collect will be called.

Returns:
    pyarrow.Table: [pyarrow Table](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html) converted from a Daft DataFrame

Note:
    This call is **blocking** and will execute the DataFrame when called

## to_arrow_iter

```python
to_arrow_iter(results_buffer_size: int | None | Literal['num_cpus']='num_cpus') -> Iterator['pyarrow.RecordBatch']
```

Return an iterator of pyarrow recordbatches for this dataframe.

Args:
    results_buffer_size: how many partitions to allow in the results buffer (defaults to the total number of CPUs
        available on the machine).
Note: A quick note on configuring asynchronous/parallel execution using `results_buffer_size`.
    The `results_buffer_size` kwarg controls how many results Daft will allow to be in the buffer while iterating.
    Once this buffer is filled, Daft will not run any more work until some partition is consumed from the buffer.
    * Increasing this value means the iterator will consume more memory and CPU resources but have higher throughput
    * Decreasing this value means the iterator will consume lower memory and CPU resources, but have lower throughput
    * Setting this value to `None` means the iterator will consume as much resources as it deems appropriate per-iteration
    The default value is the total number of CPUs available on the current machine.

Returns:
    Iterator[pyarrow.RecordBatch]: An iterator over the RecordBatches of the DataFrame.

## to_dask_dataframe

```python
to_dask_dataframe(meta: Union['pandas.DataFrame', 'pandas.Series[Any]', dict[str, Any], Iterable[Any], tuple[Any], None]=None) -> dask.DataFrame
```

Converts the current Daft DataFrame to a Dask DataFrame.

The returned Dask DataFrame will use [Dask-on-Ray](https://docs.ray.io/en/latest/ray-more-libs/dask-on-ray.html)
to execute operations on a Ray cluster.

Args:
    meta: An empty [pandas DataFrame](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html)or [Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html) that matches the dtypes and column
        names of the stream. This metadata is necessary for many algorithms in
        dask dataframe to work. For ease of use, some alternative inputs are
        also available. Instead of a DataFrame, a dict of ``{name: dtype}`` or
        iterable of ``(name, dtype)`` can be provided (note that the order of
        the names should match the order of the columns). Instead of a series, a
        tuple of ``(name, dtype)`` can be used.
        By default, this will be inferred from the underlying Daft DataFrame schema,
        with this argument supplying an optional override.

Returns:
    dask.DataFrame: A Dask DataFrame stored on a Ray cluster.

Note:
    This function can only work if Daft is running using the RayRunner.

## to_pandas

```python
to_pandas(coerce_temporal_nanoseconds: bool=False) -> pandas.DataFrame
```

Converts the current DataFrame to a [pandas DataFrame](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html).

If results have not computed yet, collect will be called.

Args:
    coerce_temporal_nanoseconds (bool): Whether to coerce temporal columns to nanoseconds. Only applicable to pandas version >= 2.0 and pyarrow version >= 13.0.0. Defaults to False. See `pyarrow.Table.to_pandas <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table.to_pandas>`__ for more information.

Returns:
    pandas.DataFrame: [pandas DataFrame](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html) converted from a Daft DataFrame

Note:
    This call is **blocking** and will execute the DataFrame when called

## to_pydict

```python
to_pydict(maps_as_pydicts: Literal['lossy', 'strict'] | None=None) -> dict[str, list[Any]]
```

Converts the current DataFrame to a python dictionary. The dictionary contains Python lists of Python objects for each column.

If results have not computed yet, collect will be called.

Args:
    maps_as_pydicts: If None (default), Map values are converted to association lists
        (`list[tuple[key, value]]`) preserving order and duplicates.
        If `"lossy"` or `"strict"`, Map values are converted to Python dicts.
        `"lossy"` keeps the last value for duplicate keys and warns.
        `"strict"` raises on duplicate keys.

Returns:
    dict[str, list[Any]]: python dict converted from a Daft DataFrame

Note:
    This call is **blocking** and will execute the DataFrame when called

## to_pylist

```python
to_pylist(maps_as_pydicts: Literal['lossy', 'strict'] | None=None) -> list[Any]
```

Converts the current Dataframe into a python list.

Args:
    maps_as_pydicts: If None (default), Map values are converted to association lists
        (`list[tuple[key, value]]`) preserving order and duplicates.
        If `"lossy"` or `"strict"`, Map values are converted to Python dicts.
        `"lossy"` keeps the last value for duplicate keys and warns.
        `"strict"` raises on duplicate keys.

Returns:
    List[dict[str, Any]]: List of python dict objects.

Warning:
    This is a convenience method over [DataFrame.iter_rows()][daft.DataFrame.iter_rows]. Users should prefer using `.iter_rows()` directly instead for lower memory utilization if they are streaming rows out of a DataFrame and don't require full materialization of the Python list.

## to_ray_dataset

```python
to_ray_dataset() -> ray.data.dataset.DataSet
```

Converts the current DataFrame to a [Ray Dataset](https://docs.ray.io/en/latest/data/api/dataset.html#ray.data.Dataset) which is useful for running distributed ML model training in Ray.

Returns:
    ray.data.dataset.DataSet: [Ray dataset](https://docs.ray.io/en/latest/data/api/dataset.html#ray.data.Dataset)

## to_torch_dataloader

```python
to_torch_dataloader(batch_size: int=1, *, pin_memory: bool=False, pin_memory_device: str='', prefetch_count: int=0) -> DaftTorchDataLoader
```

Return a DataLoader-like iterator that streams batched partitions for PyTorch training.

Begins execution of the DataFrame when iterated. Each yielded batch is a dict mapping column
names to `torch.Tensor` values (or Python lists for non-numeric columns).

For row-level shuffling, use [``shuffle``][daft.DataFrame.shuffle] or
[``sample``][daft.DataFrame.sample] on the DataFrame before calling this method.

Note:
    Batch sizing is best-effort. Batches may be smaller than `batch_size`.

Args:
    batch_size: Target number of rows per batch.
    pin_memory: If `True`, pin memory on returned tensors for faster GPU transfer.
    pin_memory_device: Optional device for pinned memory (PyTorch 2.x).
    prefetch_count: Number of batches loaded in advance. This will increase memory usage, but can
    improve throughput.

Returns:
    DaftTorchDataLoader: Iterable over batch dicts for use as
    `for batch in df.to_torch_dataloader(batch_size): ...`

## to_torch_iter_dataset

```python
to_torch_iter_dataset(shard_strategy: Literal['file'] | None=None, world_size: int | None=None, rank: int | None=None) -> torch.utils.data.IterableDataset
```

Convert the current DataFrame into a `Torch IterableDataset <https://pytorch.org/docs/stable/data.html#torch.utils.data.IterableDataset>`__ for use with PyTorch.

Begins execution of the DataFrame if it is not yet executed.

Items will be returned in pydict format: a dict of `{"column name": value}` for each row in the data.

Args:
    shard_strategy (Optional[Literal["file"]]): Strategy to use for sharding the dataset. Currently only "file" is supported.
    world_size (Optional[int]): Total number of workers for sharding. Required if shard_strategy is specified.
    rank (Optional[int]): Rank of current worker for sharding. Required if shard_strategy is specified.

Returns:
    torch.utils.data.IterableDataset: A PyTorch IterableDataset containing the data from the DataFrame.

## to_torch_map_dataset

```python
to_torch_map_dataset(shard_strategy: Literal['file'] | None=None, world_size: int | None=None, rank: int | None=None) -> torch.utils.data.Dataset
```

Convert the current DataFrame into a map-style [Torch Dataset](https://pytorch.org/docs/stable/data.html#map-style-datasets) for use with PyTorch.

This method will materialize the entire DataFrame and block on completion.

Items will be returned in pydict format: a dict of `{"column name": value}` for each row in the data.

Note:
    If you do not need random access, you may get better performance out of an IterableDataset,
    which streams data items in as soon as they are ready and does not block on full materialization.

Tip:
    This method returns results locally.
    For distributed training, you may want to use [DataFrame.to_ray_dataset()][daft.DataFrame.to_ray_dataset].

Args:
    shard_strategy (Optional[Literal["file"]]): Strategy to use for sharding the dataset. Currently only "file" is supported.
    world_size (Optional[int]): Total number of workers for sharding. Required if shard_strategy is specified.
    rank (Optional[int]): Rank of current worker for sharding. Required if shard_strategy is specified.

Returns:
    torch.utils.data.Dataset: A PyTorch Dataset containing the data from the DataFrame.

Note:
    The produced dataset is meant to be used with the single-process DataLoader,
    and does not support data sharding hooks for multi-process data loading.

## transform

```python
transform(func: Callable[..., 'DataFrame'], *args: Any, **kwargs: Any) -> DataFrame
```

Apply a function that takes and returns a DataFrame.

Allow splitting your transformation into different units of work (functions) while preserving the syntax for chaining transformations.

## union

```python
union(other: 'DataFrame') -> DataFrame
```

Returns the distinct union of two DataFrames.

Args:
    other (DataFrame): The DataFrame to union with this one.

Returns:
    DataFrame: A new DataFrame containing the distinct rows from both DataFrames.

## union_all

```python
union_all(other: 'DataFrame') -> DataFrame
```

Returns the union of two DataFrames, including duplicates.

Args:
    other (DataFrame): The DataFrame to union with this one.

Returns:
    DataFrame: A new DataFrame containing all rows from both DataFrames, including duplicates.

## union_all_by_name

```python
union_all_by_name(other: 'DataFrame') -> DataFrame
```

Returns the union of two DataFrames, including duplicates, with columns matched by name.

Args:
    other (DataFrame): The DataFrame to union with this one, matching columns by name.

Returns:
    DataFrame: A new DataFrame containing all rows from both DataFrames, including duplicates, with columns matched by name.

## union_by_name

```python
union_by_name(other: 'DataFrame') -> DataFrame
```

Returns the distinct union by name.

Args:
    other (DataFrame): The DataFrame to union with this one, matching columns by name.

Returns:
    DataFrame: A new DataFrame containing the distinct rows from both DataFrames, with columns matched by name.

## unique

```python
unique(*by: ColumnInputType) -> DataFrame
```

Computes distinct rows, dropping duplicates.

Alias for [DataFrame.distinct][daft.DataFrame.distinct].

Args:
    *by (Union[str, Expression]): columns to perform distinct on. Defaults to all columns.

Returns:
    DataFrame: DataFrame that has only distinct rows.

## unpivot

```python
unpivot(ids: ManyColumnsInputType, values: ManyColumnsInputType=[], variable_name: str='variable', value_name: str='value') -> DataFrame
```

Unpivots a DataFrame from wide to long format.

Args:
    ids (ManyColumnsInputType): Columns to keep as identifiers
    values (Optional[ManyColumnsInputType]): Columns to unpivot. If not specified, all columns except ids will be unpivoted.
    variable_name (Optional[str]): Name of the variable column. Defaults to "variable".
    value_name (Optional[str]): Name of the value column. Defaults to "value".

Returns:
    DataFrame: Unpivoted DataFrame

Tip:
    See also [melt][daft.DataFrame.melt]

## var

```python
var(*cols: ColumnInputType, ddof: int=1) -> DataFrame
```

Performs a global variance on the DataFrame.

Args:
    *cols (Union[str, Expression]): columns to compute variance for
    ddof (int): Delta degrees of freedom used in the denominator `N - ddof`.
        Defaults to 1 (sample variance).

Returns:
    DataFrame: Globally aggregated variance. Should be a single row.

## where

```python
where(predicate: Expression | str) -> DataFrame
```

Filters rows via a predicate expression, similar to SQL ``WHERE``.

Args:
    predicate (Expression): expression that keeps row if evaluates to True.

Returns:
    DataFrame: Filtered DataFrame.

## with_column

```python
with_column(column_name: str, expr: Expression) -> DataFrame
```

Adds a column to the current DataFrame with an Expression, equivalent to a ``select`` with all current columns and the new one.

Args:
    column_name (str): name of new column
    expr (Expression): expression of the new column.

Returns:
    DataFrame: DataFrame with new column.

## with_column_renamed

```python
with_column_renamed(existing: str, new: str) -> DataFrame
```

Renames a column in the current DataFrame.

If the column in the DataFrame schema does not exist, this will be a no-op.

Args:
    existing (str): name of the existing column to rename
    new (str): new name for the column

Returns:
    DataFrame: DataFrame with the column renamed.

## with_columns

```python
with_columns(columns: dict[str, Expression]) -> DataFrame
```

Adds columns to the current DataFrame with Expressions, equivalent to a ``select`` with all current columns and the new ones.

Args:
    columns (Dict[str, Expression]): Dictionary of new columns in the format { name: expression }

Returns:
    DataFrame: DataFrame with new columns.

## with_columns_renamed

```python
with_columns_renamed(cols_map: dict[str, str]) -> DataFrame
```

Renames multiple columns in the current DataFrame.

If the columns in the DataFrame schema do not exist, this will be a no-op.

Args:
    cols_map (Dict[str, str]): Dictionary of columns to rename in the format { existing: new }

Returns:
    DataFrame: DataFrame with the columns renamed.

## with_spatial_bbox

```python
with_spatial_bbox(geom_col: str) -> DataFrame
```

Add ``rtree_min_x``, ``rtree_min_y``, ``rtree_max_x``, ``rtree_max_y`` Float64 columns
holding the bounding box of ``geom_col``.

These are the column names the native spatial-join operator detects as a precomputed
R-tree index, letting it skip per-row WKB bounding-box extraction during the join. When the
DataFrame is used as a side of a spatial join (e.g. ``df.join(other, on=st_intersects(...))``),
the engine preserves these columns through to the join's build side automatically — so you
do not need to keep them in your final projection for the index to take effect. They are
dropped from the join output unless you select them explicitly.

The ``rtree_*``-named columns also persist through Parquet/Delta writes, so a table written
with them carries its spatial index for fast joins on read-back.

Args:
    geom_col: name of a Geometry (or WKB Binary) column.

Returns:
    DataFrame: DataFrame with four new Float64 columns (``rtree_min_x`` … ``rtree_max_y``).

## write_bigtable

```python
write_bigtable(project_id: str, instance_id: str, table_id: str, row_key_column: str, column_family_mappings: dict[str, str], client_kwargs: dict[str, Any] | None=None, write_kwargs: dict[str, Any] | None=None, serialize_incompatible_types: bool=True) -> DataFrame
```

Write a DataFrame into a Google Cloud Bigtable table.

Bigtable only accepts datatypes that can be converted to bytes in cells (for more details, please consult the Bigtable documentation: https://cloud.google.com/bigtable/docs/overview#data-types).
By default, `write_bigtable` automatically serializes incompatible types to JSON. This can be disabled by setting `auto_convert=False`.

This data sink transforms each row of the dataframe into Bigtable rows.
A row key is always required. The `row_key_column` parameter can be used to specify the column name to use for the row key.

Every column must also belong to a column family. The `column_family_mappings` parameter can be used to specify the column family to use for each column.
For example, if you have a column "name" and a column "age", you can specify a "user_data" column family by passing a dictionary like {"name": "user_data", "age": "user_data"}.

EXPERIMENTAL: This features is early in development and will change.

Args:
    project_id: The Google Cloud project ID.
    instance_id: The Bigtable instance ID.
    table_id: The table to write to.
    row_key_column: Column name for the row key.
    column_family_mappings: Mapping of column names to column families.
    client_kwargs: Optional dictionary of arguments to pass to the Bigtable Client constructor.
    write_kwargs: Optional dictionary of arguments to pass to the Bigtable MutationsBatcher.
    serialize_incompatible_types: Whether to automatically convert non-bytes/int values to Bigtable-compatible formats.
                                  If False, will raise an error for unsupported types. Defaults to True.

## write_clickhouse

```python
write_clickhouse(table: str, *, host: str, port: int | None=None, user: str | None=None, password: str | None=None, database: str | None=None, client_kwargs: dict[str, Any] | None=None, write_kwargs: dict[str, Any] | None=None) -> DataFrame
```

Writes the DataFrame to a ClickHouse table.

Args:
    table: Name of the ClickHouse table to write to.
    host: ClickHouse host.
    port: ClickHouse port.
    user: ClickHouse user.
    password: ClickHouse password.
    database: ClickHouse database.
    client_kwargs: Optional dictionary of arguments to pass to the ClickHouse client constructor.
    write_kwargs: Optional dictionary of arguments to pass to the ClickHouse write() method.

## write_csv

```python
write_csv(root_dir: str | pathlib.Path, write_mode: Literal['append', 'overwrite', 'overwrite-partitions']='append', partition_cols: list[ColumnInputType] | None=None, io_config: IOConfig | None=None, delimiter: str | None=None, quote: str | None=None, escape: str | None=None, header: bool | None=True, date_format: str | None=None, timestamp_format: str | None=None) -> DataFrame
```

Writes the DataFrame as CSV files, returning a new DataFrame with paths to the files that were written.

Files will be written to `<root_dir>/*` with randomly generated UUIDs as the file names.

Args:
    root_dir (str): root file path to write CSV files to.
    write_mode (str, optional): Operation mode of the write. `append` will add new data, `overwrite` will replace the contents of the root directory with new data. `overwrite-partitions` will replace only the contents in the partitions that are being written to. Defaults to "append".
    partition_cols (Optional[List[ColumnInputType]], optional): How to subpartition each partition further. Defaults to None.
    io_config (Optional[IOConfig], optional): configurations to use when interacting with remote storage.
    delimiter (Optional[str], optional): Single-character field delimiter (default `,`).
    quote (Optional[str], optional): Single-character quote used around fields containing delimiters default `"`.
    escape (Optional[str], optional): Single-character escape for special characters default `\\`.
    header (Optional[bool], optional): Whether to write a header row with column names, default True.
    date_format (Optional[str], optional): Format string for date columns. Uses chrono strftime format (e.g., "%Y-%m-%d", "%d/%m/%Y"). Defaults to None (ISO 8601 format).
    timestamp_format (Optional[str], optional): Format string for timestamp columns. Uses chrono strftime format (e.g., "%Y-%m-%d %H:%M:%S", "%+"). Defaults to None (ISO 8601 format).

Returns:
    DataFrame: The filenames that were written out as strings.

Note:
    This call is **blocking** and will execute the DataFrame when called

    **Timezone handling**: For timezone-aware timestamp columns, the timestamps are converted
    to the target timezone before formatting. For example, a timestamp stored as UTC but with
    timezone "America/New_York" will be formatted in Eastern Time, not UTC. If the timezone
    string is invalid, an error will be raised.

## write_deltalake

```python
write_deltalake(table: Union[str, pathlib.Path, 'deltalake.DeltaTable', 'UnityCatalogTable'], partition_cols: list[str] | None=None, mode: Literal['append', 'overwrite', 'error', 'ignore']='append', schema_mode: Literal['merge', 'overwrite'] | None=None, name: str | None=None, description: str | None=None, configuration: Mapping[str, str | None] | None=None, custom_metadata: dict[str, str] | None=None, dynamo_table_name: str | None=None, allow_unsafe_rename: bool=False, io_config: IOConfig | None=None, checkpoint: 'IdempotentCommit | None'=None, compression: str='snappy') -> DataFrame
```

Writes the DataFrame to a [Delta Lake](https://docs.delta.io/latest/index.html) table, returning a new DataFrame with the operations that occurred.

``write_deltalake`` supports checkpointing via the ``checkpoint=`` parameter. For the conceptual overview, see the [Checkpointing guide](../use-case/checkpointing.md).

Args:
    table (Union[str, pathlib.Path, deltalake.DeltaTable, UnityCatalogTable]): Destination [Delta Lake Table](https://delta-io.github.io/delta-rs/api/delta_table/) or table URI to write dataframe to.
    partition_cols (List[str], optional): How to subpartition each partition further. If table exists, expected to match table's existing partitioning scheme, otherwise creates the table with specified partition columns. Defaults to None.
    mode (str, optional): Operation mode of the write. `append` will add new data, `overwrite` will replace table with new data, `error` will raise an error if table already exists, and `ignore` will not write anything if table already exists. Defaults to `append`.
    schema_mode (str, optional): Schema mode of the write. If set to `overwrite`, allows replacing the schema of the table when doing `mode=overwrite`. If set to `merge`, merges the incoming schema with the existing table schema, adding new columns and allowing type evolution of existing columns. Only applicable when the table already exists.
    name (str, optional): User-provided identifier for this table.
    description (str, optional): User-provided description for this table.
    configuration (Mapping[str, Optional[str]], optional): A map containing configuration options for the metadata action.
    custom_metadata (Dict[str, str], optional): Custom metadata to add to the commit info. Keys with prefix ``daft.idempotence-`` are reserved.
    dynamo_table_name (str, optional): Name of the DynamoDB table to use when explicitly opting into
        DynamoDB locking for S3 writes. Modern supported ``deltalake`` versions use S3 conditional
        writes by default.
    allow_unsafe_rename (bool, optional): Whether to explicitly allow unsafe rename when writing to S3
        or local disk. Defaults to False.
    io_config (IOConfig, optional): configurations to use when interacting with remote storage.
    checkpoint (IdempotentCommit, optional): Bundled checkpoint store + idempotence key for an idempotent commit. When provided, the Delta commit's ``custom_metadata`` is tagged with ``daft.idempotence-key`` and retries with the same key recognize the prior attempt without producing a duplicate commit. Only ``mode='append'`` is supported. Requires the Ray runner.
    compression (str, optional): compression codec applied to every column of the written Delta data files. Defaults to "snappy". Accepts "snappy", "gzip", "zstd", "lz4", "brotli", "uncompressed", or "none" (case-insensitive). "lz4_raw" and "lzo" are not supported, because PyArrow's parquet writer cannot encode them.

Returns:
    DataFrame: The operations that occurred with this write.

Note:
    This call is **blocking** and will execute the DataFrame when called.

    Delta Lake has no unsigned integer types, so an unsigned column is
    widened to the next signed type that holds every value: ``uint8`` →
    ``short``, ``uint16`` → ``integer``, ``uint32`` → ``long``,
    ``uint64`` → ``long``. A ``uint32`` column therefore reads back as
    ``int64``. ``uint64`` values above ``2**63 - 1`` have no signed
    target and raise a ``ValueError`` rather than committing a table
    that cannot be read.

    When ``checkpoint`` is provided and ``write_deltalake`` raises
    *after* the Delta commit landed (e.g. a transient failure during
    the post-commit ``mark_committed`` bookkeeping), the user data is
    already durable in Delta. The next call with the same
    ``IdempotentCommit`` (same idempotence key) will detect the
    commit via its marker, finish the bookkeeping, and exit cleanly
    without producing a duplicate commit.

    The returned DataFrame reflects only this call's writes — empty
    (0 rows) on a recovery short-circuit, populated when a new
    commit lands. Useful for run-to-run diffing.

    Idempotence-key contract — read carefully:

    - **Same key + different inputs → silent no-op (data loss).** The
      destination already has a commit tagged with the key, so
      nothing new is written.
    - **Different key + same retry → duplicate commit.** The
      destination won't recognize the prior attempt and will commit
      again. Idempotence is broken.

    The orchestrator pattern (run-id supplied from upstream DAG context)
    avoids both naturally.

    Crashed runs leave orphan data files at the table location.
    Delta writes parquet files before the commit, so files from
    crashed attempts are not referenced by any commit but the
    bytes remain on disk.

## write_huggingface

```python
write_huggingface(repo: str, split: str='train', data_dir: str='data', revision: str='main', overwrite: bool=False, commit_message: str='Upload dataset using Daft', commit_description: str | None=None, io_config: IOConfig | None=None) -> DataFrame
```

Write a DataFrame into a Hugging Face dataset.

Args:
    repo: The ID of the repository to push to in the following format: `<user>/<dataset_name>` or `<org>/<dataset_name>`.
    split: The name of the split that will be given to that dataset.
    data_dir: Directory of the uploaded data files.
    revision: Branch to push the uploaded files to.
    overwrite: Whether to overwrite or append.
    commit_message: Message to commit while pushing.
    commit_description: Description of the commit that will be created.
    io_config: Configurations to use when interacting with remote storage.

## write_iceberg

```python
write_iceberg(table: 'pyiceberg.table.Table', mode: str='append', io_config: IOConfig | None=None, snapshot_properties: dict[str, str] | None=None, checkpoint: 'IdempotentCommit | None'=None) -> DataFrame
```

Writes the DataFrame to an [Iceberg](https://iceberg.apache.org/docs/nightly/) table, returning a new DataFrame with the operations that occurred.

Can be run in either `append` or `overwrite` mode which will either appends the rows in the DataFrame or will delete the existing rows and then append the DataFrame rows respectively.

``write_iceberg`` supports checkpointing via the ``checkpoint=`` parameter. For the conceptual overview, see the [Checkpointing guide](../use-case/checkpointing.md).

Args:
    table (pyiceberg.table.Table): Destination [PyIceberg Table](https://py.iceberg.apache.org/reference/pyiceberg/table/#pyiceberg.table.Table) to write dataframe to.
    mode (str, optional): Operation mode of the write. `append` or `overwrite` Iceberg Table. Defaults to `append`.
    io_config (IOConfig, optional): A custom IOConfig to use when accessing Iceberg object storage data. If provided, configurations set in `table` are ignored.
    snapshot_properties (dict[str, str], optional): Optional snapshot properties to set while writing to the table. Keys with prefix ``daft.idempotence-`` are reserved.
    checkpoint (IdempotentCommit, optional): Bundled checkpoint store + idempotence key for an idempotent commit. When provided, the snapshot summary is tagged with ``daft.idempotence-key`` and retries with the same key recognize the prior attempt without producing a duplicate snapshot. Only ``mode='append'`` is supported. Requires the Ray runner.

Returns:
    DataFrame: The operations that occurred with this write.

Note:
    This call is **blocking** and will execute the DataFrame when called.

    When ``checkpoint`` is provided and ``write_iceberg`` raises
    *after* the catalog commit landed (e.g. a transient failure during
    the post-commit ``mark_committed`` bookkeeping), the user data is
    already durable in Iceberg. The next call with the same
    ``IdempotentCommit`` (same idempotence key) will detect the
    snapshot via its marker, finish the bookkeeping, and exit cleanly
    without producing a duplicate snapshot.

    The returned DataFrame reflects only this call's writes — empty
    (0 rows) on a recovery short-circuit, populated when a new
    snapshot lands. Useful for run-to-run diffing.

    Idempotence-key contract — read carefully:

    - **Same key + different inputs → silent no-op (data loss).** The
      destination already has a snapshot tagged with the key, so
      nothing new is written.
    - **Different key + same retry → duplicate snapshot.** The
      destination won't recognize the prior attempt and will commit
      again. Idempotence is broken.

    The orchestrator pattern (run-id supplied from upstream DAG context)
    avoids both naturally.

    Crashed runs leave orphan data files at the warehouse location.
    Iceberg writes stage data files before the snapshot commit, so
    files from crashed attempts are not referenced by any snapshot
    but the bytes remain on disk.

## write_json

```python
write_json(root_dir: str | pathlib.Path, write_mode: Literal['append', 'overwrite', 'overwrite-partitions']='append', partition_cols: list[ColumnInputType] | None=None, io_config: IOConfig | None=None, ignore_null_fields: bool | None=False, date_format: str | None=None, timestamp_format: str | None=None) -> DataFrame
```

Writes the DataFrame as JSON files, returning a new DataFrame with paths to the files that were written.

Files will be written to `<root_dir>/*` with randomly generated UUIDs as the file names.

Args:
    root_dir (str): root file path to write JSON files to.
    write_mode (str, optional): Operation mode of the write. `append` will add new data, `overwrite` will replace the contents of the root directory with new data. `overwrite-partitions` will replace only the contents in the partitions that are being written to. Defaults to "append".
    partition_cols (Optional[List[ColumnInputType]], optional): How to subpartition each partition further. Defaults to None.
    io_config (Optional[IOConfig], optional): configurations to use when interacting with remote storage.
    ignore_null_fields (Optional[bool], optional): Whether to ignore fields with null values when writing JSON. Defaults to False.
    date_format (Optional[str], optional): Format string for date columns. Uses chrono strftime format (e.g., "%Y-%m-%d", "%d/%m/%Y"). Defaults to None (ISO 8601 format).
    timestamp_format (Optional[str], optional): Format string for timestamp columns. Uses chrono strftime format (e.g., "%Y-%m-%d %H:%M:%S", "%+"). Defaults to None (ISO 8601 format).

Returns:
    DataFrame: The filenames that were written out as strings.

Note:
    This call is **blocking** and will execute the DataFrame when called

**Timezone handling**: For timezone-aware timestamp columns, the timestamps are converted
to the target timezone before formatting. For example, a timestamp stored as UTC but with
timezone "America/New_York" will be formatted in Eastern Time, not UTC. If the timezone
string is invalid, an error will be raised.

## write_lance

```python
write_lance(uri: str | pathlib.Path, mode: Literal['create', 'append', 'overwrite', 'merge']='create', io_config: IOConfig | None=None, schema: Union[Schema, 'pyarrow.Schema'] | None=None, left_on: str | None=None, right_on: str | None=None, **kwargs: Any) -> DataFrame
```

Writes the DataFrame to a Lance table.

Args:
  uri: The URI of the Lance table to write to. Accepts a local path or an
    object-store URI like "s3://bucket/path".
  mode: The write mode. One of "create", "append", "overwrite", or "merge".
  - "create" will create the dataset if it does not exist, otherwise raise an error.
  - "append" will append to the existing dataset if it exists, otherwise raise an error.
  - "overwrite" will overwrite the existing dataset if it exists, otherwise raise an error.
  - "merge" will add new columns to the existing dataset.
  io_config (IOConfig, optional): configurations to use when interacting with remote storage.
  schema (Schema | pyarrow.Schema, optional): Desired schema to enforce during write.
    - If omitted, Daft will use the DataFrame's current schema.
    - If a pyarrow.Schema is provided, Daft will enforce the field order, types, and nullability
      by casting the data to the provided schema prior to write. Table-level (dataset) metadata present
      on the pyarrow schema is preserved during create/overwrite.
    - If the target Lance dataset already exists, the data will be cast to the existing table schema
      to ensure compatibility unless ``mode="overwrite"``.
  left_on/right_on (Optional[str]): Only supported in ``mode="merge"``. Specify the join key for aligning rows when merging new columns.
      - If omitted, defaults to ``"_rowaddr"``.
      - If ``right_on`` is omitted, it defaults to the value of ``left_on``.
      - The DataFrame passed to ``write_lance(mode="merge")`` must contain ``fragment_id`` and the join key column specified by ``right_on`` (or ``_rowaddr`` by default).
  **kwargs: Additional keyword arguments to pass to the Lance writer.

Returns:
    DataFrame: A DataFrame containing metadata about the written Lance table, such as number of fragments, number of deleted rows, number of small files, and version.

Raises:
    TypeError: If ``schema`` is provided but not a Daft Schema or a pyarrow.Schema
    ValueError: When appending and the data schema cannot be cast to the existing table schema

## write_paimon

```python
write_paimon(table: 'pypaimon.table.Table', mode: str='append') -> DataFrame
```

Writes the DataFrame to an Apache Paimon table, returning a summary DataFrame.

Args:
    table (pypaimon.table.Table): Destination Paimon table obtained via
        ``pypaimon.CatalogFactory.create(options).get_table(identifier)``.
    mode (str, optional): Write mode – ``"append"`` adds new data,
        ``"overwrite"`` replaces existing data. Defaults to ``"append"``.

Returns:
    DataFrame: A summary DataFrame with columns ``operation``, ``rows``,
    ``file_size``, and ``file_name`` describing each written file.

Note:
    This call is **blocking** and will execute the DataFrame when called.

## write_parquet

```python
write_parquet(root_dir: str | pathlib.Path, compression: str='snappy', write_mode: Literal['append', 'overwrite', 'overwrite-partitions']='append', write_success_file: bool=False, partition_cols: list[ColumnInputType] | None=None, io_config: IOConfig | None=None, column_compression: dict[str, str] | None=None, crs: str | None=None, geometry_columns: list[str] | None=None, single_file: bool=False) -> DataFrame
```

Writes the DataFrame as parquet files, returning a new DataFrame with paths to the files that were written.

Files will be written to `<root_dir>/*` with randomly generated UUIDs as the file names.

If the DataFrame contains ``Geometry`` columns, GeoParquet 1.1.0 ``"geo"`` footer metadata is
emitted automatically (WKB encoding; no CRS transforms). Use ``crs=`` to embed a CRS string,
or ``geometry_columns=`` to restrict which Geometry columns are included in the metadata.

Args:
    root_dir (str): root file path to write parquet files to. When `single_file=True`, this is the exact file path to write to.
    compression (str, optional): default compression codec applied to every column. Defaults to "snappy". Accepts "snappy", "gzip", "zstd", "lz4", "lz4_raw", "brotli", "uncompressed", or "none" (case-insensitive).
    write_mode (str, optional): Operation mode of the write. `append` will add new data, `overwrite` will replace the contents of the root directory with new data. `overwrite-partitions` will replace only the contents in the partitions that are being written to. Defaults to "append".
    write_success_file (bool, optional): Whether to write a `_SUCCESS` file upon successful completion. When `single_file=True`, writes `_SUCCESS` to the output file's parent directory. Defaults to False.
    partition_cols (Optional[List[ColumnInputType]], optional): How to subpartition each partition further. Defaults to None.
    io_config (Optional[IOConfig], optional): configurations to use when interacting with remote storage.
    column_compression (Optional[Dict[str, str]], optional): per-column compression overrides. Keys are dot-separated column paths (e.g. `"user.name"` for a nested struct field); values are codec names accepted by `compression`. Columns not listed fall back to `compression`. Defaults to None.
    crs (Optional[str], optional): CRS identifier to embed in GeoParquet `"geo"` metadata (e.g. `"OGC:CRS84"`). When `None`, the CRS key is omitted, which GeoParquet interprets as the default OGC:CRS84 (lon/lat WGS84). Only relevant when the DataFrame contains Geometry columns. Defaults to None.
    geometry_columns (Optional[List[str]], optional): explicit list of geometry column names to include in GeoParquet metadata. When `None`, all columns with a Geometry dtype are included automatically. Defaults to None.
    single_file (bool, optional): If True, coalesce all data into a single parquet file at `root_dir` (treated as the exact file path). Cannot be combined with `partition_cols` or `overwrite-partitions`. Only supported on the native runner. Defaults to False.

Returns:
    DataFrame: The filenames that were written out as strings.

Note:
    This call is **blocking** and will execute the DataFrame when called

## write_sink

```python
write_sink(sink: 'DataSink[WriteResultType]') -> DataFrame
```

Writes the DataFrame to the given DataSink.

Args:
    sink: The DataSink to write to.

Returns:
    DataFrame: A dataframe from the micropartition returned by the DataSink's `.finalize()` method.

Note:
    This call is **blocking** and will execute the DataFrame when called

## write_sql

```python
write_sql(table_name: str, conn: str | Callable[[], 'Connection'], write_mode: Literal['append', 'overwrite', 'fail']='append', column_types: dict[str, Any] | None=None, non_primitive_handling: Literal['bytes', 'str', 'error'] | None=None) -> DataFrame
```

Write the DataFrame to a SQL database and return write metrics.

The write is executed via :meth:`daft.DataFrame.write_sink` using an internal
:class:`daft.io._sql.SQLDataSink`.

Primitive columns (ints, floats, bools, strings, binary, dates, timestamps) are written by converting to a pandas DataFrame and calling :meth:`pandas.DataFrame.to_sql`, letting SQLAlchemy or ``column_types`` choose concrete SQL types.

Non-primitive columns (lists, structs, maps, tensors, images, embeddings, python objects, etc.) are normalized according to ``non_primitive_handling`` (default ``None`` behaves like ``"str"``): ``"str"`` serializes values to text (JSON for arrays/maps and other containers, ``str(..)`` otherwise), ``"bytes"`` writes UTF-8 bytes of that text, and ``"error"`` fails if such columns are present.

Args:
    table_name (str): Name of the table to write to.
    conn (str | Callable[[], "Connection"]): Connection string or factory.
    write_mode (str): Mode to write to the table. "append", "overwrite", or "fail". Defaults to "append".
    column_types (Optional[Dict[str, Any]]): Optional mapping from column names to
        SQLAlchemy types to use when creating the table or casting columns.
        Passed through to the underlying SQL engine when creating or writing
        the table.
    non_primitive_handling (Literal["bytes", "str", "error"] | None):
        Controls how non-primitive columns are normalized before reaching SQL; default ``None`` behaves like ``"str"``. Accepted values are ``"str"``, ``"bytes"``, and ``"error"``.

Returns:
    DataFrame: A single-row DataFrame containing aggregate write metrics with
        columns ``total_written_rows`` and ``total_written_bytes``.

Warning:
    This features is early in development and will likely experience API changes.

Note:
    Primitive columns still rely on pandas/SQLAlchemy (or ``column_types``) for concrete SQL types, while non-primitive columns are pre-normalized in Python according to ``non_primitive_handling`` before reaching the SQL driver.

## write_turbopuffer

```python
write_turbopuffer(namespace: str | Expression, api_key: str | None=None, region: str | None=None, distance_metric: Literal['cosine_distance', 'euclidean_squared'] | None=None, schema: dict[str, Any] | None=None, id_column: str | None=None, vector_column: str | None=None, client_kwargs: dict[str, Any] | None=None, write_kwargs: dict[str, Any] | None=None) -> DataFrame
```

Writes the DataFrame to a Turbopuffer namespace.

This method transforms each row of the dataframe into a turbopuffer document.
This means that an `id` column is always required. Optionally, the `id_column` parameter can be used to specify the column name to used for the id column.
Note that the column with the name specified by `id_column` will be renamed to "id" when written to turbopuffer.

A `vector` column is required if the namespace has a vector index. Optionally, the `vector_column` parameter can be used to specify the column name to used for the vector index.
Note that the column with the name specified by `vector_column` will be renamed to "vector" when written to turbopuffer.

All other columns become attributes.

The namespace parameter can be either a string (for a single namespace) or an expression (for multiple namespaces).
When using an expression, the data will be partitioned by the computed namespace values and written to each namespace separately.

For more details on parameters, please see the turbopuffer documentation: https://turbopuffer.com/docs/write

Args:
    namespace: The namespace to write to. Can be a string for a single namespace or an expression for multiple namespaces.
    api_key: Turbopuffer API key.
    region: Turbopuffer region.
    distance_metric: Distance metric for vector similarity ("cosine_distance", "euclidean_squared").
    schema: Optional manual schema specification.
    id_column: Optional column name for the id column. The data sink will automatically rename the column to "id" for the id column.
    vector_column: Optional column name for the vector index column. The data sink will automatically rename the column to "vector" for the vector index.
    client_kwargs: Optional dictionary of arguments to pass to the Turbopuffer client constructor.
        Explicit arguments (api_key, region) will be merged into client_kwargs.
    write_kwargs: Optional dictionary of arguments to pass to the namespace.write() method.
        Explicit arguments (distance_metric, schema) will be merged into write_kwargs.
