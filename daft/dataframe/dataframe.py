from __future__ import annotations

import warnings
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Iterable, TypeVar, Union

import pandas
import pyarrow as pa

from daft.context import get_context
from daft.dataframe.preview import DataFramePreview
from daft.dataframe.schema import DataFrameSchema
from daft.datasources import (
    CSVSourceInfo,
    JSONSourceInfo,
    ParquetSourceInfo,
    SourceInfo,
    StorageType,
)
from daft.errors import ExpressionTypeError
from daft.execution.operators import ExpressionType
from daft.expressions import ColumnExpression, Expression, col
from daft.filesystem import get_filesystem_from_path
from daft.logical import logical_plan
from daft.logical.schema import ExpressionList
from daft.runners.partitioning import (
    PartitionCacheEntry,
    PartitionSet,
    vPartition,
    vPartitionParseCSVOptions,
    vPartitionReadOptions,
    vPartitionSchemaInferenceOptions,
)
from daft.runners.pyrunner import LocalPartitionSet
from daft.types import PythonExpressionType
from daft.viz import DataFrameDisplay

if TYPE_CHECKING:
    from ray.data.dataset import Dataset as RayDataset

UDFReturnType = TypeVar("UDFReturnType", covariant=True)

ColumnInputType = Union[Expression, str]


def _get_tabular_files_scan(
    path: str, get_schema: Callable[[str], ExpressionList], source_info: SourceInfo
) -> logical_plan.TabularFilesScan:
    """Returns a TabularFilesScan LogicalPlan for a given glob filepath."""
    # Glob the path and return as a DataFrame with a column containing the filepaths
    partition_set_factory = get_context().runner().partition_set_factory()
    partition_set, filepaths_schema = partition_set_factory.glob_filepaths(path)
    cache_entry = get_context().runner().put_partition_set_into_cache(partition_set)
    filepath_plan = logical_plan.InMemoryScan(
        cache_entry=cache_entry,
        schema=filepaths_schema,
        partition_spec=logical_plan.PartitionSpec(logical_plan.PartitionScheme.UNKNOWN, partition_set.num_partitions()),
    )
    filepath_df = DataFrame(filepath_plan)

    # Sample the first 10 filepaths and infer the schema
    schema_df = filepath_df.limit(10).select(
        col(partition_set_factory.FILEPATH_COLUMN_NAME).apply(get_schema, return_type=ExpressionList).alias("schema")
    )
    schema_df.collect()
    schema_result = schema_df._result
    assert schema_result is not None
    sampled_schemas = schema_result.to_pydict()["schema"]

    # TODO: infer schema from all sampled schemas instead of just taking the first one
    schema = sampled_schemas[0]
    schema = schema.resolve()

    # Return a TabularFilesScan node that will scan from the globbed filepaths filepaths
    return logical_plan.TabularFilesScan(
        schema=schema,
        predicate=None,
        columns=None,
        source_info=source_info,
        filepaths_child=filepath_plan,
        filepaths_column_name=partition_set_factory.FILEPATH_COLUMN_NAME,
    )


class DataFrame:
    """A Daft DataFrame is a table of data. It has columns, where each column has a type and the same
    number of items (rows) as all other columns.
    """

    def __init__(self, plan: logical_plan.LogicalPlan) -> None:
        """Constructs a DataFrame according to a given LogicalPlan. Users are expected instead to call
        the classmethods on DataFrame to create a DataFrame.

        Args:
            plan: LogicalPlan describing the steps required to arrive at this DataFrame
        """
        self.__plan = plan
        self._result_cache: PartitionCacheEntry | None = None
        self._preview = DataFramePreview(preview_partition=None, dataframe_num_rows=None)

    @property
    def _plan(self) -> logical_plan.LogicalPlan:
        if self._result_cache is None:
            return self.__plan
        else:
            return logical_plan.InMemoryScan(
                self._result_cache, self.__plan.schema().to_column_expressions(), self.__plan.partition_spec()
            )

    @property
    def _result(self) -> PartitionSet | None:
        if self._result_cache is None:
            return None
        else:
            return self._result_cache.value

    def plan(self) -> logical_plan.LogicalPlan:
        """Returns `LogicalPlan` that will be executed to compute the result of this DataFrame.

        Returns:
            logical_plan.LogicalPlan: LogicalPlan to compute this DataFrame.
        """
        return self.__plan

    def explain(self, show_optimized: bool = False) -> None:
        """Prints the LogicalPlan that will be executed to produce this DataFrame.
        Defaults to showing the unoptimized plan. Use `show_optimized` to show the optimized one.

        Args:
            show_optimized (bool): shows the optimized QueryPlan instead of the unoptimized one.
        """

        if self._result_cache is not None:
            print("Result is cached and will skip computation\n")
            print(self._plan.pretty_print())

            print("However here is the logical plan used to produce this result:\n")

        plan = self.__plan
        if show_optimized:
            plan = get_context().runner().optimize(plan)
        print(plan.pretty_print())

    def num_partitions(self) -> int:
        return self.__plan.num_partitions()

    def schema(self) -> DataFrameSchema:
        """Returns the DataFrameSchema of the DataFrame, which provides information about each column

        Returns:
            DataFrameSchema: schema of the DataFrame
        """
        return DataFrameSchema.from_expression_list(self.__plan.schema())

    @property
    def column_names(self) -> list[str]:
        """Returns column names of DataFrame as a list of strings.

        Returns:
            List[str]: Column names of this DataFrame.
        """
        return [expr.name() for expr in self.__plan.schema()]

    @property
    def columns(self) -> list[ColumnExpression]:
        """Returns column of DataFrame as a list of ColumnExpressions.

        Returns:
            List[ColumnExpression]: Columns of this DataFrame.
        """
        return [expr.to_column_expression() for expr in self.__plan.schema()]

    def show(self, n: int | None = None) -> DataFrameDisplay:
        """Executes and displays the executed dataframe as a table

        Args:
            n: number of rows to show. Defaults to None which indicates showing the entire Dataframe.

        Returns:
            DataFrameDisplay: object that has a rich tabular display

            .. NOTE::
                This call is **blocking** and will execute the DataFrame when called
        """
        df = self
        if n is not None:
            df = df.limit(n)

        df.collect(num_preview_rows=n)
        result = df._result
        assert result is not None

        # If showing all rows, then we can use the resulting DataFramePreview's dataframe_num_rows since no limit was applied
        dataframe_num_rows = df._preview.dataframe_num_rows if n is None else None

        preview = DataFramePreview(
            preview_partition=df._preview.preview_partition,
            dataframe_num_rows=dataframe_num_rows,
        )

        return DataFrameDisplay(preview, df.schema())

    def __repr__(self) -> str:
        display = DataFrameDisplay(self._preview, self.schema())
        return display.__repr__()

    def _repr_html_(self) -> str:
        display = DataFrameDisplay(self._preview, self.schema())
        return display._repr_html_()

    ###
    # Creation methods
    ###

    @classmethod
    def from_pylist(cls, data: list[dict[str, Any]]) -> DataFrame:
        """Creates a DataFrame from a list of dictionaries

        Example:
            >>> df = DataFrame.from_pylist([{"foo": 1}, {"foo": 2}])

        Args:
            data: list of dictionaries, where each key is a column name

        Returns:
            DataFrame: DataFrame created from list of dictionaries
        """
        if not data:
            raise ValueError("Unable to create DataFrame from empty list")
        return cls.from_pydict(data={header: [row[header] for row in data] for header in data[0]})

    @classmethod
    def from_pydict(cls, data: dict[str, list | pa.Array]) -> DataFrame:
        """Creates a DataFrame from a Python Dictionary.

        Example:
            >>> df = DataFrame.from_pydict({"foo": [1, 2]})

        Args:
            data: Key -> Sequence[item] of data. Each Key is created as a column, and must have a value that is
                either a Python list or PyArrow array. Values must be equal in length across all keys.

        Returns:
            DataFrame: DataFrame created from dictionary of columns
        """

        block_data: dict[str, tuple[ExpressionType, Any]] = {}
        for header in data:
            arr = data[header]

            if isinstance(arr, pa.Array) or isinstance(arr, pa.ChunkedArray):
                expr_type = ExpressionType.from_arrow_type(arr.type)
                block_data[header] = (expr_type, arr.to_pylist() if ExpressionType.is_py(expr_type) else arr)
                continue

            try:
                arrow_type = pa.infer_type(arr)
            except pa.lib.ArrowInvalid:
                arrow_type = None

            if arrow_type is None or pa.types.is_nested(arrow_type):
                found_types = {type(o) for o in data[header]} - {type(None)}
                block_data[header] = (
                    (ExpressionType.python_object(), list(arr))
                    if len(found_types) > 1
                    else (PythonExpressionType(found_types.pop()), list(arr))
                )
                continue

            expr_type = ExpressionType.from_arrow_type(arrow_type)
            block_data[header] = (expr_type, list(arr) if ExpressionType.is_py(expr_type) else pa.array(arr))

        schema = ExpressionList(
            [ColumnExpression(header, expr_type=expr_type) for header, (expr_type, _) in block_data.items()]
        ).resolve()
        data_vpartition = vPartition.from_pydict(
            data={header: arr for header, (_, arr) in block_data.items()}, schema=schema, partition_id=0
        )
        result_pset = LocalPartitionSet({0: data_vpartition})

        cache_entry = get_context().runner().put_partition_set_into_cache(result_pset)

        plan = logical_plan.InMemoryScan(
            cache_entry=cache_entry,
            schema=schema,
        )
        return cls(plan)

    @classmethod
    def from_json(cls, *args, **kwargs) -> DataFrame:
        warnings.warn(f"DataFrame.from_json will be deprecated in 0.1.0 in favor of DataFrame.read_json")
        return cls.read_json(*args, **kwargs)

    @classmethod
    def read_json(
        cls,
        path: str,
    ) -> DataFrame:
        """Creates a DataFrame from line-delimited JSON file(s)

        Example:
            >>> df = DataFrame.read_json("/path/to/file.json")
            >>> df = DataFrame.read_json("/path/to/directory")
            >>> df = DataFrame.read_json("/path/to/files-*.json")
            >>> df = DataFrame.read_json("s3://path/to/files-*.json")

        Args:
            path (str): Path to JSON files (allows for wildcards)

        returns:
            DataFrame: parsed DataFrame
        """

        def get_schema(filepath: str) -> ExpressionList:
            return vPartition.from_json(
                filepath,
                partition_id=0,
                schema_options=vPartitionSchemaInferenceOptions(
                    schema=None,
                    inference_column_names=None,  # has no effect on inferring schema from JSON
                ),
                read_options=vPartitionReadOptions(
                    num_rows=100,  # sample 100 rows for inferring schema
                    column_names=None,  # read all columns
                ),
            ).get_unresolved_col_expressions()

        plan = _get_tabular_files_scan(
            path,
            get_schema,
            JSONSourceInfo(),
        )
        return cls(plan)

    @classmethod
    def from_csv(cls, *args, **kwargs) -> DataFrame:
        warnings.warn(f"DataFrame.from_csv will be deprecated in 0.1.0 in favor of DataFrame.read_csv")
        return cls.read_csv(*args, **kwargs)

    @classmethod
    def read_csv(
        cls,
        path: str,
        has_headers: bool = True,
        column_names: list[str] | None = None,
        delimiter: str = ",",
    ) -> DataFrame:
        """Creates a DataFrame from CSV file(s)

        Example:
            >>> df = DataFrame.read_csv("/path/to/file.csv")
            >>> df = DataFrame.read_csv("/path/to/directory")
            >>> df = DataFrame.read_csv("/path/to/files-*.csv")
            >>> df = DataFrame.read_csv("s3://path/to/files-*.csv")

        Args:
            path (str): Path to CSV (allows for wildcards)
            has_headers (bool): Whether the CSV has a header or not, defaults to True
            column_names (Optional[List[str]]): Custom column names to assign to the DataFrame, defaults to None
            delimiter (Str): Delimiter used in the CSV, defaults to ","

        returns:
            DataFrame: parsed DataFrame
        """

        def get_schema(filepath: str) -> ExpressionList:
            return vPartition.from_csv(
                path=filepath,
                partition_id=0,
                csv_options=vPartitionParseCSVOptions(
                    delimiter=delimiter,
                    has_headers=has_headers,
                    skip_rows_before_header=0,
                    skip_rows_after_header=0,
                ),
                schema_options=vPartitionSchemaInferenceOptions(
                    schema=None,
                    inference_column_names=column_names,  # pass in user-provided column names
                ),
                read_options=vPartitionReadOptions(
                    num_rows=100,  # sample 100 rows for schema inference
                    column_names=None,  # read all columns
                ),
            ).get_unresolved_col_expressions()

        plan = _get_tabular_files_scan(
            path,
            get_schema,
            CSVSourceInfo(
                delimiter=delimiter,
                has_headers=has_headers,
            ),
        )
        return cls(plan)

    @classmethod
    def from_parquet(cls, *args, **kwargs) -> DataFrame:
        warnings.warn(f"DataFrame.from_parquet will be deprecated in 0.1.0 in favor of DataFrame.read_parquet")
        return cls.read_parquet(*args, **kwargs)

    @classmethod
    def read_parquet(cls, path: str) -> DataFrame:
        """Creates a DataFrame from Parquet file(s)

        Example:
            >>> df = DataFrame.read_parquet("/path/to/file.parquet")
            >>> df = DataFrame.read_parquet("/path/to/directory")
            >>> df = DataFrame.read_parquet("/path/to/files-*.parquet")
            >>> df = DataFrame.read_parquet("s3://path/to/files-*.parquet")

        Args:
            path (str): Path to Parquet file (allows for wildcards)

        returns:
            DataFrame: parsed DataFrame
        """

        def get_schema(filepath: str) -> ExpressionList:
            return vPartition.from_parquet(
                filepath,
                partition_id=0,
                schema_options=vPartitionSchemaInferenceOptions(
                    schema=None,
                    inference_column_names=None,  # has no effect on schema inferencing Parquet
                ),
                read_options=vPartitionReadOptions(
                    num_rows=0,  # sample 0 rows since Parquet has metadata
                    column_names=None,  # read all columns
                ),
            ).get_unresolved_col_expressions()

        plan = _get_tabular_files_scan(
            path,
            get_schema,
            ParquetSourceInfo(),
        )
        return cls(plan)

    @classmethod
    def from_files(cls, path: str) -> DataFrame:
        """Creates a DataFrame from files in storage, where each file is one row of the DataFrame

        Example:
            >>> df = DataFrame.from_files("/path/to/files/*.jpeg")

        Args:
            path (str): path to files on disk (allows wildcards)

        Returns:
            DataFrame: DataFrame containing the path to each file as a row, along with other metadata
                parsed from the provided filesystem
        """
        fs = get_filesystem_from_path(path)
        file_details = fs.glob(path, detail=True)
        return cls.from_pylist(list(file_details.values()))

    ###
    # Write methods
    ###

    def write_parquet(
        self, root_dir: str, compression: str = "snappy", partition_cols: list[ColumnInputType] | None = None
    ) -> DataFrame:
        """Writes the DataFrame to parquet files using a `root_dir` and randomly generated UUIDs as the filepath and returns the filepaths.
        Currently generates a parquet file per partition unless `partition_cols` are used, then the number of files can equal the number of partitions times the number of values of partition col.

        Args:
            root_dir (str): root file path to write parquet files to.
            compression (str, optional): compression algorithm. Defaults to "snappy".
            partition_cols (Optional[List[ColumnInputType]], optional): How to subpartition each partition further. Currently only supports ColumnExpressions with any calls. Defaults to None.

        Returns:
            DataFrame: The filenames that were written out as strings.

            .. NOTE::
                This call is **blocking** and will execute the DataFrame when called
        """
        cols: ExpressionList | None = None
        if partition_cols is not None:
            cols = self.__column_input_to_expression(tuple(partition_cols))
            for c in cols:
                assert isinstance(c, ColumnExpression), "we cant support non ColumnExpressions for partition writing"
            df = self.repartition(self.num_partitions(), *cols)
        else:
            df = self
        plan = logical_plan.FileWrite(
            df._plan,
            root_dir=root_dir,
            partition_cols=cols,
            storage_type=StorageType.PARQUET,
            compression=compression,
        )

        # Block and write, then retrieve data and return a new disconnected DataFrame
        write_df = DataFrame(plan)
        write_df.collect()
        assert write_df._result is not None
        return DataFrame(write_df._plan)

    def write_csv(self, root_dir: str, partition_cols: list[ColumnInputType] | None = None) -> DataFrame:
        """Writes the DataFrame to CSV files using a `root_dir` and randomly generated UUIDs as the filepath and returns the filepaths.
        Currently generates a csv file per partition unless `partition_cols` are used, then the number of files can equal the number of partitions times the number of values of partition col.

        Args:
            root_dir (str): root file path to write parquet files to.
            compression (str, optional): compression algorithm. Defaults to "snappy".
            partition_cols (Optional[List[ColumnInputType]], optional): How to subpartition each partition further. Currently only supports ColumnExpressions with any calls. Defaults to None.

        Returns:
            DataFrame: The filenames that were written out as strings.

            .. NOTE::
                This call is **blocking** and will execute the DataFrame when called
        """
        cols: ExpressionList | None = None
        if partition_cols is not None:
            cols = self.__column_input_to_expression(tuple(partition_cols))
            for c in cols:
                assert isinstance(c, ColumnExpression), "we cant support non ColumnExpressions for partition writing"
            df = self.repartition(self.num_partitions(), *cols)
        else:
            df = self
        plan = logical_plan.FileWrite(
            df._plan,
            root_dir=root_dir,
            partition_cols=cols,
            storage_type=StorageType.CSV,
        )

        # Block and write, then retrieve data and return a new disconnected DataFrame
        write_df = DataFrame(plan)
        write_df.collect()
        assert write_df._result is not None
        return DataFrame(write_df._plan)

    ###
    # DataFrame operations
    ###

    def __column_input_to_expression(self, columns: Iterable[ColumnInputType]) -> ExpressionList:
        expressions = [col(c) if isinstance(c, str) else c for c in columns]
        return ExpressionList(expressions)

    def __getitem__(self, item: slice | int | str | Iterable[str | int]) -> ColumnExpression | DataFrame:
        result: ColumnExpression | None

        if isinstance(item, int):
            exprs = self._plan.schema()
            if item < -len(exprs.exprs) or item >= len(exprs.exprs):
                raise ValueError(f"{item} out of bounds for {exprs.exprs}")
            result = exprs.exprs[item]
            assert result is not None
            return result.to_column_expression()
        elif isinstance(item, str):
            exprs = self._plan.schema()
            result = exprs.get_expression_by_name(item)
            if result is None:
                raise ValueError(f"{item} not found in DataFrame schema {exprs}")
            assert result is not None
            return result.to_column_expression()  # type: ignore
        elif isinstance(item, Iterable):
            exprs = self._plan.schema()
            columns = []
            for it in item:
                if isinstance(it, str):
                    result = exprs.get_expression_by_name(it)
                    if result is None:
                        raise ValueError(f"{it} not found in DataFrame schema {exprs}")
                    columns.append(result)
                elif isinstance(it, int):
                    if it < -len(exprs.exprs) or it >= len(exprs.exprs):
                        raise ValueError(f"{it} out of bounds for {exprs.exprs}")
                    result = exprs.exprs[it]
                    assert result is not None
                    columns.append(result.to_column_expression())
                else:
                    raise ValueError(f"unknown indexing type: {type(it)}")
            return self.select(*columns)
        elif isinstance(item, slice):
            exprs = self._plan.schema()
            return self.select(*[val.to_column_expression() for val in exprs.exprs[item]])
        else:
            raise ValueError(f"unknown indexing type: {type(item)}")

    def select(self, *columns: ColumnInputType) -> DataFrame:
        """Creates a new DataFrame that `selects` that columns that are passed in from the current DataFrame.

        Example:

            >>> # names of columns as strings
            >>> df = df.select('x', 'y')
            >>>
            >>> # names of columns as expressions
            >>> df = df.select(col('x'), col('y'))
            >>>
            >>> # call expressions
            >>> df = df.select(col('x') * col('y'))
            >>>
            >>> # any mix of the above
            >>> df = df.select('x', col('y'), col('z') + 1)

        Args:
            *columns (Union[str, Expression]): columns to select from the current DataFrame

        Returns:
            DataFrame: new DataFrame that will select the passed in columns
        """
        assert len(columns) > 0
        projection = logical_plan.Projection(self._plan, self.__column_input_to_expression(columns))
        return DataFrame(projection)

    def distinct(self) -> DataFrame:
        """Computes unique rows, dropping duplicates.

        Example:
            >>> unique_df = df.distinct()

        Returns:
            DataFrame: DataFrame that has only  unique rows.
        """
        all_exprs = self._plan.schema()
        plan: logical_plan.LogicalPlan = logical_plan.LocalDistinct(self._plan, all_exprs)
        if self.num_partitions() > 1:
            plan = logical_plan.Repartition(
                plan,
                partition_by=all_exprs,
                num_partitions=self.num_partitions(),
                scheme=logical_plan.PartitionScheme.HASH,
            )
            plan = logical_plan.LocalDistinct(plan, all_exprs)
        return DataFrame(plan)

    def exclude(self, *names: str) -> DataFrame:
        """Drops columns from the current DataFrame by name.
        This is equivalent of performing a select with all the columns but the ones excluded.

        Example:
            >>> df_without_x = df.exclude('x')

        Args:
            *names (str): names to exclude

        Returns:
            DataFrame: DataFrame with some columns excluded.
        """
        names_to_skip = set(names)
        el = ExpressionList([e for e in self._plan.schema() if e.name() not in names_to_skip])
        return DataFrame(logical_plan.Projection(self._plan, el))

    def where(self, predicate: Expression) -> DataFrame:
        """Filters rows via a predicate expression.
        similar to SQL style `where`.

        Example:
            >>> filtered_df = df.where((col('x') < 10) & (col('y') == 10))

        Args:
            predicate (Expression): expression that keeps row if evaluates to True.

        Returns:
            DataFrame: Filtered DataFrame.
        """
        plan = logical_plan.Filter(self._plan, ExpressionList([predicate]))
        return DataFrame(plan)

    def with_column(self, column_name: str, expr: Expression) -> DataFrame:
        """Adds a column to the current DataFrame with an Expression.
        This is equivalent to performing a `select` with all the current columns and the new one.

        Example:
            >>> new_df = df.with_column('x+1', col('x') + 1)

        Args:
            column_name (str): name of new column
            expr (Expression): expression of the new column.

        Returns:
            DataFrame: DataFrame with new column.
        """
        prev_schema_as_cols = self._plan.schema().to_column_expressions()
        projection = logical_plan.Projection(
            self._plan, prev_schema_as_cols.union(ExpressionList([expr.alias(column_name)]), other_override=True)
        )
        return DataFrame(projection)

    def sort(self, by: ColumnInputType | list[ColumnInputType], desc: bool | list[bool] = False) -> DataFrame:
        """Sorts DataFrame globally according to column.

        Example:
            >>> sorted_df = df.sort(col('x') + col('y'))
            >>> sorted_df = df.sort([col('x'), col('y')], desc=[False, True])
            >>> sorted_df = df.sort(['z', col('x'), col('y')], desc=[True, False, True])

        Note:
            * Since this a global sort, this requires an expensive repartition which can be quite slow.
            * Supports multicolumn sorts and can have unique `descending` flag per column.
        Args:
            column (Union[ColumnInputType, List[ColumnInputType]]): column to sort by. Can be `str` or expression as well as a list of either.
            desc (Union[bool, List[bool]), optional): Sort by descending order. Defaults to False.

        Returns:
            DataFrame: Sorted DataFrame.
        """
        if not isinstance(by, list):
            by = [
                by,
            ]
        sort = logical_plan.Sort(self._plan, self.__column_input_to_expression(by), descending=desc)
        return DataFrame(sort)

    def limit(self, num: int) -> DataFrame:
        """Limits the rows returned by the DataFrame via a `head` operation.
        This is similar to how `limit` works in SQL.

        Example:
            >>> df_limited = df.limit(10) # returns 10 rows

        Args:
            num (int): maximum rows to allow.

        Returns:
            DataFrame: Limited DataFrame
        """
        local_limit = logical_plan.LocalLimit(self._plan, num=num)
        global_limit = logical_plan.GlobalLimit(local_limit, num=num)
        return DataFrame(global_limit)

    def repartition(self, num: int, *partition_by: ColumnInputType) -> DataFrame:
        """Repartitions DataFrame to `num` partitions.

        If columns are passed in, then DataFrame will be repartitioned by those, otherwise
        random repartitioning will occur.

        Example:
            >>> random_repart_df = df.repartition(4)
            >>> part_by_df = df.repartition(4, 'x', col('y') + 1)

        Args:
            num (int): number of target partitions.
            *partition_by (Union[str, Expression]): optional columns to partition by.

        Returns:
            DataFrame: Repartitioned DataFrame.
        """
        if len(partition_by) == 0:
            scheme = logical_plan.PartitionScheme.RANDOM
            exprs: ExpressionList = ExpressionList([])
        else:
            assert len(partition_by) == 1
            scheme = logical_plan.PartitionScheme.HASH
            exprs = self.__column_input_to_expression(partition_by)

        repartition_op = logical_plan.Repartition(self._plan, num_partitions=num, partition_by=exprs, scheme=scheme)
        return DataFrame(repartition_op)

    def join(
        self,
        other: DataFrame,
        on: list[ColumnInputType] | ColumnInputType | None = None,
        left_on: list[ColumnInputType] | ColumnInputType | None = None,
        right_on: list[ColumnInputType] | ColumnInputType | None = None,
        how: str = "inner",
    ) -> DataFrame:
        """Joins left (self) DataFrame on the right on a set of keys.
        Key names can be the same or different for left and right DataFrame.

        .. NOTE::
            Although self joins are supported, we currently duplicate the logical plan for the right side
            and recompute the entire tree. Caching for this is on the roadmap.

        Args:
            other (DataFrame): the right DataFrame to join on.
            on (Optional[Union[List[ColumnInputType], ColumnInputType]], optional): key or keys to join on [use if the keys on the left and right side match.]. Defaults to None.
            left_on (Optional[Union[List[ColumnInputType], ColumnInputType]], optional): key or keys to join on left DataFrame.. Defaults to None.
            right_on (Optional[Union[List[ColumnInputType], ColumnInputType]], optional): key or keys to join on right DataFrame. Defaults to None.
            how (str, optional): what type of join to performing, currently only `inner` is supported. Defaults to "inner".

        Raises:
            ValueError: if `on` is passed in and `left_on` or `right_on` is not None.
            ValueError: if `on` is None but both `left_on` and `right_on` are not defined.

        Returns:
            DataFrame: Joined DataFrame.
        """
        if on is None:
            if left_on is None or right_on is None:
                raise ValueError("If `on` is None then both `left_on` and `right_on` must not be None")
        else:
            if left_on is not None or right_on is not None:
                raise ValueError("If `on` is not None then both `left_on` and `right_on` must be None")
            left_on = on
            right_on = on
        assert how == "inner", "only inner joins are currently supported"

        left_exprs = self.__column_input_to_expression(tuple(left_on) if isinstance(left_on, list) else (left_on,))
        right_exprs = self.__column_input_to_expression(tuple(right_on) if isinstance(right_on, list) else (right_on,))
        join_op = logical_plan.Join(
            self._plan, other._plan, left_on=left_exprs, right_on=right_exprs, how=logical_plan.JoinType.INNER
        )
        return DataFrame(join_op)

    def explode(self, *columns: ColumnInputType) -> DataFrame:
        """Explodes a List column, where every element in each row's List becomes its own row, and all
        other columns in the DataFrame are duplicated across rows.

        If multiple columns are specified, each row must contain the same number of
        items in each specified column.

        Exploding Null values or empty lists will create a single Null entry (see example below).

        Example:
            >>> df = DataFrame.from_pydict({
            >>>     "x": [[1], [2, 3]],
            >>>     "y": [["a"], ["b", "c"]],
            >>>     "z": [1.0, 2.0],
            >>> ]})
            >>>
            >>> df.explode(col("x"), col("y"))
            >>>
            >>> # +------+-----------+-----+      +------+------+-----+
            >>> # | x    | y         | z   |      |  x   |  y   | z   |
            >>> # +------+-----------+-----+      +------+------+-----+
            >>> # |[1]   | ["a"]     | 1.0 |      |  1   | "a"  | 1.0 |
            >>> # +------+-----------+-----+  ->  +------+------+-----+
            >>> # |[2, 3]| ["b", "c"]| 2.0 |      |  2   | "b"  | 2.0 |
            >>> # +------+-----------+-----+      +------+------+-----+
            >>> # |[]    | []        | 3.0 |      |  3   | "c"  | 2.0 |
            >>> # +------+-----------+-----+      +------+------+-----+
            >>> # |None  | None      | 4.0 |      | None | None | 3.0 |
            >>> # +------+-----------+-----+      +------+------+-----+
            >>> #                                 | None | None | 4.0 |
            >>> #                                 +------+------+-----+

        Args:
            *columns (ColumnInputType): columns to explode

        Returns:
            DataFrame: DataFrame with exploded column
        """
        if len(columns) < 1:
            raise ValueError("At least one column to explode must be specified")
        exprs_to_explode = self.__column_input_to_expression(columns)
        explode_op = logical_plan.Explode(
            self._plan,
            ExpressionList([e._explode() for e in exprs_to_explode]),
        )
        return DataFrame(explode_op)

    def _agg(self, to_agg: list[tuple[ColumnInputType, str]], group_by: ExpressionList | None = None) -> DataFrame:
        assert len(to_agg) > 0, "no columns to aggregate."
        exprs_to_agg = self.__column_input_to_expression(tuple(e for e, _ in to_agg))
        ops = [op for _, op in to_agg]

        function_lookup = {
            "sum": Expression._sum,
            "count": Expression._count,
            "mean": Expression._mean,
            "list": Expression._list,
            "concat": Expression._concat,
            "min": Expression._min,
            "max": Expression._max,
        }

        if self.num_partitions() == 1:
            agg_exprs = []

            for e, op_name in zip(exprs_to_agg, ops):
                assert op_name in function_lookup
                agg_exprs.append((function_lookup[op_name](e).alias(e.name()), op_name))
            plan = logical_plan.LocalAggregate(self._plan, agg=agg_exprs, group_by=group_by)
            return DataFrame(plan)

        intermediate_ops = {
            "sum": ("sum",),
            "list": ("list",),
            "count": ("count",),
            "mean": ("sum", "count"),
            "min": ("min",),
            "max": ("max",),
        }

        reduction_ops = {
            "sum": ("sum",),
            "list": ("concat",),
            "count": ("sum",),
            "mean": ("sum", "sum"),
            "min": ("min",),
            "max": ("max",),
        }

        finalizer_ops_funcs = {"mean": lambda x, y: (x + 0.0) / (y + 0.0)}

        first_phase_ops: list[tuple[Expression, str]] = []
        second_phase_ops: list[tuple[Expression, str]] = []
        finalizer_phase_ops: list[Expression] = []
        need_final_projection = False
        for e, op in zip(exprs_to_agg, ops):
            assert op in intermediate_ops
            ops_to_add = intermediate_ops[op]

            e_intermediate_name = []
            for agg_op in ops_to_add:
                name = f"{e.name()}_{agg_op}"
                f = function_lookup[agg_op]
                new_e = f(e).alias(name)
                first_phase_ops.append((new_e, agg_op))
                e_intermediate_name.append(new_e.name())

            assert op in reduction_ops
            ops_to_add = reduction_ops[op]
            added_exprs = []
            for agg_op, result_name in zip(ops_to_add, e_intermediate_name):
                assert result_name is not None
                col_e = col(result_name)
                f = function_lookup[agg_op]
                added: Expression = f(col_e)
                if op in finalizer_ops_funcs:
                    name = f"{result_name}_{agg_op}"
                    added = added.alias(name)
                else:
                    added = added.alias(e.name())
                second_phase_ops.append((added, agg_op))
                added_exprs.append(added)

            if op in finalizer_ops_funcs:
                f = finalizer_ops_funcs[op]
                operand_args = []
                for ae in added_exprs:
                    col_name = ae.name()
                    assert col_name is not None
                    operand_args.append(col(col_name))
                final_name = e.name()
                assert final_name is not None
                new_e = f(*operand_args).alias(final_name)
                finalizer_phase_ops.append(new_e)
                need_final_projection = True
            else:
                for ae in added_exprs:
                    col_name = ae.name()
                    assert col_name is not None
                    finalizer_phase_ops.append(col(col_name))

        first_phase_lagg_op = logical_plan.LocalAggregate(self._plan, agg=first_phase_ops, group_by=group_by)
        repart_op: logical_plan.LogicalPlan
        if group_by is None:
            repart_op = logical_plan.Coalesce(first_phase_lagg_op, 1)
        else:
            repart_op = logical_plan.Repartition(
                first_phase_lagg_op,
                num_partitions=self._plan.num_partitions(),
                partition_by=group_by,
                scheme=logical_plan.PartitionScheme.HASH,
            )

        gagg_op = logical_plan.LocalAggregate(repart_op, agg=second_phase_ops, group_by=group_by)

        final_schema = ExpressionList(finalizer_phase_ops)

        if group_by is not None:
            final_schema = group_by.union(final_schema)

        final_op: logical_plan.LogicalPlan
        if need_final_projection:
            final_op = logical_plan.Projection(gagg_op, final_schema)
        else:
            final_op = gagg_op

        return DataFrame(final_op)

    def sum(self, *cols: ColumnInputType) -> DataFrame:
        """Performs a global sum on the DataFrame on a sequence of columns.

        Args:
            *cols (Union[str, Expression]): columns to sum
        Returns:
            DataFrame: Globally aggregated sums. Should be a single row.
        """
        assert len(cols) > 0, "no columns were passed in"
        return self._agg([(c, "sum") for c in cols])

    def mean(self, *cols: ColumnInputType) -> DataFrame:
        """Performs a global mean on the DataFrame on a sequence of columns.

        Args:
            *cols (Union[str, Expression]): columns to mean
        Returns:
            DataFrame: Globally aggregated mean. Should be a single row.
        """
        assert len(cols) > 0, "no columns were passed in"
        return self._agg([(c, "mean") for c in cols])

    def min(self, *cols: ColumnInputType) -> DataFrame:
        """Performs a global min on the DataFrame on a sequence of columns.

        Args:
            *cols (Union[str, Expression]): columns to min
        Returns:
            DataFrame: Globally aggregated min. Should be a single row.
        """
        assert len(cols) > 0, "no columns were passed in"
        return self._agg([(c, "min") for c in cols])

    def max(self, *cols: ColumnInputType) -> DataFrame:
        """Performs a global max on the DataFrame on a sequence of columns.

        Args:
            *cols (Union[str, Expression]): columns to max
        Returns:
            DataFrame: Globally aggregated max. Should be a single row.
        """
        assert len(cols) > 0, "no columns were passed in"
        return self._agg([(c, "max") for c in cols])

    def count(self, *cols: ColumnInputType) -> DataFrame:
        """Performs a global count on the DataFrame on a sequence of columns.

        Args:
            *cols (Union[str, Expression]): columns to count
        Returns:
            DataFrame: Globally aggregated count. Should be a single row.
        """
        assert len(cols) > 0, "no columns were passed in"
        return self._agg([(c, "count") for c in cols])

    def agg(self, to_agg: list[tuple[ColumnInputType, str]]) -> DataFrame:
        """Perform aggregations on this DataFrame. Allows for mixed aggregations for multiple columns
        Will return a single row that aggregated the entire DataFrame.

        Example:
        >>> df = df.agg([
        >>>     ('x', 'sum'),
        >>>     ('x', 'mean'),
        >>>     ('y', 'min'),
        >>>     ('y', 'max'),
        >>>     (col('x') + col('y'), 'max'),
        >>> ])

        Args:
            to_agg (List[Tuple[ColumnInputType, str]]): list of (column, agg_type)

        Returns:
            DataFrame: DataFrame with aggregated results
        """
        return self._agg(to_agg, group_by=None)

    def groupby(self, *group_by: ColumnInputType) -> GroupedDataFrame:
        """Performs a GroupBy on the DataFrame for Aggregation.

        Args:
            *group_by (Union[str, Expression]): columns to group by

        Returns:
            GroupedDataFrame: DataFrame to Aggregate
        """
        return GroupedDataFrame(self, self.__column_input_to_expression(group_by))

    def _materialize_results(self) -> None:
        """Materializes the results of for this DataFrame and hold a pointer to the results."""
        context = get_context()
        if self._result is None:
            self._result_cache = context.runner().run(self._plan)
            result = self._result
            assert result is not None
            result.wait()

    def collect(self, num_preview_rows: int | None = 10) -> DataFrame:
        """Computes LogicalPlan to materialize DataFrame. This is a blocking operation.

        Args:
            num_preview_rows: Number of rows to preview. Defaults to 10

        Returns:
            DataFrame: DataFrame with materialized results.
        """
        self._materialize_results()

        assert self._result is not None
        dataframe_len = len(self._result)
        requested_rows = dataframe_len if num_preview_rows is None else num_preview_rows

        # Build a DataFramePreview and cache it if we need to
        if self._preview.preview_partition is None or len(self._preview.preview_partition) < requested_rows:

            # Add a limit onto self and materialize limited data
            preview_df = self
            if num_preview_rows is not None:
                preview_df = preview_df.limit(num_preview_rows)
            preview_df._materialize_results()
            preview_results = preview_df._result
            assert preview_results is not None

            preview_partition = preview_results._get_merged_vpartition()
            self._preview = DataFramePreview(
                preview_partition=preview_partition,
                dataframe_num_rows=dataframe_len,
            )

        return self

    def to_pandas(self) -> pandas.DataFrame:
        """Converts the current DataFrame to a pandas DataFrame.
        If results have not computed yet, collect will be called.

        Returns:
            pandas.DataFrame: pandas DataFrame converted from a Daft DataFrame

            .. NOTE::
                This call is **blocking** and will execute the DataFrame when called
        """
        self.collect()
        result = self._result
        assert result is not None

        pd_df = result.to_pandas(schema=self._plan.schema())
        return pd_df

    def to_pydict(self) -> pandas.DataFrame:
        """Converts the current DataFrame to a python dict.
        If results have not computed yet, collect will be called.

        Returns:
            dict[str, list[Any]]: python dict converted from a Daft DataFrame

            .. NOTE::
                This call is **blocking** and will execute the DataFrame when called
        """
        self.collect()
        result = self._result
        assert result is not None
        return result.to_pydict()

    def to_ray_dataset(self) -> RayDataset:
        """Converts the current DataFrame to a Ray Dataset which is useful for running distributed ML model training in Ray

        .. NOTE::
            This function can only work if Daft is running using the RayRunner

        Returns:
            RayDataset: Ray dataset
        """
        from daft.runners.ray_runner import RayPartitionSet

        self.collect()
        partition_set = self._result
        assert partition_set is not None
        assert isinstance(partition_set, RayPartitionSet), "Cannot convert to Ray Dataset if not running on Ray backend"
        return partition_set.to_ray_dataset()


@dataclass
class GroupedDataFrame:
    df: DataFrame
    group_by: ExpressionList

    def __post_init__(self):
        resolved_groupby = self.group_by.resolve(self.df._plan.schema())
        for e in resolved_groupby:
            if e.resolved_type() == ExpressionType.null():
                raise ExpressionTypeError(f"Cannot groupby on null type expression: {e}")

    def __getitem__(self, item: slice | int | str | Iterable[str | int]) -> ColumnExpression | DataFrame:
        return self.df.__getitem__(item)

    def sum(self, *cols: ColumnInputType) -> DataFrame:
        """Perform grouped sum on this GroupedDataFrame.

        Args:
            *cols (Union[str, Expression]): columns to sum

        Returns:
            DataFrame: DataFrame with grouped sums.
        """
        return self.df._agg([(c, "sum") for c in cols], group_by=self.group_by)

    def mean(self, *cols: ColumnInputType) -> DataFrame:
        """Performs grouped mean on this GroupedDataFrame.

        Args:
            *cols (Union[str, Expression]): columns to mean

        Returns:
            DataFrame: DataFrame with grouped mean.
        """

        return self.df._agg([(c, "mean") for c in cols], group_by=self.group_by)

    def min(self, *cols: ColumnInputType) -> DataFrame:
        """Perform grouped min on this GroupedDataFrame.

        Args:
            *cols (Union[str, Expression]): columns to min

        Returns:
            DataFrame: DataFrame with grouped min.
        """
        return self.df._agg([(c, "min") for c in cols], group_by=self.group_by)

    def max(self, *cols: ColumnInputType) -> DataFrame:
        """Performs grouped max on this GroupedDataFrame.

        Args:
            *cols (Union[str, Expression]): columns to max

        Returns:
            DataFrame: DataFrame with grouped max.
        """

        return self.df._agg([(c, "max") for c in cols], group_by=self.group_by)

    def agg(self, to_agg: list[tuple[ColumnInputType, str]]) -> DataFrame:
        """Perform aggregations on this GroupedDataFrame. Allows for mixed aggregations.

        Example:
        >>> df = df.groupby('x').agg([
        >>>     ('x', 'sum'),
        >>>     ('x', 'mean'),
        >>>     ('y', 'min'),
        >>>     ('y', 'max'),
        >>> ])

        Args:
            to_agg (List[Tuple[ColumnInputType, str]]): list of (column, agg_type)

        Returns:
            DataFrame: DataFrame with grouped aggregations
        """
        return self.df._agg(to_agg, group_by=self.group_by)
