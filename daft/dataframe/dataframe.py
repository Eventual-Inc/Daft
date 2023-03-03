# isort: dont-add-import: from __future__ import annotations

import pathlib
import warnings
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

from daft.api_annotations import DataframePublicAPI
from daft.context import get_context
from daft.dataframe.preview import DataFramePreview
from daft.datasources import (
    CSVSourceInfo,
    JSONSourceInfo,
    ParquetSourceInfo,
    SourceInfo,
    StorageType,
)
from daft.errors import ExpressionTypeError
from daft.execution.operators import ExpressionType
from daft.expressions import Expression, col
from daft.filesystem import get_filesystem_from_path
from daft.logical import logical_plan
from daft.logical.aggregation_plan_builder import AggregationPlanBuilder
from daft.logical.schema import ExpressionList
from daft.resource_request import ResourceRequest
from daft.runners.partitioning import (
    PartitionCacheEntry,
    PartitionSet,
    vPartition,
    vPartitionParseCSVOptions,
    vPartitionReadOptions,
    vPartitionSchemaInferenceOptions,
)
from daft.runners.pyrunner import LocalPartitionSet
from daft.viz import DataFrameDisplay

if TYPE_CHECKING:
    from ray.data.dataset import Dataset as RayDataset
    import numpy as np
    import pandas
    import pyarrow as pa

from daft.logical.schema import Schema

UDFReturnType = TypeVar("UDFReturnType", covariant=True)

ColumnInputType = Union[Expression, str]
InputListType = Union[list, "np.ndarray", "pa.Array", "pa.ChunkedArray"]


def _get_tabular_files_scan(
    path: str, get_schema: Callable[[str], Schema], source_info: SourceInfo
) -> logical_plan.TabularFilesScan:
    """Returns a TabularFilesScan LogicalPlan for a given glob filepath."""
    # Glob the path and return as a DataFrame with a column containing the filepaths
    partition_set_factory = get_context().runner().partition_set_factory()
    partition_set, filepaths_schema = partition_set_factory.glob_paths_details(path, source_info)
    cache_entry = get_context().runner().put_partition_set_into_cache(partition_set)
    filepath_plan = logical_plan.InMemoryScan(
        cache_entry=cache_entry,
        schema=filepaths_schema,
        partition_spec=logical_plan.PartitionSpec(logical_plan.PartitionScheme.UNKNOWN, partition_set.num_partitions()),
    )
    filepath_df = DataFrame(filepath_plan)

    # Sample the first 10 filepaths and infer the schema
    schema_df = filepath_df.limit(10).select(
        col(partition_set_factory.FS_LISTING_PATH_COLUMN_NAME)
        .apply(get_schema, return_dtype=ExpressionList)
        .alias("schema")
    )
    schema_df.collect()
    schema_result = schema_df._result
    assert schema_result is not None
    sampled_schemas = schema_result.to_pydict()["schema"]

    # TODO: infer schema from all sampled schemas instead of just taking the first one
    schema = sampled_schemas[0]

    # Return a TabularFilesScan node that will scan from the globbed filepaths filepaths
    return logical_plan.TabularFilesScan(
        schema=schema,
        predicate=None,
        columns=None,
        source_info=source_info,
        filepaths_child=filepath_plan,
        filepaths_column_name=partition_set_factory.FS_LISTING_PATH_COLUMN_NAME,
        # Hardcoded for now.
        num_partitions=len(partition_set),
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
        if not isinstance(plan, logical_plan.LogicalPlan):
            if isinstance(plan, dict):
                raise ValueError(
                    f"DataFrames should be constructed with a dictionary of columns using `DataFrame.from_pydict`"
                )
            if isinstance(plan, list):
                raise ValueError(
                    f"DataFrames should be constructed with a list of dictionaries using `DataFrame.from_pylist`"
                )
            raise ValueError(f"Expected DataFrame to be constructed with a LogicalPlan, received: {plan}")

        self.__plan = plan
        self._result_cache: Optional[PartitionCacheEntry] = None
        self._preview = DataFramePreview(preview_partition=None, dataframe_num_rows=None)

    @property
    def _plan(self) -> logical_plan.LogicalPlan:
        if self._result_cache is None:
            return self.__plan
        else:
            return logical_plan.InMemoryScan(self._result_cache, self.__plan.schema(), self.__plan.partition_spec())

    @property
    def _result(self) -> Optional[PartitionSet]:
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

    @DataframePublicAPI
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

    @DataframePublicAPI
    def schema(self) -> Schema:
        """Returns the Schema of the DataFrame, which provides information about each column

        Returns:
            Schema: schema of the DataFrame
        """
        return self.__plan.schema()

    @property
    def column_names(self) -> List[str]:
        """Returns column names of DataFrame as a list of strings.

        Returns:
            List[str]: Column names of this DataFrame.
        """
        return self.__plan.schema().column_names()

    @property
    def columns(self) -> List[Expression]:
        """Returns column of DataFrame as a list of Expressions.

        Returns:
            List[Expression]: Columns of this DataFrame.
        """
        return [expr.to_column_expression() for expr in self.__plan.schema()]

    @DataframePublicAPI
    def show(self, n: int = 8) -> "DataFrameDisplay":
        """Executes enough of the DataFrame in order to display the first ``n`` rows

        .. NOTE::
            This call is **blocking** and will execute the DataFrame when called

        Args:
            n: number of rows to show. Defaults to 8.

        Returns:
            DataFrameDisplay: object that has a rich tabular display
        """
        df = self
        df = df.limit(n)
        df.collect(num_preview_rows=None)
        collected_preview = df._preview
        assert collected_preview is not None

        preview = DataFramePreview(
            preview_partition=collected_preview.preview_partition,
            # Override dataframe_num_rows=None, because we do not know
            # the size of the entire (un-limited) dataframe when showing
            dataframe_num_rows=None,
        )

        return DataFrameDisplay(preview, self.schema())

    @DataframePublicAPI
    def __repr__(self) -> str:
        display = DataFrameDisplay(self._preview, self.schema())
        return display.__repr__()

    @DataframePublicAPI
    def _repr_html_(self) -> str:
        display = DataFrameDisplay(self._preview, self.schema())
        return display._repr_html_()

    ###
    # Creation methods
    ###

    @classmethod
    @DataframePublicAPI
    def from_pylist(cls, data: List[Dict[str, Any]]) -> "DataFrame":
        """Creates a DataFrame from a list of dictionaries

        Example:
            >>> df = DataFrame.from_pylist([{"foo": 1}, {"foo": 2}])

        Args:
            data: list of dictionaries, where each key is a column name

        Returns:
            DataFrame: DataFrame created from list of dictionaries
        """
        headers: Set[str] = set()
        for row in data:
            if not isinstance(row, dict):
                raise ValueError(f"Expected list of dictionaries of {{column_name: value}}, received: {type(row)}")
            headers.update(row.keys())
        headers_ordered = sorted(list(headers))
        return cls.from_pydict(data={header: [row.get(header, None) for row in data] for header in headers_ordered})

    @classmethod
    @DataframePublicAPI
    def from_pydict(cls, data: Dict[str, InputListType]) -> "DataFrame":
        """Creates a DataFrame from a Python dictionary

        Example:
            >>> df = DataFrame.from_pydict({"foo": [1, 2]})

        Args:
            data: Key -> Sequence[item] of data. Each Key is created as a column, and must have a value that is
                a Python list, Numpy array or PyArrow array. Values must be equal in length across all keys.

        Returns:
            DataFrame: DataFrame created from dictionary of columns
        """
        column_lengths = {key: len(data[key]) for key in data}
        if len(set(column_lengths.values())) > 1:
            raise ValueError(
                f"Expected all columns to be of the same length, but received columns with lengths: {column_lengths}"
            )

        data_vpartition = vPartition.from_pydict(data={header: arr for header, arr in data.items()})
        result_pset = LocalPartitionSet({0: data_vpartition})

        cache_entry = get_context().runner().put_partition_set_into_cache(result_pset)

        plan = logical_plan.InMemoryScan(
            cache_entry=cache_entry,
            schema=data_vpartition.schema(),
        )
        return cls(plan)

    @classmethod
    @DataframePublicAPI
    def from_json(cls, *args, **kwargs) -> "DataFrame":
        warnings.warn(f"DataFrame.from_json will be deprecated in 0.1.0 in favor of DataFrame.read_json")
        return cls.read_json(*args, **kwargs)

    @classmethod
    @DataframePublicAPI
    def read_json(
        cls,
        path: str,
    ) -> "DataFrame":
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

        def get_schema(filepath: str) -> Schema:
            return vPartition.from_json(
                filepath,
                schema_options=vPartitionSchemaInferenceOptions(
                    schema=None,
                    inference_column_names=None,  # has no effect on inferring schema from JSON
                ),
                read_options=vPartitionReadOptions(
                    num_rows=100,  # sample 100 rows for inferring schema
                    column_names=None,  # read all columns
                ),
            ).schema()

        plan = _get_tabular_files_scan(
            path,
            get_schema,
            JSONSourceInfo(),
        )
        return cls(plan)

    @classmethod
    @DataframePublicAPI
    def from_csv(cls, *args, **kwargs) -> "DataFrame":
        warnings.warn(f"DataFrame.from_csv will be deprecated in 0.1.0 in favor of DataFrame.read_csv")
        return cls.read_csv(*args, **kwargs)

    @classmethod
    @DataframePublicAPI
    def read_csv(
        cls,
        path: str,
        has_headers: bool = True,
        column_names: Optional[List[str]] = None,
        delimiter: str = ",",
    ) -> "DataFrame":
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

        def get_schema(filepath: str) -> Schema:
            return vPartition.from_csv(
                path=filepath,
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
            ).schema()

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
    @DataframePublicAPI
    def from_parquet(cls, *args, **kwargs) -> "DataFrame":
        warnings.warn(f"DataFrame.from_parquet will be deprecated in 0.1.0 in favor of DataFrame.read_parquet")
        return cls.read_parquet(*args, **kwargs)

    @classmethod
    @DataframePublicAPI
    def read_parquet(cls, path: str) -> "DataFrame":
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

        def get_schema(filepath: str) -> Schema:
            return vPartition.from_parquet(
                filepath,
                schema_options=vPartitionSchemaInferenceOptions(
                    schema=None,
                    inference_column_names=None,  # has no effect on schema inferencing Parquet
                ),
                read_options=vPartitionReadOptions(
                    num_rows=0,  # sample 0 rows since Parquet has metadata
                    column_names=None,  # read all columns
                ),
            ).schema()

        plan = _get_tabular_files_scan(
            path,
            get_schema,
            ParquetSourceInfo(),
        )
        return cls(plan)

    @classmethod
    @DataframePublicAPI
    def from_files(cls, path: str) -> "DataFrame":
        """Creates a DataFrame of file paths and other metadata from a glob path

        Example:
            >>> df = DataFrame.from_files("/path/to/files/*.jpeg")

        Args:
            path (str): path to files on disk (allows wildcards)

        Returns:
            DataFrame: DataFrame containing the path to each file as a row, along with other metadata
                parsed from the provided filesystem
        """
        warnings.warn(
            f"DataFrame.from_files will be deprecated in 0.1.0 in favor of DataFrame.from_glob_path, which presents a more predictable set of columns for each backend and runs the file globbing on the runner instead of the driver"
        )
        fs = get_filesystem_from_path(path)
        file_details = fs.glob(path, detail=True)
        return cls.from_pylist(list(file_details.values()))

    @classmethod
    @DataframePublicAPI
    def from_glob_path(cls, path: str) -> "DataFrame":
        """Creates a DataFrame of file paths and other metadata from a glob path

        This method supports wildcards:

        1. "*" matches any number of any characters including none
        2. "?" matches any single character
        3. "[...]" matches any single character in the brackets
        4. "**" recursively matches any number of layers of directories

        The returned DataFrame will have the following columns:

        1. path: the path to the file/directory
        2. size: size of the object in bytes
        3. type: either "file" or "directory"

        Example:
            >>> df = DataFrame.from_glob_path("/path/to/files/*.jpeg")
            >>> df = DataFrame.from_glob_path("/path/to/files/**/*.jpeg")
            >>> df = DataFrame.from_glob_path("/path/to/files/**/image-?.jpeg")

        Args:
            path (str): path to files on disk (allows wildcards)

        Returns:
            DataFrame: DataFrame containing the path to each file as a row, along with other metadata
                parsed from the provided filesystem
        """
        partition_set_factory = get_context().runner().partition_set_factory()
        partition_set, filepaths_schema = partition_set_factory.glob_paths_details(path)
        cache_entry = get_context().runner().put_partition_set_into_cache(partition_set)
        filepath_plan = logical_plan.InMemoryScan(
            cache_entry=cache_entry,
            schema=filepaths_schema,
            partition_spec=logical_plan.PartitionSpec(
                logical_plan.PartitionScheme.UNKNOWN, partition_set.num_partitions()
            ),
        )
        return cls(filepath_plan)

    ###
    # Write methods
    ###

    @DataframePublicAPI
    def write_parquet(
        self,
        root_dir: Union[str, pathlib.Path],
        compression: str = "snappy",
        partition_cols: Optional[List[ColumnInputType]] = None,
    ) -> "DataFrame":
        """Writes the DataFrame as parquet files, returning a new DataFrame with paths to the files that were written

        Files will be written to ``<root_dir>/*`` with randomly generated UUIDs as the file names.

        Currently generates a parquet file per partition unless `partition_cols` are used, then the number of files can equal the number of partitions times the number of values of partition col.

        .. NOTE::
            This call is **blocking** and will execute the DataFrame when called

        Args:
            root_dir (str): root file path to write parquet files to.
            compression (str, optional): compression algorithm. Defaults to "snappy".
            partition_cols (Optional[List[ColumnInputType]], optional): How to subpartition each partition further. Currently only supports Column Expressions with any calls. Defaults to None.

        Returns:
            DataFrame: The filenames that were written out as strings.

            .. NOTE::
                This call is **blocking** and will execute the DataFrame when called
        """
        cols: Optional[ExpressionList] = None
        if partition_cols is not None:
            cols = self.__column_input_to_expression(tuple(partition_cols))
            for c in cols:
                assert c.is_column(), "we cant support non Column Expressions for partition writing"
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

    @DataframePublicAPI
    def write_csv(
        self, root_dir: Union[str, pathlib.Path], partition_cols: Optional[List[ColumnInputType]] = None
    ) -> "DataFrame":
        """Writes the DataFrame as CSV files, returning a new DataFrame with paths to the files that were written

        Files will be written to ``<root_dir>/*`` with randomly generated UUIDs as the file names.

        Currently generates a csv file per partition unless `partition_cols` are used, then the number of files can equal the number of partitions times the number of values of partition col.

        .. NOTE::
            This call is **blocking** and will execute the DataFrame when called

        Args:
            root_dir (str): root file path to write parquet files to.
            compression (str, optional): compression algorithm. Defaults to "snappy".
            partition_cols (Optional[List[ColumnInputType]], optional): How to subpartition each partition further. Currently only supports Column Expressions with any calls. Defaults to None.

        Returns:
            DataFrame: The filenames that were written out as strings.
        """
        cols: Optional[ExpressionList] = None
        if partition_cols is not None:
            cols = self.__column_input_to_expression(tuple(partition_cols))
            for c in cols:
                assert c.is_column(), "we cant support non Column Expressions for partition writing"
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

    def __getitem__(self, item: Union[slice, int, str, Iterable[Union[str, int]]]) -> Union[Expression, "DataFrame"]:
        """Gets a column from the DataFrame as an Expression (``df["mycol"]``)"""
        result: Optional[Expression]

        if isinstance(item, int):
            schema = self._plan.schema()
            if item < -len(schema) or item >= len(schema):
                raise ValueError(f"{item} out of bounds for {schema}")
            result = schema.to_column_expressions().exprs[item]
            assert result is not None
            return result
        elif isinstance(item, str):
            schema = self._plan.schema()
            result = schema[item].to_column_expression()
            return result
        elif isinstance(item, Iterable):
            schema = self._plan.schema()
            col_exprs = self._plan.schema().to_column_expressions()

            columns = []
            for it in item:
                if isinstance(it, str):
                    result = schema[it].to_column_expression()
                    if result is None:
                        raise ValueError(f"{it} not found in DataFrame schema {schema}")
                    columns.append(result)
                elif isinstance(it, int):
                    if it < -len(schema) or it >= len(schema):
                        raise ValueError(f"{it} out of bounds for {schema}")
                    result = col_exprs.exprs[it]
                    assert result is not None
                    columns.append(result)
                else:
                    raise ValueError(f"unknown indexing type: {type(it)}")
            return self.select(*columns)
        elif isinstance(item, slice):
            schema = self._plan.schema()
            columns_exprs: ExpressionList = schema.to_column_expressions()
            selected_columns = columns_exprs.exprs[item]
            return self.select(*selected_columns)
        else:
            raise ValueError(f"unknown indexing type: {type(item)}")

    @DataframePublicAPI
    def select(self, *columns: ColumnInputType) -> "DataFrame":
        """Creates a new DataFrame from the provided expressions, similar to a SQL ``SELECT``

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
        projection = logical_plan.Projection(
            self._plan,
            self.__column_input_to_expression(columns),
        )
        return DataFrame(projection)

    @DataframePublicAPI
    def distinct(self) -> "DataFrame":
        """Computes unique rows, dropping duplicates

        Example:
            >>> unique_df = df.distinct()

        Returns:
            DataFrame: DataFrame that has only  unique rows.
        """
        all_exprs = self._plan.schema().to_column_expressions()
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

    @DataframePublicAPI
    def exclude(self, *names: str) -> "DataFrame":
        """Drops columns from the current DataFrame by name

        This is equivalent of performing a select with all the columns but the ones excluded.

        Example:
            >>> df_without_x = df.exclude('x')

        Args:
            *names (str): names to exclude

        Returns:
            DataFrame: DataFrame with some columns excluded.
        """
        names_to_skip = set(names)
        el = ExpressionList([col(e.name) for e in self._plan.schema() if e.name not in names_to_skip])
        return DataFrame(logical_plan.Projection(self._plan, el))

    @DataframePublicAPI
    def where(self, predicate: Expression) -> "DataFrame":
        """Filters rows via a predicate expression, similar to SQL ``WHERE``.

        Example:
            >>> filtered_df = df.where((col('x') < 10) & (col('y') == 10))

        Args:
            predicate (Expression): expression that keeps row if evaluates to True.

        Returns:
            DataFrame: Filtered DataFrame.
        """
        plan = logical_plan.Filter(self._plan, ExpressionList([predicate]))
        return DataFrame(plan)

    @DataframePublicAPI
    def with_column(
        self, column_name: str, expr: Expression, resource_request: ResourceRequest = ResourceRequest()
    ) -> "DataFrame":
        """Adds a column to the current DataFrame with an Expression, equivalent to a ``select``
        with all current columns and the new one

        Example:
            >>> new_df = df.with_column('x+1', col('x') + 1)

        Args:
            column_name (str): name of new column
            expr (Expression): expression of the new column.
            resource_request (ResourceRequest): a custom resource request for the execution of this operation

        Returns:
            DataFrame: DataFrame with new column.
        """
        if not isinstance(resource_request, ResourceRequest):
            raise TypeError(f"resource_request should be a ResourceRequest, but got {type(resource_request)}")

        prev_schema_as_cols = self._plan.schema().to_column_expressions()
        projection = logical_plan.Projection(
            self._plan,
            prev_schema_as_cols.union(ExpressionList([expr.alias(column_name)]), other_override=True),
            custom_resource_request=resource_request,
        )
        return DataFrame(projection)

    @DataframePublicAPI
    def sort(
        self, by: Union[ColumnInputType, List[ColumnInputType]], desc: Union[bool, List[bool]] = False
    ) -> "DataFrame":
        """Sorts DataFrame globally

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

    @DataframePublicAPI
    def limit(self, num: int) -> "DataFrame":
        """Limits the rows in the DataFrame to the first ``N`` rows, similar to a SQL ``LIMIT``

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

    @DataframePublicAPI
    def count_rows(self) -> int:
        """Executes the Dataframe to count the number of rows.

        Returns:
            int: count of the number of rows in this DataFrame.
        """
        local_count_op = logical_plan.LocalCount(self._plan)
        coalease_op = logical_plan.Coalesce(local_count_op, 1)
        local_sum_op = logical_plan.LocalAggregate(coalease_op, [(Expression._sum(col("count")), "sum")])
        num_rows = DataFrame(local_sum_op).to_pydict()["count"][0]
        return num_rows

    @DataframePublicAPI
    def repartition(self, num: int, *partition_by: ColumnInputType) -> "DataFrame":
        """Repartitions DataFrame to ``num`` partitions

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
            scheme = logical_plan.PartitionScheme.HASH
            exprs = self.__column_input_to_expression(partition_by)

        repartition_op = logical_plan.Repartition(self._plan, num_partitions=num, partition_by=exprs, scheme=scheme)
        return DataFrame(repartition_op)

    @DataframePublicAPI
    def join(
        self,
        other: "DataFrame",
        on: Optional[Union[List[ColumnInputType], ColumnInputType]] = None,
        left_on: Optional[Union[List[ColumnInputType], ColumnInputType]] = None,
        right_on: Optional[Union[List[ColumnInputType], ColumnInputType]] = None,
        how: str = "inner",
    ) -> "DataFrame":
        """Column-wise join of the current DataFrame with an ``other`` DataFrame, similar to a SQL ``JOIN``

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

    @DataframePublicAPI
    def explode(self, *columns: ColumnInputType) -> "DataFrame":
        """Explodes a List column, where every element in each row's List becomes its own row, and all
        other columns in the DataFrame are duplicated across rows

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

    def _agg(self, to_agg: List[Tuple[ColumnInputType, str]], group_by: Optional[ExpressionList] = None) -> "DataFrame":
        exprs_to_agg: List[Tuple[Expression, str]] = list(
            zip(self.__column_input_to_expression([c for c, _ in to_agg]), [op for _, op in to_agg])
        )
        builder = AggregationPlanBuilder(self._plan, group_by=group_by)
        for expr, op in exprs_to_agg:
            if op == "sum":
                builder.add_sum(expr.name(), expr)
            elif op == "min":
                builder.add_min(expr.name(), expr)
            elif op == "max":
                builder.add_max(expr.name(), expr)
            elif op == "count":
                builder.add_count(expr.name(), expr)
            elif op == "list":
                builder.add_list(expr.name(), expr)
            elif op == "mean":
                builder.add_mean(expr.name(), expr)
            else:
                raise NotImplementedError(f"LogicalPlan construction for operation not implemented: {op}")
        return DataFrame(builder.build())

    @DataframePublicAPI
    def sum(self, *cols: ColumnInputType) -> "DataFrame":
        """Performs a global sum on the DataFrame

        Args:
            *cols (Union[str, Expression]): columns to sum
        Returns:
            DataFrame: Globally aggregated sums. Should be a single row.
        """
        assert len(cols) > 0, "no columns were passed in"
        return self._agg([(c, "sum") for c in cols])

    @DataframePublicAPI
    def mean(self, *cols: ColumnInputType) -> "DataFrame":
        """Performs a global mean on the DataFrame

        Args:
            *cols (Union[str, Expression]): columns to mean
        Returns:
            DataFrame: Globally aggregated mean. Should be a single row.
        """
        assert len(cols) > 0, "no columns were passed in"
        return self._agg([(c, "mean") for c in cols])

    @DataframePublicAPI
    def min(self, *cols: ColumnInputType) -> "DataFrame":
        """Performs a global min on the DataFrame

        Args:
            *cols (Union[str, Expression]): columns to min
        Returns:
            DataFrame: Globally aggregated min. Should be a single row.
        """
        assert len(cols) > 0, "no columns were passed in"
        return self._agg([(c, "min") for c in cols])

    @DataframePublicAPI
    def max(self, *cols: ColumnInputType) -> "DataFrame":
        """Performs a global max on the DataFrame

        Args:
            *cols (Union[str, Expression]): columns to max
        Returns:
            DataFrame: Globally aggregated max. Should be a single row.
        """
        assert len(cols) > 0, "no columns were passed in"
        return self._agg([(c, "max") for c in cols])

    @DataframePublicAPI
    def count(self, *cols: ColumnInputType) -> "DataFrame":
        """Performs a global count on the DataFrame

        Args:
            *cols (Union[str, Expression]): columns to count
        Returns:
            DataFrame: Globally aggregated count. Should be a single row.
        """
        assert len(cols) > 0, "no columns were passed in"
        return self._agg([(c, "count") for c in cols])

    @DataframePublicAPI
    def agg(self, to_agg: List[Tuple[ColumnInputType, str]]) -> "DataFrame":
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

    @DataframePublicAPI
    def groupby(self, *group_by: ColumnInputType) -> "GroupedDataFrame":
        """Performs a GroupBy on the DataFrame for aggregation

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

    @DataframePublicAPI
    def collect(self, num_preview_rows: Optional[int] = 8) -> "DataFrame":
        """Executes the entire DataFrame and materializes the results

        .. NOTE::
            This call is **blocking** and will execute the DataFrame when called

        Args:
            num_preview_rows: Number of rows to preview. Defaults to 10

        Returns:
            DataFrame: DataFrame with materialized results.
        """
        self._materialize_results()

        assert self._result is not None
        dataframe_len = len(self._result)
        requested_rows = dataframe_len if num_preview_rows is None else num_preview_rows

        # Build a DataFramePreview and cache it if necessary
        preview_partition_invalid = (
            self._preview.preview_partition is None or len(self._preview.preview_partition) < requested_rows
        )
        if preview_partition_invalid:
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

    def __len__(self):
        """Returns the count of rows when dataframe is materialized.
        If dataframe is not materialized yet, raises a runtime error.

        Returns:
            int: count of rows.

        """

        if self._result is not None:
            return len(self._result)

        message = (
            "Cannot call len() on an unmaterialized dataframe:"
            " either materialize your dataframe with df.collect() first before calling len(),"
            " or use `df.count_rows()` instead which will calculate the total number of rows."
        )
        raise RuntimeError(message)

    @DataframePublicAPI
    def to_pandas(self) -> "pandas.DataFrame":
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

    @DataframePublicAPI
    def to_pydict(self) -> Dict[str, List[Any]]:
        """Converts the current DataFrame to a python dictionary. The dictionary contains Python lists of Python objects for each column.

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

    @DataframePublicAPI
    def to_ray_dataset(self) -> "RayDataset":
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
        for e in self.group_by:
            if e.resolve_type(self.df._plan.schema()) == ExpressionType.null():
                raise ExpressionTypeError(f"Cannot groupby on null type expression: {e}")

    def __getitem__(self, item: Union[slice, int, str, Iterable[Union[str, int]]]) -> Union[Expression, "DataFrame"]:
        """Gets a column from the DataFrame as an Expression"""
        return self.df.__getitem__(item)

    def sum(self, *cols: ColumnInputType) -> "DataFrame":
        """Perform grouped sum on this GroupedDataFrame.

        Args:
            *cols (Union[str, Expression]): columns to sum

        Returns:
            DataFrame: DataFrame with grouped sums.
        """
        return self.df._agg([(c, "sum") for c in cols], group_by=self.group_by)

    def mean(self, *cols: ColumnInputType) -> "DataFrame":
        """Performs grouped mean on this GroupedDataFrame.

        Args:
            *cols (Union[str, Expression]): columns to mean

        Returns:
            DataFrame: DataFrame with grouped mean.
        """

        return self.df._agg([(c, "mean") for c in cols], group_by=self.group_by)

    def min(self, *cols: ColumnInputType) -> "DataFrame":
        """Perform grouped min on this GroupedDataFrame.

        Args:
            *cols (Union[str, Expression]): columns to min

        Returns:
            DataFrame: DataFrame with grouped min.
        """
        return self.df._agg([(c, "min") for c in cols], group_by=self.group_by)

    def max(self, *cols: ColumnInputType) -> "DataFrame":
        """Performs grouped max on this GroupedDataFrame.

        Args:
            *cols (Union[str, Expression]): columns to max

        Returns:
            DataFrame: DataFrame with grouped max.
        """

        return self.df._agg([(c, "max") for c in cols], group_by=self.group_by)

    def count(self) -> "DataFrame":
        """Performs grouped count on this GroupedDataFrame.

        Returns:
            DataFrame: DataFrame with grouped count per column.
        """

        return self.df._agg(
            [(c, "count") for c in self.df.column_names if c not in self.group_by.names], group_by=self.group_by
        )

    def agg(self, to_agg: List[Tuple[ColumnInputType, str]]) -> "DataFrame":
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
