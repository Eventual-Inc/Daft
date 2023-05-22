# isort: dont-add-import: from __future__ import annotations
#
# This file uses strings for forward type annotations in public APIs,
# in order to support runtime typechecking across different Python versions.
# For technical details, see https://github.com/Eventual-Inc/Daft/pull/630

import pathlib
from dataclasses import dataclass
from functools import reduce
from typing import (
    TYPE_CHECKING,
    Any,
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
from daft.convert import InputListType
from daft.dataframe.preview import DataFramePreview
from daft.datasources import StorageType
from daft.datatype import DataType
from daft.errors import ExpressionTypeError
from daft.expressions import Expression, ExpressionsProjection, col, lit
from daft.logical import logical_plan
from daft.logical.aggregation_plan_builder import AggregationPlanBuilder
from daft.resource_request import ResourceRequest
from daft.runners.partitioning import PartitionCacheEntry, PartitionSet
from daft.runners.pyrunner import LocalPartitionSet
from daft.table import Table
from daft.viz import DataFrameDisplay

if TYPE_CHECKING:
    from ray.data.dataset import Dataset as RayDataset
    import pandas as pd
    import pyarrow as pa
    import dask

from daft.logical.schema import Schema

UDFReturnType = TypeVar("UDFReturnType", covariant=True)

ColumnInputType = Union[Expression, str]


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
                    f"DataFrames should be constructed with a dictionary of columns using `daft.from_pydict`"
                )
            if isinstance(plan, list):
                raise ValueError(
                    f"DataFrames should be constructed with a list of dictionaries using `daft.from_pylist`"
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
        return [col(field.name) for field in self.__plan.schema()]

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

        return DataFrameDisplay(preview, self.schema(), num_rows=n)

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
    def _from_pylist(cls, data: List[Dict[str, Any]]) -> "DataFrame":
        """Creates a DataFrame from a list of dictionaries."""
        headers: Set[str] = set()
        for row in data:
            if not isinstance(row, dict):
                raise ValueError(f"Expected list of dictionaries of {{column_name: value}}, received: {type(row)}")
            headers.update(row.keys())
        headers_ordered = sorted(list(headers))
        return cls._from_pydict(data={header: [row.get(header, None) for row in data] for header in headers_ordered})

    @classmethod
    def _from_pydict(cls, data: Dict[str, InputListType]) -> "DataFrame":
        """Creates a DataFrame from a Python dictionary."""
        column_lengths = {key: len(data[key]) for key in data}
        if len(set(column_lengths.values())) > 1:
            raise ValueError(
                f"Expected all columns to be of the same length, but received columns with lengths: {column_lengths}"
            )

        data_vpartition = Table.from_pydict(data)
        return cls._from_tables(data_vpartition)

    @classmethod
    def _from_arrow(cls, data: Union["pa.Table", List["pa.Table"]]) -> "DataFrame":
        """Creates a DataFrame from a pyarrow Table."""
        if not isinstance(data, list):
            data = [data]
        data_vpartitions = [Table.from_arrow(table) for table in data]
        return cls._from_tables(*data_vpartitions)

    @classmethod
    def _from_pandas(cls, data: Union["pd.DataFrame", List["pd.DataFrame"]]) -> "DataFrame":
        """Creates a Daft DataFrame from a pandas DataFrame."""
        if not isinstance(data, list):
            data = [data]
        data_vpartitions = [Table.from_pandas(df) for df in data]
        return cls._from_tables(*data_vpartitions)

    @classmethod
    def _from_tables(cls, *parts: Table) -> "DataFrame":
        """Creates a Daft DataFrame from a single Table.

        Args:
            parts: The Tables that we wish to convert into a Daft DataFrame.

        Returns:
            DataFrame: Daft DataFrame created from the provided Table.
        """
        if not parts:
            raise ValueError("Can't create a DataFrame from an empty list of tables.")

        result_pset = LocalPartitionSet({i: part for i, part in enumerate(parts)})

        cache_entry = get_context().runner().put_partition_set_into_cache(result_pset)

        plan = logical_plan.InMemoryScan(
            cache_entry=cache_entry,
            schema=parts[0].schema(),
        )
        return cls(plan)

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
        cols: Optional[ExpressionsProjection] = None
        if partition_cols is not None:
            cols = self.__column_input_to_expression(tuple(partition_cols))
            for c in cols:
                assert c._is_column(), "we cant support non Column Expressions for partition writing"
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
        cols: Optional[ExpressionsProjection] = None
        if partition_cols is not None:
            cols = self.__column_input_to_expression(tuple(partition_cols))
            for c in cols:
                assert c._is_column(), "we cant support non Column Expressions for partition writing"
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

    def __column_input_to_expression(self, columns: Iterable[ColumnInputType]) -> ExpressionsProjection:
        expressions = [col(c) if isinstance(c, str) else c for c in columns]
        return ExpressionsProjection(expressions)

    def __getitem__(self, item: Union[slice, int, str, Iterable[Union[str, int]]]) -> Union[Expression, "DataFrame"]:
        """Gets a column from the DataFrame as an Expression (``df["mycol"]``)"""
        result: Optional[Expression]

        if isinstance(item, int):
            schema = self._plan.schema()
            if item < -len(schema) or item >= len(schema):
                raise ValueError(f"{item} out of bounds for {schema}")
            result = ExpressionsProjection.from_schema(schema)[item]
            assert result is not None
            return result
        elif isinstance(item, str):
            schema = self._plan.schema()
            field = schema[item]
            return col(field.name)
        elif isinstance(item, Iterable):
            schema = self._plan.schema()

            columns = []
            for it in item:
                if isinstance(it, str):
                    result = col(schema[it].name)
                    columns.append(result)
                elif isinstance(it, int):
                    if it < -len(schema) or it >= len(schema):
                        raise ValueError(f"{it} out of bounds for {schema}")
                    field = list(self._plan.schema())[it]
                    columns.append(col(field.name))
                else:
                    raise ValueError(f"unknown indexing type: {type(it)}")
            return self.select(*columns)
        elif isinstance(item, slice):
            schema = self._plan.schema()
            columns_exprs: ExpressionsProjection = ExpressionsProjection.from_schema(schema)
            selected_columns = columns_exprs[item]
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
        all_exprs = ExpressionsProjection.from_schema(self._plan.schema())
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
        el = ExpressionsProjection([col(e.name) for e in self._plan.schema() if e.name not in names_to_skip])
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
        plan = logical_plan.Filter(self._plan, ExpressionsProjection([predicate]))
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

        prev_schema_as_cols = ExpressionsProjection(
            [col(field.name) for field in self._plan.schema() if field.name != column_name]
        )
        new_schema = prev_schema_as_cols.union(ExpressionsProjection([expr.alias(column_name)]))
        projection = logical_plan.Projection(
            self._plan,
            new_schema,
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
        local_sum_op = logical_plan.LocalAggregate(coalease_op, [col("count")._sum()])
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
            exprs: ExpressionsProjection = ExpressionsProjection([])
        else:
            scheme = logical_plan.PartitionScheme.HASH
            exprs = self.__column_input_to_expression(partition_by)

        repartition_op = logical_plan.Repartition(self._plan, num_partitions=num, partition_by=exprs, scheme=scheme)
        return DataFrame(repartition_op)

    @DataframePublicAPI
    def into_partitions(self, num: int) -> "DataFrame":
        """Splits or coalesces DataFrame to ``num`` partitions. Order is preserved.

        No rebalancing is done; the minimum number of splits or merges are applied.
        (i.e. if there are 2 partitions, and change it into 3, this function will just split the bigger one)

        Example:
            >>> df_with_5_partitions = df.into_partitions(5)

        Args:
            num (int): number of target partitions.

        Returns:
            DataFrame: Dataframe with ``num`` partitions.
        """
        current_partitions = self._plan.num_partitions()

        if num > current_partitions:
            # Do a split (increase the number of partitions).
            split_op = logical_plan.Repartition(
                self._plan,
                num_partitions=num,
                scheme=logical_plan.PartitionScheme.UNKNOWN,
                partition_by=ExpressionsProjection([]),
            )
            return DataFrame(split_op)

        elif num < current_partitions:
            # Do a coalese (decrease the number of partitions).
            coalesce_op = logical_plan.Coalesce(self._plan, num)
            return DataFrame(coalesce_op)

        else:
            return self

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
    def drop_nan(self, *cols: ColumnInputType):
        """drops rows that contains NaNs. If cols is None it will drop rows with any NaN value.
        If column names are supplied, it will drop only those rows that contains NaNs in one of these columns.
        Example:
            >>> df = daft.from_pydict({"a": [1.0, 2.2, 3.5, float("nan")]})
            >>> df.drop_na()  # drops rows where any column contains NaN values
            >>> df = daft.from_pydict({"a": [1.6, 2.5, 3.3, float("nan")]})
            >>> df.drop_na("a")  # drops rows where column a contains NaN values

        Args:
            *cols (str): column names by which rows containings nans/NULLs should be filtered

        Returns:
            DataFrame: DataFrame without NaNs in specified/all columns

        """
        if len(cols) == 0:
            columns = self.__column_input_to_expression(self.column_names)
        else:
            columns = self.__column_input_to_expression(cols)
        float_columns = [
            column
            for column in columns
            if (
                column._to_field(self.schema()).dtype == DataType.float32()
                or column._to_field(self.schema()).dtype == DataType.float64()
            )
        ]

        return self.where(
            ~reduce(
                lambda x, y: x.is_null().if_else(lit(False), x) | y.is_null().if_else(lit(False), y),
                (x.float.is_nan() for x in float_columns),
            )
        )

    @DataframePublicAPI
    def drop_null(self, *cols: ColumnInputType):
        """drops rows that contains NaNs or NULLs. If cols is None it will drop rows with any NULL value.
        If column names are supplied, it will drop only those rows that contains NULLs in one of these columns.
        Example:
            >>> df = daft.from_pydict({"a": [1.0, 2.2, 3.5, float("NaN")]})
            >>> df.drop_null()  # drops rows where any column contains Null/NaN values
            >>> df = daft.from_pydict({"a": [1.6, 2.5, None, float("NaN")]})
            >>> df.drop_null("a")  # drops rows where column a contains Null/NaN values
        Args:
            *cols (str): column names by which rows containings nans should be filtered

        Returns:
            DataFrame: DataFrame without missing values in specified/all columns
        """
        if len(cols) == 0:
            columns = self.__column_input_to_expression(self.column_names)
        else:
            columns = self.__column_input_to_expression(cols)
        return self.where(~reduce(lambda x, y: x | y, (x.is_null() for x in columns)))

    @DataframePublicAPI
    def explode(self, *columns: ColumnInputType) -> "DataFrame":
        """Explodes a List column, where every element in each row's List becomes its own row, and all
        other columns in the DataFrame are duplicated across rows

        If multiple columns are specified, each row must contain the same number of
        items in each specified column.

        Exploding Null values or empty lists will create a single Null entry (see example below).

        Example:
            >>> df = daft.from_pydict({
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
        parsed_exprs = self.__column_input_to_expression(columns)
        return DataFrame(logical_plan.Explode(self._plan, parsed_exprs))

    def _agg(
        self, to_agg: List[Tuple[ColumnInputType, str]], group_by: Optional[ExpressionsProjection] = None
    ) -> "DataFrame":
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
            elif op == "concat":
                builder.add_concat(expr.name(), expr)
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
    def agg_list(self, *cols: ColumnInputType) -> "DataFrame":
        """Performs a global list agg on the DataFrame

        Args:
            *cols (Union[str, Expression]): columns to form into a list
        Returns:
            DataFrame: Globally aggregated list. Should be a single row.
        """
        assert len(cols) > 0, "no columns were passed in"
        return self._agg([(c, "list") for c in cols])

    @DataframePublicAPI
    def agg_concat(self, *cols: ColumnInputType) -> "DataFrame":
        """Performs a global list concatenation agg on the DataFrame

        Args:
            *cols (Union[str, Expression]): columns that are lists to concatenate
        Returns:
            DataFrame: Globally aggregated list. Should be a single row.
        """
        assert len(cols) > 0, "no columns were passed in"
        return self._agg([(c, "concat") for c in cols])

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
    def to_pandas(self) -> "pd.DataFrame":
        """Converts the current DataFrame to a pandas DataFrame.
        If results have not computed yet, collect will be called.

        Returns:
            pd.DataFrame: pandas DataFrame converted from a Daft DataFrame

            .. NOTE::
                This call is **blocking** and will execute the DataFrame when called
        """
        self.collect()
        result = self._result
        assert result is not None

        pd_df = result.to_pandas(schema=self._plan.schema())
        return pd_df

    @DataframePublicAPI
    def to_arrow(self) -> "pa.Table":
        """Converts the current DataFrame to a pyarrow Table.
        If results have not computed yet, collect will be called.

        Returns:
            pyarrow.Table: pyarrow Table converted from a Daft DataFrame

            .. NOTE::
                This call is **blocking** and will execute the DataFrame when called
        """
        self.collect()
        result = self._result
        assert result is not None

        return result.to_arrow()

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
        if not isinstance(partition_set, RayPartitionSet):
            raise ValueError("Cannot convert to Ray Dataset if not running on Ray backend")
        return partition_set.to_ray_dataset()

    @classmethod
    def _from_ray_dataset(cls, ds: "RayDataset") -> "DataFrame":
        """Creates a DataFrame from a Ray Dataset."""
        if get_context().runner_config.name != "ray":
            raise ValueError("Daft needs to be running on the Ray Runner for this operation")

        from daft.runners.ray_runner import RayRunnerIO

        ray_runner_io = get_context().runner().runner_io()
        assert isinstance(ray_runner_io, RayRunnerIO)

        partition_set, schema = ray_runner_io.partition_set_from_ray_dataset(ds)
        cache_entry = get_context().runner().put_partition_set_into_cache(partition_set)
        return cls(
            logical_plan.InMemoryScan(
                cache_entry=cache_entry,
                schema=schema,
                partition_spec=logical_plan.PartitionSpec(
                    logical_plan.PartitionScheme.UNKNOWN, partition_set.num_partitions()
                ),
            )
        )

    @DataframePublicAPI
    def to_dask_dataframe(
        self,
        meta: Union[
            "pd.DataFrame",
            "pd.Series",
            Dict[str, Any],
            Iterable[Any],
            Tuple[Any],
            None,
        ] = None,
    ) -> "dask.DataFrame":
        """Converts the current Daft DataFrame to a Dask DataFrame.

        The returned Dask DataFrame will use `Dask-on-Ray <https://docs.ray.io/en/latest/data/dask-on-ray.html>`__
        to execute operations on a Ray cluster.

        .. NOTE::
            This function can only work if Daft is running using the RayRunner.

        Args:
            meta: An empty pandas DataFrame or Series that matches the dtypes and column
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
        """
        from daft.runners.ray_runner import RayPartitionSet

        self.collect()
        partition_set = self._result
        assert partition_set is not None
        # TODO(Clark): Support Dask DataFrame conversion for the local runner if
        # Dask is using a non-distributed scheduler.
        if not isinstance(partition_set, RayPartitionSet):
            raise ValueError("Cannot convert to Dask DataFrame if not running on Ray backend")
        return partition_set.to_dask_dataframe(meta)

    @classmethod
    @DataframePublicAPI
    def _from_dask_dataframe(cls, ddf: "dask.DataFrame") -> "DataFrame":
        """Creates a Daft DataFrame from a Dask DataFrame."""
        # TODO(Clark): Support Dask DataFrame conversion for the local runner if
        # Dask is using a non-distributed scheduler.
        if get_context().runner_config.name != "ray":
            raise ValueError("Daft needs to be running on the Ray Runner for this operation")

        from daft.runners.ray_runner import RayRunnerIO

        ray_runner_io = get_context().runner().runner_io()
        assert isinstance(ray_runner_io, RayRunnerIO)

        partition_set, schema = ray_runner_io.partition_set_from_dask_dataframe(ddf)
        cache_entry = get_context().runner().put_partition_set_into_cache(partition_set)
        return cls(
            logical_plan.InMemoryScan(
                cache_entry=cache_entry,
                schema=schema,
                partition_spec=logical_plan.PartitionSpec(
                    logical_plan.PartitionScheme.UNKNOWN, partition_set.num_partitions()
                ),
            )
        )


@dataclass
class GroupedDataFrame:
    df: DataFrame
    group_by: ExpressionsProjection

    def __post_init__(self):
        resolved_groupby_schema = self.group_by.resolve_schema(self.df._plan.schema())
        for field, e in zip(resolved_groupby_schema, self.group_by):
            if field.dtype == DataType.null():
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
        groupby_name_set = self.group_by.to_name_set()
        return self.df._agg(
            [(c, "count") for c in self.df.column_names if c not in groupby_name_set], group_by=self.group_by
        )

    def agg_list(self) -> "DataFrame":
        """Performs grouped list on this GroupedDataFrame.

        Returns:
            DataFrame: DataFrame with grouped list per column.
        """
        groupby_name_set = self.group_by.to_name_set()
        return self.df._agg(
            [(c, "list") for c in self.df.column_names if c not in groupby_name_set], group_by=self.group_by
        )

    def agg_concat(self) -> "DataFrame":
        """Performs grouped concat on this GroupedDataFrame.

        Returns:
            DataFrame: DataFrame with grouped concatenated list per column.
        """
        groupby_name_set = self.group_by.to_name_set()
        return self.df._agg(
            [(c, "concat") for c in self.df.column_names if c not in groupby_name_set], group_by=self.group_by
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
