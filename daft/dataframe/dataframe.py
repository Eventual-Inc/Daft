from __future__ import annotations

import io
import uuid
from dataclasses import dataclass
from functools import partial
from typing import (
    IO,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

import pandas
import pyarrow as pa
import pyarrow.parquet as papq
from pyarrow import csv, json

from daft.context import get_context
from daft.dataframe.schema import DataFrameSchema
from daft.datasources import (
    CSVSourceInfo,
    InMemorySourceInfo,
    JSONSourceInfo,
    ParquetSourceInfo,
    StorageType,
)
from daft.execution.operators import ExpressionType
from daft.expressions import ColumnExpression, Expression, col
from daft.filesystem import get_filesystem_from_path
from daft.logical import logical_plan
from daft.logical.schema import ExpressionList
from daft.runners.partitioning import PartitionSet
from daft.viz import DataFrameDisplay

UDFReturnType = TypeVar("UDFReturnType", covariant=True)

ColumnInputType = Union[Expression, str]


def _sample_with_pyarrow(
    loader_func: Callable[[IO], pa.Table],
    filepath: str,
    max_bytes: int = 5 * 1024**2,
) -> ExpressionList:
    fs = get_filesystem_from_path(filepath)
    sampled_bytes = io.BytesIO()
    with fs.open(filepath, compression="infer") as f:
        lines = f.readlines(max_bytes)
        for line in lines:
            sampled_bytes.write(line)
    sampled_bytes.seek(0)
    sampled_tbl = loader_func(sampled_bytes)
    fields = [(field.name, field.type) for field in sampled_tbl.schema]
    schema = ExpressionList(
        [ColumnExpression(name, expr_type=ExpressionType.from_arrow_type(type_)) for name, type_ in fields]
    )
    assert schema is not None, f"Unable to read file {filepath} to determine schema"
    return schema


def _get_filepaths(path: str):
    fs = get_filesystem_from_path(path)
    if fs.isdir(path):
        return fs.ls(path)
    elif fs.isfile(path):
        return [path]
    return fs.expand_path(path, recursive=True)


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
        self._plan = plan
        self._result: Optional[PartitionSet] = None

    def plan(self) -> logical_plan.LogicalPlan:
        """Returns `LogicalPlan` that will be executed to compute the result of this DataFrame.

        Returns:
            logical_plan.LogicalPlan: LogicalPlan to compute this DataFrame.
        """
        return self._plan

    def num_partitions(self) -> int:
        return self._plan.num_partitions()

    def schema(self) -> DataFrameSchema:
        """Returns the DataFrameSchema of the DataFrame, which provides information about each column

        Returns:
            DataFrameSchema: schema of the DataFrame
        """
        return DataFrameSchema.from_expression_list(self._plan.schema())

    def column_names(self) -> List[str]:
        """Returns column names of DataFrame as a list of strings.

        Returns:
            List[str]: Column names of this DataFrame.
        """
        return [expr.name() for expr in self._plan.schema()]

    def show(self, n: int = -1) -> DataFrameDisplay:
        """Executes and displays the executed dataframe as a table

        Args:
            n: number of rows to show. Defaults to -1.

        Returns:
            DataFrameDisplay: object that has a rich tabular display

            .. NOTE::
                This call is **blocking** and will execute the DataFrame when called
        """
        df = self
        if n != -1:
            df = df.limit(n)
        execution_result = df.to_pandas()
        return DataFrameDisplay(execution_result, df.schema())

    def __repr__(self) -> str:
        return self.schema().__repr__()

    def _repr_html_(self) -> str:
        return self.schema()._repr_html_()

    ###
    # Creation methods
    ###

    @classmethod
    def from_pylist(cls, data: List[Dict[str, Any]]) -> DataFrame:
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
        schema = ExpressionList(
            [
                ColumnExpression(header, expr_type=ExpressionType.from_py_type(type(data[0][header])))
                for header in data[0]
            ]
        )
        plan = logical_plan.Scan(
            schema=schema,
            predicate=None,
            columns=None,
            source_info=InMemorySourceInfo(data={header: [row[header] for row in data] for header in data[0]}),
        )
        return cls(plan)

    @classmethod
    def from_pydict(cls, data: Dict[str, Any]) -> DataFrame:
        """Creates a DataFrame from an In-Memory Data columnar source that is passed in as `data`.

        Example:
            >>> df = DataFrame.from_pydict({"foo": [1, 2]})

        Args:
            data: Key -> Sequence[item] of data. Each Key is created as a column.

        Returns:
            DataFrame: DataFrame created from dictionary of columns
        """
        schema = ExpressionList(
            [ColumnExpression(header, expr_type=ExpressionType.from_py_type(type(data[header][0]))) for header in data]
        )
        plan = logical_plan.Scan(
            schema=schema,
            predicate=None,
            columns=None,
            source_info=InMemorySourceInfo(data=data),
        )
        return cls(plan)

    @classmethod
    def from_json(
        cls,
        path: str,
    ) -> DataFrame:
        """Creates a DataFrame from line-delimited JSON file(s)

        Example:
            >>> df = DataFrame.from_json("/path/to/file.json")
            >>> df = DataFrame.from_json("/path/to/directory")
            >>> df = DataFrame.from_json("/path/to/files-*.json")
            >>> df = DataFrame.from_json("s3://path/to/files-*.json")

        Args:
            path (str): Path to JSON files (allows for wildcards)

        returns:
            DataFrame: parsed DataFrame
        """
        filepaths = _get_filepaths(path)

        if len(filepaths) == 0:
            raise ValueError(f"No JSON files found at {path}")

        schema = _sample_with_pyarrow(json.read_json, filepaths[0])

        plan = logical_plan.Scan(
            schema=schema,
            predicate=None,
            columns=None,
            source_info=JSONSourceInfo(filepaths=filepaths),
        )
        return cls(plan)

    @classmethod
    def from_csv(
        cls,
        path: str,
        has_headers: bool = True,
        column_names: Optional[List[str]] = None,
        delimiter: str = ",",
    ) -> DataFrame:
        """Creates a DataFrame from CSV file(s)

        Example:
            >>> df = DataFrame.from_csv("/path/to/file.csv")
            >>> df = DataFrame.from_csv("/path/to/directory")
            >>> df = DataFrame.from_csv("/path/to/files-*.csv")
            >>> df = DataFrame.from_csv("s3://path/to/files-*.csv")

        Args:
            path (str): Path to CSV (allows for wildcards)
            has_headers (bool): Whether the CSV has a header or not, defaults to True
            column_names (Optional[List[str]]): Custom column names to assign to the DataFrame, defaults to None
            delimiter (Str): Delimiter used in the CSV, defaults to ","

        returns:
            DataFrame: parsed DataFrame
        """
        filepaths = _get_filepaths(path)

        if len(filepaths) == 0:
            raise ValueError(f"No CSV files found at {path}")

        schema = _sample_with_pyarrow(
            partial(
                csv.read_csv,
                parse_options=csv.ParseOptions(
                    delimiter=delimiter,
                ),
                read_options=csv.ReadOptions(
                    # Column names will be read from the first CSV row if column_names is None/empty and has_headers
                    autogenerate_column_names=(not has_headers) and (column_names is None),
                    column_names=column_names,
                    # If user specifies that CSV has headers, and also provides column names, we skip the header row
                    skip_rows_after_names=1 if has_headers and column_names is not None else 0,
                ),
            ),
            filepaths[0],
        )
        plan = logical_plan.Scan(
            schema=schema,
            predicate=None,
            columns=None,
            source_info=CSVSourceInfo(
                filepaths=filepaths,
                delimiter=delimiter,
                has_headers=has_headers,
            ),
        )
        return cls(plan)

    @classmethod
    def from_parquet(cls, path: str) -> DataFrame:
        """Creates a DataFrame from Parquet file(s)

        Example:
            >>> df = DataFrame.from_parquet("/path/to/file.parquet")
            >>> df = DataFrame.from_parquet("/path/to/directory")
            >>> df = DataFrame.from_parquet("/path/to/files-*.parquet")
            >>> df = DataFrame.from_parquet("s3://path/to/files-*.parquet")

        Args:
            path (str): Path to Parquet file (allows for wildcards)

        returns:
            DataFrame: parsed DataFrame
        """
        filepaths = _get_filepaths(path)

        if len(filepaths) == 0:
            raise ValueError(f"No Parquet files found at {path}")

        filepath = filepaths[0]
        fs = get_filesystem_from_path(filepath)
        with fs.open(filepath, "rb") as f:
            # Read first Parquet file to ascertain schema
            schema = ExpressionList(
                [
                    ColumnExpression(field.name, expr_type=ExpressionType.from_arrow_type(field.type))
                    for field in papq.ParquetFile(f).metadata.schema.to_arrow_schema()
                ]
            )

        plan = logical_plan.Scan(
            schema=schema,
            predicate=None,
            columns=None,
            source_info=ParquetSourceInfo(
                filepaths=filepaths,
            ),
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
        self, root_dir: str, compression: str = "snappy", partition_cols: Optional[List[ColumnInputType]] = None
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
        cols: Optional[ExpressionList] = None
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
        data = write_df._result.to_pydict()
        return DataFrame.from_pydict(data)

    def write_csv(self, root_dir: str, partition_cols: Optional[List[ColumnInputType]] = None) -> DataFrame:
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
        cols: Optional[ExpressionList] = None
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
        data = write_df._result.to_pydict()
        return DataFrame.from_pydict(data)

    ###
    # DataFrame operations
    ###

    def __column_input_to_expression(self, columns: Iterable[ColumnInputType]) -> ExpressionList:
        expressions = [col(c) if isinstance(c, str) else c for c in columns]
        return ExpressionList(expressions)

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
        gb = self.groupby(*[col(e.name()) for e in all_exprs])
        first_e_name = [e.name() for e in all_exprs][0]
        dummy_col_name = str(uuid.uuid4())
        return gb.agg([(col(first_e_name).alias(dummy_col_name), "min")]).exclude(dummy_col_name)

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

    def sort(
        self, by: Union[ColumnInputType, List[ColumnInputType]], desc: Union[bool, List[bool]] = False
    ) -> DataFrame:
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
        on: Optional[Union[List[ColumnInputType], ColumnInputType]] = None,
        left_on: Optional[Union[List[ColumnInputType], ColumnInputType]] = None,
        right_on: Optional[Union[List[ColumnInputType], ColumnInputType]] = None,
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
            exprs_to_explode,
        )
        return DataFrame(explode_op)

    def _agg(self, to_agg: List[Tuple[ColumnInputType, str]], group_by: Optional[ExpressionList] = None) -> DataFrame:
        assert len(to_agg) > 0, "no columns to aggregate."
        exprs_to_agg = self.__column_input_to_expression(tuple(e for e, _ in to_agg))
        ops = [op for _, op in to_agg]

        function_lookup = {
            "sum": Expression._sum,
            "count": Expression._count,
            "min": Expression._min,
            "max": Expression._max,
            "count": Expression._count,
        }
        intermediate_ops = {
            "sum": ("sum",),
            "count": ("count",),
            "mean": ("sum", "count"),
            "min": ("min",),
            "max": ("max",),
        }

        reduction_ops = {"sum": ("sum",), "count": ("sum",), "mean": ("sum", "sum"), "min": ("min",), "max": ("max",)}

        finalizer_ops_funcs = {"mean": lambda x, y: (x + 0.0) / (y + 0.0)}

        first_phase_ops: List[Tuple[Expression, str]] = []
        second_phase_ops: List[Tuple[Expression, str]] = []
        finalizer_phase_ops: List[Expression] = []
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

    def groupby(self, *group_by: ColumnInputType) -> GroupedDataFrame:
        """Performs a GroupBy on the DataFrame for Aggregation.

        Args:
            *group_by (Union[str, Expression]): columns to group by

        Returns:
            GroupedDataFrame: DataFrame to Aggregate
        """
        return GroupedDataFrame(self, self.__column_input_to_expression(group_by))

    def collect(self) -> DataFrame:
        """Computes LogicalPlan to materialize DataFrame. This is a blocking operation.

        Returns:
            DataFrame: DataFrame with cached results.
        """
        context = get_context()
        if self._result is None:
            self._result = context.runner().run(self._plan)
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
        assert self._result is not None
        pd_df = self._result.to_pandas(schema=self._plan.schema())
        del self._result
        self._result = None
        return pd_df


@dataclass
class GroupedDataFrame:
    df: DataFrame
    group_by: ExpressionList

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

    def agg(self, to_agg: List[Tuple[ColumnInputType, str]]) -> DataFrame:
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
