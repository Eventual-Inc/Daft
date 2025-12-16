# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations
#
# This file uses strings for forward type annotations in public APIs,
# in order to support runtime typechecking across different Python versions.
# For technical details, see https://github.com/Eventual-Inc/Daft/pull/630

import io
import multiprocessing
import os
import pathlib
import typing
import warnings
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import partial, reduce
from typing import TYPE_CHECKING, Any, Callable, Concatenate, Literal, Optional, ParamSpec, TypeVar, Union, overload

from daft.api_annotations import DataframePublicAPI
from daft.context import get_context
from daft.convert import InputListType
from daft.daft import DistributedPhysicalPlan, FileFormat, IOConfig, JoinStrategy, JoinType, WriteMode
from daft.dataframe.display import MermaidOptions
from daft.dataframe.preview import Preview, PreviewAlign, PreviewColumn, PreviewFormat, PreviewFormatter
from daft.datatype import DataType
from daft.errors import ExpressionTypeError
from daft.execution.native_executor import NativeExecutor
from daft.expressions import Expression, ExpressionsProjection, col, lit
from daft.logical.builder import LogicalPlanBuilder
from daft.recordbatch import MicroPartition
from daft.runners import get_or_create_runner
from daft.runners.partitioning import (
    LocalPartitionSet,
    MaterializedResult,
    PartitionCacheEntry,
    PartitionSet,
    PartitionT,
)
from daft.utils import ColumnInputType, ManyColumnsInputType, column_inputs_to_expressions, in_notebook

if TYPE_CHECKING:
    import dask
    import deltalake
    import pandas
    import pyarrow
    import pyiceberg
    import ray
    import torch

    from daft.io import DataSink
    from daft.io.catalog import DataCatalogTable
    from daft.io.sink import WriteResultType
    from daft.unity_catalog import UnityCatalogTable

from daft.schema import Schema

UDFReturnType = TypeVar("UDFReturnType", covariant=True)
T = TypeVar("T")
R = TypeVar("R")
P = ParamSpec("P")


def to_logical_plan_builder(*parts: MicroPartition) -> LogicalPlanBuilder:
    """Creates a Daft DataFrame from a single RecordBatch.

    Args:
        parts: The Tables that we wish to convert into a Daft DataFrame.

    Returns:
        DataFrame: Daft DataFrame created from the provided Table.
    """
    if not parts:
        raise ValueError("Can't create a DataFrame from an empty list of tables.")

    result_pset = LocalPartitionSet()

    for i, part in enumerate(parts):
        result_pset.set_partition_from_table(i, part)

    cache_entry = get_or_create_runner().put_partition_set_into_cache(result_pset)
    size_bytes = result_pset.size_bytes()
    num_rows = len(result_pset)

    assert size_bytes is not None, "In-memory data should always have non-None size in bytes"
    return LogicalPlanBuilder.from_in_memory_scan(
        cache_entry, parts[0].schema(), result_pset.num_partitions(), size_bytes, num_rows=num_rows
    )


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class DataFrame:
    """A Daft DataFrame is a table of data.

    It has columns, where each column has a type and the same number of items (rows) as all other columns.
    """

    def __init__(self, builder: LogicalPlanBuilder) -> None:
        """Constructs a DataFrame according to a given LogicalPlan.

        Users are expected instead to call the classmethods on DataFrame to create a DataFrame.

        Args:
            builder: LogicalPlan describing the steps required to arrive at this DataFrame
        """
        if not isinstance(builder, LogicalPlanBuilder):
            if isinstance(builder, dict):
                raise ValueError(
                    "DataFrames should be constructed with a dictionary of columns using `daft.from_pydict`"
                )
            if isinstance(builder, list):
                raise ValueError(
                    "DataFrames should be constructed with a list of dictionaries using `daft.from_pylist`"
                )
            raise ValueError(f"Expected DataFrame to be constructed with a LogicalPlanBuilder, received: {builder}")

        self.__builder = builder
        self._result_cache: Optional[PartitionCacheEntry] = None
        self._preview = Preview(partition=None, total_rows=None)
        self._num_preview_rows = get_context().daft_execution_config.num_preview_rows

    @property
    def _builder(self) -> LogicalPlanBuilder:
        if self._result_cache is None:
            return self.__builder
        else:
            num_partitions = self._result_cache.num_partitions()
            size_bytes = self._result_cache.size_bytes()
            num_rows = self._result_cache.num_rows()

            # Partition set should always be set on cache entry.
            assert (
                num_partitions is not None and size_bytes is not None and num_rows is not None
            ), "Partition set should always be set on cache entry"

            return self.__builder.from_in_memory_scan(
                self._result_cache,
                self.__builder.schema(),
                num_partitions=num_partitions,
                size_bytes=size_bytes,
                num_rows=num_rows,
            )

    def _get_current_builder(self) -> LogicalPlanBuilder:
        """Returns the current logical plan builder, without any caching optimizations."""
        return self.__builder

    @property
    def _result(self) -> Optional[PartitionSet[PartitionT]]:
        if self._result_cache is None:
            return None
        else:
            return self._result_cache.value

    def pipe(
        self,
        function: Callable[Concatenate["DataFrame", P], T],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        """Apply the function to this DataFrame.

        Args:
            function (Callable[Concatenate["DataFrame", P], T]): Function to apply.
            *args (P.args): Positional arguments to pass to the function.
            **kwargs (P.kwargs): Keyword arguments to pass to the function.

        Returns:
            Result of applying the function on this DataFrame.

        Examples:
            >>> import daft
            >>>
            >>> df = daft.from_pydict({"x": [1, 2, 3]})
            >>>
            >>> def square(df, column: str):
            ...     return df.select((df[column] * df[column]).alias(column))
            >>>
            >>> df.pipe(square, "x").show()
            ╭───────╮
            │ x     │
            │ ---   │
            │ Int64 │
            ╞═══════╡
            │ 1     │
            ├╌╌╌╌╌╌╌┤
            │ 4     │
            ├╌╌╌╌╌╌╌┤
            │ 9     │
            ╰───────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)
        """
        return function(self, *args, **kwargs)

    @DataframePublicAPI
    def explain(
        self, show_all: bool = False, format: str = "ascii", simple: bool = False, file: Optional[io.IOBase] = None
    ) -> Any:
        r"""Prints the (logical and physical) plans that will be executed to produce this DataFrame.

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

        Examples:
            >>> import daft
            >>>
            >>> df = daft.from_pydict({"x": [1, 2, 3]})
            >>>
            >>> def double(df, column: str):
            ...     return df.select((df[column] * df[column]).alias(column))
            >>>
            >>> df = df.pipe(double, "x")
            >>>
            >>> df.explain()
            == Unoptimized Logical Plan ==
            <BLANKLINE>
            * Project: col(x) * col(x) as x
            |
            * Source:
            |   Number of partitions = 1
            |   Output schema = x#Int64
            <BLANKLINE>
            <BLANKLINE>
            <BLANKLINE>
            Set `show_all=True` to also see the Optimized and Physical plans. This will run the query optimizer.

        """
        is_cached = self._result_cache is not None
        if format == "mermaid":
            from daft.dataframe.display import MermaidFormatter
            from daft.utils import in_notebook

            instance = MermaidFormatter(self.__builder, show_all, simple, is_cached)
            if file is not None:
                # if we are printing to a file, we print the markdown representation of the plan
                text = instance._repr_markdown_()
                print(text, file=file)
            if in_notebook():
                # if in a notebook, we return the class instance and let jupyter display it
                return instance
            else:
                # if we are not in a notebook, we return the raw markdown instead of the class instance
                return repr(instance)

        print_to_file = partial(print, file=file)

        if self._result_cache is not None:
            print_to_file("Result is cached and will skip computation\n")
            print_to_file(self._builder.pretty_print(simple, format=format))

            print_to_file("However here is the logical plan used to produce this result:\n", file=file)

        builder = self.__builder
        print_to_file("== Unoptimized Logical Plan ==\n")
        print_to_file(builder.pretty_print(simple, format=format))
        if show_all:
            print_to_file("\n== Optimized Logical Plan ==\n")
            execution_config = get_context().daft_execution_config
            builder = builder.optimize(execution_config)
            print_to_file(builder.pretty_print(simple))
            print_to_file("\n== Physical Plan ==\n")
            if get_or_create_runner().name != "native":
                from daft.daft import DistributedPhysicalPlan

                distributed_plan = DistributedPhysicalPlan.from_logical_plan_builder(
                    builder._builder, "<tmp>", execution_config
                )
                if format == "ascii":
                    print_to_file(distributed_plan.repr_ascii(simple))
                elif format == "mermaid":
                    print_to_file(distributed_plan.repr_mermaid(MermaidOptions(simple)))
            else:
                native_executor = NativeExecutor()
                print_to_file(
                    native_executor.pretty_print(builder, get_context().daft_execution_config, simple, format=format)
                )
        else:
            print_to_file(
                "\n \nSet `show_all=True` to also see the Optimized and Physical plans. This will run the query optimizer.",
            )
        return None

    def num_partitions(self) -> int | None:
        """Returns the number of partitions that will be used to execute this DataFrame.

        The query optimizer may change the partitioning strategy. This method runs the optimizer
        and then inspects the resulting physical plan scheduler to determine how many partitions
        the execution will use.

        Returns:
            int: The number of partitions in the optimized physical execution plan.

        Examples:
            >>> import daft
            >>>
            >>> daft.set_runner_ray()  # doctest: +SKIP
            >>>
            >>> # Create a DataFrame with 1000 rows
            >>> df = daft.from_pydict({"x": list(range(1000))})
            >>>
            >>> # Partition count may depend on default config or optimizer decisions
            >>> df.num_partitions()  # doctest: +SKIP
            1
            >>>
            >>> # You can repartition manually (if supported), and then inspect again:
            >>> df2 = df.repartition(10)  # doctest: +SKIP
            >>> df2.num_partitions()  # doctest: +SKIP
            10
        """
        runner_name = get_or_create_runner().name
        # Native runner does not support num_partitions
        if runner_name == "native":
            return None
        else:
            execution_config = get_context().daft_execution_config
            optimized = self._builder.optimize(execution_config)
            distributed_plan = DistributedPhysicalPlan.from_logical_plan_builder(
                optimized._builder, "<tmp>", execution_config
            )
            return distributed_plan.num_partitions()

    @DataframePublicAPI
    def schema(self) -> Schema:
        """Returns the Schema of the DataFrame, which provides information about each column, as a Python object.

        Returns:
            Schema: schema of the DataFrame

        Examples:
            >>> import daft
            >>>
            >>> df = daft.from_pydict({"x": [1, 2, 3], "y": ["a", "b", "c"]})
            >>> df.schema()
            ╭─────────────┬────────╮
            │ column_name ┆ type   │
            ╞═════════════╪════════╡
            │ x           ┆ Int64  │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ y           ┆ String │
            ╰─────────────┴────────╯
            <BLANKLINE>
        """
        return self.__builder.schema()

    @property
    def column_names(self) -> list[str]:
        """Returns column names of DataFrame as a list of strings.

        Returns:
            List[str]: Column names of this DataFrame.
        """
        return self.__builder.schema().column_names()

    @property
    def columns(self) -> list[Expression]:
        """Returns column of DataFrame as a list of Expressions.

        Returns:
            List[Expression]: Columns of this DataFrame.
        """
        return [col(field.name) for field in self.__builder.schema()]

    @DataframePublicAPI
    def __iter__(self) -> Iterator[dict[str, Any]]:
        """Alias of `self.iter_rows()` with default arguments for convenient access of data.

        Returns:
            Iterator[dict[str, Any]]: An iterator over the rows of the DataFrame, where each row is a dictionary
            mapping column names to values.

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"foo": [1, 2, 3], "bar": ["a", "b", "c"]})
            >>> for row in df:
            ...     print(row)
            {'foo': 1, 'bar': 'a'}
            {'foo': 2, 'bar': 'b'}
            {'foo': 3, 'bar': 'c'}

        Tip:
            See also [`df.iter_rows()`][daft.DataFrame.iter_rows]: iterator over rows with more options
        """
        return self.iter_rows(results_buffer_size=None)

    @DataframePublicAPI
    def iter_rows(
        self,
        results_buffer_size: Union[Optional[int], Literal["num_cpus"]] = "num_cpus",
        column_format: Literal["python", "arrow"] = "python",
    ) -> Iterator[dict[str, Any]]:
        """Return an iterator of rows for this dataframe.

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

        Examples:
            >>> import daft
            >>>
            >>> df = daft.from_pydict({"foo": [1, 2, 3], "bar": ["a", "b", "c"]})
            >>> for row in df.iter_rows():
            ...     print(row)
            {'foo': 1, 'bar': 'a'}
            {'foo': 2, 'bar': 'b'}
            {'foo': 3, 'bar': 'c'}

        Tip:
            See also [`df.iter_partitions()`][daft.DataFrame.iter_partitions]: iterator over entire partitions instead of single rows
        """
        if results_buffer_size == "num_cpus":
            results_buffer_size = multiprocessing.cpu_count()

        def arrow_iter_rows(table: "pyarrow.Table") -> Iterator[dict[str, Any]]:
            columns = table.columns
            for i in range(len(table)):
                row = {col._name: col[i] for col in columns}
                yield row

        def python_iter_rows(pydict: dict[str, list[Any]], num_rows: int) -> Iterator[dict[str, Any]]:
            for i in range(num_rows):
                row = {key: value[i] for (key, value) in pydict.items()}
                yield row

        if self._result is not None:
            # If the dataframe has already finished executing,
            # use the precomputed results.
            if column_format == "python":
                yield from python_iter_rows(self.to_pydict(), len(self))
            elif column_format == "arrow":
                yield from arrow_iter_rows(self.to_arrow())
            else:
                raise ValueError(
                    f"Unsupported column_format: {column_format}, supported formats are 'python' and 'arrow'"
                )
        else:
            # Execute the dataframe in a streaming fashion.
            partitions_iter = get_or_create_runner().run_iter_tables(
                self._builder, results_buffer_size=results_buffer_size
            )

            # Iterate through partitions.
            for partition in partitions_iter:
                if column_format == "python":
                    yield from python_iter_rows(partition.to_pydict(), len(partition))
                elif column_format == "arrow":
                    yield from arrow_iter_rows(partition.to_arrow())
                else:
                    raise ValueError(
                        f"Unsupported column_format: {column_format}, supported formats are 'python' and 'arrow'"
                    )

    @DataframePublicAPI
    def to_arrow_iter(
        self,
        results_buffer_size: Union[Optional[int], Literal["num_cpus"]] = "num_cpus",
    ) -> Iterator["pyarrow.RecordBatch"]:
        """Return an iterator of pyarrow recordbatches for this dataframe.

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

        Examples:
            >>> import daft
            >>>
            >>> df = daft.from_pydict({"foo": [1, 2, 3], "bar": ["a", "b", "c"]})
            >>> for batch in df.to_arrow_iter():
            ...     print(batch)
            pyarrow.RecordBatch
            foo: int64
            bar: large_string
            ----
            foo: [1,2,3]
            bar: ["a","b","c"]
        """
        if results_buffer_size == "num_cpus":
            results_buffer_size = multiprocessing.cpu_count()
        if results_buffer_size is not None and not results_buffer_size > 0:
            raise ValueError(f"Provided `results_buffer_size` value must be > 0, received: {results_buffer_size}")

        results = self._result
        if results is not None:
            # If the dataframe has already finished executing,
            # use the precomputed results.

            for _, result in results.items():
                yield from (result.micropartition().to_arrow().to_batches())
        else:
            # Execute the dataframe in a streaming fashion.
            partitions_iter = get_or_create_runner().run_iter_tables(
                self._builder, results_buffer_size=results_buffer_size
            )

            # Iterate through partitions.
            for partition in partitions_iter:
                yield from partition.to_arrow().to_batches()

    @DataframePublicAPI
    def iter_partitions(
        self, results_buffer_size: Union[Optional[int], Literal["num_cpus"]] = "num_cpus"
    ) -> Iterator[Union[MicroPartition, "ray.ObjectRef"]]:
        """Begin executing this dataframe and return an iterator over the partitions.

        Each partition will be returned as a daft.recordbatch object (if using Python runner backend)
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

        Examples:
            >>> import daft
            >>>
            >>> daft.set_runner_ray()  # doctest: +SKIP
            >>>
            >>> df = daft.from_pydict({"foo": [1, 2, 3], "bar": ["a", "b", "c"]}).into_partitions(2)
            >>> for part in df.iter_partitions():
            ...     print(part)  # doctest: +SKIP
            MicroPartition with 3 rows:
            TableState: Loaded. 1 tables
            ╭───────┬────────╮
            │ foo   ┆ bar    │
            │ ---   ┆ ---    │
            │ Int64 ┆ String │
            ╞═══════╪════════╡
            │ 1     ┆ a      │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ 2     ┆ b      │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ 3     ┆ c      │
            ╰───────┴────────╯
            <BLANKLINE>
            <BLANKLINE>
            Statistics: missing
        """
        if results_buffer_size == "num_cpus":
            results_buffer_size = multiprocessing.cpu_count()
        elif results_buffer_size is not None and not results_buffer_size > 0:
            raise ValueError(f"Provided `results_buffer_size` value must be > 0, received: {results_buffer_size}")

        results = self._result
        if results is not None:
            # If the dataframe has already finished executing,
            # use the precomputed results.
            for mat_result in results.values():
                yield mat_result.partition()

        else:
            # Execute the dataframe in a streaming fashion.
            results_iter: Iterator[MaterializedResult[Any]] = get_or_create_runner().run_iter(
                self._builder, results_buffer_size=results_buffer_size
            )
            for result in results_iter:
                yield result.partition()

    def _populate_preview(self) -> None:
        """Populates the preview of the DataFrame, if it is not already populated."""
        results = self._result
        if results is None:
            return

        num_preview_rows = min(self._num_preview_rows, len(self))
        preview_partition_invalid = self._preview.partition is None or len(self._preview.partition) != num_preview_rows
        if preview_partition_invalid:
            preview_parts = results._get_preview_micropartitions(num_preview_rows)
            preview_results = LocalPartitionSet()
            for i, part in enumerate(preview_parts):
                preview_results.set_partition_from_table(i, part)
            preview_partition = preview_results._get_merged_micropartition(self.schema())
            self._preview = Preview(
                partition=preview_partition,
                total_rows=len(self),
            )

    @DataframePublicAPI
    def __repr__(self) -> str:
        self._populate_preview()
        preview = PreviewFormatter(self._preview, self.schema())
        return preview.__repr__()

    @DataframePublicAPI
    def _repr_html_(self) -> str:
        self._populate_preview()
        preview = PreviewFormatter(self._preview, self.schema())
        try:
            if in_notebook() and self._preview.partition is not None:
                try:
                    interactive_html = preview._generate_interactive_html()
                    return interactive_html
                except Exception:
                    pass

            return preview._repr_html_()
        except ImportError:
            return preview._repr_html_()

    ###
    # Creation methods
    ###

    @classmethod
    def _from_pylist(cls, data: list[dict[str, Any]]) -> "DataFrame":
        """Creates a DataFrame from a list of dictionaries."""
        headers: set[str] = set()
        for row in data:
            if not isinstance(row, dict):
                raise ValueError(f"Expected list of dictionaries of {{column_name: value}}, received: {type(row)}")
            headers.update(row.keys())
        headers_ordered = sorted(list(headers))
        return cls._from_pydict(data={header: [row.get(header, None) for row in data] for header in headers_ordered})

    @classmethod
    def _from_pydict(cls, data: Mapping[str, InputListType]) -> "DataFrame":
        """Creates a DataFrame from a Python dictionary."""
        column_lengths = {key: len(data[key]) for key in data}
        if len(set(column_lengths.values())) > 1:
            raise ValueError(
                f"Expected all columns to be of the same length, but received columns with lengths: {column_lengths}"
            )

        data_micropartition = MicroPartition.from_pydict(data)
        return cls._from_micropartitions(data_micropartition)

    @classmethod
    def _from_arrow(cls, data: Union["pyarrow.Table", list["pyarrow.Table"], Iterable["pyarrow.Table"]]) -> "DataFrame":
        """Creates a DataFrame from a `pyarrow Table <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html>`__."""
        if isinstance(data, Iterable):
            data = list(data)
        if not isinstance(data, list):
            data = [data]
        parts = [MicroPartition.from_arrow(table) for table in data]
        return cls._from_micropartitions(*parts)

    @classmethod
    def _from_pandas(cls, data: Union["pandas.DataFrame", list["pandas.DataFrame"]]) -> "DataFrame":
        """Creates a Daft DataFrame from a `pandas DataFrame <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`__."""
        if not isinstance(data, list):
            data = [data]
        parts = [MicroPartition.from_pandas(df) for df in data]
        return cls._from_micropartitions(*parts)

    @classmethod
    def _from_micropartitions(cls, *parts: MicroPartition) -> "DataFrame":
        """Creates a Daft DataFrame from MicroPartition(s).

        Args:
            parts: The Tables that we wish to convert into a Daft DataFrame.

        Returns:
            DataFrame: Daft DataFrame created from the provided Table.
        """
        if not parts:
            raise ValueError("Can't create a DataFrame from an empty list of tables.")

        result_pset = LocalPartitionSet()

        for i, part in enumerate(parts):
            result_pset.set_partition_from_table(i, part)

        cache_entry = get_or_create_runner().put_partition_set_into_cache(result_pset)
        size_bytes = result_pset.size_bytes()
        num_rows = len(result_pset)

        assert size_bytes is not None, "In-memory data should always have non-None size in bytes"
        builder = LogicalPlanBuilder.from_in_memory_scan(
            cache_entry, parts[0].schema(), result_pset.num_partitions(), size_bytes, num_rows=num_rows
        )

        df = cls(builder)
        df._result_cache = cache_entry

        # build preview
        df._populate_preview()
        return df

    @classmethod
    def _from_schema(cls, schema: Schema) -> "DataFrame":
        """Creates a Daft DataFrom from a Schema.

        Args:
            schema: The Schema to convert into a DataFrame.

        Returns:
            DataFrame: Daft DataFrame with "column_name" and "type" fields.
        """
        pydict: dict[str, list[str]] = {"column_name": [], "type": []}
        for field in schema:
            pydict["column_name"].append(field.name)
            pydict["type"].append(str(field.dtype))
        return DataFrame._from_pydict(pydict)

    ###
    # Write methods
    ###

    @DataframePublicAPI
    def write_parquet(
        self,
        root_dir: Union[str, pathlib.Path],
        compression: str = "snappy",
        write_mode: Literal["append", "overwrite", "overwrite-partitions"] = "append",
        partition_cols: Optional[list[ColumnInputType]] = None,
        io_config: Optional[IOConfig] = None,
    ) -> "DataFrame":
        """Writes the DataFrame as parquet files, returning a new DataFrame with paths to the files that were written.

        Files will be written to `<root_dir>/*` with randomly generated UUIDs as the file names.

        Args:
            root_dir (str): root file path to write parquet files to.
            compression (str, optional): compression algorithm. Defaults to "snappy".
            write_mode (str, optional): Operation mode of the write. `append` will add new data, `overwrite` will replace the contents of the root directory with new data. `overwrite-partitions` will replace only the contents in the partitions that are being written to. Defaults to "append".
            partition_cols (Optional[List[ColumnInputType]], optional): How to subpartition each partition further. Defaults to None.
            io_config (Optional[IOConfig], optional): configurations to use when interacting with remote storage.

        Returns:
            DataFrame: The filenames that were written out as strings.

        Note:
            This call is **blocking** and will execute the DataFrame when called

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1, 2, 3], "y": ["a", "b", "c"]})
            >>> df.write_parquet("output_dir", write_mode="overwrite")  # doctest: +SKIP

        Tip:
            See also [`df.write_csv()`][daft.DataFrame.write_csv] and [`df.write_json()`][daft.DataFrame.write_json]
            Other formats for writing DataFrames
        """
        if write_mode not in ["append", "overwrite", "overwrite-partitions"]:
            raise ValueError(
                f"Only support `append`, `overwrite`, or `overwrite-partitions` mode. {write_mode} is unsupported"
            )
        if write_mode == "overwrite-partitions" and partition_cols is None:
            raise ValueError("Partition columns must be specified to use `overwrite-partitions` mode.")

        io_config = get_context().daft_planning_config.default_io_config if io_config is None else io_config

        cols: Optional[list[Expression]] = None
        if partition_cols is not None:
            cols = self.__column_input_to_expression(tuple(partition_cols))

        builder = self._builder.write_tabular(
            root_dir=root_dir,
            partition_cols=cols,
            write_mode=WriteMode.from_str(write_mode),
            file_format=FileFormat.Parquet,
            compression=compression,
            io_config=io_config,
        )
        # Block and write, then retrieve data
        write_df = DataFrame(builder)
        write_df.collect()
        assert write_df._result is not None

        # Populate and return a new disconnected DataFrame
        result_df = DataFrame(write_df._builder)
        result_df._result_cache = write_df._result_cache
        result_df._preview = write_df._preview
        return result_df

    @DataframePublicAPI
    def write_csv(
        self,
        root_dir: Union[str, pathlib.Path],
        write_mode: Literal["append", "overwrite", "overwrite-partitions"] = "append",
        partition_cols: Optional[list[ColumnInputType]] = None,
        io_config: Optional[IOConfig] = None,
    ) -> "DataFrame":
        """Writes the DataFrame as CSV files, returning a new DataFrame with paths to the files that were written.

        Files will be written to `<root_dir>/*` with randomly generated UUIDs as the file names.

        Args:
            root_dir (str): root file path to write parquet files to.
            write_mode (str, optional): Operation mode of the write. `append` will add new data, `overwrite` will replace the contents of the root directory with new data. `overwrite-partitions` will replace only the contents in the partitions that are being written to. Defaults to "append".
            partition_cols (Optional[List[ColumnInputType]], optional): How to subpartition each partition further. Defaults to None.
            io_config (Optional[IOConfig], optional): configurations to use when interacting with remote storage.

        Returns:
            DataFrame: The filenames that were written out as strings.

        Note:
            This call is **blocking** and will execute the DataFrame when called

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1, 2, 3], "y": ["a", "b", "c"]})
            >>> df.write_csv("output_dir", write_mode="overwrite")  # doctest: +SKIP

        Tip:
            See also [`df.write_parquet()`][daft.DataFrame.write_parquet] and [`df.write_json()`][daft.DataFrame.write_json]
            other formats for writing DataFrames

        """
        if write_mode not in ["append", "overwrite", "overwrite-partitions"]:
            raise ValueError(
                f"Only support `append`, `overwrite`, or `overwrite-partitions` mode. {write_mode} is unsupported"
            )
        if write_mode == "overwrite-partitions" and partition_cols is None:
            raise ValueError("Partition columns must be specified to use `overwrite-partitions` mode.")

        io_config = get_context().daft_planning_config.default_io_config if io_config is None else io_config

        cols: Optional[list[Expression]] = None
        if partition_cols is not None:
            cols = self.__column_input_to_expression(tuple(partition_cols))

        builder = self._builder.write_tabular(
            root_dir=root_dir,
            partition_cols=cols,
            write_mode=WriteMode.from_str(write_mode),
            file_format=FileFormat.Csv,
            io_config=io_config,
        )

        # Block and write, then retrieve data
        write_df = DataFrame(builder)
        write_df.collect()
        assert write_df._result is not None

        # Populate and return a new disconnected DataFrame
        result_df = DataFrame(write_df._builder)
        result_df._result_cache = write_df._result_cache
        result_df._preview = write_df._preview
        return result_df

    @DataframePublicAPI
    def write_json(
        self,
        root_dir: Union[str, pathlib.Path],
        write_mode: Literal["append", "overwrite", "overwrite-partitions"] = "append",
        partition_cols: Optional[list[ColumnInputType]] = None,
        io_config: Optional[IOConfig] = None,
    ) -> "DataFrame":
        """Writes the DataFrame as JSON files, returning a new DataFrame with paths to the files that were written.

        Files will be written to `<root_dir>/*` with randomly generated UUIDs as the file names.

        Args:
            root_dir (str): root file path to write JSON files to.
            write_mode (str, optional): Operation mode of the write. `append` will add new data, `overwrite` will replace the contents of the root directory with new data. `overwrite-partitions` will replace only the contents in the partitions that are being written to. Defaults to "append".
            partition_cols (Optional[List[ColumnInputType]], optional): How to subpartition each partition further. Defaults to None.
            io_config (Optional[IOConfig], optional): configurations to use when interacting with remote storage.

        Returns:
            DataFrame: The filenames that were written out as strings.

        Note:
            This call is **blocking** and will execute the DataFrame when called

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1, 2, 3], "y": ["a", "b", "c"]})
            >>> df.write_json("output_dir", write_mode="overwrite")  # doctest: +SKIP

        Warning:
            Currently only supported with the Native runner!
        """
        if write_mode not in ["append", "overwrite", "overwrite-partitions"]:
            raise ValueError(
                f"Only support `append`, `overwrite`, or `overwrite-partitions` mode. {write_mode} is unsupported"
            )
        if write_mode == "overwrite-partitions" and partition_cols is None:
            raise ValueError("Partition columns must be specified to use `overwrite-partitions` mode.")

        io_config = get_context().daft_planning_config.default_io_config if io_config is None else io_config

        cols: Optional[list[Expression]] = None
        if partition_cols is not None:
            cols = self.__column_input_to_expression(tuple(partition_cols))

        builder = self._builder.write_tabular(
            root_dir=root_dir,
            partition_cols=cols,
            write_mode=WriteMode.from_str(write_mode),
            file_format=FileFormat.Json,
            io_config=io_config,
        )
        # Block and write, then retrieve data
        write_df = DataFrame(builder)
        write_df.collect()
        assert write_df._result is not None

        # Populate and return a new disconnected DataFrame
        result_df = DataFrame(write_df._builder)
        result_df._result_cache = write_df._result_cache
        result_df._preview = write_df._preview
        return result_df

    @DataframePublicAPI
    def write_iceberg(
        self, table: "pyiceberg.table.Table", mode: str = "append", io_config: Optional[IOConfig] = None
    ) -> "DataFrame":
        """Writes the DataFrame to an [Iceberg](https://iceberg.apache.org/docs/nightly/) table, returning a new DataFrame with the operations that occurred.

        Can be run in either `append` or `overwrite` mode which will either appends the rows in the DataFrame or will delete the existing rows and then append the DataFrame rows respectively.

        Args:
            table (pyiceberg.table.Table): Destination [PyIceberg Table](https://py.iceberg.apache.org/reference/pyiceberg/table/#pyiceberg.table.Table) to write dataframe to.
            mode (str, optional): Operation mode of the write. `append` or `overwrite` Iceberg Table. Defaults to `append`.
            io_config (IOConfig, optional): A custom IOConfig to use when accessing Iceberg object storage data. If provided, configurations set in `table` are ignored.

        Returns:
            DataFrame: The operations that occurred with this write.

        Note:
            This call is **blocking** and will execute the DataFrame when called

        Examples:
            >>> import pyiceberg
            >>> import daft
            >>>
            >>> table = pyiceberg.Table(...)  # doctest: +SKIP
            >>> df = daft.from_pydict({"user_id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
            >>> df = df.write_iceberg(table, mode="overwrite")  # doctest: +SKIP

        """
        import pyarrow as pa
        import pyiceberg
        from packaging.version import parse

        from daft.io.iceberg._iceberg import _convert_iceberg_file_io_properties_to_io_config

        if len(table.spec().fields) > 0 and parse(pyiceberg.__version__) < parse("0.7.0"):
            raise ValueError("pyiceberg>=0.7.0 is required to write to a partitioned table")

        if parse(pyiceberg.__version__) < parse("0.6.0"):
            raise ValueError(f"Write Iceberg is only supported on pyiceberg>=0.6.0, found {pyiceberg.__version__}")

        if parse(pa.__version__) < parse("12.0.1"):
            raise ValueError(
                f"Write Iceberg is only supported on pyarrow>=12.0.1, found {pa.__version__}. See this issue for more information: https://github.com/apache/arrow/issues/37054#issuecomment-1668644887"
            )

        if mode not in ["append", "overwrite"]:
            raise ValueError(f"Only support `append` or `overwrite` mode. {mode} is unsupported")

        io_config = (
            _convert_iceberg_file_io_properties_to_io_config(table.io.properties) if io_config is None else io_config
        )
        io_config = get_context().daft_planning_config.default_io_config if io_config is None else io_config

        operations = []
        path = []
        rows = []
        size = []

        builder = self._builder.write_iceberg(table, io_config)
        write_df = DataFrame(builder)
        write_df.collect()

        write_result = write_df.to_pydict()
        assert "data_file" in write_result
        data_files = write_result["data_file"]

        if mode == "overwrite":
            deleted_files = table.scan().plan_files()
        else:
            deleted_files = []

        schema = table.schema()
        partitioning: dict[str, list[Any]] = {
            schema.find_field(field.source_id).name: [] for field in table.spec().fields
        }

        for data_file in data_files:
            operations.append("ADD")
            path.append(data_file.file_path)
            rows.append(data_file.record_count)
            size.append(data_file.file_size_in_bytes)

            for field in partitioning.keys():
                partitioning[field].append(getattr(data_file.partition, field, None))

        for pf in deleted_files:
            data_file = pf.file
            operations.append("DELETE")
            path.append(data_file.file_path)
            rows.append(data_file.record_count)
            size.append(data_file.file_size_in_bytes)

            for field in partitioning.keys():
                partitioning[field].append(getattr(data_file.partition, field, None))

        if parse(pyiceberg.__version__) >= parse("0.7.0"):
            from pyiceberg.table import ALWAYS_TRUE, TableProperties

            if parse(pyiceberg.__version__) >= parse("0.8.0"):
                from pyiceberg.utils.properties import property_as_bool

                property_as_bool = property_as_bool
            else:
                from pyiceberg.table import PropertyUtil

                property_as_bool = PropertyUtil.property_as_bool

            tx = table.transaction()

            if mode == "overwrite":
                tx.delete(delete_filter=ALWAYS_TRUE)

            update_snapshot = tx.update_snapshot()

            manifest_merge_enabled = mode == "append" and property_as_bool(
                tx.table_metadata.properties,
                TableProperties.MANIFEST_MERGE_ENABLED,
                TableProperties.MANIFEST_MERGE_ENABLED_DEFAULT,
            )

            append_method = update_snapshot.merge_append if manifest_merge_enabled else update_snapshot.fast_append

            with append_method() as append_files:
                for data_file in data_files:
                    append_files.append_data_file(data_file)

            tx.commit_transaction()
        else:
            from pyiceberg.table import _MergingSnapshotProducer
            from pyiceberg.table.snapshots import Operation

            operations_map = {
                "append": Operation.APPEND,
                "overwrite": Operation.OVERWRITE,
            }

            merge = _MergingSnapshotProducer(operation=operations_map[mode], table=table)

            for data_file in data_files:
                merge.append_data_file(data_file)

            merge.commit()

        with_operations = {
            "operation": pa.array(operations, type=pa.string()),
            "rows": pa.array(rows, type=pa.int64()),
            "file_size": pa.array(size, type=pa.int64()),
            "file_name": pa.array([fp for fp in path], type=pa.string()),
        }

        if partitioning:
            with_operations["partitioning"] = pa.StructArray.from_arrays(
                partitioning.values(), names=partitioning.keys()
            )

        from daft import from_pydict

        # NOTE: We are losing the history of the plan here.
        # This is due to the fact that the logical plan of the write_iceberg returns datafiles but we want to return the above data
        return from_pydict(with_operations)

    @DataframePublicAPI
    def write_deltalake(
        self,
        table: Union[str, pathlib.Path, "DataCatalogTable", "deltalake.DeltaTable", "UnityCatalogTable"],
        partition_cols: Optional[list[str]] = None,
        mode: Literal["append", "overwrite", "error", "ignore"] = "append",
        schema_mode: Optional[Literal["merge", "overwrite"]] = None,
        name: Optional[str] = None,
        description: Optional[str] = None,
        configuration: Optional[Mapping[str, Optional[str]]] = None,
        custom_metadata: Optional[dict[str, str]] = None,
        dynamo_table_name: Optional[str] = None,
        allow_unsafe_rename: bool = False,
        io_config: Optional[IOConfig] = None,
    ) -> "DataFrame":
        """Writes the DataFrame to a [Delta Lake](https://docs.delta.io/latest/index.html) table, returning a new DataFrame with the operations that occurred.

        Args:
            table (Union[str, pathlib.Path, DataCatalogTable, deltalake.DeltaTable, UnityCatalogTable]): Destination [Delta Lake Table](https://delta-io.github.io/delta-rs/api/delta_table/) or table URI to write dataframe to.
            partition_cols (List[str], optional): How to subpartition each partition further. If table exists, expected to match table's existing partitioning scheme, otherwise creates the table with specified partition columns. Defaults to None.
            mode (str, optional): Operation mode of the write. `append` will add new data, `overwrite` will replace table with new data, `error` will raise an error if table already exists, and `ignore` will not write anything if table already exists. Defaults to `append`.
            schema_mode (str, optional): Schema mode of the write. If set to `overwrite`, allows replacing the schema of the table when doing `mode=overwrite`. Schema mode `merge` is currently not supported.
            name (str, optional): User-provided identifier for this table.
            description (str, optional): User-provided description for this table.
            configuration (Mapping[str, Optional[str]], optional): A map containing configuration options for the metadata action.
            custom_metadata (Dict[str, str], optional): Custom metadata to add to the commit info.
            dynamo_table_name (str, optional): Name of the DynamoDB table to be used as the locking provider if writing to S3.
            allow_unsafe_rename (bool, optional): Whether to allow unsafe rename when writing to S3 or local disk. Defaults to False.
            io_config (IOConfig, optional): configurations to use when interacting with remote storage.

        Returns:
            DataFrame: The operations that occurred with this write.

        Note:
            This call is **blocking** and will execute the DataFrame when called

        Examples:
            >>> import daft
            >>> import deltalake
            >>> df = daft.from_pydict({"x": [1, 2, 3], "y": ["a", "b", "c"]})
            >>> df.write_deltalake("s3://my-bucket/my-deltalake-table")  # doctest: +SKIP
        """
        import json

        import deltalake
        import pyarrow as pa
        from deltalake.exceptions import TableNotFoundError
        from packaging.version import parse

        from daft import from_pydict
        from daft.dependencies import unity_catalog
        from daft.filesystem import get_protocol_from_path
        from daft.io import DataCatalogTable
        from daft.io.delta_lake._deltalake import delta_schema_to_pyarrow
        from daft.io.delta_lake.delta_lake_write import (
            AddAction,
            convert_pa_schema_to_delta,
            create_table_with_add_actions,
        )
        from daft.io.object_store_options import io_config_to_storage_options

        def _create_metadata_param(metadata: Optional[dict[str, str]]) -> Any:
            """From deltalake>=0.20.0 onwards, custom_metadata has to be passed as CommitProperties.

            Args:
                metadata

            Returns:
                DataFrame: metadata for deltalake<0.20.0, otherwise CommitProperties with custom_metadata
            """
            if parse(deltalake.__version__) < parse("0.20.0"):
                return metadata
            else:
                from deltalake import CommitProperties

                return CommitProperties(custom_metadata=metadata)

        if schema_mode == "merge":
            raise ValueError("Schema mode' merge' is not currently supported for write_deltalake.")

        if parse(deltalake.__version__) < parse("0.14.0"):
            raise ValueError(f"Write delta lake is only supported on deltalake>=0.14.0, found {deltalake.__version__}")

        io_config = get_context().daft_planning_config.default_io_config if io_config is None else io_config

        # Retrieve table_uri and storage_options from various backends
        table_uri: str
        storage_options: dict[str, str]

        if isinstance(table, deltalake.DeltaTable):
            table_uri = table.table_uri
            storage_options = table._storage_options or {}
            new_storage_options = io_config_to_storage_options(io_config, table_uri)
            storage_options.update(new_storage_options or {})
        else:
            if isinstance(table, str):
                table_uri = os.path.expanduser(table)
            elif isinstance(table, pathlib.Path):
                table_uri = str(table)
            elif unity_catalog.module_available() and isinstance(table, unity_catalog.UnityCatalogTable):
                table_uri = table.table_uri
                io_config = table.io_config
            elif isinstance(table, DataCatalogTable):
                table_uri = table.table_uri(io_config)
            else:
                raise ValueError(f"Expected table to be a path or a DeltaTable, received: {type(table)}")

            if io_config is None:
                raise ValueError(
                    "io_config was not provided to write_deltalake and could not be retrieved from defaults."
                )

            storage_options = io_config_to_storage_options(io_config, table_uri) or {}
            try:
                table = deltalake.DeltaTable(table_uri, storage_options=storage_options)
            except TableNotFoundError:
                table = None

        # see: https://delta-io.github.io/delta-rs/usage/writing/writing-to-s3-with-locking-provider/
        scheme = get_protocol_from_path(table_uri)
        if scheme == "s3" or scheme == "s3a":
            if dynamo_table_name is not None:
                storage_options["AWS_S3_LOCKING_PROVIDER"] = "dynamodb"
                storage_options["DELTA_DYNAMO_TABLE_NAME"] = dynamo_table_name
            else:
                storage_options["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"

                if not allow_unsafe_rename:
                    warnings.warn("No DynamoDB table specified for Delta Lake locking. Defaulting to unsafe writes.")
        elif scheme == "file":
            if allow_unsafe_rename:
                storage_options["MOUNT_ALLOW_UNSAFE_RENAME"] = "true"

        pyarrow_schema = pa.schema((f.name, f.dtype.to_arrow_dtype()) for f in self.schema())

        large_dtypes = True
        delta_schema = convert_pa_schema_to_delta(pyarrow_schema, large_dtypes=large_dtypes)

        if table:
            if partition_cols and partition_cols != table.metadata().partition_columns:
                raise ValueError(
                    f"Expected partition columns to match that of the existing table ({table.metadata().partition_columns}), but received: {partition_cols}"
                )
            else:
                partition_cols = table.metadata().partition_columns

            table.update_incremental()

            table_schema = delta_schema_to_pyarrow(table.schema())
            if Schema.from_pyarrow_schema(delta_schema) != Schema.from_pyarrow_schema(table_schema) and not (
                mode == "overwrite" and schema_mode == "overwrite"
            ):
                raise ValueError(
                    "Schema of data does not match table schema\n"
                    f"Data schema:\n{delta_schema}\nTable Schema:\n{table_schema}"
                )
            if mode == "error":
                raise AssertionError("Delta table already exists, write mode set to error.")
            elif mode == "ignore":
                return from_pydict(
                    {
                        "operation": pa.array([], type=pa.string()),
                        "rows": pa.array([], type=pa.int64()),
                        "file_size": pa.array([], type=pa.int64()),
                        "file_name": pa.array([], type=pa.string()),
                    }
                )
            version = table.version() + 1
        else:
            version = 0

        if partition_cols is not None:
            for c in partition_cols:
                if self.schema()[c].dtype == DataType.binary():
                    raise NotImplementedError("Binary partition columns are not yet supported for Delta Lake writes")

        builder = self._builder.write_deltalake(
            table_uri,
            mode,
            version,
            large_dtypes,
            io_config=io_config,
            partition_cols=partition_cols,
        )
        write_df = DataFrame(builder)
        write_df.collect()

        write_result = write_df.to_pydict()
        assert "add_action" in write_result
        add_actions: list[AddAction] = write_result["add_action"]

        operations = []
        paths = []
        rows = []
        sizes = []

        for add_action in add_actions:
            stats = json.loads(add_action.stats)
            operations.append("ADD")
            paths.append(add_action.path)
            rows.append(stats["numRecords"])
            sizes.append(add_action.size)

        if table is None:
            create_table_with_add_actions(
                table_uri,
                delta_schema,
                add_actions,
                mode,
                partition_cols or [],
                name,
                description,
                configuration,
                storage_options,
                custom_metadata,
            )
        else:
            if mode == "overwrite":
                old_actions = pa.record_batch(table.get_add_actions())
                old_actions_dict = old_actions.to_pydict()
                for i in range(old_actions.num_rows):
                    operations.append("DELETE")
                    paths.append(old_actions_dict["path"][i])
                    rows.append(old_actions_dict["num_records"][i])
                    sizes.append(old_actions_dict["size_bytes"][i])

            metadata_param = _create_metadata_param(custom_metadata)
            if parse(deltalake.__version__) < parse("1.0.0"):
                table._table.create_write_transaction(
                    add_actions, mode, partition_cols or [], delta_schema, None, metadata_param
                )
            else:
                table._table.create_write_transaction(
                    add_actions,
                    mode,
                    partition_cols or [],
                    deltalake.Schema.from_arrow(delta_schema),
                    None,
                    metadata_param,
                )
            table.update_incremental()

        with_operations = from_pydict(
            {
                "operation": pa.array(operations, type=pa.string()),
                "rows": pa.array(rows, type=pa.int64()),
                "file_size": pa.array(sizes, type=pa.int64()),
                "file_name": pa.array([os.path.basename(fp) for fp in paths], type=pa.string()),
            }
        )

        return with_operations

    @DataframePublicAPI
    def write_sink(self, sink: "DataSink[WriteResultType]") -> "DataFrame":
        """Writes the DataFrame to the given DataSink.

        Args:
            sink: The DataSink to write to.

        Returns:
            DataFrame: A dataframe from the micropartition returned by the DataSink's `.finalize()` method.

        Note:
            This call is **blocking** and will execute the DataFrame when called
        """
        sink.start()

        builder = self._builder.write_datasink(sink.name(), sink)
        write_df = DataFrame(builder)
        write_df.collect()

        results = write_df.to_pydict()
        assert "write_results" in results
        micropartition = sink.finalize(results["write_results"])
        if micropartition.schema() != sink.schema():
            raise ValueError(
                f"Schema mismatch between the data sink's schema and the result's schema:\nSink schema:\n{sink.schema()}\nResult schema:\n{micropartition.schema()}"
            )
        # TODO(desmond): Connect the old and new logical plan builders so that a .explain() shows the
        # plan from the source all the way to the sink to the sink's results. In theory we can do this
        # for all other sinks too.
        return DataFrame._from_micropartitions(micropartition)

    @DataframePublicAPI
    def write_lance(
        self,
        uri: Union[str, pathlib.Path],
        mode: Literal["create", "append", "overwrite", "merge"] = "create",
        io_config: Optional[IOConfig] = None,
        schema: Optional[Union[Schema, "pyarrow.Schema"]] = None,
        left_on: Optional[str] = None,
        right_on: Optional[str] = None,
        **kwargs: Any,
    ) -> "DataFrame":
        """Writes the DataFrame to a Lance table.

        Args:
          uri: The URI of the Lance table to write to
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

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"a": [1, 2, 3, 4]})
            >>> df.write_lance("/tmp/lance/my_table.lance")  # doctest: +SKIP
            ╭───────────────┬──────────────────┬─────────────────┬─────────╮
            │ num_fragments ┆ num_deleted_rows ┆ num_small_files ┆ version │
            │ ---           ┆ ---              ┆ ---             ┆ ---     │
            │ Int64         ┆ Int64            ┆ Int64           ┆ Int64   │
            ╞═══════════════╪══════════════════╪═════════════════╪═════════╡
            │ 1             ┆ 0                ┆ 1               ┆ 1       │
            ╰───────────────┴──────────────────┴─────────────────┴─────────╯
            <BLANKLINE>
            (Showing first 1 of 1 rows)
            >>> daft.read_lance("/tmp/lance/my_table.lance").collect()  # doctest: +SKIP
            ╭───────╮
            │ a     │
            │ ---   │
            │ Int64 │
            ╞═══════╡
            │ 1     │
            ├╌╌╌╌╌╌╌┤
            │ 2     │
            ├╌╌╌╌╌╌╌┤
            │ 3     │
            ├╌╌╌╌╌╌╌┤
            │ 4     │
            ╰───────╯
            <BLANKLINE>
            (Showing first 4 of 4 rows)
            >>> # Pass additional keyword arguments to the Lance writer
            >>> # All additional keyword arguments are passed to `lance.write_fragments`
            >>> df.write_lance("/tmp/lance/my_table.lance", mode="overwrite", max_bytes_per_file=1024)  # doctest: +SKIP
            ╭───────────────┬──────────────────┬─────────────────┬─────────╮
            │ num_fragments ┆ num_deleted_rows ┆ num_small_files ┆ version │
            │ ---           ┆ ---              ┆ ---             ┆ ---     │
            │ Int64         ┆ Int64            ┆ Int64           ┆ Int64   │
            ╞═══════════════╪══════════════════╪═════════════════╪═════════╡
            │ 1             ┆ 0                ┆ 1               ┆ 2       │
            ╰───────────────┴──────────────────┴─────────────────┴─────────╯
            <BLANKLINE>
            (Showing first 1 of 1 rows)
        """
        from daft import context as _context
        from daft.io.lance.lance_data_sink import LanceDataSink
        from daft.io.object_store_options import io_config_to_storage_options

        if schema is None:
            schema = self.schema()

        # Non-merge modes do not support schema evolution or custom join keys
        if mode != "merge":
            sanitized_kwargs = {k: v for k, v in kwargs.items() if k not in ("left_on", "right_on")}
            sink = LanceDataSink(uri, schema, mode, io_config, **sanitized_kwargs)
            return self.write_sink(sink)

        # Merge mode semantics
        try:
            import lance
        except ImportError as e:
            raise ImportError(
                "Unable to import the `lance` package, please ensure that Daft is installed with the lance extra dependency: `pip install daft[lance]`"
            ) from e

        io_config = _context.get_context().daft_planning_config.default_io_config if io_config is None else io_config
        storage_options = io_config_to_storage_options(io_config, str(uri) if isinstance(uri, pathlib.Path) else uri)

        # Attempt to load dataset; if not exists, behave like create
        lance_ds = None
        try:
            lance_ds = lance.dataset(uri, storage_options=storage_options)
        except (ValueError, FileNotFoundError, OSError) as _e:
            lance_ds = None

        if lance_ds is None:
            sanitized_kwargs = {k: v for k, v in kwargs.items() if k not in ("left_on", "right_on")}
            sink = LanceDataSink(uri, schema, "create", io_config, **sanitized_kwargs)
            return self.write_sink(sink)

        # Dataset exists: detect schema evolution by checking new columns in incoming DF
        existing_fields: set[str] = set()
        try:
            existing_fields = {getattr(f, "name", str(f)) for f in lance_ds.schema}
        except Exception:
            names = []
            try:
                names = list(getattr(lance_ds.schema, "names", []))
            except Exception:
                try:
                    names = [getattr(f, "name", str(f)) for f in getattr(lance_ds.schema, "fields", [])]
                except Exception:
                    names = []
            existing_fields = set(names)

        meta_exclusions = {"fragment_id", "_rowaddr", "_rowid"}
        new_cols = [c for c in self.column_names if c not in existing_fields and c not in meta_exclusions]

        if len(new_cols) == 0:
            # Pure append: no schema evolution. Ensure merge-specific params are not forwarded.
            sanitized_kwargs = {k: v for k, v in kwargs.items() if k not in ("left_on", "right_on")}

            sink = LanceDataSink(uri, schema, "append", io_config, **sanitized_kwargs)
            return self.write_sink(sink)

        # Schema evolution: route to per-fragment merge keyed by provided business key or default '_rowaddr'
        join_left = left_on or "_rowaddr"
        join_right = right_on or join_left
        if "fragment_id" not in self.column_names:
            raise ValueError(
                "DataFrame must contain 'fragment_id' column for per-fragment merge in mode='merge'. Read from Lance to include 'fragment_id'."
            )
        if join_right not in self.column_names:
            hint = (
                " Read from Lance with default_scan_options={'with_rowaddr': True} to include '_rowaddr'."
                if join_right == "_rowaddr"
                else ""
            )
            raise ValueError(
                f"DataFrame must contain join key column '{join_right}' for per-fragment merge in mode='merge'." + hint
            )

        from daft.io.lance.lance_merge_column import merge_columns_from_df

        merge_columns_from_df(
            df=self,
            lance_ds=lance_ds,
            uri=uri,
            left_on=join_left,
            right_on=join_right,
            storage_options=storage_options,
        )

        # Build and return stats DataFrame similar to sink.finalize
        dataset = lance.dataset(uri, storage_options=storage_options)
        stats = dataset.stats.dataset_stats()
        from daft.dependencies import pa as _pa
        from daft.recordbatch import MicroPartition

        return DataFrame._from_micropartitions(
            MicroPartition.from_pydict(
                {
                    "num_fragments": _pa.array([stats["num_fragments"]], type=_pa.int64()),
                    "num_deleted_rows": _pa.array([stats["num_deleted_rows"]], type=_pa.int64()),
                    "num_small_files": _pa.array([stats["num_small_files"]], type=_pa.int64()),
                    "version": _pa.array([dataset.version], type=_pa.int64()),
                }
            )
        )

    @DataframePublicAPI
    def write_turbopuffer(
        self,
        namespace: Union[str, Expression],
        api_key: Optional[str] = None,
        region: Optional[str] = None,
        distance_metric: Optional[Literal["cosine_distance", "euclidean_squared"]] = None,
        schema: Optional[dict[str, Any]] = None,
        id_column: Optional[str] = None,
        vector_column: Optional[str] = None,
        client_kwargs: Optional[dict[str, Any]] = None,
        write_kwargs: Optional[dict[str, Any]] = None,
    ) -> "DataFrame":
        """Writes the DataFrame to a Turbopuffer namespace.

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
        """
        from daft.io.turbopuffer.turbopuffer_data_sink import TurbopufferDataSink

        sink = TurbopufferDataSink(
            namespace, api_key, region, distance_metric, schema, id_column, vector_column, client_kwargs, write_kwargs
        )
        return self.write_sink(sink)

    @DataframePublicAPI
    def write_clickhouse(
        self,
        table: str,
        *,
        host: str,
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        client_kwargs: Optional[dict[str, Any]] = None,
        write_kwargs: Optional[dict[str, Any]] = None,
    ) -> "DataFrame":
        """Writes the DataFrame to a ClickHouse table.

        Args:
            table: Name of the ClickHouse table to write to.
            host: ClickHouse host.
            port: ClickHouse port.
            user: ClickHouse user.
            password: ClickHouse password.
            database: ClickHouse database.
            client_kwargs: Optional dictionary of arguments to pass to the ClickHouse client constructor.
            write_kwargs: Optional dictionary of arguments to pass to the ClickHouse write() method.

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"a": [1, 2, 3, 4]})  # doctest: +SKIP
            >>> df.write_clickhouse(table="", host="", port=8123, user="", password="")  # doctest: +SKIP
            ╭────────────────────┬─────────────────────╮
            │ total_written_rows ┆ total_written_bytes │
            │ ---                ┆ ---                 │
            │ Int64              ┆ Int64               │
            ╞════════════════════╪═════════════════════╡
            │ 4                  ┆ 32                  │
            ╰────────────────────┴─────────────────────╯
        """
        from daft.io.clickhouse.clickhouse_data_sink import ClickHouseDataSink

        sink = ClickHouseDataSink(
            table,
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            client_kwargs=client_kwargs,
            write_kwargs=write_kwargs,
        )
        return self.write_sink(sink)

    def write_huggingface(
        self,
        repo: str,
        split: str = "train",
        data_dir: str = "data",
        revision: str = "main",
        overwrite: bool = False,
        commit_message: str = "Upload dataset using Daft",
        commit_description: Optional[str] = None,
        io_config: Optional[IOConfig] = None,
    ) -> "DataFrame":
        """Write a DataFrame into a Hugging Face dataset.

        Args:
            repo: The ID of the repository to push to in the following format: `<user>/<dataset_name>` or `<org>/<dataset_name>`.
            split: The name of the split that will be given to that dataset.
            data_dir: Directory of the uploaded data files.
            revision: Branch to push the uploaded files to.
            overwrite: Whether to overwrite or append.
            commit_message: Message to commit while pushing.
            commit_description: Description of the commit that will be created.
            io_config: Configurations to use when interacting with remote storage.
        """
        from daft.io.huggingface.sink import HuggingFaceSink

        io_config = get_context().daft_planning_config.default_io_config if io_config is None else io_config

        sink = HuggingFaceSink(
            repo, split, data_dir, revision, overwrite, commit_message, commit_description, io_config.hf
        )
        return self.write_sink(sink)

    def write_bigtable(
        self,
        project_id: str,
        instance_id: str,
        table_id: str,
        row_key_column: str,
        column_family_mappings: dict[str, str],
        client_kwargs: Optional[dict[str, Any]] = None,
        write_kwargs: Optional[dict[str, Any]] = None,
        serialize_incompatible_types: bool = True,
    ) -> "DataFrame":
        """Write a DataFrame into a Google Cloud Bigtable table.

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
        """
        from daft.io.bigtable.bigtable_data_sink import BigtableDataSink

        sink = BigtableDataSink(
            project_id, instance_id, table_id, row_key_column, column_family_mappings, client_kwargs, write_kwargs
        )

        # Preprocess the DataFrame using the sink's validation and preprocessing logic
        df_to_write = sink._preprocess_dataframe(self, serialize_incompatible_types)

        return df_to_write.write_sink(sink)

    ###
    # DataFrame operations
    ###

    def __column_input_to_expression(self, columns: Iterable[ColumnInputType]) -> list[Expression]:
        # TODO(Kevin): remove this method and use _column_inputs_to_expressions
        return [col(c) if isinstance(c, str) else c for c in columns]

    def _wildcard_inputs_to_expressions(self, columns: tuple[ManyColumnsInputType, ...]) -> list[Expression]:
        """Handles wildcard argument column inputs."""
        column_input: Iterable[ColumnInputType] = columns[0] if len(columns) == 1 else columns  # type: ignore
        return column_inputs_to_expressions(column_input)

    if TYPE_CHECKING:

        @overload
        def __getitem__(self, item: int) -> Expression: ...
        @overload
        def __getitem__(self, item: str) -> Expression: ...
        @overload
        def __getitem__(self, item: slice) -> "DataFrame": ...
        @overload
        def __getitem__(self, item: Iterable) -> "DataFrame": ...  # type: ignore

    def __getitem__(self, item: Union[int, str, slice, Iterable[Union[str, int]]]) -> Union[Expression, "DataFrame"]:
        """Gets a column from the DataFrame as an Expression (``df["mycol"]``).

        Args:
            item (Union[int, str, slice, Iterable[Union[str, int]]]): The column to get. Can be an integer index, a string column name, a slice for multiple columns, or an iterable of column names or indices.

        Returns:
            Union[Expression, DataFrame]: If a single column is requested, returns an Expression representing that column.
            If multiple columns are requested (via a slice or iterable), returns a new DataFrame containing those columns.

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})
            >>> df["a"]  # Get a single column
            col(a)
            >>> df["b"]  # Get another single column
            col(b)
            >>> df[0]  # Get the first column by index
            col(a)
            >>> df[1:3]  # Get a slice of columns
            ╭───────┬───────╮
            │ b     ┆ c     │
            │ ---   ┆ ---   │
            │ Int64 ┆ Int64 │
            ╰───────┴───────╯
            <BLANKLINE>
            (No data to display: Dataframe not materialized, use .collect() to materialize)
            >>> df[["a", "c"]]  # Get multiple columns by name
            ╭───────┬───────╮
            │ a     ┆ c     │
            │ ---   ┆ ---   │
            │ Int64 ┆ Int64 │
            ╰───────┴───────╯
            <BLANKLINE>
            (No data to display: Dataframe not materialized, use .collect() to materialize)
            >>> df[["a", 1]]  # Get multiple columns by name and index
            ╭───────┬───────╮
            │ a     ┆ b     │
            │ ---   ┆ ---   │
            │ Int64 ┆ Int64 │
            ╰───────┴───────╯
            <BLANKLINE>
            (No data to display: Dataframe not materialized, use .collect() to materialize)
            >>> df[0:2]  # Get a slice of columns by index
            ╭───────┬───────╮
            │ a     ┆ b     │
            │ ---   ┆ ---   │
            │ Int64 ┆ Int64 │
            ╰───────┴───────╯
            <BLANKLINE>
            (No data to display: Dataframe not materialized, use .collect() to materialize)
            >>> df[["a", "b", 2]]  # Get a mix of column names and indices
            ╭───────┬───────┬───────╮
            │ a     ┆ b     ┆ c     │
            │ ---   ┆ ---   ┆ ---   │
            │ Int64 ┆ Int64 ┆ Int64 │
            ╰───────┴───────┴───────╯
            <BLANKLINE>
            (No data to display: Dataframe not materialized, use .collect() to materialize)

        """
        result: Optional[Expression]

        if isinstance(item, int):
            schema = self._builder.schema()
            if item < -len(schema) or item >= len(schema):
                raise ValueError(f"{item} out of bounds for {schema}")
            result = ExpressionsProjection.from_schema(schema)[item]
            assert result is not None
            return result
        elif isinstance(item, str):
            schema = self._builder.schema()
            if item not in schema.column_names() and item != "*":
                raise ValueError(f"{item} does not exist in schema {schema}")

            return col(item)
        elif isinstance(item, Iterable):
            schema = self._builder.schema()

            columns = []
            for it in item:
                if isinstance(it, str):
                    result = col(schema[it].name)
                    columns.append(result)
                elif isinstance(it, int):
                    if it < -len(schema) or it >= len(schema):
                        raise ValueError(f"{it} out of bounds for {schema}")
                    field = list(self._builder.schema())[it]
                    columns.append(col(field.name))
                else:
                    raise ValueError(f"unknown indexing type: {type(it)}")
            return self.select(*columns)
        elif isinstance(item, slice):
            schema = self._builder.schema()
            columns_exprs: ExpressionsProjection = ExpressionsProjection.from_schema(schema)
            selected_columns = columns_exprs[item]
            return self.select(*selected_columns)
        else:
            raise ValueError(f"unknown indexing type: {type(item)}")

    def _add_monotonically_increasing_id(self, column_name: Optional[str] = None) -> "DataFrame":
        """Generates a column of monotonically increasing unique ids for the DataFrame.

        The implementation of this method puts the partition number in the upper 28 bits, and the row number in each partition
        in the lower 36 bits. This allows for 2^28 ≈ 268 million partitions and 2^36 ≈ 68 billion rows per partition.

        Args:
            column_name (Optional[str], optional): name of the new column. Defaults to "id".

        Returns:
            DataFrame: DataFrame with a new column of monotonically increasing ids.

        Examples:
            >>> import daft
            >>> daft.set_runner_ray()  # doctest: +SKIP
            >>>
            >>> df = daft.from_pydict({"a": [1, 2, 3, 4]}).into_partitions(2)
            >>> df = df._add_monotonically_increasing_id()
            >>> df.show()  # doctest: +SKIP
            ╭─────────────┬───────╮
            │ id          ┆ a     │
            │ ---         ┆ ---   │
            │ UInt64      ┆ Int64 │
            ╞═════════════╪═══════╡
            │ 0           ┆ 1     │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 1           ┆ 2     │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 68719476736 ┆ 3     │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 68719476737 ┆ 4     │
            ╰─────────────┴───────╯
            <BLANKLINE>
            (Showing first 4 of 4 rows)
        """
        builder = self._builder.add_monotonically_increasing_id(column_name)
        return DataFrame(builder)

    @DataframePublicAPI
    def select(self, *columns: ColumnInputType, **projections: Expression) -> "DataFrame":
        """Creates a new DataFrame from the provided expressions, similar to a SQL ``SELECT``.

        Args:
            *columns (Union[str, Expression]): columns to select from the current DataFrame
            **projections (Expression): additional projections in kwarg format.

        Returns:
            DataFrame: new DataFrame that will select the passed in columns

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6], "z": [7, 8, 9]})
            >>> df = df.select("x", daft.col("y"), daft.col("z") + 1)
            >>> df.show()
            ╭───────┬───────┬───────╮
            │ x     ┆ y     ┆ z     │
            │ ---   ┆ ---   ┆ ---   │
            │ Int64 ┆ Int64 ┆ Int64 │
            ╞═══════╪═══════╪═══════╡
            │ 1     ┆ 4     ┆ 8     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 2     ┆ 5     ┆ 9     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 3     ┆ 6     ┆ 10    │
            ╰───────┴───────┴───────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)
        """
        selection = column_inputs_to_expressions(columns)
        selection += [expr.alias(alias) for (alias, expr) in projections.items()]
        builder = self._builder.select(selection)
        return DataFrame(builder)

    @DataframePublicAPI
    def describe(self) -> "DataFrame":
        """Returns the Schema of the DataFrame, which provides information about each column, as a new DataFrame.

        Returns:
            DataFrame: A dataframe where each row is a column name and its corresponding type.

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"]})
            >>> df.describe().show()
            ╭─────────────┬────────╮
            │ column_name ┆ type   │
            │ ---         ┆ ---    │
            │ String      ┆ String │
            ╞═════════════╪════════╡
            │ a           ┆ Int64  │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ b           ┆ String │
            ╰─────────────┴────────╯
            <BLANKLINE>
            (Showing first 2 of 2 rows)
        """
        builder = self.__builder.describe()
        return DataFrame(builder)

    @DataframePublicAPI
    def summarize(self) -> "DataFrame":
        """Returns column statistics for the DataFrame.

        Returns:
            DataFrame: new DataFrame with the computed column statistics.

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6], "z": [7, 8, 9]})
            >>> df.summarize().show()  # doctest: +SKIP
            ╭────────┬────────┬────────┬────────────┬────────┬─────────────┬───────────────────────╮
            │ column ┆ type   ┆ min    ┆      …     ┆ count  ┆ count_nulls ┆ approx_count_distinct │
            │ ---    ┆ ---    ┆ ---    ┆            ┆ ---    ┆ ---         ┆ ---                   │
            │ String ┆ String ┆ String ┆ (1 hidden) ┆ UInt64 ┆ UInt64      ┆ UInt64                │
            ╞════════╪════════╪════════╪════════════╪════════╪═════════════╪═══════════════════════╡
            │ x      ┆ Int64  ┆ 1      ┆ …          ┆ 3      ┆ 0           ┆ 3                     │
            ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ y      ┆ Int64  ┆ 4      ┆ …          ┆ 3      ┆ 0           ┆ 3                     │
            ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ z      ┆ Int64  ┆ 7      ┆ …          ┆ 3      ┆ 0           ┆ 3                     │
            ╰────────┴────────┴────────┴────────────┴────────┴─────────────┴───────────────────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)
        """
        builder = self._builder.summarize()
        return DataFrame(builder)

    @DataframePublicAPI
    def distinct(self, *on: ColumnInputType) -> "DataFrame":
        """Computes distinct rows, dropping duplicates.

        Optionally, specify a subset of columns to perform distinct on.

        Args:
            *on (Union[str, Expression]): columns to perform distinct on. Defaults to all columns.

        Returns:
            DataFrame: DataFrame that has only distinct rows.

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1, 2, 2], "y": [4, 5, 5], "z": [7, 8, 8]})
            >>> distinct_df = df.distinct()
            >>> distinct_df = distinct_df.sort("x")
            >>> distinct_df.show()
            ╭───────┬───────┬───────╮
            │ x     ┆ y     ┆ z     │
            │ ---   ┆ ---   ┆ ---   │
            │ Int64 ┆ Int64 ┆ Int64 │
            ╞═══════╪═══════╪═══════╡
            │ 1     ┆ 4     ┆ 7     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 2     ┆ 5     ┆ 8     │
            ╰───────┴───────┴───────╯
            <BLANKLINE>
            (Showing first 2 of 2 rows)
            >>> # Pass a subset of columns to perform distinct on
            >>> # Note that output for z is non-deterministic. Both 8 and 9 are possible.
            >>> df = daft.from_pydict({"x": [1, 2, 2], "y": [4, 5, 5], "z": [7, 8, 9]})
            >>> df.distinct("x", daft.col("y")).sort("x").show()
            ╭───────┬───────┬───────╮
            │ x     ┆ y     ┆ z     │
            │ ---   ┆ ---   ┆ ---   │
            │ Int64 ┆ Int64 ┆ Int64 │
            ╞═══════╪═══════╪═══════╡
            │ 1     ┆ 4     ┆ 7     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 2     ┆ 5     ┆ 8     │
            ╰───────┴───────┴───────╯
            <BLANKLINE>
            (Showing first 2 of 2 rows)
        """
        builder = self._builder.distinct(self.__column_input_to_expression(on))
        return DataFrame(builder)

    @DataframePublicAPI
    def unique(self, *by: ColumnInputType) -> "DataFrame":
        """Computes distinct rows, dropping duplicates.

        Alias for [DataFrame.distinct][daft.DataFrame.distinct].

        Args:
            *by (Union[str, Expression]): columns to perform distinct on. Defaults to all columns.

        Returns:
            DataFrame: DataFrame that has only distinct rows.

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1, 2, 2], "y": [4, 5, 5], "z": [7, 8, 8]})
            >>> distinct_df = df.unique()
            >>> distinct_df = distinct_df.sort("x")
            >>> distinct_df.show()
            ╭───────┬───────┬───────╮
            │ x     ┆ y     ┆ z     │
            │ ---   ┆ ---   ┆ ---   │
            │ Int64 ┆ Int64 ┆ Int64 │
            ╞═══════╪═══════╪═══════╡
            │ 1     ┆ 4     ┆ 7     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 2     ┆ 5     ┆ 8     │
            ╰───────┴───────┴───────╯
            <BLANKLINE>
            (Showing first 2 of 2 rows)
        """
        return self.distinct(*by)

    @DataframePublicAPI
    def drop_duplicates(self, *subset: ColumnInputType) -> "DataFrame":
        """Computes distinct rows, dropping duplicates.

        Alias for [DataFrame.distinct][daft.DataFrame.distinct].

        Args:
            *subset (Union[str, Expression]): columns to perform distinct on. Defaults to all columns.

        Returns:
            DataFrame: DataFrame that has only distinct rows.

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1, 2, 2], "y": [4, 5, 5], "z": [7, 8, 8]})
            >>> distinct_df = df.drop_duplicates()
            >>> distinct_df = distinct_df.sort("x")
            >>> distinct_df.show()
            ╭───────┬───────┬───────╮
            │ x     ┆ y     ┆ z     │
            │ ---   ┆ ---   ┆ ---   │
            │ Int64 ┆ Int64 ┆ Int64 │
            ╞═══════╪═══════╪═══════╡
            │ 1     ┆ 4     ┆ 7     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 2     ┆ 5     ┆ 8     │
            ╰───────┴───────┴───────╯
            <BLANKLINE>
            (Showing first 2 of 2 rows)
        """
        return self.distinct(*subset)

    @DataframePublicAPI
    def sample(
        self,
        fraction: Optional[float] = None,
        size: Optional[int] = None,
        with_replacement: bool = False,
        seed: Optional[int] = None,
    ) -> "DataFrame":
        """Samples rows from the DataFrame.

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

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6], "z": [7, 8, 9]})
            >>> # Sample by fraction (backward compatible positional argument)
            >>> sampled_df = df.sample(0.5)
            >>> sampled_df = sampled_df.collect()
            >>> # sampled_df.show()
            >>> # ╭───────┬───────┬───────╮
            >>> # │ x     ┆ y     ┆ z     │
            >>> # │ ---   ┆ ---   ┆ ---   │
            >>> # │ Int64 ┆ Int64 ┆ Int64 │
            >>> # ╞═══════╪═══════╪═══════╡
            >>> # │ 3     ┆ 6     ┆ 9     │
            >>> # ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            >>> # │ 1     ┆ 4     ┆ 7     │
            >>> # ╰───────┴───────┴───────╯
            >>> # <BLANKLINE>
            >>> # (Showing first 2 of 2 rows)
            >>> # Samples will vary from output to output
            >>> # here is a sample output
            >>> # ╭───────┬───────┬───────╮
            >>> # │ x     ┆ y     ┆ z     │
            >>> # │ ---   ┆ ---   ┆ ---   │
            >>> # │ Int64 ┆ Int64 ┆ Int64 │
            >>> # |═══════╪═══════╪═══════╡
            >>> # │ 2     ┆ 5     ┆ 8     │
            >>> # ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            >>> # │ 3     ┆ 6     ┆ 9     │
            >>> # ╰───────┴───────┴───────╯
            >>> # Sample by exact number of rows
            >>> sampled_df = df.sample(size=2)
            >>> sampled_df = sampled_df.collect()
        """
        if fraction is not None and size is not None:
            raise ValueError("Must specify either `fraction` or `size`, but not both")
        if fraction is None and size is None:
            raise ValueError("Must specify either `fraction` or `size`")
        if fraction is not None:
            if fraction < 0.0 or fraction > 1.0:
                raise ValueError(f"fraction should be between 0.0 and 1.0, but got {fraction}")
        if size is not None:
            if size < 0:
                raise ValueError(f"size should be non-negative, but got {size}")
            if get_or_create_runner().name == "ray":
                raise ValueError(
                    "Sample by size only works on the native runner right now. "
                    "Please use `daft.set_runner_native()` to switch to the native runner, "
                    "or use `fraction` instead of `size` for sampling."
                )

        builder = self._builder.sample(fraction, size, with_replacement, seed)
        return DataFrame(builder)

    @DataframePublicAPI
    def exclude(self, *names: str) -> "DataFrame":
        """Drops columns from the current DataFrame by name.

        This is equivalent of performing a select with all the columns but the ones excluded.

        Args:
            *names (str): names to exclude

        Returns:
            DataFrame: DataFrame with some columns excluded.

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6], "z": [7, 8, 9]})
            >>> df_without_x = df.exclude("x")
            >>> df_without_x.show()
            ╭───────┬───────╮
            │ y     ┆ z     │
            │ ---   ┆ ---   │
            │ Int64 ┆ Int64 │
            ╞═══════╪═══════╡
            │ 4     ┆ 7     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 5     ┆ 8     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 6     ┆ 9     │
            ╰───────┴───────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)
        """
        builder = self._builder.exclude(list(names))
        return DataFrame(builder)

    @DataframePublicAPI
    def filter(self, predicate: Union[Expression, str]) -> "DataFrame":
        """Filters rows via a predicate expression, similar to SQL ``WHERE``.

        Alias for [daft.DataFrame.where][daft.DataFrame.where].

        Args:
            predicate (Expression): expression that keeps row if evaluates to True.

        Returns:
            DataFrame: Filtered DataFrame.

        Tip:
            See also [.where(predicate)][daft.DataFrame.where]

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 6, 6], "z": [7, 8, 9]})
            >>> df.filter((df["x"] > 1) & (df["y"] > 1)).collect()
            ╭───────┬───────┬───────╮
            │ x     ┆ y     ┆ z     │
            │ ---   ┆ ---   ┆ ---   │
            │ Int64 ┆ Int64 ┆ Int64 │
            ╞═══════╪═══════╪═══════╡
            │ 2     ┆ 6     ┆ 8     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 3     ┆ 6     ┆ 9     │
            ╰───────┴───────┴───────╯
            <BLANKLINE>
            (Showing first 2 of 2 rows)

        """
        return self.where(predicate)

    @DataframePublicAPI
    def where(self, predicate: Union[Expression, str]) -> "DataFrame":
        """Filters rows via a predicate expression, similar to SQL ``WHERE``.

        Args:
            predicate (Expression): expression that keeps row if evaluates to True.

        Returns:
            DataFrame: Filtered DataFrame.

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 6, 6], "z": [7, 8, 9]})
            >>> df.where((df["x"] > 1) & (df["y"] > 1)).collect()
            ╭───────┬───────┬───────╮
            │ x     ┆ y     ┆ z     │
            │ ---   ┆ ---   ┆ ---   │
            │ Int64 ┆ Int64 ┆ Int64 │
            ╞═══════╪═══════╪═══════╡
            │ 2     ┆ 6     ┆ 8     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 3     ┆ 6     ┆ 9     │
            ╰───────┴───────┴───────╯
            <BLANKLINE>
            (Showing first 2 of 2 rows)

            You can also use a string expression as a predicate.

            Note: this will use the method `sql_expr` to parse the string into an expression
            this may raise an error if the expression is not yet supported in the sql engine.

            >>> import daft
            >>> df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6], "z": [7, 9, 9]})
            >>> df.where("z = 9 AND y > 5").collect()
            ╭───────┬───────┬───────╮
            │ x     ┆ y     ┆ z     │
            │ ---   ┆ ---   ┆ ---   │
            │ Int64 ┆ Int64 ┆ Int64 │
            ╞═══════╪═══════╪═══════╡
            │ 3     ┆ 6     ┆ 9     │
            ╰───────┴───────┴───────╯
            <BLANKLINE>
            (Showing first 1 of 1 rows)
        """
        if isinstance(predicate, str):
            from daft.sql.sql import sql_expr

            predicate = sql_expr(predicate)
        builder = self._builder.filter(predicate)
        return DataFrame(builder)

    @DataframePublicAPI
    def with_column(
        self,
        column_name: str,
        expr: Expression,
    ) -> "DataFrame":
        """Adds a column to the current DataFrame with an Expression, equivalent to a ``select`` with all current columns and the new one.

        Args:
            column_name (str): name of new column
            expr (Expression): expression of the new column.

        Returns:
            DataFrame: DataFrame with new column.

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1, 2, 3]})
            >>> new_df = df.with_column("x+1", df["x"] + 1)
            >>> new_df.show()
            ╭───────┬───────╮
            │ x     ┆ x+1   │
            │ ---   ┆ ---   │
            │ Int64 ┆ Int64 │
            ╞═══════╪═══════╡
            │ 1     ┆ 2     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 2     ┆ 3     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 3     ┆ 4     │
            ╰───────┴───────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)
        """
        return self.with_columns({column_name: expr})

    @DataframePublicAPI
    def with_columns(
        self,
        columns: dict[str, Expression],
    ) -> "DataFrame":
        """Adds columns to the current DataFrame with Expressions, equivalent to a ``select`` with all current columns and the new ones.

        Args:
            columns (Dict[str, Expression]): Dictionary of new columns in the format { name: expression }

        Returns:
            DataFrame: DataFrame with new columns.

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
            >>> new_df = df.with_columns({"foo": df["x"] + 1, "bar": df["y"] - df["x"]})
            >>> new_df.show()
            ╭───────┬───────┬───────┬───────╮
            │ x     ┆ y     ┆ foo   ┆ bar   │
            │ ---   ┆ ---   ┆ ---   ┆ ---   │
            │ Int64 ┆ Int64 ┆ Int64 ┆ Int64 │
            ╞═══════╪═══════╪═══════╪═══════╡
            │ 1     ┆ 4     ┆ 2     ┆ 3     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 2     ┆ 5     ┆ 3     ┆ 3     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 3     ┆ 6     ┆ 4     ┆ 3     │
            ╰───────┴───────┴───────┴───────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)
        """
        new_columns = [col.alias(name) for name, col in columns.items()]

        builder = self._builder.with_columns(new_columns)
        return DataFrame(builder)

    @DataframePublicAPI
    def with_column_renamed(self, existing: str, new: str) -> "DataFrame":
        """Renames a column in the current DataFrame.

        If the column in the DataFrame schema does not exist, this will be a no-op.

        Args:
            existing (str): name of the existing column to rename
            new (str): new name for the column

        Returns:
            DataFrame: DataFrame with the column renamed.

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
            >>> df.with_column_renamed("x", "foo").show()
            ╭───────┬───────╮
            │ foo   ┆ y     │
            │ ---   ┆ ---   │
            │ Int64 ┆ Int64 │
            ╞═══════╪═══════╡
            │ 1     ┆ 4     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 2     ┆ 5     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 3     ┆ 6     │
            ╰───────┴───────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)
        """
        builder = self._builder.with_column_renamed(existing, new)
        return DataFrame(builder)

    @DataframePublicAPI
    def with_columns_renamed(self, cols_map: dict[str, str]) -> "DataFrame":
        """Renames multiple columns in the current DataFrame.

        If the columns in the DataFrame schema do not exist, this will be a no-op.

        Args:
            cols_map (Dict[str, str]): Dictionary of columns to rename in the format { existing: new }

        Returns:
            DataFrame: DataFrame with the columns renamed.

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
            >>> df.with_columns_renamed({"x": "foo", "y": "bar"}).show()
            ╭───────┬───────╮
            │ foo   ┆ bar   │
            │ ---   ┆ ---   │
            │ Int64 ┆ Int64 │
            ╞═══════╪═══════╡
            │ 1     ┆ 4     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 2     ┆ 5     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 3     ┆ 6     │
            ╰───────┴───────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)
        """
        builder = self._builder.with_columns_renamed(cols_map)
        return DataFrame(builder)

    @DataframePublicAPI
    def sort(
        self,
        by: Union[ColumnInputType, list[ColumnInputType]],
        desc: Union[bool, list[bool]] = False,
        nulls_first: Optional[Union[bool, list[bool]]] = None,
    ) -> "DataFrame":
        """Sorts DataFrame globally.

        Args:
            by (Union[ColumnInputType, List[ColumnInputType]]): column to sort by. Can be `str` or expression as well as a list of either.
            desc (Union[bool, List[bool]), optional): Sort by descending order. Defaults to False.
            nulls_first (Union[bool, List[bool]), optional): Sort by nulls first. Defaults to nulls being treated as the greatest value.

        Returns:
            DataFrame: Sorted DataFrame.

        Note:
            * Since this a global sort, this requires an expensive repartition which can be quite slow.
            * Supports multicolumn sorts and can have unique `descending` and `nulls_first` flags per column.

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"x": [3, 2, 1], "y": [6, 4, 5]})
            >>> sorted_df = df.sort(df["x"] + df["y"])
            >>> sorted_df.show()
            ╭───────┬───────╮
            │ x     ┆ y     │
            │ ---   ┆ ---   │
            │ Int64 ┆ Int64 │
            ╞═══════╪═══════╡
            │ 2     ┆ 4     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 1     ┆ 5     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 3     ┆ 6     │
            ╰───────┴───────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

            You can also sort by multiple columns, and specify the 'descending' flag for each column:

            >>> df = daft.from_pydict({"x": [1, 2, 1, 2], "y": [9, 8, 7, 6]})
            >>> sorted_df = df.sort(["x", "y"], [True, False])
            >>> sorted_df.show()
            ╭───────┬───────╮
            │ x     ┆ y     │
            │ ---   ┆ ---   │
            │ Int64 ┆ Int64 │
            ╞═══════╪═══════╡
            │ 2     ┆ 6     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 2     ┆ 8     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 1     ┆ 7     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 1     ┆ 9     │
            ╰───────┴───────╯
            <BLANKLINE>
            (Showing first 4 of 4 rows)

            You can also specify null positioning (first/last) for each column

            >>> df = daft.from_pydict({"x": [1, 2, 1, 2, None], "y": [9, 8, None, 6, None]})
            >>> sorted_df = df.sort(["x", "y"], [True, False], nulls_first=[True, True])
            >>> sorted_df.show()
            ╭───────┬───────╮
            │ x     ┆ y     │
            │ ---   ┆ ---   │
            │ Int64 ┆ Int64 │
            ╞═══════╪═══════╡
            │ None  ┆ None  │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 2     ┆ 6     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 2     ┆ 8     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 1     ┆ None  │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 1     ┆ 9     │
            ╰───────┴───────╯
            <BLANKLINE>
            (Showing first 5 of 5 rows)
        """
        if not isinstance(by, list):
            by = [
                by,
            ]

        if nulls_first is None:
            nulls_first = desc

        sort_by = self.__column_input_to_expression(by)

        builder = self._builder.sort(sort_by=sort_by, descending=desc, nulls_first=nulls_first)
        return DataFrame(builder)

    @DataframePublicAPI
    def limit(self, num: int) -> "DataFrame":
        """Limits the rows in the DataFrame to the first ``N`` rows, similar to a SQL ``LIMIT``.

        Args:
            num (int): maximum rows to allow.

        Returns:
            DataFrame: Limited DataFrame

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1, 2, 3, 4, 5, 6, 7]})
            >>> df_limited = df.limit(5)  # returns 5 rows
            >>> df_limited.show()
            ╭───────╮
            │ x     │
            │ ---   │
            │ Int64 │
            ╞═══════╡
            │ 1     │
            ├╌╌╌╌╌╌╌┤
            │ 2     │
            ├╌╌╌╌╌╌╌┤
            │ 3     │
            ├╌╌╌╌╌╌╌┤
            │ 4     │
            ├╌╌╌╌╌╌╌┤
            │ 5     │
            ╰───────╯
            <BLANKLINE>
            (Showing first 5 of 5 rows)

        """
        builder = self._builder.limit(num, eager=False)
        return DataFrame(builder)

    @DataframePublicAPI
    def offset(self, num: int) -> "DataFrame":
        """Returns a new DataFrame by skipping the first ``N`` rows, similar to a SQL ``Offset``.

        Args:
            num (int): the number of rows to skip

        Returns:
            DataFrame: A new DataFrame by skipping the first ``N`` rows

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1, 2, 3, 4, 5, 6, 7]})
            >>> df = df.offset(1).limit(5)  # skip the first row and return 5 rows
            >>> df.show()
            ╭───────╮
            │ x     │
            │ ---   │
            │ Int64 │
            ╞═══════╡
            │ 2     │
            ├╌╌╌╌╌╌╌┤
            │ 3     │
            ├╌╌╌╌╌╌╌┤
            │ 4     │
            ├╌╌╌╌╌╌╌┤
            │ 5     │
            ├╌╌╌╌╌╌╌┤
            │ 6     │
            ╰───────╯
            <BLANKLINE>
            (Showing first 5 of 5 rows)

        """
        builder = self._builder.offset(num)
        return DataFrame(builder)

    def _shard(self, strategy: Literal["file"], world_size: int, rank: int) -> "DataFrame":
        """Shards the descendent scan node of the dataframe using the given sharding strategy.

        If there are more than one scan nodes that are descendents of this shard operator,
        this will not work.

        Only "file" strategy is supported for now for file-based sharding.

        This is currently an internal API that should be used with dataloading APIs like .to_torch_iter_dataset().
        """
        if strategy != "file":
            raise ValueError("Only file-based sharding is supported")
        if world_size <= 0:
            raise ValueError("World size for sharding must be greater than zero")
        if rank >= world_size:
            raise ValueError("Rank must be less than the world size for sharding")
        builder = self._builder.shard(strategy, world_size, rank)
        return DataFrame(builder)

    @DataframePublicAPI
    def count_rows(self) -> int:
        """Executes the Dataframe to count the number of rows.

        Returns:
            int: count of the number of rows in this DataFrame.

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6], "z": [7, 8, 9]})
            >>> df.count_rows()
            3

        Note:
            This will execute the DataFrame and return the number of rows in it.

        """
        if self._result is not None:
            return len(self._result)
        builder = self._builder.count()
        count_df = DataFrame(builder)
        # Expects builder to produce a single-partition, single-row DataFrame containing
        # a "count" column, where the lone value represents the row count for the DataFrame.
        return count_df.to_pydict()["count"][0]

    @DataframePublicAPI
    def repartition(self, num: Optional[int], *partition_by: ColumnInputType) -> "DataFrame":
        """Repartitions DataFrame to ``num`` partitions.

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

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6], "z": [7, 8, 9]})
            >>> repartitioned_df = df.repartition(3)

        """
        if get_or_create_runner().name == "native":
            warnings.warn(
                "DataFrame.repartition not supported on the NativeRunner. This will be a no-op. Please use the RayRunner via `daft.set_runner_ray()` instead if you need to repartition."
            )
        if len(partition_by) == 0:
            warnings.warn(
                "No columns specified for repartition, so doing a random shuffle. If you do not require rebalancing of "
                "partitions, you may instead prefer using `df.into_partitions(N)` which is a cheaper operation that "
                "avoids shuffling data."
            )
            builder = self._builder.random_shuffle(num)
        else:
            builder = self._builder.hash_repartition(num, self.__column_input_to_expression(partition_by))
        return DataFrame(builder)

    @DataframePublicAPI
    def into_partitions(self, num: int) -> "DataFrame":
        """Splits or coalesces DataFrame to ``num`` partitions. Order is preserved.

        This will naively greedily split partitions in a round-robin fashion to hit the targeted number of partitions.
        The number of rows/size in a given partition is not taken into account during the splitting.

        Args:
            num (int): number of target partitions.

        Returns:
            DataFrame: Dataframe with `num` partitions.
        """
        if get_or_create_runner().name == "native":
            warnings.warn(
                "DataFrame.into_partitions not supported on the NativeRunner. This will be a no-op. Please use the RayRunner via `daft.set_runner_ray()` instead if you need to repartition."
            )

        builder = self._builder.into_partitions(num)
        return DataFrame(builder)

    @DataframePublicAPI
    def into_batches(self, batch_size: int) -> "DataFrame":
        """Splits or coalesces DataFrame to partitions of size ``batch_size``.

        Note:
            Batch sizing is performed on a best-effort basis.
            The heuristic is to emit a batch when we have enough rows to fill `batch_size * 0.8` rows.
            This approach prioritizes processing efficiency over uniform batch sizes, especially when using the Ray Runner, as batches can be distributed over the cluster.
            The exception to this is that the last batch will be the remainder of the total number of rows in the DataFrame.

        Args:
            batch_size (int): number of target rows per partition.

        Returns:
            DataFrame: Dataframe with `batch_size` rows per partition.

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]})
            >>> df = df.into_batches(2)
            >>> for i, block in enumerate(df.to_arrow_iter()):
            ...     assert len(block) == 2, f"Expected batch size 2, got {len(block)}"
        """
        if batch_size <= 0:
            raise ValueError("batch_size must be greater than 0")

        builder = self._builder.into_batches(batch_size)
        return DataFrame(builder)

    @DataframePublicAPI
    def join(
        self,
        other: "DataFrame",
        on: Optional[Union[list[ColumnInputType], ColumnInputType]] = None,
        left_on: Optional[Union[list[ColumnInputType], ColumnInputType]] = None,
        right_on: Optional[Union[list[ColumnInputType], ColumnInputType]] = None,
        how: Literal["inner", "left", "right", "outer", "anti", "semi", "cross"] = "inner",
        strategy: Optional[Literal["hash", "sort_merge", "broadcast"]] = None,
        prefix: Optional[str] = None,
        suffix: Optional[str] = None,
    ) -> "DataFrame":
        """Column-wise join of the current DataFrame with an ``other`` DataFrame, similar to a SQL ``JOIN``.

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

        Examples:
            >>> import daft
            >>> from daft import col
            >>> df1 = daft.from_pydict({"a": ["w", "x", "y"], "b": [1, 2, 3]})
            >>> df2 = daft.from_pydict({"a": ["x", "y", "z"], "b": [20, 30, 40]})
            >>> joined_df = df1.join(df2, left_on=df1["a"], right_on=df2["a"])
            >>> joined_df.show()
            ╭────────┬───────┬─────────╮
            │ a      ┆ b     ┆ right.b │
            │ ---    ┆ ---   ┆ ---     │
            │ String ┆ Int64 ┆ Int64   │
            ╞════════╪═══════╪═════════╡
            │ x      ┆ 2     ┆ 20      │
            ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
            │ y      ┆ 3     ┆ 30      │
            ╰────────┴───────┴─────────╯
            <BLANKLINE>
            (Showing first 2 of 2 rows)
        """
        if how == "cross":
            if any(side_on is not None for side_on in [on, left_on, right_on]):
                raise ValueError("In a cross join, `on`, `left_on`, and `right_on` cannot be set")
            if strategy is not None:
                raise ValueError("In a cross join, `strategy` cannot be set")
            left_on = []
            right_on = []
        elif on is None:
            if left_on is None or right_on is None:
                raise ValueError("If `on` is None then both `left_on` and `right_on` must not be None")
        else:
            if left_on is not None or right_on is not None:
                raise ValueError("If `on` is not None then both `left_on` and `right_on` must be None")
            left_on = on
            right_on = on

        join_type = JoinType.from_join_type_str(how)
        join_strategy = JoinStrategy.from_join_strategy_str(strategy) if strategy is not None else None

        if join_strategy == JoinStrategy.SortMerge and join_type != JoinType.Inner:
            raise ValueError("Sort merge join only supports inner joins")
        elif join_strategy == JoinStrategy.Broadcast and join_type == JoinType.Outer:
            raise ValueError("Broadcast join does not support outer joins")

        left_exprs = self.__column_input_to_expression(tuple(left_on) if isinstance(left_on, list) else (left_on,))
        right_exprs = self.__column_input_to_expression(tuple(right_on) if isinstance(right_on, list) else (right_on,))
        builder = self._builder.join(
            other._builder,
            left_on=left_exprs,
            right_on=right_exprs,
            how=join_type,
            strategy=join_strategy,
            prefix=prefix,
            suffix=suffix,
        )
        return DataFrame(builder)

    @DataframePublicAPI
    def concat(self, other: "DataFrame") -> "DataFrame":
        """Concatenates two DataFrames together in a "vertical" concatenation.

        The resulting DataFrame has number of rows equal to the sum of the number of rows of the input DataFrames.

        Args:
            other (DataFrame): other DataFrame to concatenate

        Returns:
            DataFrame: DataFrame with rows from `self` on top and rows from `other` at the bottom.

        Note:
            DataFrames being concatenated **must have exactly the same schema**. You may wish to use the
            [df.select()][daft.DataFrame.select] and [expr.cast()][daft.expressions.Expression.cast] methods
            to ensure schema compatibility before concatenation.

        Examples:
            >>> import daft
            >>> df1 = daft.from_pydict({"a": [1, 2], "b": [3, 4]})
            >>> df2 = daft.from_pydict({"a": [5, 6], "b": [7, 8]})
            >>> concatenated_df = df1.concat(df2)
            >>> concatenated_df.show()
            ╭───────┬───────╮
            │ a     ┆ b     │
            │ ---   ┆ ---   │
            │ Int64 ┆ Int64 │
            ╞═══════╪═══════╡
            │ 1     ┆ 3     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 2     ┆ 4     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 5     ┆ 7     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 6     ┆ 8     │
            ╰───────┴───────╯
            <BLANKLINE>
            (Showing first 4 of 4 rows)
        """
        if self.schema() != other.schema():
            raise ValueError(
                f"DataFrames must have exactly the same schema for concatenation!\nExpected:\n{self.schema()}\n\nReceived:\n{other.schema()}"
            )
        builder = self._builder.concat(other._builder)
        return DataFrame(builder)

    @DataframePublicAPI
    def drop_nan(self, *cols: ColumnInputType) -> "DataFrame":
        """Drops rows that contains NaNs. If cols is None it will drop rows with any NaN value.

        If column names are supplied, it will drop only those rows that contains NaNs in one of these columns.

        Args:
            *cols (str): column names by which rows containing nans/NULLs should be filtered

        Returns:
            DataFrame: DataFrame without NaNs in specified/all columns

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"a": [1.0, 2.2, 3.5, float("nan")]})
            >>> df.drop_nan().collect()  # drops rows where any column contains NaN values
            ╭─────────╮
            │ a       │
            │ ---     │
            │ Float64 │
            ╞═════════╡
            │ 1       │
            ├╌╌╌╌╌╌╌╌╌┤
            │ 2.2     │
            ├╌╌╌╌╌╌╌╌╌┤
            │ 3.5     │
            ╰─────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

            >>> import daft
            >>> df = daft.from_pydict({"a": [1.6, 2.5, 3.3, float("nan")]})
            >>> df.drop_nan("a").collect()  # drops rows where column `a` contains NaN values
            ╭─────────╮
            │ a       │
            │ ---     │
            │ Float64 │
            ╞═════════╡
            │ 1.6     │
            ├╌╌╌╌╌╌╌╌╌┤
            │ 2.5     │
            ├╌╌╌╌╌╌╌╌╌┤
            │ 3.3     │
            ╰─────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

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

        # avoid superfluous .where with empty iterable when nothing to filter.
        if not float_columns:
            return self

        from daft.functions import is_nan, when

        return self.where(
            ~reduce(
                lambda x, y: when(x.is_null(), lit(False)).otherwise(x) | when(y.is_null(), lit(False)).otherwise(y),
                (is_nan(x) for x in float_columns),
            )
        )

    @DataframePublicAPI
    def drop_null(self, *cols: ColumnInputType) -> "DataFrame":
        """Drops rows that contains NaNs or NULLs. If cols is None it will drop rows with any NULL value.

        If column names are supplied, it will drop only those rows that contains NULLs in one of these columns.

        Args:
            *cols (str): column names by which rows containing nans should be filtered

        Returns:
            DataFrame: DataFrame without missing values in specified/all columns

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"a": [1.6, 2.5, None, float("NaN")]})
            >>> df.drop_null("a").collect()
            ╭─────────╮
            │ a       │
            │ ---     │
            │ Float64 │
            ╞═════════╡
            │ 1.6     │
            ├╌╌╌╌╌╌╌╌╌┤
            │ 2.5     │
            ├╌╌╌╌╌╌╌╌╌┤
            │ NaN     │
            ╰─────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)


        """
        if len(cols) == 0:
            columns = self.__column_input_to_expression(self.column_names)
        else:
            columns = self.__column_input_to_expression(cols)
        return self.where(~reduce(lambda x, y: x | y, (x.is_null() for x in columns)))

    @DataframePublicAPI
    def explode(self, *columns: ColumnInputType) -> "DataFrame":
        """Explodes a List column, where every element in each row's List becomes its own row, and all other columns in the DataFrame are duplicated across rows.

        If multiple columns are specified, each row must contain the same number of items in each specified column.

        Exploding Null values or empty lists will create a single Null entry (see example below).

        Args:
            *columns (ColumnInputType): columns to explode

        Returns:
            DataFrame: DataFrame with exploded column

        Examples:
            >>> import daft
            >>> df = daft.from_pydict(
            ...     {
            ...         "x": [[1], [2, 3]],
            ...         "y": [["a"], ["b", "c"]],
            ...         "z": [
            ...             [1.0],
            ...             [2.0, 2.0],
            ...         ],
            ...     }
            ... )
            >>> df.collect()
            ╭─────────────┬──────────────┬───────────────╮
            │ x           ┆ y            ┆ z             │
            │ ---         ┆ ---          ┆ ---           │
            │ List[Int64] ┆ List[String] ┆ List[Float64] │
            ╞═════════════╪══════════════╪═══════════════╡
            │ [1]         ┆ [a]          ┆ [1]           │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ [2, 3]      ┆ [b, c]       ┆ [2, 2]        │
            ╰─────────────┴──────────────┴───────────────╯
            <BLANKLINE>
            (Showing first 2 of 2 rows)
            >>> df.explode(df["x"], df["y"]).collect()
            ╭───────┬────────┬───────────────╮
            │ x     ┆ y      ┆ z             │
            │ ---   ┆ ---    ┆ ---           │
            │ Int64 ┆ String ┆ List[Float64] │
            ╞═══════╪════════╪═══════════════╡
            │ 1     ┆ a      ┆ [1]           │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2     ┆ b      ┆ [2, 2]        │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 3     ┆ c      ┆ [2, 2]        │
            ╰───────┴────────┴───────────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

            Example with Null values and empty lists:

            >>> df2 = daft.from_pydict(
            ...     {"id": [1, 2, 3, 4], "values": [[1, 2], [], None, [3]], "labels": [["a", "b"], [], None, ["c"]]}
            ... )
            >>> df2.collect()
            ╭───────┬─────────────┬──────────────╮
            │ id    ┆ values      ┆ labels       │
            │ ---   ┆ ---         ┆ ---          │
            │ Int64 ┆ List[Int64] ┆ List[String] │
            ╞═══════╪═════════════╪══════════════╡
            │ 1     ┆ [1, 2]      ┆ [a, b]       │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2     ┆ []          ┆ []           │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 3     ┆ None        ┆ None         │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 4     ┆ [3]         ┆ [c]          │
            ╰───────┴─────────────┴──────────────╯
            <BLANKLINE>
            (Showing first 4 of 4 rows)
            >>> df2.explode(df2["values"], df2["labels"]).collect()
            ╭───────┬────────┬────────╮
            │ id    ┆ values ┆ labels │
            │ ---   ┆ ---    ┆ ---    │
            │ Int64 ┆ Int64  ┆ String │
            ╞═══════╪════════╪════════╡
            │ 1     ┆ 1      ┆ a      │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ 1     ┆ 2      ┆ b      │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ 2     ┆ None   ┆ None   │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ 3     ┆ None   ┆ None   │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ 4     ┆ 3      ┆ c      │
            ╰───────┴────────┴────────╯
            <BLANKLINE>
            (Showing first 5 of 5 rows)

        """
        parsed_exprs = self.__column_input_to_expression(columns)
        builder = self._builder.explode(parsed_exprs)
        return DataFrame(builder)

    @DataframePublicAPI
    def unpivot(
        self,
        ids: ManyColumnsInputType,
        values: ManyColumnsInputType = [],
        variable_name: str = "variable",
        value_name: str = "value",
    ) -> "DataFrame":
        """Unpivots a DataFrame from wide to long format.

        Args:
            ids (ManyColumnsInputType): Columns to keep as identifiers
            values (Optional[ManyColumnsInputType]): Columns to unpivot. If not specified, all columns except ids will be unpivoted.
            variable_name (Optional[str]): Name of the variable column. Defaults to "variable".
            value_name (Optional[str]): Name of the value column. Defaults to "value".

        Returns:
            DataFrame: Unpivoted DataFrame

        Tip:
            See also [melt][daft.DataFrame.melt]

        Examples:
            >>> import daft
            >>> df = daft.from_pydict(
            ...     {
            ...         "year": [2020, 2021, 2022],
            ...         "Jan": [10, 30, 50],
            ...         "Feb": [20, 40, 60],
            ...     }
            ... )
            >>> df = df.unpivot("year", ["Jan", "Feb"], variable_name="month", value_name="inventory")
            >>> df = df.sort("year")
            >>> df.show()
            ╭───────┬────────┬───────────╮
            │ year  ┆ month  ┆ inventory │
            │ ---   ┆ ---    ┆ ---       │
            │ Int64 ┆ String ┆ Int64     │
            ╞═══════╪════════╪═══════════╡
            │ 2020  ┆ Jan    ┆ 10        │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2020  ┆ Feb    ┆ 20        │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2021  ┆ Jan    ┆ 30        │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2021  ┆ Feb    ┆ 40        │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2022  ┆ Jan    ┆ 50        │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2022  ┆ Feb    ┆ 60        │
            ╰───────┴────────┴───────────╯
            <BLANKLINE>
            (Showing first 6 of 6 rows)

        """
        ids_exprs = column_inputs_to_expressions(ids)
        values_exprs = column_inputs_to_expressions(values)

        builder = self._builder.unpivot(ids_exprs, values_exprs, variable_name, value_name)
        return DataFrame(builder)

    @DataframePublicAPI
    def melt(
        self,
        ids: ManyColumnsInputType,
        values: ManyColumnsInputType = [],
        variable_name: str = "variable",
        value_name: str = "value",
    ) -> "DataFrame":
        """Alias for unpivot.

        Args:
            ids (ManyColumnsInputType): Columns to keep as identifiers
            values (Optional[ManyColumnsInputType]): Columns to unpivot. If not specified, all columns except ids will be unpivoted.
            variable_name (Optional[str]): Name of the variable column. Defaults to "variable".
            value_name (Optional[str]): Name of the value column. Defaults to "value".

        Returns:
            DataFrame: Unpivoted DataFrame

        Examples:
            >>> import daft
            >>> df = daft.from_pydict(
            ...     {
            ...         "year": [2020, 2021, 2022],
            ...         "Jan": [10, 30, 50],
            ...         "Feb": [20, 40, 60],
            ...     }
            ... )
            >>> df = df.melt("year", ["Jan", "Feb"], variable_name="month", value_name="inventory")
            >>> df = df.sort("year")
            >>> df.show()
            ╭───────┬────────┬───────────╮
            │ year  ┆ month  ┆ inventory │
            │ ---   ┆ ---    ┆ ---       │
            │ Int64 ┆ String ┆ Int64     │
            ╞═══════╪════════╪═══════════╡
            │ 2020  ┆ Jan    ┆ 10        │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2020  ┆ Feb    ┆ 20        │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2021  ┆ Jan    ┆ 30        │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2021  ┆ Feb    ┆ 40        │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2022  ┆ Jan    ┆ 50        │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2022  ┆ Feb    ┆ 60        │
            ╰───────┴────────┴───────────╯
            <BLANKLINE>
            (Showing first 6 of 6 rows)

        Tip:
            See also [unpivot][daft.DataFrame.unpivot]
        """
        return self.unpivot(ids, values, variable_name, value_name)

    @DataframePublicAPI
    def transform(self, func: Callable[..., "DataFrame"], *args: Any, **kwargs: Any) -> "DataFrame":
        """Apply a function that takes and returns a DataFrame.

        Allow splitting your transformation into different units of work (functions) while preserving the syntax for chaining transformations.

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"col_a": [1, 2, 3, 4]})
            >>> def add_1(df):
            ...     df = df.select(daft.col("col_a") + 1)
            ...     return df
            >>> def multiply_x(df, x):
            ...     df = df.select(daft.col("col_a") * x)
            ...     return df
            >>> df = df.transform(add_1).transform(multiply_x, 4)
            >>> df.show()
            ╭───────╮
            │ col_a │
            │ ---   │
            │ Int64 │
            ╞═══════╡
            │ 8     │
            ├╌╌╌╌╌╌╌┤
            │ 12    │
            ├╌╌╌╌╌╌╌┤
            │ 16    │
            ├╌╌╌╌╌╌╌┤
            │ 20    │
            ╰───────╯
            <BLANKLINE>
            (Showing first 4 of 4 rows)

        Args:
            func: A function that takes and returns a DataFrame.
            *args: Positional arguments to pass to func.
            **kwargs: Keyword arguments to pass to func.

        Returns:
            DataFrame: Transformed DataFrame.
        """
        result = func(self, *args, **kwargs)
        assert isinstance(
            result, DataFrame
        ), f"Func returned an instance of type [{type(result)}], should have been DataFrame."
        return result

    def _agg(
        self,
        to_agg: Iterable[Expression],
        group_by: Optional[ExpressionsProjection] = None,
    ) -> "DataFrame":
        builder = self._builder.agg(list(to_agg), list(group_by) if group_by is not None else None)
        return DataFrame(builder)

    def _map_agg_string_to_expr(self, expr: Expression, op: str) -> Expression:
        if op == "sum":
            return expr.sum()
        elif op == "count":
            return expr.count()
        elif op == "min":
            return expr.min()
        elif op == "max":
            return expr.max()
        elif op == "mean":
            return expr.mean()
        elif op == "any_value":
            return expr.any_value()
        elif op == "list":
            return expr.list_agg()
        elif op == "set":
            return expr.list_agg_distinct()
        elif op == "concat":
            return expr.string_agg()
        elif op == "skew":
            return expr.skew()

        raise NotImplementedError(f"Aggregation {op} is not implemented.")

    def _apply_agg_fn(
        self,
        fn: Callable[[Expression], Expression],
        cols: tuple[ManyColumnsInputType, ...],
        group_by: Optional[ExpressionsProjection] = None,
    ) -> "DataFrame":
        if len(cols) == 0:
            warnings.warn("No columns specified; performing aggregation on all columns.")

            groupby_name_set = set() if group_by is None else group_by.to_name_set()
            cols = tuple(c for c in self.column_names if c not in groupby_name_set)
        exprs = self._wildcard_inputs_to_expressions(cols)
        return self._agg([fn(c) for c in exprs], group_by)

    def _map_groups(self, udf: Expression, group_by: Optional[ExpressionsProjection] = None) -> "DataFrame":
        builder = self._builder.map_groups(udf, list(group_by) if group_by is not None else None)
        return DataFrame(builder)

    @DataframePublicAPI
    def sum(self, *cols: ManyColumnsInputType) -> "DataFrame":
        """Performs a global sum on the DataFrame.

        Args:
            *cols (Union[str, Expression]): columns to sum
        Returns:
            DataFrame: Globally aggregated sums. Should be a single row.

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"col_a": [1, 2, 3]})
            >>> df = df.sum("col_a")
            >>> df.show()
            ╭───────╮
            │ col_a │
            │ ---   │
            │ Int64 │
            ╞═══════╡
            │ 6     │
            ╰───────╯
            <BLANKLINE>
            (Showing first 1 of 1 rows)
        """
        return self._apply_agg_fn(Expression.sum, cols)

    @DataframePublicAPI
    def mean(self, *cols: ColumnInputType) -> "DataFrame":
        """Performs a global mean on the DataFrame.

        Args:
            *cols (Union[str, Expression]): columns to mean
        Returns:
            DataFrame: Globally aggregated mean. Should be a single row.

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"col_a": [1, 2, 3]})
            >>> df = df.mean("col_a")
            >>> df.show()
            ╭─────────╮
            │ col_a   │
            │ ---     │
            │ Float64 │
            ╞═════════╡
            │ 2       │
            ╰─────────╯
            <BLANKLINE>
            (Showing first 1 of 1 rows)
        """
        return self._apply_agg_fn(Expression.mean, cols)

    @DataframePublicAPI
    def stddev(self, *cols: ColumnInputType) -> "DataFrame":
        """Performs a global standard deviation on the DataFrame.

        Args:
            *cols (Union[str, Expression]): columns to stddev
        Returns:
            DataFrame: Globally aggregated standard deviation. Should be a single row.

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"col_a": [0, 1, 2]})
            >>> df = df.stddev("col_a")
            >>> df.show()
            ╭───────────────────╮
            │ col_a             │
            │ ---               │
            │ Float64           │
            ╞═══════════════════╡
            │ 0.816496580927726 │
            ╰───────────────────╯
            <BLANKLINE>
            (Showing first 1 of 1 rows)

        """
        return self._apply_agg_fn(Expression.stddev, cols)

    @DataframePublicAPI
    def min(self, *cols: ColumnInputType) -> "DataFrame":
        """Performs a global min on the DataFrame.

        Args:
            *cols (Union[str, Expression]): columns to min
        Returns:
            DataFrame: Globally aggregated min. Should be a single row.

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"col_a": [1, 2, 3]})
            >>> df = df.min("col_a")
            >>> df.show()
            ╭───────╮
            │ col_a │
            │ ---   │
            │ Int64 │
            ╞═══════╡
            │ 1     │
            ╰───────╯
            <BLANKLINE>
            (Showing first 1 of 1 rows)
        """
        return self._apply_agg_fn(Expression.min, cols)

    @DataframePublicAPI
    def max(self, *cols: ColumnInputType) -> "DataFrame":
        """Performs a global max on the DataFrame.

        Args:
            *cols (Union[str, Expression]): columns to max
        Returns:
            DataFrame: Globally aggregated max. Should be a single row.

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"col_a": [1, 2, 3]})
            >>> df = df.max("col_a")
            >>> df.show()
            ╭───────╮
            │ col_a │
            │ ---   │
            │ Int64 │
            ╞═══════╡
            │ 3     │
            ╰───────╯
            <BLANKLINE>
            (Showing first 1 of 1 rows)
        """
        return self._apply_agg_fn(Expression.max, cols)

    @DataframePublicAPI
    def any_value(self, *cols: ColumnInputType) -> "DataFrame":
        """Returns an arbitrary value on this DataFrame.

        Values for each column are not guaranteed to be from the same row.

        Args:
            *cols (Union[str, Expression]): columns to get an arbitrary value from
        Returns:
            DataFrame: DataFrame with any values.

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"col_a": [1, 2, 3]})
            >>> df = df.any_value("col_a")
            >>> df.show()
            ╭───────╮
            │ col_a │
            │ ---   │
            │ Int64 │
            ╞═══════╡
            │ 1     │
            ╰───────╯
            <BLANKLINE>
            (Showing first 1 of 1 rows)
        """
        return self._apply_agg_fn(Expression.any_value, cols)

    @DataframePublicAPI
    def count(self, *cols: ColumnInputType) -> "DataFrame":
        """Performs a global count on the DataFrame.

        Args:
            *cols (Union[str, Expression]): columns to count
        Returns:
            DataFrame: Globally aggregated count. Should be a single row.


        Examples:
            If no columns are specified (i.e. in the case you call `df.count()`), or only the literal string "*",
            this functions very similarly to a COUNT(*) operation in SQL and will return a new dataframe with a
            single column with the name "count".

            >>> import daft
            >>> from daft import col
            >>> df = daft.from_pydict({"foo": [1, None, None], "bar": [None, 2, 2], "baz": [3, 4, 5]})
            >>> df.count().show()  # equivalent to df.count("*").show()
            ╭────────╮
            │ count  │
            │ ---    │
            │ UInt64 │
            ╞════════╡
            │ 3      │
            ╰────────╯
            <BLANKLINE>
            (Showing first 1 of 1 rows)

            However, specifying some column names would instead change the behavior to count all non-null values,
            similar to a SQL command for `SELECT COUNT(foo), COUNT(bar) FROM df`. Also, using `df.count(col("*"))`
            will expand out into count() for each column.

            >>> df.count("foo", "bar").show()
            ╭────────┬────────╮
            │ foo    ┆ bar    │
            │ ---    ┆ ---    │
            │ UInt64 ┆ UInt64 │
            ╞════════╪════════╡
            │ 1      ┆ 2      │
            ╰────────┴────────╯
            <BLANKLINE>
            (Showing first 1 of 1 rows)

            >>> df.count(df["*"]).show()
            ╭────────┬────────┬────────╮
            │ foo    ┆ bar    ┆ baz    │
            │ ---    ┆ ---    ┆ ---    │
            │ UInt64 ┆ UInt64 ┆ UInt64 │
            ╞════════╪════════╪════════╡
            │ 1      ┆ 2      ┆ 3      │
            ╰────────┴────────┴────────╯
            <BLANKLINE>
            (Showing first 1 of 1 rows)

        """
        # Special case: treat this as a COUNT(*) operation which is likely what most people would expect
        # If user passes in "*", also do this behavior (by default it would count each column individually)
        if (
            len(cols) == 0
            or (len(cols) == 1 and isinstance(cols[0], str) and cols[0] == "*")
            or (len(cols) == 1 and isinstance(cols[0], int))
        ):
            builder = self._builder.count()
            return DataFrame(builder)

        if any(isinstance(c, str) and c == "*" for c in cols):
            # we do not support hybrid count-all and count-nonnull
            raise ValueError("Cannot call count() with both * and column names")

        # Otherwise, perform a column-wise count on the specified columns
        return self._apply_agg_fn(Expression.count, cols)

    @DataframePublicAPI
    def agg_list(self, *cols: ColumnInputType) -> "DataFrame":
        """Performs a global list agg on the DataFrame.

        Args:
            *cols (Union[str, Expression]): columns to form into a list
        Returns:
            DataFrame: Globally aggregated list. Should be a single row.

        Examples:
            >>> import daft
            >>> from daft import col
            >>> df = daft.from_pydict({"col_a": [1, 2, 3]})
            >>> df = df.agg_list("col_a")
            >>> df.show()
            ╭─────────────╮
            │ col_a       │
            │ ---         │
            │ List[Int64] │
            ╞═════════════╡
            │ [1, 2, 3]   │
            ╰─────────────╯
            <BLANKLINE>
            (Showing first 1 of 1 rows)
        """
        return self._apply_agg_fn(Expression.list_agg, cols)

    @DataframePublicAPI
    def agg_set(self, *cols: ColumnInputType) -> "DataFrame":
        """Performs a global set agg on the DataFrame (ignoring nulls).

        Args:
            *cols (Union[str, Expression]): columns to form into a set

        Returns:
            DataFrame: Globally aggregated set. Should be a single row.

        Examples:
            >>> import daft
            >>> from daft import col
            >>> df = daft.from_pydict({"col_a": [1, 2, 2, 3]})
            >>> df = df.agg_set("col_a")
            >>> df.show()
            ╭─────────────╮
            │ col_a       │
            │ ---         │
            │ List[Int64] │
            ╞═════════════╡
            │ [1, 2, 3]   │
            ╰─────────────╯
            <BLANKLINE>
            (Showing first 1 of 1 rows)
        """
        return self._apply_agg_fn(Expression.list_agg_distinct, cols)

    @DataframePublicAPI
    def agg_concat(self, *cols: ColumnInputType) -> "DataFrame":
        """Performs a global list concatenation agg on the DataFrame.

        Args:
            *cols (Union[str, Expression]): columns that are lists to concatenate
        Returns:
            DataFrame: Globally aggregated list. Should be a single row.

        Examples:
            >>> import daft
            >>> from daft import col
            >>> df = daft.from_pydict({"col_a": [[1, 2], [3, 4]]})
            >>> df = df.agg_concat("col_a")
            >>> df.show()
            ╭──────────────╮
            │ col_a        │
            │ ---          │
            │ List[Int64]  │
            ╞══════════════╡
            │ [1, 2, 3, 4] │
            ╰──────────────╯
            <BLANKLINE>
            (Showing first 1 of 1 rows)
        """
        return self._apply_agg_fn(Expression.string_agg, cols)

    @DataframePublicAPI
    def agg(self, *to_agg: Union[Expression, Iterable[Expression]]) -> "DataFrame":
        """Perform aggregations on this DataFrame.

        Allows for mixed aggregations for multiple columns and will return a single row that aggregated the entire DataFrame.

        Args:
            *to_agg (Expression): aggregation expressions

        Returns:
            DataFrame: DataFrame with aggregated results

        Examples:
            >>> import daft
            >>> from daft import col
            >>> df = daft.from_pydict(
            ...     {"student_id": [1, 2, 3, 4], "test1": [0.5, 0.4, 0.6, 0.7], "test2": [0.9, 0.8, 0.7, 1.0]}
            ... )
            >>> agg_df = df.agg(
            ...     df["test1"].mean(),
            ...     df["test2"].mean(),
            ...     ((df["test1"] + df["test2"]) / 2).min().alias("total_min"),
            ...     ((df["test1"] + df["test2"]) / 2).max().alias("total_max"),
            ... )
            >>> agg_df.show()
            ╭─────────┬────────────────────┬────────────────────┬───────────╮
            │ test1   ┆ test2              ┆ total_min          ┆ total_max │
            │ ---     ┆ ---                ┆ ---                ┆ ---       │
            │ Float64 ┆ Float64            ┆ Float64            ┆ Float64   │
            ╞═════════╪════════════════════╪════════════════════╪═══════════╡
            │ 0.55    ┆ 0.8500000000000001 ┆ 0.6000000000000001 ┆ 0.85      │
            ╰─────────┴────────────────────┴────────────────────┴───────────╯
            <BLANKLINE>
            (Showing first 1 of 1 rows)

        """
        to_agg_list = (
            list(to_agg[0])
            if (len(to_agg) == 1 and not isinstance(to_agg[0], Expression))
            else list(typing.cast("tuple[Expression]", to_agg))
        )

        for expr in to_agg_list:
            if not isinstance(expr, Expression):
                raise ValueError(f"DataFrame.agg() only accepts expression type, received: {type(expr)}")

        return self._agg(to_agg_list, group_by=None)

    @DataframePublicAPI
    def groupby(self, *group_by: ManyColumnsInputType) -> "GroupedDataFrame":
        """Performs a GroupBy on the DataFrame for aggregation.

        Args:
            *group_by (Union[str, Expression]): columns to group by

        Returns:
            GroupedDataFrame: DataFrame to Aggregate

        Examples:
            >>> import daft
            >>> from daft import col
            >>> df = daft.from_pydict(
            ...     {
            ...         "pet": ["cat", "dog", "dog", "cat"],
            ...         "age": [1, 2, 3, 4],
            ...         "name": ["Alex", "Jordan", "Sam", "Riley"],
            ...     }
            ... )
            >>> grouped_df = df.groupby("pet").agg(
            ...     df["age"].min().alias("min_age"),
            ...     df["age"].max().alias("max_age"),
            ...     df["pet"].count().alias("count"),
            ...     df["name"].any_value(),
            ... )
            >>> grouped_df = grouped_df.sort("pet")
            >>> grouped_df.show()
            ╭────────┬─────────┬─────────┬────────┬────────╮
            │ pet    ┆ min_age ┆ max_age ┆ count  ┆ name   │
            │ ---    ┆ ---     ┆ ---     ┆ ---    ┆ ---    │
            │ String ┆ Int64   ┆ Int64   ┆ UInt64 ┆ String │
            ╞════════╪═════════╪═════════╪════════╪════════╡
            │ cat    ┆ 1       ┆ 4       ┆ 2      ┆ Alex   │
            ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ dog    ┆ 2       ┆ 3       ┆ 2      ┆ Jordan │
            ╰────────┴─────────┴─────────┴────────┴────────╯
            <BLANKLINE>
            (Showing first 2 of 2 rows)

        """
        return GroupedDataFrame(self, ExpressionsProjection(self._wildcard_inputs_to_expressions(group_by)))

    @DataframePublicAPI
    def pivot(
        self,
        group_by: ManyColumnsInputType,
        pivot_col: ColumnInputType,
        value_col: ColumnInputType,
        agg_fn: str,
        names: Optional[list[str]] = None,
    ) -> "DataFrame":
        """Pivots a column of the DataFrame and performs an aggregation on the values.

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

        Examples:
            >>> import daft
            >>> data = {
            ...     "id": [1, 2, 3, 4],
            ...     "version": ["3.8", "3.8", "3.9", "3.9"],
            ...     "platform": ["macos", "macos", "macos", "windows"],
            ...     "downloads": [100, 200, 150, 250],
            ... }
            >>> df = daft.from_pydict(data)
            >>> df = df.pivot("version", "platform", "downloads", "sum")
            >>>
            >>> df = df.sort("version").select("version", "windows", "macos")
            >>> df.show()
            ╭─────────┬─────────┬───────╮
            │ version ┆ windows ┆ macos │
            │ ---     ┆ ---     ┆ ---   │
            │ String  ┆ Int64   ┆ Int64 │
            ╞═════════╪═════════╪═══════╡
            │ 3.8     ┆ None    ┆ 300   │
            ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 3.9     ┆ 250     ┆ 150   │
            ╰─────────┴─────────┴───────╯
            <BLANKLINE>
            (Showing first 2 of 2 rows)


        """
        group_by_expr = column_inputs_to_expressions(group_by)
        [pivot_col_expr, value_col_expr] = column_inputs_to_expressions([pivot_col, value_col])
        agg_expr = self._map_agg_string_to_expr(value_col_expr, agg_fn)

        if names is None:
            names = self.select(pivot_col_expr).distinct().to_pydict()[pivot_col_expr.name()]
            names = [str(x) for x in names]
        builder = self._builder.pivot(group_by_expr, pivot_col_expr, value_col_expr, agg_expr, names)
        return DataFrame(builder)

    @DataframePublicAPI
    def union(self, other: "DataFrame") -> "DataFrame":
        """Returns the distinct union of two DataFrames.

        Args:
            other (DataFrame): The DataFrame to union with this one.

        Returns:
            DataFrame: A new DataFrame containing the distinct rows from both DataFrames.

        Examples:
            >>> import daft
            >>> df1 = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
            >>> df2 = daft.from_pydict({"x": [3, 4, 5], "y": [6, 7, 8]})
            >>> df1.union(df2).sort("x").show()
            ╭───────┬───────╮
            │ x     ┆ y     │
            │ ---   ┆ ---   │
            │ Int64 ┆ Int64 │
            ╞═══════╪═══════╡
            │ 1     ┆ 4     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 2     ┆ 5     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 3     ┆ 6     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 4     ┆ 7     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 5     ┆ 8     │
            ╰───────┴───────╯
            <BLANKLINE>
            (Showing first 5 of 5 rows)
        """
        builder = self._builder.union(other._builder)
        return DataFrame(builder)

    @DataframePublicAPI
    def union_all(self, other: "DataFrame") -> "DataFrame":
        """Returns the union of two DataFrames, including duplicates.

        Args:
            other (DataFrame): The DataFrame to union with this one.

        Returns:
            DataFrame: A new DataFrame containing all rows from both DataFrames, including duplicates.

        Examples:
            >>> import daft
            >>> df1 = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
            >>> df2 = daft.from_pydict({"x": [3, 2, 1], "y": [6, 5, 4]})
            >>> df1.union_all(df2).sort("x").show()
            ╭───────┬───────╮
            │ x     ┆ y     │
            │ ---   ┆ ---   │
            │ Int64 ┆ Int64 │
            ╞═══════╪═══════╡
            │ 1     ┆ 4     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 1     ┆ 4     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 2     ┆ 5     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 2     ┆ 5     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 3     ┆ 6     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 3     ┆ 6     │
            ╰───────┴───────╯
            <BLANKLINE>
            (Showing first 6 of 6 rows)
        """
        builder = self._builder.union(other._builder, is_all=True)
        return DataFrame(builder)

    @DataframePublicAPI
    def union_by_name(self, other: "DataFrame") -> "DataFrame":
        """Returns the distinct union by name.

        Args:
            other (DataFrame): The DataFrame to union with this one, matching columns by name.

        Returns:
            DataFrame: A new DataFrame containing the distinct rows from both DataFrames, with columns matched by name.

        Examples:
            >>> import daft
            >>> df1 = daft.from_pydict({"x": [1, 2], "y": [4, 5], "w": [9, 10]})
            >>> df2 = daft.from_pydict({"y": [6, 7], "z": ["a", "b"]})
            >>> df1.union_by_name(df2).sort("y").show()
            ╭───────┬───────┬───────┬────────╮
            │ x     ┆ y     ┆ w     ┆ z      │
            │ ---   ┆ ---   ┆ ---   ┆ ---    │
            │ Int64 ┆ Int64 ┆ Int64 ┆ String │
            ╞═══════╪═══════╪═══════╪════════╡
            │ 1     ┆ 4     ┆ 9     ┆ None   │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ 2     ┆ 5     ┆ 10    ┆ None   │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ None  ┆ 6     ┆ None  ┆ a      │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ None  ┆ 7     ┆ None  ┆ b      │
            ╰───────┴───────┴───────┴────────╯
            <BLANKLINE>
            (Showing first 4 of 4 rows)
        """
        builder = self._builder.union(other._builder, is_all=False, is_by_name=True)
        return DataFrame(builder)

    @DataframePublicAPI
    def union_all_by_name(self, other: "DataFrame") -> "DataFrame":
        """Returns the union of two DataFrames, including duplicates, with columns matched by name.

        Args:
            other (DataFrame): The DataFrame to union with this one, matching columns by name.

        Returns:
            DataFrame: A new DataFrame containing all rows from both DataFrames, including duplicates, with columns matched by name.

        Examples:
            >>> import daft
            >>> df1 = daft.from_pydict({"x": [1, 2], "y": [4, 5], "w": [9, 10]})
            >>> df2 = daft.from_pydict({"y": [6, 6, 7, 7], "z": ["a", "a", "b", "b"]})
            >>> df1.union_all_by_name(df2).sort("y").show()
            ╭───────┬───────┬───────┬────────╮
            │ x     ┆ y     ┆ w     ┆ z      │
            │ ---   ┆ ---   ┆ ---   ┆ ---    │
            │ Int64 ┆ Int64 ┆ Int64 ┆ String │
            ╞═══════╪═══════╪═══════╪════════╡
            │ 1     ┆ 4     ┆ 9     ┆ None   │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ 2     ┆ 5     ┆ 10    ┆ None   │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ None  ┆ 6     ┆ None  ┆ a      │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ None  ┆ 6     ┆ None  ┆ a      │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ None  ┆ 7     ┆ None  ┆ b      │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ None  ┆ 7     ┆ None  ┆ b      │
            ╰───────┴───────┴───────┴────────╯
            <BLANKLINE>
            (Showing first 6 of 6 rows)
        """
        builder = self._builder.union(other._builder, is_all=True, is_by_name=True)
        return DataFrame(builder)

    @DataframePublicAPI
    def intersect(self, other: "DataFrame") -> "DataFrame":
        """Returns the intersection of two DataFrames.

        Args:
            other (DataFrame): DataFrame to intersect with

        Returns:
            DataFrame: DataFrame with the intersection of the two DataFrames

        Examples:
            >>> import daft
            >>> df1 = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
            >>> df2 = daft.from_pydict({"a": [1, 2, 3], "b": [4, 8, 6]})
            >>> df = df1.intersect(df2)
            >>> df = df.sort("a")
            >>> df.show()
            ╭───────┬───────╮
            │ a     ┆ b     │
            │ ---   ┆ ---   │
            │ Int64 ┆ Int64 │
            ╞═══════╪═══════╡
            │ 1     ┆ 4     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 3     ┆ 6     │
            ╰───────┴───────╯
            <BLANKLINE>
            (Showing first 2 of 2 rows)

        """
        builder = self._builder.intersect(other._builder)
        return DataFrame(builder)

    @DataframePublicAPI
    def intersect_all(self, other: "DataFrame") -> "DataFrame":
        """Returns the intersection of two DataFrames, including duplicates.

        Args:
            other (DataFrame): DataFrame to intersect with

        Returns:
            DataFrame: DataFrame with the intersection of the two DataFrames, including duplicates

        Examples:
            >>> import daft
            >>> df1 = daft.from_pydict({"a": [1, 2, 2], "b": [4, 6, 6]})
            >>> df2 = daft.from_pydict({"a": [1, 1, 2, 2], "b": [4, 4, 6, 6]})
            >>> df1.intersect_all(df2).sort("a").collect()
            ╭───────┬───────╮
            │ a     ┆ b     │
            │ ---   ┆ ---   │
            │ Int64 ┆ Int64 │
            ╞═══════╪═══════╡
            │ 1     ┆ 4     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 2     ┆ 6     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 2     ┆ 6     │
            ╰───────┴───────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        """
        builder = self._builder.intersect_all(other._builder)
        return DataFrame(builder)

    @DataframePublicAPI
    def except_distinct(self, other: "DataFrame") -> "DataFrame":
        """Returns the set difference of two DataFrames.

        Args:
            other (DataFrame): DataFrame to except with

        Returns:
            DataFrame: DataFrame with the set difference of the two DataFrames

        Examples:
            >>> import daft
            >>> df1 = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
            >>> df2 = daft.from_pydict({"a": [1, 2, 3], "b": [4, 8, 6]})
            >>> df1.except_distinct(df2).collect()
            ╭───────┬───────╮
            │ a     ┆ b     │
            │ ---   ┆ ---   │
            │ Int64 ┆ Int64 │
            ╞═══════╪═══════╡
            │ 2     ┆ 5     │
            ╰───────┴───────╯
            <BLANKLINE>
            (Showing first 1 of 1 rows)

        """
        builder = self._builder.except_distinct(other._builder)
        return DataFrame(builder)

    @DataframePublicAPI
    def except_all(self, other: "DataFrame") -> "DataFrame":
        """Returns the set difference of two DataFrames, considering duplicates.

        Args:
            other (DataFrame): DataFrame to except with

        Returns:
            DataFrame: DataFrame with the set difference of the two DataFrames, considering duplicates

        Examples:
            >>> import daft
            >>> df1 = daft.from_pydict({"a": [1, 1, 2, 2], "b": [4, 4, 6, 6]})
            >>> df2 = daft.from_pydict({"a": [1, 2, 2], "b": [4, 6, 6]})
            >>> df1.except_all(df2).collect()
            ╭───────┬───────╮
            │ a     ┆ b     │
            │ ---   ┆ ---   │
            │ Int64 ┆ Int64 │
            ╞═══════╪═══════╡
            │ 1     ┆ 4     │
            ╰───────┴───────╯
            <BLANKLINE>
            (Showing first 1 of 1 rows)

        """
        builder = self._builder.except_all(other._builder)
        return DataFrame(builder)

    def _materialize_results(self) -> None:
        """Materializes the results of for this DataFrame and hold a pointer to the results."""
        if self._result is None:
            self._result_cache = get_or_create_runner().run(self._builder)
            result = self._result
            assert result is not None
            result.wait()

    @DataframePublicAPI
    def collect(self, num_preview_rows: Optional[int] = 8) -> "DataFrame":
        """Executes the entire DataFrame and materializes the results.

        Args:
            num_preview_rows: Number of rows to preview. Defaults to 8.

        Returns:
            DataFrame: DataFrame with materialized results.

        Note:
            This call is **blocking** and will execute the DataFrame when called

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
            >>> df = df.collect()
            >>> df.show()
            ╭───────┬───────╮
            │ x     ┆ y     │
            │ ---   ┆ ---   │
            │ Int64 ┆ Int64 │
            ╞═══════╪═══════╡
            │ 1     ┆ 4     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 2     ┆ 5     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 3     ┆ 6     │
            ╰───────┴───────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)
        """
        self._materialize_results()
        assert self._result is not None
        dataframe_len = len(self._result)
        if num_preview_rows is not None:
            self._num_preview_rows = num_preview_rows
        else:
            self._num_preview_rows = dataframe_len
        return self

    def _construct_show_preview(self, n: int) -> Preview:
        """Helper for .show() which will construct the underlying Preview object."""
        preview_partition = self._preview.partition
        total_rows = self._preview.total_rows

        # Truncate n to the length of the DataFrame, if we have it.
        if total_rows is not None and n > total_rows:
            n = total_rows

        # Construct the PreviewPartition
        if preview_partition is None or len(preview_partition) < n:
            # Preview partition doesn't exist or doesn't contain enough rows, so we need to compute a
            # new one from scratch.
            builder = self._builder.limit(n, eager=True)

            # Iteratively retrieve partitions until enough data has been materialized
            tables = []
            seen = 0
            for table in get_or_create_runner().run_iter_tables(builder, results_buffer_size=1):
                tables.append(table)
                seen += len(table)
                if seen >= n:
                    break

            preview_partition = MicroPartition.concat_or_empty(tables, self.schema())
            if len(preview_partition) > n:
                preview_partition = preview_partition.slice(0, n)
            elif len(preview_partition) < n:
                # Iterator short-circuited before reaching n, so we know that we have the full DataFrame.
                total_rows = n = len(preview_partition)
            preview = Preview(
                partition=preview_partition,
                total_rows=total_rows,
            )
        elif len(preview_partition) > n:
            # Preview partition is cached but has more rows that we need, so use the appropriate slice.
            truncated_preview_partition = preview_partition.slice(0, n)
            preview = Preview(
                partition=truncated_preview_partition,
                total_rows=total_rows,
            )
        else:
            assert len(preview_partition) == n
            # Preview partition is cached and has exactly the number of rows that we need, so use it directly.
            preview = self._preview

        return preview

    @DataframePublicAPI
    def show(
        self,
        n: int = 8,
        format: Optional[PreviewFormat] = None,
        verbose: bool = False,
        max_width: int = 30,
        align: PreviewAlign = "left",
        columns: Optional[list[PreviewColumn]] = None,
    ) -> None:
        """Executes enough of the DataFrame in order to display the first ``n`` rows.

        If IPython is installed, this will use IPython's `display` utility to pretty-print in a
        notebook/REPL environment. Otherwise, this will fall back onto a naive Python `print`.

        If no format is given, then daft's truncating preview format is used.
            - The output is a 'fancy' table with rounded corners.
            - Headers contain the column's data type.
            - Columns are truncated to 30 characters.
            - The table's overall width is limited to 10 columns.

        Args:
            n: number of rows to show. Defaults to 8.
            format (PreviewFormat): the box-drawing format e.g. "fancy" or "markdown".
            verbose (bool): verbose will print header info
            max_width (int): global max column width
            align (PreviewAlign): global column align
            columns (list[PreviewColumn]): column overrides

        Note:
            This call is **blocking** and will execute the DataFrame when called

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6], "z": [7, 8, 9]})
            >>> df.show()  # doctest: +SKIP
            >>> df.show(format="markdown")  # doctest: +SKIP
            >>> df.show(max_width=50)  # doctest: +SKIP
            >>> df.show(align="left")  # doctest: +SKIP

        Tip: Usage
            - If columns are given, their length MUST match the schema.
            - If columns are given, their settings override any global settings.

        """
        schema = self.schema()
        preview = self._construct_show_preview(n)
        preview_formatter = PreviewFormatter(
            preview,
            schema,
            format,
            **{
                "verbose": verbose,
                "max_width": max_width,
                "align": align,
                "columns": columns,
            },
        )

        try:
            from IPython.display import HTML, display

            if in_notebook() and preview.partition is not None:
                try:
                    interactive_html = preview_formatter._generate_interactive_html()
                    display(HTML(interactive_html), clear=True)
                    return None
                except Exception:
                    pass

            display(preview_formatter, clear=True)
        except ImportError:
            print(preview_formatter)
        return None

    def __len__(self) -> int:
        """Returns the count of rows when dataframe is materialized.

        If dataframe is not materialized yet, raises a runtime error.

        Returns:
            int: count of rows.

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
            >>> df = df.collect()
            >>> len(df)
            3

        """
        if self._result is not None:
            return len(self._result)

        message = (
            "Cannot call len() on an unmaterialized dataframe:"
            " either materialize your dataframe with df.collect() first before calling len(),"
            " or use `df.count_rows()` instead which will calculate the total number of rows."
        )
        raise RuntimeError(message)

    def __contains__(self, col_name: str) -> bool:
        """Returns whether the column exists in the dataframe.

        Args:
            col_name (str): column name

        Returns:
            bool: whether the column exists in the dataframe.

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6], "z": [7, 8, 9]})
            >>> "x" in df
            True

        """
        return col_name in self.column_names

    @DataframePublicAPI
    def to_pandas(self, coerce_temporal_nanoseconds: bool = False) -> "pandas.DataFrame":
        """Converts the current DataFrame to a [pandas DataFrame](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html).

        If results have not computed yet, collect will be called.

        Args:
            coerce_temporal_nanoseconds (bool): Whether to coerce temporal columns to nanoseconds. Only applicable to pandas version >= 2.0 and pyarrow version >= 13.0.0. Defaults to False. See `pyarrow.Table.to_pandas <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table.to_pandas>`__ for more information.

        Returns:
            pandas.DataFrame: [pandas DataFrame](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html) converted from a Daft DataFrame

        Note:
            This call is **blocking** and will execute the DataFrame when called

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
            >>> pd_df = df.to_pandas()
            >>> print(pd_df)
               a  b
            0  1  4
            1  2  5
            2  3  6
        """
        self.collect()
        result = self._result
        assert result is not None

        pd_df = result.to_pandas(
            schema=self._builder.schema(),
            coerce_temporal_nanoseconds=coerce_temporal_nanoseconds,
        )
        return pd_df

    @DataframePublicAPI
    def to_arrow(self) -> "pyarrow.Table":
        """Converts the current DataFrame to a [pyarrow Table](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html).

        If results have not computed yet, collect will be called.

        Returns:
            pyarrow.Table: [pyarrow Table](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html) converted from a Daft DataFrame

        Note:
            This call is **blocking** and will execute the DataFrame when called

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
            >>> arrow_table = df.to_arrow()
            >>> print(arrow_table)
            pyarrow.Table
            a: int64
            b: int64
            ----
            a: [[1,2,3]]
            b: [[4,5,6]]

        Tip:
            See also [DataFrame.to_arrow_iter()][daft.DataFrame.to_arrow_iter] for
            a streaming iterator over the rows of the DataFrame as Arrow RecordBatches.
        """
        import pyarrow as pa

        arrow_rb_iter = self.to_arrow_iter(results_buffer_size=None)
        return pa.Table.from_batches(arrow_rb_iter, schema=self.schema().to_pyarrow_schema())

    @DataframePublicAPI
    def to_pydict(self) -> dict[str, list[Any]]:
        """Converts the current DataFrame to a python dictionary. The dictionary contains Python lists of Python objects for each column.

        If results have not computed yet, collect will be called.

        Returns:
            dict[str, list[Any]]: python dict converted from a Daft DataFrame

        Note:
            This call is **blocking** and will execute the DataFrame when called

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"a": [1, 2, 3, 4], "b": [2, 4, 3, 1]})
            >>> print(df.to_pydict())
            {'a': [1, 2, 3, 4], 'b': [2, 4, 3, 1]}

        Tip:
            See also [DataFrame.to_pylist()][daft.DataFrame.to_pylist] for
            a convenience method that converts the DataFrame to a list of Python dict objects.
        """
        self.collect()
        result = self._result
        assert result is not None
        return result.to_pydict(schema=self.schema())

    @DataframePublicAPI
    def to_pylist(self) -> list[Any]:
        """Converts the current Dataframe into a python list.

        Returns:
            List[dict[str, Any]]: List of python dict objects.

        Warning:
            This is a convenience method over [DataFrame.iter_rows()][daft.DataFrame.iter_rows]. Users should prefer using `.iter_rows()` directly instead for lower memory utilization if they are streaming rows out of a DataFrame and don't require full materialization of the Python list.

        Examples:
            >>> import daft
            >>> from daft import col
            >>> df = daft.from_pydict({"a": [1, 2, 3, 4], "b": [2, 4, 3, 1]})
            >>> print(df.to_pylist())
            [{'a': 1, 'b': 2}, {'a': 2, 'b': 4}, {'a': 3, 'b': 3}, {'a': 4, 'b': 1}]

        Tip: See also
            [df.iter_rows()][daft.DataFrame.iter_rows]: streaming iterator over individual rows in a DataFrame
        """
        return list(self.iter_rows())

    @DataframePublicAPI
    def to_torch_map_dataset(
        self,
        shard_strategy: Optional[Literal["file"]] = None,
        world_size: Optional[int] = None,
        rank: Optional[int] = None,
    ) -> "torch.utils.data.Dataset":
        """Convert the current DataFrame into a map-style [Torch Dataset](https://pytorch.org/docs/stable/data.html#map-style-datasets) for use with PyTorch.

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

        Examples:
            >>> import daft
            >>> import torch  # doctest: +SKIP
            >>> df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
            >>> torch_dataset = df.to_torch_map_dataset()  # doctest: +SKIP

        Tip:
            This method returns results locally.
            For distributed training, you may want to use [DataFrame.to_ray_dataset()][daft.DataFrame.to_ray_dataset].
        """
        from daft.dataframe.to_torch import DaftTorchDataset

        if shard_strategy is not None:
            if world_size is None or rank is None:
                raise ValueError("world_size and rank must be specified when using sharding")
            df = self._shard(shard_strategy, world_size, rank)
        else:
            df = self

        return DaftTorchDataset(df.to_pydict(), len(df))

    @DataframePublicAPI
    def to_torch_iter_dataset(
        self,
        shard_strategy: Optional[Literal["file"]] = None,
        world_size: Optional[int] = None,
        rank: Optional[int] = None,
    ) -> "torch.utils.data.IterableDataset":
        """Convert the current DataFrame into a `Torch IterableDataset <https://pytorch.org/docs/stable/data.html#torch.utils.data.IterableDataset>`__ for use with PyTorch.

        Begins execution of the DataFrame if it is not yet executed.

        Items will be returned in pydict format: a dict of `{"column name": value}` for each row in the data.

        Args:
            shard_strategy (Optional[Literal["file"]]): Strategy to use for sharding the dataset. Currently only "file" is supported.
            world_size (Optional[int]): Total number of workers for sharding. Required if shard_strategy is specified.
            rank (Optional[int]): Rank of current worker for sharding. Required if shard_strategy is specified.

        Returns:
            torch.utils.data.IterableDataset: A PyTorch IterableDataset containing the data from the DataFrame.

        Examples:
            >>> import daft
            >>> import torch  # doctest: +SKIP
            >>> df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
            >>> torch_iter_dataset = df.to_torch_iter_dataset()  # doctest: +SKIP
            >>> list(torch.utils.data.DataLoader(torch_iter_dataset))  # doctest: +SKIP
            [{'x': tensor([1]), 'y': tensor([4])}, {'x': tensor([2]), 'y': tensor([5])}, {'x': tensor([3]), 'y': tensor([6])}]

        Note:
            The produced dataset is meant to be used with the single-process DataLoader,
            and does not support data sharding hooks for multi-process data loading.

            Do keep in mind that Daft is already using multithreading or multiprocessing under the hood
            to compute the data stream that feeds this dataset.

        Tip:
            This method returns results locally.
            For distributed training, you may want to use [DataFrame.to_ray_dataset()][daft.DataFrame.to_ray_dataset].
        """
        from daft.dataframe.to_torch import DaftTorchIterableDataset

        # TODO(desmond): We need to take in the batch size and number of epochs. So that when we shard, we can ensure that each shard produces
        # the same number of batches without coordination.

        if shard_strategy is not None:
            if world_size is None or rank is None:
                raise ValueError("world_size and rank must be specified when using sharding")
            df = self._shard(shard_strategy, world_size, rank)
        else:
            df = self

        return DaftTorchIterableDataset(df)

    @DataframePublicAPI
    def to_ray_dataset(self) -> "ray.data.dataset.DataSet":
        """Converts the current DataFrame to a [Ray Dataset](https://docs.ray.io/en/latest/data/api/dataset.html#ray.data.Dataset) which is useful for running distributed ML model training in Ray.

        Returns:
            ray.data.dataset.DataSet: [Ray dataset](https://docs.ray.io/en/latest/data/api/dataset.html#ray.data.Dataset)

        Examples:
            >>> import daft
            >>> daft.set_runner_ray()  # doctest: +SKIP
            >>> df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
            >>> ray_dataset = df.to_ray_dataset()  # doctest: +SKIP

        Note:
            This function can only work if Daft is running using the RayRunner
        """
        from daft.runners.ray_runner import RayPartitionSet

        self.collect()
        partition_set = self._result
        assert partition_set is not None
        if not isinstance(partition_set, RayPartitionSet):
            raise ValueError("Cannot convert to Ray Dataset if not running on Ray backend")
        return partition_set.to_ray_dataset()

    @classmethod
    def _from_ray_dataset(cls, ds: "ray.data.dataset.DataSet") -> "DataFrame":
        """Creates a DataFrame from a [Ray Dataset](https://docs.ray.io/en/latest/data/api/dataset.html#ray.data.Dataset)."""
        from ray.exceptions import RayTaskError

        if get_or_create_runner().name != "ray":
            raise ValueError("Daft needs to be running on the Ray Runner for this operation")

        from daft.runners.ray_runner import RayRunnerIO

        ray_runner_io = get_or_create_runner().runner_io()
        assert isinstance(ray_runner_io, RayRunnerIO)

        partition_set, schema = ray_runner_io.partition_set_from_ray_dataset(ds)
        cache_entry = get_or_create_runner().put_partition_set_into_cache(partition_set)
        try:
            size_bytes = partition_set.size_bytes()
        except RayTaskError as e:
            import pyarrow as pa
            from packaging.version import parse

            if "extension<arrow.fixed_shape_tensor>" in str(e) and parse(pa.__version__) < parse("13.0.0"):
                raise ValueError(
                    f"Reading Ray Dataset tensors is only supported with PyArrow >= 13.0.0, found {pa.__version__}. See this issue for more information: https://github.com/apache/arrow/pull/35933"
                ) from e
            raise e

        num_rows = len(partition_set)
        assert size_bytes is not None, "In-memory data should always have non-None size in bytes"
        builder = LogicalPlanBuilder.from_in_memory_scan(
            cache_entry,
            schema=schema,
            num_partitions=partition_set.num_partitions(),
            size_bytes=size_bytes,
            num_rows=num_rows,
        )
        df = cls(builder)
        df._result_cache = cache_entry

        # build preview
        context = get_context()
        num_preview_rows = context.daft_execution_config.num_preview_rows
        dataframe_num_rows = len(df)
        if dataframe_num_rows > num_preview_rows:
            preview_results, _ = ray_runner_io.partition_set_from_ray_dataset(ds.limit(num_preview_rows))
        else:
            preview_results = partition_set

        # set preview
        preview_partition = preview_results._get_merged_micropartition(df.schema())
        df._preview = Preview(
            partition=preview_partition,
            total_rows=dataframe_num_rows,
        )
        return df

    @DataframePublicAPI
    def to_dask_dataframe(
        self,
        meta: Union[
            "pandas.DataFrame",
            "pandas.Series[Any]",
            dict[str, Any],
            Iterable[Any],
            tuple[Any],
            None,
        ] = None,
    ) -> "dask.DataFrame":
        """Converts the current Daft DataFrame to a Dask DataFrame.

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

        Examples:
            >>> import daft
            >>> daft.set_runner_ray()  # doctest: +SKIP
            >>> df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
            >>> dask_df = df.to_dask_dataframe()  # doctest: +SKIP

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
        if get_or_create_runner().name != "ray":
            raise ValueError("Daft needs to be running on the Ray Runner for this operation")

        from daft.runners.ray_runner import RayRunnerIO

        ray_runner_io = get_or_create_runner().runner_io()
        assert isinstance(ray_runner_io, RayRunnerIO)

        partition_set, schema = ray_runner_io.partition_set_from_dask_dataframe(ddf)
        cache_entry = get_or_create_runner().put_partition_set_into_cache(partition_set)
        size_bytes = partition_set.size_bytes()
        num_rows = len(partition_set)
        assert size_bytes is not None, "In-memory data should always have non-None size in bytes"
        builder = LogicalPlanBuilder.from_in_memory_scan(
            cache_entry,
            schema=schema,
            num_partitions=partition_set.num_partitions(),
            size_bytes=size_bytes,
            num_rows=num_rows,
        )

        df = cls(builder)
        df._result_cache = cache_entry

        # build preview
        context = get_context()
        num_preview_rows = context.daft_execution_config.num_preview_rows
        dataframe_num_rows = len(df)
        if dataframe_num_rows > num_preview_rows:
            preview_results, _ = ray_runner_io.partition_set_from_dask_dataframe(ddf.loc[: num_preview_rows - 1])
        else:
            preview_results = partition_set

        # set preview
        preview_partition = preview_results._get_merged_micropartition(df.schema())
        df._preview = Preview(
            partition=preview_partition,
            total_rows=dataframe_num_rows,
        )
        return df


@dataclass
class GroupedDataFrame:
    df: DataFrame
    group_by: ExpressionsProjection

    def __post_init__(self) -> None:
        resolved_groupby_schema = self.group_by.resolve_schema(self.df._builder.schema())
        for field, e in zip(resolved_groupby_schema, self.group_by):
            if field.dtype == DataType.null():
                raise ExpressionTypeError(f"Cannot groupby on null type expression: {e}")

    if TYPE_CHECKING:

        @overload
        def __getitem__(self, item: int) -> Expression: ...
        @overload
        def __getitem__(self, item: str) -> Expression: ...
        @overload
        def __getitem__(self, item: slice) -> DataFrame: ...
        @overload
        def __getitem__(self, item: Iterable) -> "DataFrame": ...  # type: ignore

    def __getitem__(self, item: Union[int, str, slice, Iterable[Union[str, int]]]) -> Union[Expression, DataFrame]:
        """Gets a column from the DataFrame as an Expression."""
        return self.df.__getitem__(item)

    def sum(self, *cols: ColumnInputType) -> DataFrame:
        """Perform grouped sum on this GroupedDataFrame.

        Args:
            *cols (Union[str, Expression]): columns to sum

        Returns:
            DataFrame: DataFrame with grouped sums.
        """
        return self.df._apply_agg_fn(Expression.sum, cols, self.group_by)

    def mean(self, *cols: ColumnInputType) -> DataFrame:
        """Performs grouped mean on this GroupedDataFrame.

        Args:
            *cols (Union[str, Expression]): columns to mean

        Returns:
            DataFrame: DataFrame with grouped mean.
        """
        return self.df._apply_agg_fn(Expression.mean, cols, self.group_by)

    def stddev(self, *cols: ColumnInputType) -> DataFrame:
        """Performs grouped standard deviation on this GroupedDataFrame.

        Args:
            *cols (Union[str, Expression]): columns to stddev

        Returns:
            DataFrame: DataFrame with grouped standard deviation.

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"keys": ["a", "a", "a", "b"], "col_a": [0, 1, 2, 100]})
            >>> df = df.groupby("keys").stddev()
            >>> df = df.sort("keys")
            >>> df.show()
            ╭────────┬───────────────────╮
            │ keys   ┆ col_a             │
            │ ---    ┆ ---               │
            │ String ┆ Float64           │
            ╞════════╪═══════════════════╡
            │ a      ┆ 0.816496580927726 │
            ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ b      ┆ 0                 │
            ╰────────┴───────────────────╯
            <BLANKLINE>
            (Showing first 2 of 2 rows)

        """
        return self.df._apply_agg_fn(Expression.stddev, cols, self.group_by)

    def min(self, *cols: ColumnInputType) -> DataFrame:
        """Perform grouped min on this GroupedDataFrame.

        Args:
            *cols (Union[str, Expression]): columns to min

        Returns:
            DataFrame: DataFrame with grouped min.
        """
        return self.df._apply_agg_fn(Expression.min, cols, self.group_by)

    def max(self, *cols: ColumnInputType) -> DataFrame:
        """Performs grouped max on this GroupedDataFrame.

        Args:
            *cols (Union[str, Expression]): columns to max

        Returns:
            DataFrame: DataFrame with grouped max.
        """
        return self.df._apply_agg_fn(Expression.max, cols, self.group_by)

    def any_value(self, *cols: ColumnInputType) -> DataFrame:
        """Returns an arbitrary value on this GroupedDataFrame.

        Values for each column are not guaranteed to be from the same row.

        Args:
            *cols (Union[str, Expression]): columns to get

        Returns:
            DataFrame: DataFrame with any values.
        """
        return self.df._apply_agg_fn(Expression.any_value, cols, self.group_by)

    def count(self, *cols: ColumnInputType) -> DataFrame:
        """Performs grouped count on this GroupedDataFrame.

        Returns:
            DataFrame: DataFrame with grouped count per column.
        """
        return self.df._apply_agg_fn(Expression.count, cols, self.group_by)

    def skew(self, *cols: ColumnInputType) -> DataFrame:
        """Performs grouped skew on this GroupedDataFrame.

        Returns:
            DataFrame: DataFrame with the grouped skew per column.
        """
        return self.df._apply_agg_fn(Expression.skew, cols, self.group_by)

    def list_agg(self, *cols: ColumnInputType) -> DataFrame:
        """Performs grouped list on this GroupedDataFrame.

        Returns:
            DataFrame: DataFrame with grouped list per column.
        """
        return self.df._apply_agg_fn(Expression.list_agg, cols, self.group_by)

    def list_agg_distinct(self, *cols: ColumnInputType) -> DataFrame:
        """Performs grouped list distinct on this GroupedDataFrame (ignoring nulls).

        Args:
            *cols (Union[str, Expression]): columns to form into a set

        Returns:
            DataFrame: DataFrame with grouped list distinct per column.
        """
        return self.df._apply_agg_fn(Expression.list_agg_distinct, cols, self.group_by)

    def string_agg(self, *cols: ColumnInputType) -> DataFrame:
        """Performs grouped string concat on this GroupedDataFrame.

        Returns:
            DataFrame: DataFrame with grouped string concatenated per column.
        """
        return self.df._apply_agg_fn(Expression.string_agg, cols, self.group_by)

    def agg(self, *to_agg: Union[Expression, Iterable[Expression]]) -> DataFrame:
        """Perform aggregations on this GroupedDataFrame. Allows for mixed aggregations.

        Args:
            *to_agg (Union[Expression, Iterable[Expression]]): aggregation expressions

        Returns:
            DataFrame: DataFrame with grouped aggregations

        Examples:
            >>> import daft
            >>> from daft import col
            >>> df = daft.from_pydict(
            ...     {
            ...         "pet": ["cat", "dog", "dog", "cat"],
            ...         "age": [1, 2, 3, 4],
            ...         "name": ["Alex", "Jordan", "Sam", "Riley"],
            ...     }
            ... )
            >>> grouped_df = df.groupby("pet").agg(
            ...     df["age"].min().alias("min_age"),
            ...     df["age"].max().alias("max_age"),
            ...     df["pet"].count().alias("count"),
            ...     df["name"].any_value(),
            ... )
            >>> grouped_df = grouped_df.sort("pet")
            >>> grouped_df.show()
            ╭────────┬─────────┬─────────┬────────┬────────╮
            │ pet    ┆ min_age ┆ max_age ┆ count  ┆ name   │
            │ ---    ┆ ---     ┆ ---     ┆ ---    ┆ ---    │
            │ String ┆ Int64   ┆ Int64   ┆ UInt64 ┆ String │
            ╞════════╪═════════╪═════════╪════════╪════════╡
            │ cat    ┆ 1       ┆ 4       ┆ 2      ┆ Alex   │
            ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ dog    ┆ 2       ┆ 3       ┆ 2      ┆ Jordan │
            ╰────────┴─────────┴─────────┴────────┴────────╯
            <BLANKLINE>
            (Showing first 2 of 2 rows)

        """
        to_agg_list = (
            list(to_agg[0])
            if (len(to_agg) == 1 and not isinstance(to_agg[0], Expression))
            else list(typing.cast("tuple[Expression]", to_agg))
        )

        for expr in to_agg_list:
            if not isinstance(expr, Expression):
                raise ValueError(f"GroupedDataFrame.agg() only accepts expression type, received: {type(expr)}")

        return self.df._agg(to_agg_list, group_by=self.group_by)

    def map_groups(self, udf: Expression) -> DataFrame:
        """Apply a user-defined function to each group. The name of the resultant column will default to the name of the first input column.

        Args:
            udf (Expression): User-defined function to apply to each group.

        Returns:
            DataFrame: DataFrame with grouped aggregations

        Examples:
            >>> import daft, statistics
            >>>
            >>> df = daft.from_pydict({"group": ["a", "a", "a", "b", "b", "b"], "data": [1, 20, 30, 4, 50, 600]})
            >>>
            >>> @daft.udf(return_dtype=daft.DataType.float64())
            ... def std_dev(data):
            ...     return [statistics.stdev(data)]
            >>>
            >>> df = df.groupby("group").map_groups(std_dev(df["data"]))
            >>> df = df.sort("group")
            >>> df.show()
            ╭────────┬────────────────────╮
            │ group  ┆ data               │
            │ ---    ┆ ---                │
            │ String ┆ Float64            │
            ╞════════╪════════════════════╡
            │ a      ┆ 14.730919862656235 │
            ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ b      ┆ 331.62026476076517 │
            ╰────────┴────────────────────╯
            <BLANKLINE>
            (Showing first 2 of 2 rows)

        """
        return self.df._map_groups(udf, group_by=self.group_by)
