# isort: dont-add-import: from __future__ import annotations
#
# This file uses strings for forward type annotations in public APIs,
# in order to support runtime typechecking across different Python versions.
# For technical details, see https://github.com/Eventual-Inc/Daft/pull/630

import io
import os
import pathlib
import warnings
from dataclasses import dataclass
from functools import partial, reduce
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Mapping,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)
from urllib.parse import urlparse

from daft.api_annotations import DataframePublicAPI
from daft.context import get_context
from daft.convert import InputListType
from daft.daft import FileFormat, IOConfig, JoinStrategy, JoinType, ResourceRequest, resolve_expr
from daft.dataframe.preview import DataFramePreview
from daft.datatype import DataType
from daft.errors import ExpressionTypeError
from daft.expressions import Expression, ExpressionsProjection, col, lit
from daft.logical.builder import LogicalPlanBuilder
from daft.runners.partitioning import PartitionCacheEntry, PartitionSet
from daft.runners.pyrunner import LocalPartitionSet
from daft.table import MicroPartition
from daft.viz import DataFrameDisplay

if TYPE_CHECKING:
    import dask
    import deltalake
    import pandas
    import pyarrow
    import pyiceberg
    import ray
    import torch

    from daft.io import DataCatalogTable

from daft.logical.schema import Schema

UDFReturnType = TypeVar("UDFReturnType", covariant=True)

ColumnInputType = Union[Expression, str]

ManyColumnsInputType = Union[ColumnInputType, Iterable[ColumnInputType]]


class DataFrame:
    """A Daft DataFrame is a table of data. It has columns, where each column has a type and the same
    number of items (rows) as all other columns.
    """

    def __init__(self, builder: LogicalPlanBuilder) -> None:
        """Constructs a DataFrame according to a given LogicalPlan. Users are expected instead to call
        the classmethods on DataFrame to create a DataFrame.

        Args:
            plan: LogicalPlan describing the steps required to arrive at this DataFrame
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
        self._preview = DataFramePreview(preview_partition=None, dataframe_num_rows=None)
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
    def _result(self) -> Optional[PartitionSet]:
        if self._result_cache is None:
            return None
        else:
            return self._result_cache.value

    @DataframePublicAPI
    def explain(self, show_all: bool = False, simple: bool = False, file: Optional[io.IOBase] = None) -> None:
        """Prints the (logical and physical) plans that will be executed to produce this DataFrame.
        Defaults to showing the unoptimized logical plan. Use ``show_all=True`` to show the unoptimized logical plan,
        the optimized logical plan, and the physical plan.

        Args:
            show_all (bool): Whether to show the optimized logical plan and the physical plan in addition to the
                unoptimized logical plan.
            simple (bool): Whether to only show the type of op for each node in the plan, rather than showing details
                of how each op is configured.
            file (Optional[io.IOBase]): Location to print the output to, or defaults to None which defaults to the default location for
                print (in Python, that should be sys.stdout)
        """
        print_to_file = partial(print, file=file)

        if self._result_cache is not None:
            print_to_file("Result is cached and will skip computation\n")
            print_to_file(self._builder.pretty_print(simple))

            print_to_file("However here is the logical plan used to produce this result:\n", file=file)

        builder = self.__builder
        print_to_file("== Unoptimized Logical Plan ==\n")
        print_to_file(builder.pretty_print(simple))
        if show_all:
            print_to_file("\n== Optimized Logical Plan ==\n")
            builder = builder.optimize()
            print_to_file(builder.pretty_print(simple))
            print_to_file("\n== Physical Plan ==\n")
            physical_plan_scheduler = builder.to_physical_plan_scheduler(get_context().daft_execution_config)
            print_to_file(physical_plan_scheduler.pretty_print(simple))
        else:
            print_to_file(
                "\n \nSet `show_all=True` to also see the Optimized and Physical plans. This will run the query optimizer.",
            )

    def num_partitions(self) -> int:
        daft_execution_config = get_context().daft_execution_config
        # We need to run the optimizer since that could change the number of partitions
        return self.__builder.optimize().to_physical_plan_scheduler(daft_execution_config).num_partitions()

    @DataframePublicAPI
    def schema(self) -> Schema:
        """Returns the Schema of the DataFrame, which provides information about each column

        Returns:
            Schema: schema of the DataFrame
        """
        return self.__builder.schema()

    @property
    def column_names(self) -> List[str]:
        """Returns column names of DataFrame as a list of strings.

        Returns:
            List[str]: Column names of this DataFrame.
        """
        return self.__builder.schema().column_names()

    @property
    def columns(self) -> List[Expression]:
        """Returns column of DataFrame as a list of Expressions.

        Returns:
            List[Expression]: Columns of this DataFrame.
        """
        return [col(field.name) for field in self.__builder.schema()]

    @DataframePublicAPI
    def __iter__(self) -> Iterator[Dict[str, Any]]:
        """Return an iterator of rows for this dataframe.

        Each row will be a pydict of the form { "key" : value }.
        """

        if self._result is not None:
            # If the dataframe has already finished executing,
            # use the precomputed results.
            pydict = self.to_pydict()
            for i in range(len(self)):
                row = {key: value[i] for (key, value) in pydict.items()}
                yield row

        else:
            # Execute the dataframe in a streaming fashion.
            context = get_context()
            partitions_iter = context.runner().run_iter_tables(self._builder, results_buffer_size=1)

            # Iterate through partitions.
            for partition in partitions_iter:
                pydict = partition.to_pydict()

                # Yield invidiual rows from the partition.
                for i in range(len(partition)):
                    row = {key: value[i] for (key, value) in pydict.items()}
                    yield row

    @DataframePublicAPI
    def iter_partitions(
        self, results_buffer_size: Optional[int] = 1
    ) -> Iterator[Union[MicroPartition, "ray.ObjectRef[MicroPartition]"]]:
        """Begin executing this dataframe and return an iterator over the partitions.

        Each partition will be returned as a daft.Table object (if using Python runner backend)
        or a ray ObjectRef (if using Ray runner backend).

        Args:
            results_buffer_size: how many partitions to allow in the results buffer (defaults to 1).
                Setting this value will buffer results up to the provided size and provide backpressure
                to dataframe execution based on the rate of consumption from the returned iterator. Setting this to
                `None` will result in a buffer of unbounded size, causing the dataframe to run asynchronously
                to completion.
        """
        if results_buffer_size is not None and not results_buffer_size > 0:
            raise ValueError(f"Provided `results_buffer_size` value must be > 0, received: {results_buffer_size}")

        if self._result is not None:
            # If the dataframe has already finished executing,
            # use the precomputed results.
            for mat_result in self._result.values():
                yield mat_result.partition()

        else:
            # Execute the dataframe in a streaming fashion.
            context = get_context()
            results_iter = context.runner().run_iter(self._builder, results_buffer_size=results_buffer_size)
            for result in results_iter:
                yield result.partition()

    def _populate_preview(self) -> None:
        """Populates the preview of the DataFrame, if it is not already populated."""
        if self._result is None:
            return

        preview_partition_invalid = (
            self._preview.preview_partition is None or len(self._preview.preview_partition) < self._num_preview_rows
        )
        if preview_partition_invalid:
            preview_parts = self._result._get_preview_vpartition(self._num_preview_rows)
            preview_results = LocalPartitionSet()
            for i, part in enumerate(preview_parts):
                preview_results.set_partition_from_table(i, part)
            preview_partition = preview_results._get_merged_vpartition()
            self._preview = DataFramePreview(
                preview_partition=preview_partition,
                dataframe_num_rows=len(self),
            )

    @DataframePublicAPI
    def __repr__(self) -> str:
        self._populate_preview()
        display = DataFrameDisplay(self._preview, self.schema())
        return display.__repr__()

    @DataframePublicAPI
    def _repr_html_(self) -> str:
        self._populate_preview()
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

        data_vpartition = MicroPartition.from_pydict(data)
        return cls._from_tables(data_vpartition)

    @classmethod
    def _from_arrow(cls, data: Union["pyarrow.Table", List["pyarrow.Table"]]) -> "DataFrame":
        """Creates a DataFrame from a `pyarrow Table <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html>`__."""
        if not isinstance(data, list):
            data = [data]
        data_vpartitions = [MicroPartition.from_arrow(table) for table in data]
        return cls._from_tables(*data_vpartitions)

    @classmethod
    def _from_pandas(cls, data: Union["pandas.DataFrame", List["pandas.DataFrame"]]) -> "DataFrame":
        """Creates a Daft DataFrame from a `pandas DataFrame <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`__."""
        if not isinstance(data, list):
            data = [data]
        data_vpartitions = [MicroPartition.from_pandas(df) for df in data]
        return cls._from_tables(*data_vpartitions)

    @classmethod
    def _from_tables(cls, *parts: MicroPartition) -> "DataFrame":
        """Creates a Daft DataFrame from a single Table.

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

        context = get_context()
        cache_entry = context.runner().put_partition_set_into_cache(result_pset)
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

    ###
    # Write methods
    ###

    @DataframePublicAPI
    def write_parquet(
        self,
        root_dir: Union[str, pathlib.Path],
        compression: str = "snappy",
        partition_cols: Optional[List[ColumnInputType]] = None,
        io_config: Optional[IOConfig] = None,
    ) -> "DataFrame":
        """Writes the DataFrame as parquet files, returning a new DataFrame with paths to the files that were written

        Files will be written to ``<root_dir>/*`` with randomly generated UUIDs as the file names.

        .. NOTE::
            This call is **blocking** and will execute the DataFrame when called

        Args:
            root_dir (str): root file path to write parquet files to.
            compression (str, optional): compression algorithm. Defaults to "snappy".
            partition_cols (Optional[List[ColumnInputType]], optional): How to subpartition each partition further. Defaults to None.
            io_config (Optional[IOConfig], optional): configurations to use when interacting with remote storage.

        Returns:
            DataFrame: The filenames that were written out as strings.

            .. NOTE::
                This call is **blocking** and will execute the DataFrame when called
        """
        io_config = get_context().daft_planning_config.default_io_config if io_config is None else io_config

        cols: Optional[List[Expression]] = None
        if partition_cols is not None:
            cols = self.__column_input_to_expression(tuple(partition_cols))

        builder = self._builder.write_tabular(
            root_dir=root_dir,
            partition_cols=cols,
            file_format=FileFormat.Parquet,
            compression=compression,
            io_config=io_config,
        )
        # Block and write, then retrieve data
        write_df = DataFrame(builder)
        write_df.collect()
        assert write_df._result is not None

        if len(write_df) > 0:
            # Populate and return a new disconnected DataFrame
            result_df = DataFrame(write_df._builder)
            result_df._result_cache = write_df._result_cache
            result_df._preview = write_df._preview
            return result_df
        else:
            from daft import from_pydict
            from daft.table.table_io import write_empty_tabular

            file_path = write_empty_tabular(
                root_dir, FileFormat.Parquet, self.schema(), compression=compression, io_config=io_config
            )

            return from_pydict(
                {
                    "path": [file_path],
                }
            )

    @DataframePublicAPI
    def write_csv(
        self,
        root_dir: Union[str, pathlib.Path],
        partition_cols: Optional[List[ColumnInputType]] = None,
        io_config: Optional[IOConfig] = None,
    ) -> "DataFrame":
        """Writes the DataFrame as CSV files, returning a new DataFrame with paths to the files that were written

        Files will be written to ``<root_dir>/*`` with randomly generated UUIDs as the file names.

        .. NOTE::
            This call is **blocking** and will execute the DataFrame when called

        Args:
            root_dir (str): root file path to write parquet files to.
            partition_cols (Optional[List[ColumnInputType]], optional): How to subpartition each partition further. Defaults to None.
            io_config (Optional[IOConfig], optional): configurations to use when interacting with remote storage.

        Returns:
            DataFrame: The filenames that were written out as strings.
        """
        io_config = get_context().daft_planning_config.default_io_config if io_config is None else io_config

        cols: Optional[List[Expression]] = None
        if partition_cols is not None:
            cols = self.__column_input_to_expression(tuple(partition_cols))
        builder = self._builder.write_tabular(
            root_dir=root_dir,
            partition_cols=cols,
            file_format=FileFormat.Csv,
            io_config=io_config,
        )

        # Block and write, then retrieve data
        write_df = DataFrame(builder)
        write_df.collect()
        assert write_df._result is not None

        if len(write_df) > 0:
            # Populate and return a new disconnected DataFrame
            result_df = DataFrame(write_df._builder)
            result_df._result_cache = write_df._result_cache
            result_df._preview = write_df._preview
            return result_df
        else:
            from daft import from_pydict
            from daft.table.table_io import write_empty_tabular

            file_path = write_empty_tabular(root_dir, FileFormat.Csv, self.schema(), io_config=io_config)

            return from_pydict(
                {
                    "path": [file_path],
                }
            )

    @DataframePublicAPI
    def write_iceberg(self, table: "pyiceberg.table.Table", mode: str = "append") -> "DataFrame":
        """Writes the DataFrame to an `Iceberg <https://iceberg.apache.org/docs/nightly/>`__ table, returning a new DataFrame with the operations that occurred.
        Can be run in either `append` or `overwrite` mode which will either appends the rows in the DataFrame or will delete the existing rows and then append the DataFrame rows respectively.

        .. NOTE::
            This call is **blocking** and will execute the DataFrame when called

        Args:
            table (pyiceberg.table.Table): Destination `PyIceberg Table <https://py.iceberg.apache.org/reference/pyiceberg/table/#pyiceberg.table.Table>`__ to write dataframe to.
            mode (str, optional): Operation mode of the write. `append` or `overwrite` Iceberg Table. Defaults to "append".

        Returns:
            DataFrame: The operations that occurred with this write.
        """

        if len(table.spec().fields) > 0:
            raise ValueError("Cannot write to partitioned Iceberg tables")

        import pyarrow as pa
        import pyiceberg
        from packaging.version import parse

        if parse(pyiceberg.__version__) < parse("0.6.0"):
            raise ValueError(f"Write Iceberg is only supported on pyiceberg>=0.6.0, found {pyiceberg.__version__}")

        if parse(pa.__version__) < parse("12.0.1"):
            raise ValueError(
                f"Write Iceberg is only supported on pyarrow>=12.0.1, found {pa.__version__}. See this issue for more information: https://github.com/apache/arrow/issues/37054#issuecomment-1668644887"
            )

        from pyiceberg.table import _MergingSnapshotProducer
        from pyiceberg.table.snapshots import Operation

        operations = []
        path = []
        rows = []
        size = []

        if mode == "append":
            operation = Operation.APPEND
        elif mode == "overwrite":
            operation = Operation.OVERWRITE
        else:
            raise ValueError(f"Only support `append` or `overwrite` mode. {mode} is unsupported")

        # We perform the merge here since table is not pickle-able
        # We should be able to move to a transaction API for iceberg 0.7.0
        merge = _MergingSnapshotProducer(operation=operation, table=table)

        builder = self._builder.write_iceberg(table)
        write_df = DataFrame(builder)
        write_df.collect()

        write_result = write_df.to_pydict()
        assert "data_file" in write_result
        data_files = write_result["data_file"]

        if operation == Operation.OVERWRITE:
            deleted_files = table.scan().plan_files()
        else:
            deleted_files = []

        for data_file in data_files:
            merge.append_data_file(data_file)
            operations.append("ADD")
            path.append(data_file.file_path)
            rows.append(data_file.record_count)
            size.append(data_file.file_size_in_bytes)

        for pf in deleted_files:
            data_file = pf.file
            operations.append("DELETE")
            path.append(data_file.file_path)
            rows.append(data_file.record_count)
            size.append(data_file.file_size_in_bytes)

        merge.commit()

        from daft import from_pydict

        with_operations = from_pydict(
            {
                "operation": pa.array(operations, type=pa.string()),
                "rows": pa.array(rows, type=pa.int64()),
                "file_size": pa.array(size, type=pa.int64()),
                "file_name": pa.array([os.path.basename(fp) for fp in path], type=pa.string()),
            }
        )
        # NOTE: We are losing the history of the plan here.
        # This is due to the fact that the logical plan of the write_iceberg returns datafiles but we want to return the above data
        return with_operations

    @DataframePublicAPI
    def write_deltalake(
        self,
        table: Union[str, pathlib.Path, "DataCatalogTable", "deltalake.DeltaTable"],
        mode: Literal["append", "overwrite", "error", "ignore"] = "append",
        schema_mode: Optional[Literal["merge", "overwrite"]] = None,
        name: Optional[str] = None,
        description: Optional[str] = None,
        configuration: Optional[Mapping[str, Optional[str]]] = None,
        custom_metadata: Optional[Dict[str, str]] = None,
        dynamo_table_name: Optional[str] = None,
        io_config: Optional[IOConfig] = None,
    ) -> "DataFrame":
        """Writes the DataFrame to a `Delta Lake <https://docs.delta.io/latest/index.html>`__ table, returning a new DataFrame with the operations that occurred.

        .. NOTE::
            This call is **blocking** and will execute the DataFrame when called

        Args:
            table (Union[str, pathlib.Path, DataCatalogTable, deltalake.DeltaTable]): Destination `Delta Lake Table <https://delta-io.github.io/delta-rs/api/delta_table/>`__ or table URI to write dataframe to.
            mode (str, optional): Operation mode of the write. `append` will add new data, `overwrite` will replace table with new data, `error` will raise an error if table already exists, and `ignore` will not write anything if table already exists. Defaults to "append".
            schema_mode (str, optional): Schema mode of the write. If set to `overwrite`, allows replacing the schema of the table when doing `mode=overwrite`. Schema mode `merge` is currently not supported.
            name (str, optional): User-provided identifier for this table.
            description (str, optional): User-provided description for this table.
            configuration (Mapping[str, Optional[str]], optional): A map containing configuration options for the metadata action.
            custom_metadata (Dict[str, str], optional): Custom metadata to add to the commit info.
            dynamo_table_name (str, optional): Name of the DynamoDB table to be used as the locking provider if writing to S3.
            io_config (IOConfig, optional): configurations to use when interacting with remote storage.

        Returns:
            DataFrame: The operations that occurred with this write.
        """

        import json

        import deltalake
        import pyarrow as pa
        from deltalake.schema import _convert_pa_schema_to_delta
        from deltalake.writer import (
            try_get_deltatable,
            write_deltalake_pyarrow,
        )
        from packaging.version import parse

        from daft import from_pydict
        from daft.io import DataCatalogTable
        from daft.io.object_store_options import io_config_to_storage_options

        if schema_mode == "merge":
            raise ValueError("Schema mode' merge' is not currently supported for write_deltalake.")

        if parse(deltalake.__version__) < parse("0.14.0"):
            raise ValueError(f"Write delta lake is only supported on deltalake>=0.14.0, found {deltalake.__version__}")

        io_config = get_context().daft_planning_config.default_io_config if io_config is None else io_config

        if isinstance(table, (str, pathlib.Path, DataCatalogTable)):
            if isinstance(table, str):
                table_uri = table
            elif isinstance(table, pathlib.Path):
                table_uri = str(table)
            else:
                table_uri = table.table_uri(io_config)

            storage_options = io_config_to_storage_options(io_config, table_uri) or {}
            table = try_get_deltatable(table_uri, storage_options=storage_options)
        elif isinstance(table, deltalake.DeltaTable):
            table_uri = table.table_uri
            storage_options = table._storage_options or {}
            new_storage_options = io_config_to_storage_options(io_config, table_uri)
            storage_options.update(new_storage_options or {})
        else:
            raise ValueError(f"Expected table to be a path or a DeltaTable, received: {type(table)}")

        # see: https://delta-io.github.io/delta-rs/usage/writing/writing-to-s3-with-locking-provider/
        scheme = urlparse(table_uri).scheme
        if scheme == "s3" or scheme == "s3a":
            if dynamo_table_name is not None:
                storage_options["AWS_S3_LOCKING_PROVIDER"] = "dynamodb"
                storage_options["DELTA_DYNAMO_TABLE_NAME"] = dynamo_table_name
            else:
                storage_options["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"
                warnings.warn("No DynamoDB table specified for Delta Lake locking. Defaulting to unsafe writes.")

        pyarrow_schema = pa.schema((f.name, f.dtype.to_arrow_dtype()) for f in self.schema())
        delta_schema = _convert_pa_schema_to_delta(pyarrow_schema, large_dtypes=True)

        if table:
            table.update_incremental()

            table_schema = table.schema().to_pyarrow(as_large_types=True)
            if delta_schema != table_schema and not (mode == "overwrite" and schema_mode == "overwrite"):
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

        builder = self._builder.write_deltalake(
            table_uri,
            mode,
            version,
            large_dtypes=True,
            io_config=io_config,
        )
        write_df = DataFrame(builder)
        write_df.collect()

        write_result = write_df.to_pydict()
        assert "data_file" in write_result
        data_files = write_result["data_file"]
        add_action = []

        operations = []
        paths = []
        rows = []
        sizes = []

        for data_file in data_files:
            stats = json.loads(data_file.stats)
            operations.append("ADD")
            paths.append(data_file.path)
            rows.append(stats["numRecords"])
            sizes.append(data_file.size)

            add_action.append(data_file)

        if table is None:
            write_deltalake_pyarrow(
                table_uri,
                delta_schema,
                add_action,
                mode,
                [],
                name,
                description,
                configuration,
                storage_options,
                custom_metadata,
            )
        else:
            if mode == "overwrite":
                old_actions = table.get_add_actions()
                old_actions_dict = old_actions.to_pydict()
                for i in range(old_actions.num_rows):
                    operations.append("DELETE")
                    paths.append(old_actions_dict["path"][i])
                    rows.append(old_actions_dict["num_records"][i])
                    sizes.append(old_actions_dict["size_bytes"][i])

            table._table.create_write_transaction(add_action, mode, [], delta_schema, None, custom_metadata)
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
    def write_lance(
        self,
        uri: Union[str, pathlib.Path],
        mode: Literal["create", "append", "overwrite"] = "create",
        io_config: Optional[IOConfig] = None,
        **kwargs,
    ) -> "DataFrame":
        """
        Writes the DataFrame to a Lance table
        Note:
            `write_lance` requires python 3.9 or higher
        Args:
          uri: The URI of the Lance table to write to
          mode: The write mode. One of "create", "append", or "overwrite"
          io_config (IOConfig, optional): configurations to use when interacting with remote storage.
          **kwargs: Additional keyword arguments to pass to the Lance writer
        Example:
        --------


        >>> import daft
        >>> df = daft.from_pydict({"a": [1, 2, 3, 4]})
        >>> df.write_lance("/tmp/lance/my_table.lance")
        ╭───────────────┬──────────────────┬─────────────────┬─────────╮
        │ num_fragments ┆ num_deleted_rows ┆ num_small_files ┆ version │
        │ ---           ┆ ---              ┆ ---             ┆ ---     │
        │ Int64         ┆ Int64            ┆ Int64           ┆ Int64   │
        ╞═══════════════╪══════════════════╪═════════════════╪═════════╡
        │ 1             ┆ 0                ┆ 1               ┆ 1       │
        ╰───────────────┴──────────────────┴─────────────────┴─────────╯
        <BLANKLINE>
        (Showing first 1 of 1 rows)

        >>> daft.read_lance("/tmp/lance/my_table.lance").collect()
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


        # Pass additional keyword arguments to the Lance writer
        # All additional keyword arguments are passed to `lance.write_fragments`
        >>> df.write_lance("/tmp/lance/my_table.lance", mode="overwrite", max_bytes_per_file=1024)
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
        import sys

        from daft import from_pydict

        if sys.version_info < (3, 9):
            raise ValueError("'write_lance' requires python 3.9 or higher")
        try:
            import lance
            import pyarrow as pa

        except ImportError:
            raise ImportError("lance is not installed. Please install lance using `pip install getdaft[lance]`")

        io_config = get_context().daft_planning_config.default_io_config if io_config is None else io_config
        if isinstance(uri, (str, pathlib.Path)):
            if isinstance(uri, str):
                table_uri = uri
            elif isinstance(uri, pathlib.Path):
                table_uri = str(uri)
            else:
                table_uri = uri
        pyarrow_schema = pa.schema((f.name, f.dtype.to_arrow_dtype()) for f in self.schema())

        try:
            table = lance.dataset(table_uri)

        except ValueError:
            table = None

        version = 0
        if table:
            table_schema = table.schema
            version = table.latest_version
            if pyarrow_schema != table_schema and not (mode == "overwrite"):
                raise ValueError(
                    "Schema of data does not match table schema\n"
                    f"Data schema:\n{pyarrow_schema}\nTable Schema:\n{table_schema}"
                )

        builder = self._builder.write_lance(
            table_uri,
            mode,
            io_config=io_config,
            kwargs=kwargs,
        )
        write_df = DataFrame(builder)
        write_df.collect()

        write_result = write_df.to_pydict()
        assert "fragments" in write_result
        fragments = write_result["fragments"]

        if mode == "create" or mode == "overwrite":
            operation = lance.LanceOperation.Overwrite(pyarrow_schema, fragments)
        elif mode == "append":
            operation = lance.LanceOperation.Append(fragments)

        dataset = lance.LanceDataset.commit(table_uri, operation, read_version=version)
        stats = dataset.stats.dataset_stats()

        tbl = from_pydict(
            {
                "num_fragments": pa.array([stats["num_fragments"]], type=pa.int64()),
                "num_deleted_rows": pa.array([stats["num_deleted_rows"]], type=pa.int64()),
                "num_small_files": pa.array([stats["num_small_files"]], type=pa.int64()),
                "version": pa.array([dataset.version], type=pa.int64()),
            }
        )
        return tbl

    ###
    # DataFrame operations
    ###

    def __column_input_to_expression(self, columns: Iterable[ColumnInputType]) -> List[Expression]:
        # TODO(Kevin): remove this method and use _column_inputs_to_expressions
        return [col(c) if isinstance(c, str) else c for c in columns]

    def _is_column_input(self, x: Any) -> bool:
        return isinstance(x, str) or isinstance(x, Expression)

    def _column_inputs_to_expressions(self, columns: ManyColumnsInputType) -> List[Expression]:
        """
        Inputs to dataframe operations can be passed in as individual arguments or an iterable.
        In addition, they may be strings or Expressions.
        This method normalizes the inputs to a list of Expressions.
        """

        column_iter: Iterable[ColumnInputType] = [columns] if self._is_column_input(columns) else columns  # type: ignore
        return [col(c) if isinstance(c, str) else c for c in column_iter]

    def _wildcard_inputs_to_expressions(self, columns: Tuple[ManyColumnsInputType, ...]) -> List[Expression]:
        """Handles wildcard argument column inputs"""

        column_input: Iterable[ColumnInputType] = columns[0] if len(columns) == 1 else columns  # type: ignore
        return self._column_inputs_to_expressions(column_input)

    def __getitem__(self, item: Union[slice, int, str, Iterable[Union[str, int]]]) -> Union[Expression, "DataFrame"]:
        """Gets a column from the DataFrame as an Expression (``df["mycol"]``)"""
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
            expr, _ = resolve_expr(col(item)._expr, schema._schema)
            return Expression._from_pyexpr(expr)
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
        in the lower 36 bits. This allows for 2^28 ≈ 268 million partitions and 2^40 ≈ 68 billion rows per partition.

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"a": [1, 2, 3, 4]}).into_partitions(2)
            >>> df = df._add_monotonically_increasing_id()
            >>> df.show()
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

        Args:
            column_name (Optional[str], optional): name of the new column. Defaults to "id".

        Returns:
            DataFrame: DataFrame with a new column of monotonically increasing ids.
        """

        builder = self._builder.add_monotonically_increasing_id(column_name)
        return DataFrame(builder)

    @DataframePublicAPI
    def select(self, *columns: ColumnInputType) -> "DataFrame":
        """Creates a new DataFrame from the provided expressions, similar to a SQL ``SELECT``

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6], "z": [7, 8, 9]})
            >>> df = df.select('x', daft.col('y'), daft.col('z') + 1)
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

        Args:
            *columns (Union[str, Expression]): columns to select from the current DataFrame

        Returns:
            DataFrame: new DataFrame that will select the passed in columns
        """
        assert len(columns) > 0
        builder = self._builder.select(self.__column_input_to_expression(columns))
        return DataFrame(builder)

    @DataframePublicAPI
    def distinct(self) -> "DataFrame":
        """Computes unique rows, dropping duplicates

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1, 2, 2], "y": [4, 5, 5], "z": [7, 8, 8]})
            >>> unique_df = df.distinct()
            >>> unique_df.show()
            ╭───────┬───────┬───────╮
            │ x     ┆ y     ┆ z     │
            │ ---   ┆ ---   ┆ ---   │
            │ Int64 ┆ Int64 ┆ Int64 │
            ╞═══════╪═══════╪═══════╡
            │ 2     ┆ 5     ┆ 8     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 1     ┆ 4     ┆ 7     │
            ╰───────┴───────┴───────╯
            <BLANKLINE>
            (Showing first 2 of 2 rows)

        Returns:
            DataFrame: DataFrame that has only  unique rows.
        """
        ExpressionsProjection.from_schema(self._builder.schema())
        builder = self._builder.distinct()
        return DataFrame(builder)

    @DataframePublicAPI
    def sample(
        self,
        fraction: float,
        with_replacement: bool = False,
        seed: Optional[int] = None,
    ) -> "DataFrame":
        """Samples a fraction of rows from the DataFrame

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6], "z": [7, 8, 9]})
            >>> sampled_df = df.sample(0.5)
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

        Args:
            fraction (float): fraction of rows to sample.
            with_replacement (bool, optional): whether to sample with replacement. Defaults to False.
            seed (Optional[int], optional): random seed. Defaults to None.

        Returns:
            DataFrame: DataFrame with a fraction of rows.
        """
        if fraction < 0.0 or fraction > 1.0:
            raise ValueError(f"fraction should be between 0.0 and 1.0, but got {fraction}")

        builder = self._builder.sample(fraction, with_replacement, seed)
        return DataFrame(builder)

    @DataframePublicAPI
    def exclude(self, *names: str) -> "DataFrame":
        """Drops columns from the current DataFrame by name

        This is equivalent of performing a select with all the columns but the ones excluded.

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6], "z": [7, 8, 9]})
            >>> df_without_x = df.exclude('x')
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

        Args:
            *names (str): names to exclude

        Returns:
            DataFrame: DataFrame with some columns excluded.
        """
        builder = self._builder.exclude(list(names))
        return DataFrame(builder)

    @DataframePublicAPI
    def where(self, predicate: Expression) -> "DataFrame":
        """Filters rows via a predicate expression, similar to SQL ``WHERE``.

        Example:

            >>> import daft
            >>> df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6], "z": [7, 8, 9]})
            >>> df.where((col('x') > 1) & (col('y') > 1)).collect()
            ╭───────┬───────┬───────╮
            │ x     ┆ y     ┆ z     │
            │ ---   ┆ ---   ┆ ---   │
            │ Int64 ┆ Int64 ┆ Int64 │
            ╞═══════╪═══════╪═══════╡
            │ 2     ┆ 5     ┆ 8     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 3     ┆ 6     ┆ 9     │
            ╰───────┴───────┴───────╯
            <BLANKLINE>
            (Showing first 2 of 2 rows)

        Args:
            predicate (Expression): expression that keeps row if evaluates to True.

        Returns:
            DataFrame: Filtered DataFrame.
        """
        builder = self._builder.filter(predicate)
        return DataFrame(builder)

    @DataframePublicAPI
    def with_column(
        self,
        column_name: str,
        expr: Expression,
        resource_request: ResourceRequest = ResourceRequest(),
    ) -> "DataFrame":
        """Adds a column to the current DataFrame with an Expression, equivalent to a ``select``
        with all current columns and the new one

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1, 2, 3]})
            >>> new_df = df.with_column('x+1', col('x') + 1)
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

        Args:
            column_name (str): name of new column
            expr (Expression): expression of the new column.
            resource_request (ResourceRequest): a custom resource request for the execution of this operation

        Returns:
            DataFrame: DataFrame with new column.
        """
        return self.with_columns({column_name: expr}, resource_request)

    @DataframePublicAPI
    def with_columns(
        self,
        columns: Dict[str, Expression],
        resource_request: ResourceRequest = ResourceRequest(),
    ) -> "DataFrame":
        """Adds columns to the current DataFrame with Expressions, equivalent to a ``select``
        with all current columns and the new ones

        Example:
            >>> import daft
            >>> df = daft.from_pydict({'x': [1, 2, 3], 'y': [4, 5, 6]})
            >>> new_df = df.with_columns({'foo': df['x'] + 1,'bar': df['y'] - df['x']})
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

        Args:
            columns (Dict[str, Expression]): Dictionary of new columns in the format { name: expression }
            resource_request (ResourceRequest): a custom resource request for the execution of this operation

        Returns:
            DataFrame: DataFrame with new columns.
        """
        if not isinstance(resource_request, ResourceRequest):
            raise TypeError(f"resource_request should be a ResourceRequest, but got {type(resource_request)}")

        new_columns = [col.alias(name) for name, col in columns.items()]

        builder = self._builder.with_columns(new_columns, resource_request)
        return DataFrame(builder)

    @DataframePublicAPI
    def sort(
        self,
        by: Union[ColumnInputType, List[ColumnInputType]],
        desc: Union[bool, List[bool]] = False,
    ) -> "DataFrame":
        """Sorts DataFrame globally

        Note:
            * Since this a global sort, this requires an expensive repartition which can be quite slow.
            * Supports multicolumn sorts and can have unique `descending` flag per column.

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"x": [3, 2, 1], "y": [6, 4, 5]})
            >>> sorted_df = df.sort(col('x') + col('y'))
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
        sort_by = self.__column_input_to_expression(by)
        builder = self._builder.sort(sort_by=sort_by, descending=desc)
        return DataFrame(builder)

    @DataframePublicAPI
    def limit(self, num: int) -> "DataFrame":
        """Limits the rows in the DataFrame to the first ``N`` rows, similar to a SQL ``LIMIT``

        Example:
            >>> import daft
            >>> df = df = daft.from_pydict({"x": [1, 2, 3, 4, 5, 6, 7]})
            >>> df_limited = df.limit(5) # returns 5 rows
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

        Args:
            num (int): maximum rows to allow.
            eager (bool): whether to maximize for latency (time to first result) by eagerly executing
                only one partition at a time, or throughput by executing multiple limits at a time

        Returns:
            DataFrame: Limited DataFrame
        """
        builder = self._builder.limit(num, eager=False)
        return DataFrame(builder)

    @DataframePublicAPI
    def count_rows(self) -> int:
        """Executes the Dataframe to count the number of rows.

        Returns:
            int: count of the number of rows in this DataFrame.
        """
        builder = self._builder.count()
        count_df = DataFrame(builder)
        # Expects builder to produce a single-partition, single-row DataFrame containing
        # a "count" column, where the lone value represents the row count for the DataFrame.
        return count_df.to_pydict()["count"][0]

    @DataframePublicAPI
    def repartition(self, num: Optional[int], *partition_by: ColumnInputType) -> "DataFrame":
        """Repartitions DataFrame to ``num`` partitions

        If columns are passed in, then DataFrame will be repartitioned by those, otherwise
        random repartitioning will occur.

        .. NOTE::

            This function will globally shuffle your data, which is potentially a very expensive operation.

            If instead you merely wish to "split" or "coalesce" partitions to obtain a target number of partitions,
            you mean instead wish to consider using :meth:`DataFrame.into_partitions <daft.DataFrame.into_partitions>`
            which avoids shuffling of data in favor of splitting/coalescing adjacent partitions where appropriate.

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6], "z": [7, 8, 9]})
            >>> repartitioned_df = df.repartition(3)
            >>> repartitioned_df.num_partitions()
            3

        Args:
            num (Optional[int]): Number of target partitions; if None, the number of partitions will not be changed.
            *partition_by (Union[str, Expression]): Optional columns to partition by.

        Returns:
            DataFrame: Repartitioned DataFrame.
        """
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

        No rebalancing is done; the minimum number of splits or merges are applied.
        (i.e. if there are 2 partitions, and change it into 3, this function will just split the bigger one)

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6], "z": [7, 8, 9]})
            >>> df_with_5_partitions = df.into_partitions(5)
            >>> df_with_5_partitions.num_partitions()
            5

        Args:
            num (int): number of target partitions.

        Returns:
            DataFrame: Dataframe with ``num`` partitions.
        """
        builder = self._builder.into_partitions(num)
        return DataFrame(builder)

    @DataframePublicAPI
    def join(
        self,
        other: "DataFrame",
        on: Optional[Union[List[ColumnInputType], ColumnInputType]] = None,
        left_on: Optional[Union[List[ColumnInputType], ColumnInputType]] = None,
        right_on: Optional[Union[List[ColumnInputType], ColumnInputType]] = None,
        how: str = "inner",
        strategy: Optional[str] = None,
    ) -> "DataFrame":
        """Column-wise join of the current DataFrame with an ``other`` DataFrame, similar to a SQL ``JOIN``

        .. NOTE::
            Although self joins are supported, we currently duplicate the logical plan for the right side
            and recompute the entire tree. Caching for this is on the roadmap.

        Args:
            other (DataFrame): the right DataFrame to join on.
            on (Optional[Union[List[ColumnInputType], ColumnInputType]], optional): key or keys to join on [use if the keys on the left and right side match.]. Defaults to None.
            left_on (Optional[Union[List[ColumnInputType], ColumnInputType]], optional): key or keys to join on left DataFrame. Defaults to None.
            right_on (Optional[Union[List[ColumnInputType], ColumnInputType]], optional): key or keys to join on right DataFrame. Defaults to None.
            how (str, optional): what type of join to perform; currently "inner", "left", "right", and "outer" are supported. Defaults to "inner".
            strategy (Optional[str]): The join strategy (algorithm) to use; currently "hash", "sort_merge", "broadcast", and None are supported, where None
                chooses the join strategy automatically during query optimization. The default is None.

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
        join_type = JoinType.from_join_type_str(how)
        join_strategy = JoinStrategy.from_join_strategy_str(strategy) if strategy is not None else None

        if join_strategy == JoinStrategy.SortMerge and join_type != JoinType.Inner:
            raise ValueError("Sort merge join only supports inner joins")
        elif join_strategy == JoinStrategy.Broadcast and join_type == JoinType.Outer:
            raise ValueError("Broadcast join does not support outer joins")
        elif join_strategy == JoinStrategy.Broadcast and join_type == JoinType.Anti:
            raise ValueError("Broadcast join does not support Anti joins")

        left_exprs = self.__column_input_to_expression(tuple(left_on) if isinstance(left_on, list) else (left_on,))
        right_exprs = self.__column_input_to_expression(tuple(right_on) if isinstance(right_on, list) else (right_on,))
        builder = self._builder.join(
            other._builder,
            left_on=left_exprs,
            right_on=right_exprs,
            how=join_type,
            strategy=join_strategy,
        )
        return DataFrame(builder)

    @DataframePublicAPI
    def concat(self, other: "DataFrame") -> "DataFrame":
        """Concatenates two DataFrames together in a "vertical" concatenation. The resulting DataFrame
        has number of rows equal to the sum of the number of rows of the input DataFrames.

        .. NOTE::
            DataFrames being concatenated **must have exactly the same schema**. You may wish to use the
            :meth:`df.select() <daft.DataFrame.select>` and :meth:`expr.cast() <daft.Expression.cast>` methods
            to ensure schema compatibility before concatenation.

        Args:
            other (DataFrame): other DataFrame to concatenate

        Returns:
            DataFrame: DataFrame with rows from `self` on top and rows from `other` at the bottom.
        """
        if self.schema() != other.schema():
            raise ValueError(
                f"DataFrames must have exactly the same schema for concatenation!\nExpected:\n{self.schema()}\n\nReceived:\n{other.schema()}"
            )
        builder = self._builder.concat(other._builder)
        return DataFrame(builder)

    @DataframePublicAPI
    def drop_nan(self, *cols: ColumnInputType):
        """Drops rows that contains NaNs. If cols is None it will drop rows with any NaN value.
        If column names are supplied, it will drop only those rows that contains NaNs in one of these columns.

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"a": [1.0, 2.2, 3.5, float("nan")]})
            >>> df.drop_nan().collect() # drops rows where any column contains NaN values
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

            >>> df = daft.from_pydict({"a": [1.6, 2.5, 3.3, float("nan")]})
            >>> df.drop_nan("a").collect()  # drops rows where column a contains NaN values
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

        Args:
            *cols (str): column names by which rows containing nans/NULLs should be filtered

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
        """Drops rows that contains NaNs or NULLs. If cols is None it will drop rows with any NULL value.
        If column names are supplied, it will drop only those rows that contains NULLs in one of these columns.

        Example:
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

        Args:
            *cols (str): column names by which rows containing nans should be filtered

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
            >>> df.explode(col("x"), col("y")).collect()
            ╭───────┬──────┬───────────────╮
            │ x     ┆ y    ┆ z             │
            │ ---   ┆ ---  ┆ ---           │
            │ Int64 ┆ Utf8 ┆ List[Float64] │
            ╞═══════╪══════╪═══════════════╡
            │ 1     ┆ a    ┆ [1]           │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2     ┆ b    ┆ [2, 2]        │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 3     ┆ c    ┆ [2, 2]        │
            ╰───────┴──────┴───────────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Args:
            *columns (ColumnInputType): columns to explode

        Returns:
            DataFrame: DataFrame with exploded column
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

        Example:
            >>> import daft
            >>> df = daft.from_pydict({
            ...     "year": [2020, 2021, 2022],
            ...     "Jan": [10, 30, 50],
            ...     "Feb": [20, 40, 60],
            ... })
            >>> df = df.unpivot("year", ["Jan", "Feb"], variable_name="month", value_name="inventory")
            >>> df = df.sort("year")
            >>> df.show()
            ╭───────┬───────┬───────────╮
            │ year  ┆ month ┆ inventory │
            │ ---   ┆ ---   ┆ ---       │
            │ Int64 ┆ Utf8  ┆ Int64     │
            ╞═══════╪═══════╪═══════════╡
            │ 2020  ┆ Jan   ┆ 10        │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2020  ┆ Feb   ┆ 20        │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2021  ┆ Jan   ┆ 30        │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2021  ┆ Feb   ┆ 40        │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2022  ┆ Jan   ┆ 50        │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2022  ┆ Feb   ┆ 60        │
            ╰───────┴───────┴───────────╯
            <BLANKLINE>
            (Showing first 6 of 6 rows)

        Args:
            ids (ManyColumnsInputType): Columns to keep as identifiers
            values (Optional[ManyColumnsInputType]): Columns to unpivot. If not specified, all columns except ids will be unpivoted.
            variable_name (Optional[str]): Name of the variable column. Defaults to "variable".
            value_name (Optional[str]): Name of the value column. Defaults to "value".

        Returns:
            DataFrame: Unpivoted DataFrame

        See also:
            `melt`
        """
        ids_exprs = self._column_inputs_to_expressions(ids)
        values_exprs = self._column_inputs_to_expressions(values)

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
        """Alias for unpivot

        See also:
            `unpivot`
        """
        return self.unpivot(ids, values, variable_name, value_name)

    @DataframePublicAPI
    def transform(self, func: Callable[..., "DataFrame"], *args: Any, **kwargs: Any) -> "DataFrame":
        """Apply a function that takes and returns a DataFrame.
        Allow splitting your transformation into different units of work (functions) while preserving the syntax for chaining transformations.
        Example:
            >>> import daft
            >>> df = daft.from_pydict({"col_a":[1,2,3,4]})
            >>> def add_1(df):
            ...     df = df.select(daft.col("col_a") + 1)
            ...     return df
            ...
            >>> def multiply_x(df, x):
            ...     df = df.select(daft.col("col_a") * x)
            ...     return df
            ...
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
        assert isinstance(result, DataFrame), (
            "Func returned an instance of type [%s], " "should have been DataFrame." % type(result)
        )
        return result

    def _agg(self, to_agg: List[Expression], group_by: Optional[ExpressionsProjection] = None) -> "DataFrame":
        builder = self._builder.agg(to_agg, list(group_by) if group_by is not None else None)
        return DataFrame(builder)

    def _agg_tuple_to_expression(self, agg_tuple: Tuple[ColumnInputType, str]) -> Expression:
        expr, op = agg_tuple

        if isinstance(expr, str):
            expr = col(expr)

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
            return expr.agg_list()
        elif op == "concat":
            return expr.agg_concat()

        raise NotImplementedError(f"Aggregation {op} is not implemented.")

    def _agg_inputs_to_expressions(
        self, to_agg: Tuple[Union[Expression, Iterable[Expression]], ...]
    ) -> List[Expression]:
        def is_agg_column_input(x: Any) -> bool:
            # aggs currently support Expression or tuple of (ColumnInputType, str) [deprecated]
            if isinstance(x, Expression):
                return True
            if isinstance(x, tuple) and len(x) == 2:
                tuple_type = list(map(type, x))
                return tuple_type == [Expression, str] or tuple_type == [str, str]
            return False

        columns: Iterable[Expression] = to_agg[0] if len(to_agg) == 1 and not is_agg_column_input(to_agg[0]) else to_agg  # type: ignore

        if any(isinstance(col, tuple) for col in columns):
            warnings.warn(
                "Tuple arguments in aggregations is deprecated and will be removed "
                "in Daft v0.3. Please use aggregation expressions instead.",
                DeprecationWarning,
            )
            return [self._agg_tuple_to_expression(col) if isinstance(col, tuple) else col for col in columns]  # type: ignore
        else:
            return list(columns)

    def _apply_agg_fn(
        self,
        fn: Callable[[Expression], Expression],
        cols: Tuple[ManyColumnsInputType, ...],
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
        """Performs a global sum on the DataFrame

        Args:
            *cols (Union[str, Expression]): columns to sum
        Returns:
            DataFrame: Globally aggregated sums. Should be a single row.
        """
        return self._apply_agg_fn(Expression.sum, cols)

    @DataframePublicAPI
    def mean(self, *cols: ColumnInputType) -> "DataFrame":
        """Performs a global mean on the DataFrame

        Args:
            *cols (Union[str, Expression]): columns to mean
        Returns:
            DataFrame: Globally aggregated mean. Should be a single row.
        """
        return self._apply_agg_fn(Expression.mean, cols)

    @DataframePublicAPI
    def min(self, *cols: ColumnInputType) -> "DataFrame":
        """Performs a global min on the DataFrame

        Args:
            *cols (Union[str, Expression]): columns to min
        Returns:
            DataFrame: Globally aggregated min. Should be a single row.
        """
        return self._apply_agg_fn(Expression.min, cols)

    @DataframePublicAPI
    def max(self, *cols: ColumnInputType) -> "DataFrame":
        """Performs a global max on the DataFrame

        Args:
            *cols (Union[str, Expression]): columns to max
        Returns:
            DataFrame: Globally aggregated max. Should be a single row.
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
        """
        return self._apply_agg_fn(Expression.any_value, cols)

    @DataframePublicAPI
    def count(self, *cols: ColumnInputType) -> "DataFrame":
        """Performs a global count on the DataFrame

        Args:
            *cols (Union[str, Expression]): columns to count
        Returns:
            DataFrame: Globally aggregated count. Should be a single row.
        """
        return self._apply_agg_fn(Expression.count, cols)

    @DataframePublicAPI
    def agg_list(self, *cols: ColumnInputType) -> "DataFrame":
        """Performs a global list agg on the DataFrame

        Args:
            *cols (Union[str, Expression]): columns to form into a list
        Returns:
            DataFrame: Globally aggregated list. Should be a single row.
        """
        return self._apply_agg_fn(Expression.agg_list, cols)

    @DataframePublicAPI
    def agg_concat(self, *cols: ColumnInputType) -> "DataFrame":
        """Performs a global list concatenation agg on the DataFrame

        Args:
            *cols (Union[str, Expression]): columns that are lists to concatenate
        Returns:
            DataFrame: Globally aggregated list. Should be a single row.
        """
        return self._apply_agg_fn(Expression.agg_concat, cols)

    @DataframePublicAPI
    def agg(self, *to_agg: Union[Expression, Iterable[Expression]]) -> "DataFrame":
        """Perform aggregations on this DataFrame. Allows for mixed aggregations for multiple columns
        Will return a single row that aggregated the entire DataFrame.

        Example:
            >>> import daft
            >>> from daft import col
            >>> df = daft.from_pydict({
            ...     "student_id": [1, 2, 3, 4],
            ...     "test1": [0.5, 0.4, 0.6, 0.7],
            ...     "test2": [0.9, 0.8, 0.7, 1.0]
            ... })
            >>> agg_df = df.agg(
            ...     col("test1").mean(),
            ...     col("test2").mean(),
            ...     ((col("test1") + col("test2"))/2).min().alias("total_min"),
            ...     ((col("test1") + col("test2"))/2).max().alias("total_max"),
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

        Args:
            *to_agg (Expression): aggregation expressions

        Returns:
            DataFrame: DataFrame with aggregated results
        """
        return self._agg(self._agg_inputs_to_expressions(to_agg), group_by=None)

    @DataframePublicAPI
    def groupby(self, *group_by: ManyColumnsInputType) -> "GroupedDataFrame":
        """Performs a GroupBy on the DataFrame for aggregation

        Example:
            >>> import daft
            >>> from daft import col
            >>> df = daft.from_pydict({
            ...     "pet": ["cat", "dog", "dog", "cat"],
            ...     "age": [1, 2, 3, 4],
            ...     "name": ["Alex", "Jordan", "Sam", "Riley"]
            ... })
            >>> grouped_df = df.groupby("pet").agg(
            ...     col("age").min().alias("min_age"),
            ...     col("age").max().alias("max_age"),
            ...     col("pet").count().alias("count"),
            ...     col("name").any_value()
            ... )
            >>> grouped_df.show()
            ╭──────┬─────────┬─────────┬────────┬────────╮
            │ pet  ┆ min_age ┆ max_age ┆ count  ┆ name   │
            │ ---  ┆ ---     ┆ ---     ┆ ---    ┆ ---    │
            │ Utf8 ┆ Int64   ┆ Int64   ┆ UInt64 ┆ Utf8   │
            ╞══════╪═════════╪═════════╪════════╪════════╡
            │ cat  ┆ 1       ┆ 4       ┆ 2      ┆ Alex   │
            ├╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ dog  ┆ 2       ┆ 3       ┆ 2      ┆ Jordan │
            ╰──────┴─────────┴─────────┴────────┴────────╯
            <BLANKLINE>
            (Showing first 2 of 2 rows)

        Args:
            *group_by (Union[str, Expression]): columns to group by

        Returns:
            GroupedDataFrame: DataFrame to Aggregate
        """
        return GroupedDataFrame(self, ExpressionsProjection(self._wildcard_inputs_to_expressions(group_by)))

    @DataframePublicAPI
    def pivot(
        self,
        group_by: ManyColumnsInputType,
        pivot_col: ColumnInputType,
        value_col: ColumnInputType,
        agg_fn: str,
        names: Optional[List[str]] = None,
    ) -> "DataFrame":
        """Pivots a column of the DataFrame and performs an aggregation on the values.

        .. NOTE::
            You may wish to provide a list of distinct values to pivot on, which is more efficient as it avoids
            a distinct operation. Without this list, Daft will perform a distinct operation on the pivot column to
            determine the unique values to pivot on.

        Example:
            >>> import daft
            >>> data = {
            ...     "id": [1, 2, 3, 4],
            ...     "version": ["3.8", "3.8", "3.9", "3.9"],
            ...     "platform": ["macos", "macos", "macos", "windows"],
            ...     "downloads": [100, 200, 150, 250],
            ... }
            >>> df = daft.from_pydict(data)
            >>> df = df.pivot("version", "platform", "downloads", "sum")
            >>> df.show()
            ╭─────────┬─────────┬───────╮
            │ version ┆ windows ┆ macos │
            │ ---     ┆ ---     ┆ ---   │
            │ Utf8    ┆ Int64   ┆ Int64 │
            ╞═════════╪═════════╪═══════╡
            │ 3.9     ┆ 250     ┆ 150   │
            ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 3.8     ┆ None    ┆ 300   │
            ╰─────────┴─────────┴───────╯
            <BLANKLINE>
            (Showing first 2 of 2 rows)

        Args:
            group_by (ManyColumnsInputType): columns to group by
            pivot_col (Union[str, Expression]): column to pivot
            value_col (Union[str, Expression]): column to aggregate
            agg_fn (str): aggregation function to apply
            names (Optional[List[str]]): names of the pivoted columns

        Returns:
            DataFrame: DataFrame with pivoted columns

        """
        group_by_expr = self._column_inputs_to_expressions(group_by)
        [pivot_col_expr, value_col_expr] = self._column_inputs_to_expressions([pivot_col, value_col])
        agg_expr = self._agg_tuple_to_expression((value_col_expr, agg_fn))

        if names is None:
            names = self.select(pivot_col_expr).distinct().to_pydict()[pivot_col_expr.name()]
            names = [str(x) for x in names]
        builder = self._builder.pivot(group_by_expr, pivot_col_expr, value_col_expr, agg_expr, names)
        return DataFrame(builder)

    def _materialize_results(self) -> None:
        """Materializes the results of for this DataFrame and hold a pointer to the results."""
        context = get_context()
        if self._result is None:
            self._result_cache = context.runner().run(self._builder)
            result = self._result
            assert result is not None
            result.wait()

    @DataframePublicAPI
    def collect(self, num_preview_rows: Optional[int] = 8) -> "DataFrame":
        """Executes the entire DataFrame and materializes the results

        .. NOTE::
            This call is **blocking** and will execute the DataFrame when called

        Args:
            num_preview_rows: Number of rows to preview. Defaults to 8.

        Returns:
            DataFrame: DataFrame with materialized results.
        """
        self._materialize_results()

        assert self._result is not None
        dataframe_len = len(self._result)
        if num_preview_rows is not None:
            self._num_preview_rows = num_preview_rows
        else:
            self._num_preview_rows = dataframe_len
        return self

    def _construct_show_display(self, n: int) -> "DataFrameDisplay":
        """Helper for .show() which will construct the underlying DataFrameDisplay object"""
        preview_partition = self._preview.preview_partition
        total_rows = self._preview.dataframe_num_rows

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
            for table in get_context().runner().run_iter_tables(builder, results_buffer_size=1):
                tables.append(table)
                seen += len(table)
                if seen >= n:
                    break

            preview_partition = MicroPartition.concat(tables)
            if len(preview_partition) > n:
                preview_partition = preview_partition.slice(0, n)
            elif len(preview_partition) < n:
                # Iterator short-circuited before reaching n, so we know that we have the full DataFrame.
                total_rows = n = len(preview_partition)
            preview = DataFramePreview(
                preview_partition=preview_partition,
                dataframe_num_rows=total_rows,
            )
        elif len(preview_partition) > n:
            # Preview partition is cached but has more rows that we need, so use the appropriate slice.
            truncated_preview_partition = preview_partition.slice(0, n)
            preview = DataFramePreview(
                preview_partition=truncated_preview_partition,
                dataframe_num_rows=total_rows,
            )
        else:
            assert len(preview_partition) == n
            # Preview partition is cached and has exactly the number of rows that we need, so use it directly.
            preview = self._preview

        return DataFrameDisplay(preview, self.schema(), num_rows=n)

    @DataframePublicAPI
    def show(self, n: int = 8) -> None:
        """Executes enough of the DataFrame in order to display the first ``n`` rows

        If IPython is installed, this will use IPython's `display` utility to pretty-print in a
        notebook/REPL environment. Otherwise, this will fall back onto a naive Python `print`.

        .. NOTE::
            This call is **blocking** and will execute the DataFrame when called

        Args:
            n: number of rows to show. Defaults to 8.
        """
        dataframe_display = self._construct_show_display(n)
        try:
            from IPython.display import display

            display(dataframe_display, clear=True)
        except ImportError:
            print(dataframe_display)
        return None

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

    def __contains__(self, col_name: str) -> bool:
        """Returns whether the column exists in the dataframe.

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6], "z": [7, 8, 9]})
            >>> 'x' in df
            True

        Args:
            col_name (str): column name

        Returns:
            bool: whether the column exists in the dataframe.
        """
        return col_name in self.column_names

    @DataframePublicAPI
    def to_pandas(
        self, cast_tensors_to_ray_tensor_dtype: bool = False, coerce_temporal_nanoseconds: bool = False
    ) -> "pandas.DataFrame":
        """Converts the current DataFrame to a `pandas DataFrame <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`__.
        If results have not computed yet, collect will be called.

        Args:
            cast_tensors_to_ray_tensor_dtype (bool): Whether to cast tensors to Ray tensor dtype. Defaults to False.
            coerce_temporal_nanoseconds (bool): Whether to coerce temporal columns to nanoseconds. Only applicable to pandas version >= 2.0 and pyarrow version >= 13.0.0. Defaults to False. See `pyarrow.Table.to_pandas <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table.to_pandas>`__ for more information.

        Returns:
            pandas.DataFrame: `pandas DataFrame <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`__ converted from a Daft DataFrame

            .. NOTE::
                This call is **blocking** and will execute the DataFrame when called
        """
        self.collect()
        result = self._result
        assert result is not None

        pd_df = result.to_pandas(
            schema=self._builder.schema(),
            cast_tensors_to_ray_tensor_dtype=cast_tensors_to_ray_tensor_dtype,
            coerce_temporal_nanoseconds=coerce_temporal_nanoseconds,
        )
        return pd_df

    @DataframePublicAPI
    def to_arrow(self, cast_tensors_to_ray_tensor_dtype: bool = False) -> "pyarrow.Table":
        """Converts the current DataFrame to a `pyarrow Table <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html>`__.
        If results have not computed yet, collect will be called.

        Returns:
            pyarrow.Table: `pyarrow Table <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html>`__ converted from a Daft DataFrame

            .. NOTE::
                This call is **blocking** and will execute the DataFrame when called
        """
        self.collect()
        result = self._result
        assert result is not None

        return result.to_arrow(cast_tensors_to_ray_tensor_dtype)

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
    def to_torch_map_dataset(self) -> "torch.utils.data.Dataset":
        """Convert the current DataFrame into a map-style
        `Torch Dataset <https://pytorch.org/docs/stable/data.html#map-style-datasets>`__
        for use with PyTorch.

        This method will materialize the entire DataFrame and block on completion.

        Items will be returned in pydict format: a dict of `{"column name": value}` for each row in the data.

        .. NOTE::
            If you do not need random access, you may get better performance out of an IterableDataset,
            which streams data items in as soon as they are ready and does not block on full materialization.

        .. NOTE::
            This method returns results locally.
            For distributed training, you may want to use ``DataFrame.to_ray_dataset()``.
        """
        from daft.dataframe.to_torch import DaftTorchDataset

        return DaftTorchDataset(self.to_pydict(), len(self))

    @DataframePublicAPI
    def to_torch_iter_dataset(self) -> "torch.utils.data.IterableDataset":
        """Convert the current DataFrame into a
        `Torch IterableDataset <https://pytorch.org/docs/stable/data.html#torch.utils.data.IterableDataset>`__
        for use with PyTorch.

        Begins execution of the DataFrame if it is not yet executed.

        Items will be returned in pydict format: a dict of `{"column name": value}` for each row in the data.

        .. NOTE::
            The produced dataset is meant to be used with the single-process DataLoader,
            and does not support data sharding hooks for multi-process data loading.

            Do keep in mind that Daft is already using multithreading or multiprocessing under the hood
            to compute the data stream that feeds this dataset.

        .. NOTE::
            This method returns results locally.
            For distributed training, you may want to use ``DataFrame.to_ray_dataset()``.
        """
        from daft.dataframe.to_torch import DaftTorchIterableDataset

        return DaftTorchIterableDataset(self)

    @DataframePublicAPI
    def to_ray_dataset(self) -> "ray.data.dataset.DataSet":
        """Converts the current DataFrame to a `Ray Dataset <https://docs.ray.io/en/latest/data/api/dataset.html#ray.data.Dataset>`__ which is useful for running distributed ML model training in Ray

        .. NOTE::
            This function can only work if Daft is running using the RayRunner

        Returns:
            ray.data.dataset.DataSet: `Ray dataset <https://docs.ray.io/en/latest/data/api/dataset.html#ray.data.Dataset>`__
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
        """Creates a DataFrame from a `Ray Dataset <https://docs.ray.io/en/latest/data/api/dataset.html#ray.data.Dataset>`__."""
        from ray.exceptions import RayTaskError

        context = get_context()
        if context.runner_config.name != "ray":
            raise ValueError("Daft needs to be running on the Ray Runner for this operation")

        from daft.runners.ray_runner import RayRunnerIO

        ray_runner_io = context.runner().runner_io()
        assert isinstance(ray_runner_io, RayRunnerIO)

        partition_set, schema = ray_runner_io.partition_set_from_ray_dataset(ds)
        cache_entry = context.runner().put_partition_set_into_cache(partition_set)
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
        num_preview_rows = context.daft_execution_config.num_preview_rows
        dataframe_num_rows = len(df)
        if dataframe_num_rows > num_preview_rows:
            preview_results, _ = ray_runner_io.partition_set_from_ray_dataset(ds.limit(num_preview_rows))
        else:
            preview_results = partition_set

        # set preview
        preview_partition = preview_results._get_merged_vpartition()
        df._preview = DataFramePreview(
            preview_partition=preview_partition,
            dataframe_num_rows=dataframe_num_rows,
        )
        return df

    @DataframePublicAPI
    def to_dask_dataframe(
        self,
        meta: Union[
            "pandas.DataFrame",
            "pandas.Series",
            Dict[str, Any],
            Iterable[Any],
            Tuple[Any],
            None,
        ] = None,
    ) -> "dask.DataFrame":
        """Converts the current Daft DataFrame to a Dask DataFrame.

        The returned Dask DataFrame will use `Dask-on-Ray <https://docs.ray.io/en/latest/ray-more-libs/dask-on-ray.html>`__
        to execute operations on a Ray cluster.

        .. NOTE::
            This function can only work if Daft is running using the RayRunner.

        Args:
            meta: An empty pandas `DataFrame <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`__ or `Series <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html>`__ that matches the dtypes and column
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
        context = get_context()
        if context.runner_config.name != "ray":
            raise ValueError("Daft needs to be running on the Ray Runner for this operation")

        from daft.runners.ray_runner import RayRunnerIO

        ray_runner_io = context.runner().runner_io()
        assert isinstance(ray_runner_io, RayRunnerIO)

        partition_set, schema = ray_runner_io.partition_set_from_dask_dataframe(ddf)
        cache_entry = context.runner().put_partition_set_into_cache(partition_set)
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
        num_preview_rows = context.daft_execution_config.num_preview_rows
        dataframe_num_rows = len(df)
        if dataframe_num_rows > num_preview_rows:
            preview_results, _ = ray_runner_io.partition_set_from_dask_dataframe(ddf.loc[: num_preview_rows - 1])
        else:
            preview_results = partition_set

        # set preview
        preview_partition = preview_results._get_merged_vpartition()
        df._preview = DataFramePreview(
            preview_partition=preview_partition,
            dataframe_num_rows=dataframe_num_rows,
        )
        return df


@dataclass
class GroupedDataFrame:
    df: DataFrame
    group_by: ExpressionsProjection

    def __post_init__(self):
        resolved_groupby_schema = self.group_by.resolve_schema(self.df._builder.schema())
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
        return self.df._apply_agg_fn(Expression.sum, cols, self.group_by)

    def mean(self, *cols: ColumnInputType) -> "DataFrame":
        """Performs grouped mean on this GroupedDataFrame.

        Args:
            *cols (Union[str, Expression]): columns to mean

        Returns:
            DataFrame: DataFrame with grouped mean.
        """
        return self.df._apply_agg_fn(Expression.mean, cols, self.group_by)

    def min(self, *cols: ColumnInputType) -> "DataFrame":
        """Perform grouped min on this GroupedDataFrame.

        Args:
            *cols (Union[str, Expression]): columns to min

        Returns:
            DataFrame: DataFrame with grouped min.
        """
        return self.df._apply_agg_fn(Expression.min, cols, self.group_by)

    def max(self, *cols: ColumnInputType) -> "DataFrame":
        """Performs grouped max on this GroupedDataFrame.

        Args:
            *cols (Union[str, Expression]): columns to max

        Returns:
            DataFrame: DataFrame with grouped max.
        """
        return self.df._apply_agg_fn(Expression.max, cols, self.group_by)

    def any_value(self, *cols: ColumnInputType) -> "DataFrame":
        """Returns an arbitrary value on this GroupedDataFrame.
        Values for each column are not guaranteed to be from the same row.

        Args:
            *cols (Union[str, Expression]): columns to get

        Returns:
            DataFrame: DataFrame with any values.
        """
        return self.df._apply_agg_fn(Expression.any_value, cols, self.group_by)

    def count(self, *cols: ColumnInputType) -> "DataFrame":
        """Performs grouped count on this GroupedDataFrame.

        Returns:
            DataFrame: DataFrame with grouped count per column.
        """
        return self.df._apply_agg_fn(Expression.count, cols, self.group_by)

    def agg_list(self, *cols: ColumnInputType) -> "DataFrame":
        """Performs grouped list on this GroupedDataFrame.

        Returns:
            DataFrame: DataFrame with grouped list per column.
        """
        return self.df._apply_agg_fn(Expression.agg_list, cols, self.group_by)

    def agg_concat(self, *cols: ColumnInputType) -> "DataFrame":
        """Performs grouped concat on this GroupedDataFrame.

        Returns:
            DataFrame: DataFrame with grouped concatenated list per column.
        """
        return self.df._apply_agg_fn(Expression.agg_concat, cols, self.group_by)

    def agg(self, *to_agg: Union[Expression, Iterable[Expression]]) -> "DataFrame":
        """Perform aggregations on this GroupedDataFrame. Allows for mixed aggregations.

        Example:
            >>> import daft
            >>> from daft import col
            >>> df = daft.from_pydict({
            ...     "pet": ["cat", "dog", "dog", "cat"],
            ...     "age": [1, 2, 3, 4],
            ...     "name": ["Alex", "Jordan", "Sam", "Riley"]
            ... })
            >>> grouped_df = df.groupby("pet").agg(
            ...     col("age").min().alias("min_age"),
            ...     col("age").max().alias("max_age"),
            ...     col("pet").count().alias("count"),
            ...     col("name").any_value()
            ... )
            >>> grouped_df.show()
            ╭──────┬─────────┬─────────┬────────┬────────╮
            │ pet  ┆ min_age ┆ max_age ┆ count  ┆ name   │
            │ ---  ┆ ---     ┆ ---     ┆ ---    ┆ ---    │
            │ Utf8 ┆ Int64   ┆ Int64   ┆ UInt64 ┆ Utf8   │
            ╞══════╪═════════╪═════════╪════════╪════════╡
            │ cat  ┆ 1       ┆ 4       ┆ 2      ┆ Alex   │
            ├╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ dog  ┆ 2       ┆ 3       ┆ 2      ┆ Jordan │
            ╰──────┴─────────┴─────────┴────────┴────────╯
            <BLANKLINE>
            (Showing first 2 of 2 rows)

        Args:
            *to_agg (Union[Expression, Iterable[Expression]]): aggregation expressions

        Returns:
            DataFrame: DataFrame with grouped aggregations
        """
        return self.df._agg(self.df._agg_inputs_to_expressions(to_agg), group_by=self.group_by)

    def map_groups(self, udf: Expression) -> "DataFrame":
        """Apply a user-defined function to each group. The name of the resultant column will default to the name of the first input column.

        Example:
            >>> import daft, statistics
            >>> df = daft.from_pydict({"group": ["a", "a", "a", "b", "b", "b"], "data": [1, 20, 30, 4, 50, 600]})
            >>> @daft.udf(return_dtype=daft.DataType.float64())
            ... def std_dev(data):
            ...     return [statistics.stdev(data.to_pylist())]
            >>> df = df.groupby("group").map_groups(std_dev(df["data"]))
            >>> df.show()
            ╭───────┬────────────────────╮
            │ group ┆ data               │
            │ ---   ┆ ---                │
            │ Utf8  ┆ Float64            │
            ╞═══════╪════════════════════╡
            │ a     ┆ 14.730919862656235 │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ b     ┆ 331.62026476076517 │
            ╰───────┴────────────────────╯
            <BLANKLINE>
            (Showing first 2 of 2 rows)

        Args:
            udf (Expression): User-defined function to apply to each group.

        Returns:
            DataFrame: DataFrame with grouped aggregations
        """
        return self.df._map_groups(udf, group_by=self.group_by)
