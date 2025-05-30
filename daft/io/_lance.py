# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Optional

from daft import context
from daft.api_annotations import PublicAPI
from daft.daft import IOConfig, PyPartitionField, PyPushdowns, PyRecordBatch, ScanOperatorHandle, ScanTask
from daft.dataframe import DataFrame
from daft.io.object_store_options import io_config_to_storage_options
from daft.io.scan import ScanOperator
from daft.logical.builder import LogicalPlanBuilder
from daft.logical.schema import Schema
from daft.recordbatch import RecordBatch

if TYPE_CHECKING:
    import lance


def _lancedb_table_factory_function(
    ds: "lance.LanceDataset", fragment_id: int, required_columns: Optional[list[str]]
) -> Iterator[PyRecordBatch]:
    fragment = ds.get_fragment(fragment_id)
    assert fragment is not None, RuntimeError(f"Unable to find lance fragment {fragment_id}")
    return (
        RecordBatch.from_arrow_record_batches([rb], rb.schema)._recordbatch
        for rb in fragment.to_batches(columns=required_columns)
    )


@PublicAPI
def read_lance(url: str, io_config: Optional[IOConfig] = None) -> DataFrame:
    """Create a DataFrame from a LanceDB table.

    Args:
        url: URL to the LanceDB table (supports remote URLs to object stores such as `s3://` or `gs://`)
        io_config: A custom IOConfig to use when accessing LanceDB data. Defaults to None.

    Returns:
        DataFrame: a DataFrame with the schema converted from the specified LanceDB table

    Note:
        This function requires the use of [LanceDB](https://lancedb.github.io/lancedb/), which is the Python library for the LanceDB project.
        To ensure that this is installed with Daft, you may install: `pip install daft[lance]`

    Examples:
        Read a local LanceDB table:
        >>> df = daft.read_lance("s3://my-lancedb-bucket/data/")
        >>> df.show()

        Read a LanceDB table from a public S3 bucket:
        >>> from daft.io import S3Config
        >>> s3_config = S3Config(region="us-west-2", anonymous=True)
        >>> df = daft.read_lance("s3://daft-public-data/lance/words-test-dataset", io_config=s3_config)
        >>> df.show()
    """
    try:
        import lance
    except ImportError as e:
        raise ImportError(
            "Unable to import the `lance` package, please ensure that Daft is installed with the lance extra dependency: `pip install daft[lance]`"
        ) from e

    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config
    storage_options = io_config_to_storage_options(io_config, url)

    ds = lance.dataset(url, storage_options=storage_options)
    iceberg_operator = LanceDBScanOperator(ds)

    handle = ScanOperatorHandle.from_python_scan_operator(iceberg_operator)
    builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
    return DataFrame(builder)


class LanceDBScanOperator(ScanOperator):
    def __init__(self, ds: "lance.LanceDataset"):
        self._ds = ds

    def name(self) -> str:
        return "LanceDBScanOperator"

    def display_name(self) -> str:
        return f"LanceDBScanOperator({self._ds.uri})"

    def schema(self) -> Schema:
        return Schema.from_pyarrow_schema(self._ds.schema)

    def partitioning_keys(self) -> list[PyPartitionField]:
        return []

    def can_absorb_filter(self) -> bool:
        return False

    def can_absorb_limit(self) -> bool:
        return False

    def can_absorb_select(self) -> bool:
        return False

    def multiline_display(self) -> list[str]:
        return [
            self.display_name(),
            f"Schema = {self.schema()}",
        ]

    def to_scan_tasks(self, pushdowns: PyPushdowns) -> Iterator[ScanTask]:
        required_columns: Optional[list[str]]
        if pushdowns.columns is None:
            required_columns = None
        else:
            filter_required_column_names = pushdowns.filter_required_column_names()
            required_columns = (
                pushdowns.columns
                if filter_required_column_names is None
                else pushdowns.columns + filter_required_column_names
            )

        # TODO: figure out how to translate Pushdowns into LanceDB filters
        filters = None
        fragments = self._ds.get_fragments(filter=filters)
        for i, fragment in enumerate(fragments):
            # TODO: figure out how if we can get this metadata from LanceDB fragments cheaply
            size_bytes = None
            stats = None

            # NOTE: `fragment.count_rows()` should result in 1 IO call for the data file
            # (1 fragment = 1 data file) and 1 more IO call for the deletion file (if present).
            # This could potentially be expensive to perform serially if there are thousands of files.
            # Given that num_rows isn't leveraged for much at the moment, and without statistics
            # we will probably end up materializing the data anyways for any operations, we leave this
            # as None.
            num_rows = None

            yield ScanTask.python_factory_func_scan_task(
                module=_lancedb_table_factory_function.__module__,
                func_name=_lancedb_table_factory_function.__name__,
                func_args=(self._ds, fragment.fragment_id, required_columns),
                schema=self.schema()._schema,
                num_rows=num_rows,
                size_bytes=size_bytes,
                pushdowns=pushdowns,
                stats=stats,
            )
