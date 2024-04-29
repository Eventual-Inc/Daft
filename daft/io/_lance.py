# isort: dont-add-import: from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Dict, Iterator, List, Optional

import lance

from daft import context
from daft.api_annotations import PublicAPI
from daft.daft import IOConfig, Pushdowns, PyTable, ScanOperatorHandle, ScanTask
from daft.dataframe import DataFrame
from daft.io.scan import PartitionField, ScanOperator
from daft.logical.builder import LogicalPlanBuilder
from daft.logical.schema import Schema
from daft.table import Table

if TYPE_CHECKING:
    pass


def _lancedb_factory_function(fragment: lance.LanceFragment, pushdowns: Pushdowns) -> Callable[[], List[PyTable]]:
    def f() -> List[PyTable]:
        return [
            Table.from_arrow_record_batches([rb], fragment.schema)._table
            for rb in fragment.to_batches(columns=pushdowns.columns)
        ]

    return f


@PublicAPI
def read_lance(url: str, io_config: Optional["IOConfig"] = None) -> DataFrame:
    """Create a DataFrame from a LanceDB table

    .. NOTE::
        This function requires the use of `LanceDB <https://lancedb.github.io/lancedb/>`_, which is the Python
        library for the LanceDB project.

    Args:
        url: URL to the LanceDB table (supports remote URLs to object stores such as `s3://` or `gs://`)
        io_config: A custom IOConfig to use when accessing LanceDB data. Defaults to None.

    Returns:
        DataFrame: a DataFrame with the schema converted from the specified LanceDB table
    """

    # TODO: Convert this to LanceDB-compatible storage options instead of defaulting to `None`
    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config
    storage_options = None

    iceberg_operator = LanceDBScanOperator(url, storage_options=storage_options)

    handle = ScanOperatorHandle.from_python_scan_operator(iceberg_operator)
    builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
    return DataFrame(builder)


class LanceDBScanOperator(ScanOperator):
    def __init__(self, uri: str, storage_options: Optional[Dict[str, Any]] = None):
        self.uri = uri

        # TODO: If not pickleable, we can make this ephemeral and just the URI
        self._ds = lance.dataset(self.uri, storage_options=storage_options)

    def display_name(self) -> str:
        return f"LanceDBScanOperator({self.uri})"

    def schema(self) -> Schema:
        return Schema.from_pyarrow_schema(self._ds.schema)

    def partitioning_keys(self) -> List[PartitionField]:
        return []

    def can_absorb_filter(self) -> bool:
        return False

    def can_absorb_limit(self) -> bool:
        return False

    def can_absorb_select(self) -> bool:
        return False

    def multiline_display(self) -> List[str]:
        return [
            self.display_name(),
            f"Schema = {self.schema()}",
        ]

    def to_scan_tasks(self, pushdowns: Pushdowns) -> Iterator[ScanTask]:
        # TODO: figure out how to translate Pushdowns into LanceDB filters
        filters = None

        fragments = self._ds.get_fragments(filter=filters)

        for i, fragment in enumerate(fragments):
            # TODO: figure out how if we can get this metadata from LanceDB fragments cheaply
            size_bytes = None
            num_rows = None
            stats = None

            yield ScanTask.python_factory_func_scan_task(
                func=_lancedb_factory_function(fragment, pushdowns),
                descriptor=f"{self.display_name()}[{i}]",
                schema=self.schema()._schema,
                num_rows=num_rows,
                size_bytes=size_bytes,
                pushdowns=pushdowns,
                stats=stats,
            )
