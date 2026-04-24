from __future__ import annotations

from typing import TYPE_CHECKING

from daft.io.source import DataSource, DataSourceTask

from daft.daft import (
    StorageConfig,
)
from daft.schema import Schema
from daft.io.iceberg.metadata import (
    convert_iceberg_schema,
    convert_iceberg_partition_spec,
    resolve_iceberg_schema,
)


from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.table import Table as IcebergTable
from pyiceberg.table import TableMetadata as IcebergTableMetadata

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from pyiceberg.partitioning import PartitionField as IcebergPartitionField
    from pyiceberg.partitioning import PartitionSpec as IcebergPartitionSpec
    from daft.io.pushdowns import Pushdowns
    from daft.io.partitioning import PartitionField

    from daft.dataframe import DataFrame
    from daft.io.partitioning import PartitionField
    from daft.io.pushdowns import Pushdowns


class IcebergSource(DataSource):
    _table: IcebergTable
    _snapshot_id: int | None
    _storage_config: StorageConfig
    _schema: Schema
    _partition_fields: list[PartitionField]

    def __init__(
        self,
        table: IcebergTable,
        snapshot_id: int | None,
        storage_config: StorageConfig,
    ):
        self._table = table
        self._snapshot_id = snapshot_id
        self._storage_config = storage_config
        # Convert the Iceberg schema to a Daft schema
        iceberg_schema = resolve_iceberg_schema(table.metadata, snapshot_id)
        iceberg_partition_spec = table.spec()
        self._schema = convert_iceberg_schema(iceberg_schema)
        self._partition_fields = convert_iceberg_partition_spec(
            iceberg_schema, iceberg_partition_spec
        )

    @property
    def name(self) -> str:
        return f"IcebergSource({'.'.join(self._table.name())})"

    @property
    def schema(self) -> Schema:
        return self._schema

    def get_partition_fields(self) -> list[PartitionField]:
        """Returns the partitioning fields for this data source."""
        return self._partition_fields

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[DataSourceTask]:
        # Convert the columns to a tuple of strings for projection pushdown
        selected_fields = tuple(pushdowns.columns) if pushdowns.columns else ("*",)

        # Convert the row filter to an Iceberg expression
        # row_filter = convert_expression_to_iceberg(pushdowns.filters)

        # TODO
        tasks = self._table.scan(
            row_filter=None,
            selected_fields=selected_fields,
            snapshot_id=self._snapshot_id,
            limit=pushdowns.limit,
        )

        # TODO: emit warning
        # if len(self._partition_fields) > 0 and pushdowns.partition_filters is None:
        #     logger.warning(
        #         "%s has Partitioning Keys: %s but no partition filter was specified. This will result in a full table scan.",
        #         self.name,
        #         self.get_partition_fields(),
        #     )

        yield
