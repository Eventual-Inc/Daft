from __future__ import annotations

import warnings
from collections.abc import Iterator

from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.partitioning import PartitionField as IcebergPartitionField
from pyiceberg.partitioning import PartitionSpec as IcebergPartitionSpec
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.table import Table

from daft.daft import (
    FileFormatConfig,
    ParquetSourceConfig,
    Pushdowns,
    ScanTask,
    StorageConfig,
)
from daft.datatype import DataType
from daft.expressions.expressions import col
from daft.io.scan import PartitionField, ScanOperator, make_partition_field
from daft.logical.schema import Field, Schema


def _iceberg_partition_field_to_daft_partition_field(
    iceberg_schema: IcebergSchema, pfield: IcebergPartitionField
) -> PartitionField:
    name = pfield.name
    source_id = pfield.source_id
    source_field = iceberg_schema.find_field(source_id)
    source_name = source_field.name
    daft_field = Field.create(
        source_name, DataType.from_arrow_type(schema_to_pyarrow(iceberg_schema.find_type(source_name)))
    )
    transform = pfield.transform
    iceberg_result_type = transform.result_type(source_field.field_type)
    arrow_result_type = schema_to_pyarrow(iceberg_result_type)
    daft_result_type = DataType.from_arrow_type(arrow_result_type)
    result_field = Field.create(name, daft_result_type)

    from pyiceberg.transforms import (
        DayTransform,
        HourTransform,
        IdentityTransform,
        MonthTransform,
        YearTransform,
    )

    expr = None
    if isinstance(transform, IdentityTransform):
        expr = col(source_name)
        if source_name != name:
            expr = expr.alias(name)
    elif isinstance(transform, YearTransform):
        expr = col(source_name).dt.year().alias(name)
    elif isinstance(transform, MonthTransform):
        expr = col(source_name).dt.month().alias(name)
    elif isinstance(transform, DayTransform):
        expr = col(source_name).dt.day().alias(name)
    elif isinstance(transform, HourTransform):
        warnings.warn(
            "HourTransform not implemented, Please make a comment: https://github.com/Eventual-Inc/Daft/issues/1606"
        )
    else:
        warnings.warn(f"{transform} not implemented, Please make an issue!")

    return make_partition_field(result_field, daft_field, transform=expr)


def iceberg_partition_spec_to_fields(iceberg_schema: IcebergSchema, spec: IcebergPartitionSpec) -> list[PartitionField]:
    return [_iceberg_partition_field_to_daft_partition_field(iceberg_schema, field) for field in spec.fields]


class IcebergScanOperator(ScanOperator):
    def __init__(self, iceberg_table: Table, storage_config: StorageConfig) -> None:
        super().__init__()
        self._table = iceberg_table
        self._storage_config = storage_config
        arrow_schema = schema_to_pyarrow(iceberg_table.schema())
        self._schema = Schema.from_pyarrow_schema(arrow_schema)
        self._partition_keys = iceberg_partition_spec_to_fields(self._table.schema(), self._table.spec())

    def schema(self) -> Schema:
        return self._schema

    def partitioning_keys(self) -> list[PartitionField]:
        return self._partition_keys

    def to_scan_tasks(self, pushdowns: Pushdowns) -> Iterator[ScanTask]:
        limit = pushdowns.limit
        iceberg_tasks = self._table.scan(limit=limit).plan_files()

        limit_files = limit is not None and pushdowns.filters is None

        scan_tasks = []

        if limit is not None:
            rows_left = limit
        else:
            rows_left = 0
        for task in iceberg_tasks:
            if limit_files and (rows_left <= 0):
                break
            file = task.file
            path = file.file_path
            record_count = file.record_count
            file_format = file.file_format
            if file_format == "PARQUET":
                file_format_config = FileFormatConfig.from_parquet_config(ParquetSourceConfig())
            else:
                # TODO: Support ORC and AVRO when we can read it
                raise NotImplementedError(f"{file_format} for iceberg not implemented!")

            if len(task.delete_files) > 0:
                raise NotImplementedError(f"Iceberg Merge-on-Read currently not supported, please make an issue!")

            # TODO: Thread in PartitionSpec to each ScanTask: P1
            # TODO: Thread in Statistics to each ScanTask: P2

            st = ScanTask.catalog_scan_task(
                file=path,
                file_format=file_format_config,
                schema=self._schema._schema,
                num_rows=record_count,
                storage_config=self._storage_config,
                size_bytes=file.file_size_in_bytes,
                pushdowns=pushdowns,
            )
            rows_left -= record_count
            scan_tasks.append(st)
        return iter(scan_tasks)

    def can_absorb_filter(self) -> bool:
        return False

    def can_absorb_limit(self) -> bool:
        return False

    def can_absorb_select(self) -> bool:
        return True
