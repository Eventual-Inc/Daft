from __future__ import annotations

import logging
import warnings
from collections.abc import Iterator

import pyarrow as pa
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.partitioning import PartitionField as IcebergPartitionField
from pyiceberg.partitioning import PartitionSpec as IcebergPartitionSpec
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.schema import visit
from pyiceberg.table import Table
from pyiceberg.typedef import Record

import daft
from daft.daft import (
    FileFormatConfig,
    ParquetSourceConfig,
    PartitionTransform,
    Pushdowns,
    ScanTask,
    StorageConfig,
)
from daft.datatype import DataType
from daft.iceberg.schema_field_id_mapping_visitor import SchemaFieldIdMappingVisitor
from daft.io.scan import PartitionField, ScanOperator, make_partition_field
from daft.logical.schema import Field, Schema

logger = logging.getLogger(__name__)


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
        BucketTransform,
        DayTransform,
        HourTransform,
        IdentityTransform,
        MonthTransform,
        TruncateTransform,
        YearTransform,
    )

    tfm = None
    if isinstance(transform, IdentityTransform):
        tfm = PartitionTransform.identity()
    elif isinstance(transform, YearTransform):
        tfm = PartitionTransform.year()
    elif isinstance(transform, MonthTransform):
        tfm = PartitionTransform.month()
    elif isinstance(transform, DayTransform):
        tfm = PartitionTransform.day()
    elif isinstance(transform, HourTransform):
        tfm = PartitionTransform.hour()
    elif isinstance(transform, BucketTransform):
        n = transform.num_buckets
        tfm = PartitionTransform.iceberg_bucket(n)
    elif isinstance(transform, TruncateTransform):
        w = transform.width
        tfm = PartitionTransform.iceberg_truncate(w)
    else:
        warnings.warn(f"{transform} not implemented, Please make an issue!")
    return make_partition_field(result_field, daft_field, transform=tfm)


def iceberg_partition_spec_to_fields(iceberg_schema: IcebergSchema, spec: IcebergPartitionSpec) -> list[PartitionField]:
    return [_iceberg_partition_field_to_daft_partition_field(iceberg_schema, field) for field in spec.fields]


class IcebergScanOperator(ScanOperator):
    def __init__(self, iceberg_table: Table, snapshot_id: int | None, storage_config: StorageConfig) -> None:
        super().__init__()
        self._table = iceberg_table
        self._snapshot_id = snapshot_id
        self._storage_config = storage_config

        iceberg_schema = (
            iceberg_table.schema()
            if self._snapshot_id is None
            else self._table.scan(snapshot_id=self._snapshot_id).projection()
        )
        arrow_schema = schema_to_pyarrow(iceberg_schema)
        self._field_id_mapping = visit(iceberg_schema, SchemaFieldIdMappingVisitor())
        self._schema = Schema.from_pyarrow_schema(arrow_schema)

        self._partition_keys = iceberg_partition_spec_to_fields(iceberg_schema, self._table.spec())

    def schema(self) -> Schema:
        return self._schema

    def display_name(self) -> str:
        return f"IcebergScanOperator({'.'.join(self._table.name())})"

    def partitioning_keys(self) -> list[PartitionField]:
        return self._partition_keys

    def _iceberg_record_to_partition_spec(self, spec: IcebergPartitionSpec, record: Record) -> daft.table.Table | None:
        partition_fields = iceberg_partition_spec_to_fields(self._table.schema(), spec)
        arrays = dict()
        assert len(record._position_to_field_name) == len(partition_fields)
        for idx, pfield in enumerate(partition_fields):
            field = Field._from_pyfield(pfield.field)
            field_name = field.name
            field_dtype = field.dtype
            arrow_type = field_dtype.to_arrow_dtype()
            arrays[field_name] = daft.Series.from_arrow(pa.array([record[idx]], type=arrow_type), name=field_name).cast(
                field_dtype
            )
        if len(arrays) > 0:
            return daft.table.Table.from_pydict(arrays)
        else:
            return None

    def multiline_display(self) -> list[str]:
        return [
            self.display_name(),
            f"Schema = {self._schema}",
            f"Partitioning keys = {self.partitioning_keys}",
            # TODO(Clark): Improve repr of storage config here.
            f"Storage config = {self._storage_config}",
        ]

    def to_scan_tasks(self, pushdowns: Pushdowns) -> Iterator[ScanTask]:
        limit = pushdowns.limit
        iceberg_tasks = self._table.scan(limit=limit, snapshot_id=self._snapshot_id).plan_files()

        limit_files = limit is not None and pushdowns.filters is None and pushdowns.partition_filters is None

        if len(self.partitioning_keys()) > 0 and pushdowns.partition_filters is None:
            logging.warning(
                "%s has Partitioning Keys: %s but no partition filter was specified. This will result in a full table scan.",
                self.display_name(),
                self.partitioning_keys(),
            )
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
                file_format_config = FileFormatConfig.from_parquet_config(
                    ParquetSourceConfig(field_id_mapping=self._field_id_mapping)
                )
            else:
                # TODO: Support ORC and AVRO when we can read it
                raise NotImplementedError(f"{file_format} for iceberg not implemented!")

            iceberg_delete_files = [f.file_path for f in task.delete_files]

            # TODO: Thread in Statistics to each ScanTask: P2
            pspec = self._iceberg_record_to_partition_spec(self._table.specs()[file.spec_id], file.partition)
            st = ScanTask.catalog_scan_task(
                file=path,
                file_format=file_format_config,
                schema=self._schema._schema,
                num_rows=record_count,
                storage_config=self._storage_config,
                size_bytes=file.file_size_in_bytes,
                iceberg_delete_files=iceberg_delete_files,
                pushdowns=pushdowns,
                partition_values=pspec._table if pspec is not None else None,
                stats=None,
            )
            if st is None:
                continue
            rows_left -= record_count
            scan_tasks.append(st)
        return iter(scan_tasks)

    def can_absorb_filter(self) -> bool:
        return False

    def can_absorb_limit(self) -> bool:
        return False

    def can_absorb_select(self) -> bool:
        return True
