from __future__ import annotations

from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.partitioning import PartitionField as IcebergPartitionField
from pyiceberg.partitioning import PartitionSpec as IcebergPartitionSpec
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.table import TableMetadata as IcebergTableMetadata
from pyiceberg.types import IcebergType
from pyiceberg.transforms import (
    BucketTransform as IcebergBucketTransform,
    DayTransform as IcebergDayTransform,
    HourTransform as IcebergHourTransform,
    IdentityTransform as IcebergIdentityTransform,
    MonthTransform as IcebergMonthTransform,
    Transform as IcebergTransform,
    TruncateTransform as IcebergTruncateTransform,
    YearTransform as IcebergYearTransform,
)

from daft.io.partitioning import PartitionField, PartitionTransform
from daft.schema import DataType, Field, Schema



def convert_iceberg_schema(schema: IcebergSchema) -> Schema:
    """Converts a PyIceberg schema to a Daft schema."""
    return Schema.from_pyarrow_schema(schema_to_pyarrow(schema))


def resolve_iceberg_schema(metadata: IcebergTableMetadata, snapshot_id: int | None) -> IcebergSchema:
    """Resolves the IcebergSchema at the given snapshot ID or the current schema if no snapshot ID is provided."""
    if snapshot_id is None:
        return metadata.schema()
    if snapshot := metadata.snapshot_by_id(snapshot_id):
        if snapshot.schema_id is not None:
            return metadata.schema_by_id(snapshot.schema_id) or metadata.schema()
    return metadata.schema()


def convert_iceberg_partition_spec(schema: IcebergSchema, spec: IcebergPartitionSpec) -> list[PartitionField]:
    """Converts a PyIceberg partition spec to a Daft partition spec."""
    return [convert_iceberg_partition_field(schema, field) for field in spec.fields]


def convert_iceberg_partition_field(schema: IcebergSchema, field: IcebergPartitionField) -> PartitionField:
    """Converts a PyIceberg partition field to a Daft partition field."""
    source_id = field.source_id
    source_field = schema.find_field(source_id)
    source_name = source_field.name
    source_type = convert_iceberg_data_type(source_field.field_type)
    transform, result_type = convert_iceberg_transform(field.transform, source_type)
    return PartitionField.create(
        Field.create(field.name, result_type),
        Field.create(source_name, source_type),
        transform,
    )


def convert_iceberg_transform(
    transform: IcebergTransform, source_type: DataType
) -> tuple[PartitionTransform, DataType]:
    """Converts a PyIceberg transform to a Daft partition transform."""
    if isinstance(transform, IcebergIdentityTransform):
        return PartitionTransform.identity(), source_type
    elif isinstance(transform, IcebergYearTransform):
        return PartitionTransform.year(), DataType.int32()
    elif isinstance(transform, IcebergMonthTransform):
        return PartitionTransform.month(), DataType.int32()
    elif isinstance(transform, IcebergDayTransform):
        return PartitionTransform.day(), DataType.date()
    elif isinstance(transform, IcebergHourTransform):
        return PartitionTransform.hour(), DataType.int32()
    elif isinstance(transform, IcebergBucketTransform):
        return PartitionTransform.iceberg_bucket(transform.num_buckets), DataType.int32()
    elif isinstance(transform, IcebergTruncateTransform):
        return PartitionTransform.iceberg_truncate(transform.width), source_type
    else:
        raise NotImplementedError(f"Unsupported partition transform: {transform}")


def convert_iceberg_data_type(dtype: IcebergType) -> DataType:
    """Converts a PyIceberg data type to a Daft data type."""
    return DataType.from_arrow_type(schema_to_pyarrow(dtype))
