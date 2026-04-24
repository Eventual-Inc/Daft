from __future__ import annotations

from uuid import uuid4

import pytest

pyiceberg = pytest.importorskip("pyiceberg")

from pyiceberg.partitioning import PartitionField as IcebergPartitionField
from pyiceberg.partitioning import PartitionSpec as IcebergPartitionSpec
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.table.metadata import TableMetadataV2
from pyiceberg.table.snapshots import Snapshot
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    TruncateTransform,
    VoidTransform,
    YearTransform,
)
from pyiceberg.types import (
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)

from daft.datatype import DataType
from daft.io.iceberg._metadata import (
    convert_iceberg_data_type,
    convert_iceberg_partition_field,
    convert_iceberg_partition_spec,
    convert_iceberg_schema,
    convert_iceberg_transform,
    resolve_iceberg_schema,
)
from daft.io.partitioning import PartitionTransform


# -- convert_iceberg_data_type -------------------------------------------------


@pytest.mark.parametrize(
    "iceberg_type, expected",
    [
        (IntegerType(), DataType.int32()),
        (LongType(), DataType.int64()),
        (StringType(), DataType.string()),
        (BooleanType(), DataType.bool()),
        (FloatType(), DataType.float32()),
        (DoubleType(), DataType.float64()),
        (DateType(), DataType.date()),
        (TimestampType(), DataType.timestamp("us", None)),
    ],
    ids=["int", "long", "string", "bool", "float", "double", "date", "timestamp"],
)
def test_convert_iceberg_data_type(iceberg_type, expected):
    assert convert_iceberg_data_type(iceberg_type) == expected


# -- convert_iceberg_schema ----------------------------------------------------


def test_convert_iceberg_schema():
    schema = IcebergSchema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType(), required=False),
        NestedField(field_id=3, name="value", field_type=DoubleType(), required=False),
    )
    result = convert_iceberg_schema(schema)
    assert result.column_names() == ["id", "name", "value"]
    assert result["id"].dtype == DataType.int64()
    assert result["name"].dtype == DataType.string()
    assert result["value"].dtype == DataType.float64()


# -- convert_iceberg_transform -------------------------------------------------


@pytest.mark.parametrize(
    "transform, source_type, expected_transform, expected_dtype",
    [
        (IdentityTransform(), DataType.int64(), PartitionTransform.identity(), DataType.int64()),
        (IdentityTransform(), DataType.string(), PartitionTransform.identity(), DataType.string()),
        (YearTransform(), DataType.date(), PartitionTransform.year(), DataType.int32()),
        (MonthTransform(), DataType.date(), PartitionTransform.month(), DataType.int32()),
        (DayTransform(), DataType.timestamp("us", None), PartitionTransform.day(), DataType.date()),
        (HourTransform(), DataType.timestamp("us", None), PartitionTransform.hour(), DataType.int32()),
        (BucketTransform(16), DataType.int64(), PartitionTransform.iceberg_bucket(16), DataType.int32()),
        (TruncateTransform(10), DataType.string(), PartitionTransform.iceberg_truncate(10), DataType.string()),
        (TruncateTransform(5), DataType.int64(), PartitionTransform.iceberg_truncate(5), DataType.int64()),
    ],
    ids=[
        "identity-int",
        "identity-string",
        "year",
        "month",
        "day",
        "hour",
        "bucket-16",
        "truncate-string",
        "truncate-int",
    ],
)
def test_convert_iceberg_transform(transform, source_type, expected_transform, expected_dtype):
    result_transform, result_dtype = convert_iceberg_transform(transform, source_type)
    assert result_transform == expected_transform
    assert result_dtype == expected_dtype


def test_convert_iceberg_transform_unsupported():
    with pytest.raises(NotImplementedError, match="Unsupported partition transform"):
        convert_iceberg_transform(VoidTransform(), DataType.int64())


# -- convert_iceberg_partition_field -------------------------------------------


SIMPLE_SCHEMA = IcebergSchema(
    NestedField(field_id=1, name="id", field_type=LongType(), required=True),
    NestedField(field_id=2, name="ts", field_type=TimestampType(), required=False),
    NestedField(field_id=3, name="name", field_type=StringType(), required=False),
)


def test_convert_partition_field_identity():
    pf = convert_iceberg_partition_field(
        SIMPLE_SCHEMA,
        IcebergPartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="id_identity"),
    )
    assert pf.field.name == "id_identity"
    assert pf.field.dtype == DataType.int64()
    assert pf.source_field.name == "id"
    assert pf.source_field.dtype == DataType.int64()
    assert pf.transform == PartitionTransform.identity()


def test_convert_partition_field_bucket():
    pf = convert_iceberg_partition_field(
        SIMPLE_SCHEMA,
        IcebergPartitionField(source_id=1, field_id=1001, transform=BucketTransform(4), name="id_bucket"),
    )
    assert pf.field.name == "id_bucket"
    assert pf.field.dtype == DataType.int32()
    assert pf.transform == PartitionTransform.iceberg_bucket(4)


def test_convert_partition_field_day():
    pf = convert_iceberg_partition_field(
        SIMPLE_SCHEMA,
        IcebergPartitionField(source_id=2, field_id=1002, transform=DayTransform(), name="ts_day"),
    )
    assert pf.field.name == "ts_day"
    assert pf.field.dtype == DataType.date()
    assert pf.source_field.name == "ts"


# -- convert_iceberg_partition_spec --------------------------------------------


def test_convert_partition_spec_multiple_fields():
    spec = IcebergPartitionSpec(
        IcebergPartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="id_identity"),
        IcebergPartitionField(source_id=2, field_id=1001, transform=DayTransform(), name="ts_day"),
        spec_id=0,
    )
    result = convert_iceberg_partition_spec(SIMPLE_SCHEMA, spec)
    assert len(result) == 2
    assert result[0].field.name == "id_identity"
    assert result[1].field.name == "ts_day"


def test_convert_partition_spec_empty():
    spec = IcebergPartitionSpec(spec_id=0)
    result = convert_iceberg_partition_spec(SIMPLE_SCHEMA, spec)
    assert result == []


# -- resolve_iceberg_schema ----------------------------------------------------


def _make_metadata(schemas, current_schema_id, snapshots=None):
    return TableMetadataV2(
        format_version=2,
        table_uuid=uuid4(),
        location="s3://bucket/table",
        last_updated_ms=0,
        last_column_id=max(f.field_id for s in schemas for f in s.fields),
        schemas=schemas,
        current_schema_id=current_schema_id,
        partition_specs=[IcebergPartitionSpec(spec_id=0)],
        default_spec_id=0,
        last_partition_id=0,
        snapshots=snapshots or [],
        sort_orders=[],
        default_sort_order_id=0,
        last_sequence_number=0,
    )


SCHEMA_V0 = IcebergSchema(
    NestedField(field_id=1, name="id", field_type=LongType(), required=True),
    schema_id=0,
)

SCHEMA_V1 = IcebergSchema(
    NestedField(field_id=1, name="id", field_type=LongType(), required=True),
    NestedField(field_id=2, name="name", field_type=StringType(), required=False),
    schema_id=1,
)


def test_resolve_schema_no_snapshot():
    metadata = _make_metadata([SCHEMA_V0, SCHEMA_V1], current_schema_id=0)
    result = resolve_iceberg_schema(metadata, None)
    assert len(result.fields) == 1
    assert result.find_field(1).name == "id"


def test_resolve_schema_with_snapshot():
    snap = Snapshot(
        snapshot_id=100,
        parent_snapshot_id=None,
        sequence_number=1,
        timestamp_ms=0,
        manifest_list="s3://bucket/manifests/snap-100.avro",
        schema_id=1,
        summary={"operation": "append"},
    )
    metadata = _make_metadata([SCHEMA_V0, SCHEMA_V1], current_schema_id=0, snapshots=[snap])
    result = resolve_iceberg_schema(metadata, 100)
    assert len(result.fields) == 2
    assert result.find_field(2).name == "name"


def test_resolve_schema_missing_snapshot_falls_back():
    metadata = _make_metadata([SCHEMA_V0, SCHEMA_V1], current_schema_id=0)
    result = resolve_iceberg_schema(metadata, 9999)
    assert len(result.fields) == 1
