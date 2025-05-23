from __future__ import annotations

import pytest

from daft.io.partitioning import PartitionField, PartitionTransform
from daft.schema import DataType, Field


def test_partition_field():
    field = Field.create("test_field", DataType.int64())
    source_field = Field.create("source_field", DataType.int64())
    transform = PartitionTransform.identity()

    partition_field = PartitionField.create(field, source_field, transform)
    assert partition_field.field == field
    assert partition_field.source_field == source_field
    assert partition_field.transform == transform

    partition_field = PartitionField.create(field)
    assert partition_field.field == field
    assert partition_field.source_field is None
    assert partition_field.transform is None


@pytest.mark.parametrize(
    "transform_factory,transform_type,transform_args",
    [
        (PartitionTransform.identity, "identity", None),
        (PartitionTransform.year, "year", None),
        (PartitionTransform.month, "month", None),
        (PartitionTransform.day, "day", None),
        (PartitionTransform.hour, "hour", None),
        (PartitionTransform.iceberg_bucket, "iceberg_bucket", [10]),
        (PartitionTransform.iceberg_truncate, "iceberg_truncate", [10]),
    ],
)
def test_partition_transform_types(transform_factory, transform_type, transform_args):
    if transform_args:
        transform = transform_factory(*transform_args)
    else:
        transform = transform_factory()

    for other_transform_type in ["identity", "year", "month", "day", "hour", "iceberg_bucket", "iceberg_truncate"]:
        is_other_transform = transform_type == other_transform_type
        assert getattr(transform, f"is_{other_transform_type}")() == is_other_transform

    if transform_type == "iceberg_bucket":
        assert transform.num_buckets == transform_args[0]

    if transform_type == "iceberg_truncate":
        assert transform.width == transform_args[0]


def test_partition_transform_direct_initialization():
    with pytest.raises(ValueError, match="We do not support creating a PartitionTransform directly"):
        PartitionTransform()


def test_partition_transform_repr():
    transform = PartitionTransform.identity()
    assert repr(transform) == transform._partition_transform.__repr__()
