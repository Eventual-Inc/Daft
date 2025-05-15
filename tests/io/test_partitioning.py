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
    "transform_factory,expected_type,expected_params",
    [
        (PartitionTransform.identity, "identity", None),
        (PartitionTransform.year, "year", None),
        (PartitionTransform.month, "month", None),
        (PartitionTransform.day, "day", None),
        (PartitionTransform.hour, "hour", None),
        (PartitionTransform.iceberg_bucket, "iceberg_bucket", {"n": 10}),
        (PartitionTransform.iceberg_truncate, "iceberg_truncate", {"w": 10}),
    ],
)
def test_partition_transform_types(transform_factory, expected_type, expected_params):
    if expected_params:
        transform = transform_factory(**expected_params)
    else:
        transform = transform_factory()

    for transform_type in ["identity", "year", "month", "day", "hour", "iceberg_bucket", "iceberg_truncate"]:
        is_expected = transform_type == expected_type
        assert getattr(transform, f"is_{transform_type}")() == is_expected

    if expected_type == "iceberg_bucket":
        assert transform.get_iceberg_bucket_n() == expected_params["n"]

    if expected_type == "iceberg_truncate":
        assert transform.get_iceberg_truncate_w() == expected_params["w"]


def test_partition_transform_direct_initialization():
    with pytest.raises(ValueError, match="We do not support creating a PartitionTransform directly"):
        PartitionTransform()


def test_partition_transform_repr():
    transform = PartitionTransform.identity()
    assert repr(transform) == transform._partition_transform.__repr__()
