"""Partition fields exposed to the optimizer depend on live snapshot metadata."""

from __future__ import annotations

from unittest.mock import Mock

import pytest

pyiceberg = pytest.importorskip("pyiceberg")

from pyiceberg.manifest import ManifestContent, ManifestFile
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import BucketTransform, IdentityTransform, UnknownTransform, VoidTransform
from pyiceberg.typedef import Record
from pyiceberg.types import LongType, NestedField

from daft.daft import StorageConfig
from daft.io import IOConfig
from daft.io.iceberg.iceberg_scan import IcebergDataSource


def _manifest(spec_id, content=ManifestContent.DATA, added=1, existing=0):
    return ManifestFile.from_args(
        partition_spec_id=spec_id,
        content=content,
        added_files_count=added,
        existing_files_count=existing,
    )


@pytest.fixture
def partitioned_table():
    schema = Schema(NestedField(1, "x", LongType(), required=False))
    spec = PartitionSpec(PartitionField(1, 1000, IdentityTransform(), "x"), spec_id=1)
    table = Mock()
    table.schema.return_value = schema
    table.scan.return_value.projection.return_value = schema
    table.spec.return_value = spec
    table.specs.return_value = {0: PartitionSpec(spec_id=0), 1: spec}
    table.current_snapshot.return_value.manifests.return_value = [_manifest(1)]
    return table


def _source(table, snapshot_id=None):
    return IcebergDataSource(table, snapshot_id, StorageConfig(multithreaded_io=False, io_config=IOConfig()))


@pytest.mark.parametrize("content,added,existing", [(ManifestContent.DATA, 0, 0), (ManifestContent.DELETES, 1, 0)])
def test_irrelevant_manifests_do_not_require_partition_specs(partitioned_table, content, added, existing):
    # Neither a deleted-only data manifest nor a delete manifest can contribute live data files.
    partitioned_table.current_snapshot.return_value.manifests.return_value = [
        _manifest(99, content, added, existing),
        _manifest(1),
    ]

    assert [field.field.name for field in _source(partitioned_table).get_partition_fields()] == ["x"]


@pytest.mark.parametrize("added,existing", [(None, 0), (0, None), (None, None), (0, 1)])
def test_manifests_with_possible_live_files_restrict_partition_fields(partitioned_table, added, existing):
    partitioned_table.current_snapshot.return_value.manifests.return_value = [
        _manifest(1),
        _manifest(0, added=added, existing=existing),
    ]

    assert _source(partitioned_table).get_partition_fields() == []


def test_missing_live_partition_spec_disables_partition_fields(partitioned_table):
    partitioned_table.current_snapshot.return_value.manifests.return_value = [_manifest(99)]

    assert _source(partitioned_table).get_partition_fields() == []


def test_partition_metadata_failure_is_retried_and_success_cached(partitioned_table):
    manifests = partitioned_table.current_snapshot.return_value.manifests
    manifests.side_effect = [OSError("temporary manifest read failure"), [_manifest(1)]]
    source = _source(partitioned_table)

    assert source.get_partition_fields() == []
    assert [field.field.name for field in source.get_partition_fields()] == ["x"]
    assert [field.field.name for field in source.get_partition_fields()] == ["x"]
    assert manifests.call_count == 2


def test_empty_shared_partition_fields_are_cached(partitioned_table):
    manifests = partitioned_table.current_snapshot.return_value.manifests
    manifests.return_value = [_manifest(1), _manifest(0)]
    source = _source(partitioned_table)

    assert source.get_partition_fields() == []
    assert source.get_partition_fields() == []
    manifests.assert_called_once_with(partitioned_table.io)


def test_unpartitioned_table_does_not_read_manifests(partitioned_table):
    partitioned_table.spec.return_value = PartitionSpec(spec_id=0)

    assert _source(partitioned_table).get_partition_fields() == []
    partitioned_table.current_snapshot.assert_not_called()


def test_explicit_snapshot_determines_live_partition_fields(partitioned_table):
    partitioned_table.snapshot_by_id.return_value.manifests.return_value = [_manifest(0)]

    assert _source(partitioned_table, snapshot_id=123).get_partition_fields() == []
    partitioned_table.snapshot_by_id.assert_called_once_with(123)
    partitioned_table.current_snapshot.assert_not_called()


@pytest.mark.parametrize("has_snapshot", [False, True])
def test_no_live_manifests_preserve_current_partition_fields(partitioned_table, has_snapshot):
    if has_snapshot:
        partitioned_table.current_snapshot.return_value.manifests.return_value = []
    else:
        partitioned_table.current_snapshot.return_value = None

    assert [field.field.name for field in _source(partitioned_table).get_partition_fields()] == ["x"]


def test_unsupported_partition_transform_is_not_exposed_as_identity(partitioned_table):
    spec = PartitionSpec(
        PartitionField(1, 1000, UnknownTransform("unsupported"), "unsupported_x"),
        PartitionField(1, 1001, IdentityTransform(), "x"),
        spec_id=1,
    )
    partitioned_table.spec.return_value = spec
    partitioned_table.specs.return_value[1] = spec

    with pytest.warns(UserWarning, match="not implemented"):
        source = _source(partitioned_table)

    assert [field.field.name for field in source.get_partition_fields()] == ["x"]


def test_void_field_preserves_record_positions_without_filling_null_constants(partitioned_table):
    spec = PartitionSpec(
        PartitionField(1, 1000, VoidTransform(), "removed_x"),
        PartitionField(1, 1001, IdentityTransform(), "x"),
        spec_id=1,
    )
    partitioned_table.spec.return_value = spec
    partitioned_table.specs.return_value[1] = spec
    source = _source(partitioned_table)

    assert [field.field.name for field in source.get_partition_fields()] == ["x"]
    pruning_values, identity_values = source._iceberg_record_to_partition_values(spec, Record(None, 42))
    assert pruning_values.to_pydict() == {"x": [42]}
    assert identity_values.to_pydict() == {"x": [42]}


def test_derived_partition_name_cannot_overwrite_identity_column(partitioned_table):
    partitioned_table.schema.return_value = Schema(
        NestedField(1, "a", LongType(), required=False),
        NestedField(2, "b", LongType(), required=False),
    )
    spec = PartitionSpec(
        PartitionField(1, 1000, IdentityTransform(), "a_part"),
        PartitionField(2, 1001, BucketTransform(8), "a"),
        spec_id=1,
    )
    partitioned_table.spec.return_value = spec
    partitioned_table.specs.return_value[1] = spec
    source = _source(partitioned_table)

    assert [field.field.name for field in source.get_partition_fields()] == ["a"]
    pruning_values, identity_values = source._iceberg_record_to_partition_values(spec, Record(42, 3))
    assert pruning_values.to_pydict() == {"a": [42]}
    assert identity_values.to_pydict() == {"a": [42]}
