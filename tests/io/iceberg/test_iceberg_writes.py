from __future__ import annotations

import datetime
import decimal
from pathlib import Path
from unittest.mock import patch
from urllib.parse import unquote, urlparse

import pyarrow as pa
import pyarrow.fs as pafs
import pytest

from daft.daft import ScanTask
from daft.filesystem import _resolve_paths_and_filesystem
from daft.io import IOConfig
from tests.conftest import get_tests_daft_runner_name

pyiceberg = pytest.importorskip("pyiceberg")

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    TruncateTransform,
    YearTransform,
)
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
)
from pyiceberg.types import NestedField as _NestedField

import daft
from daft.io.writer import IcebergWriter
from daft.recordbatch.micropartition import MicroPartition


@pytest.fixture(scope="function", params=[False, True], ids=["no_mock", "with_mock"])
def patch_scan_task_file_path_scheme(request):
    """Fixture to patch ScanTask.catalog_scan_task to use file:/ scheme."""
    use_mock = request.param

    if use_mock:
        original_catalog_scan_task = ScanTask.catalog_scan_task

        def patched_catalog_scan_task(file: str, **kwargs):
            normalized_file = file.replace("file:///", "file:/")
            return original_catalog_scan_task(file=normalized_file, **kwargs)

        with patch("daft.daft.ScanTask.catalog_scan_task", side_effect=patched_catalog_scan_task):
            yield
    else:
        yield


@pytest.fixture(scope="function")
def local_catalog(tmpdir):
    catalog = SqlCatalog(
        "default",
        uri=f"sqlite:///{tmpdir}/pyiceberg_catalog.db",
        warehouse=f"file://{tmpdir}",
    )
    catalog.create_namespace("default")
    yield catalog
    catalog.engine.dispose()


def test_read_iceberg_accepts_pathlike_metadata(local_catalog, patch_scan_task_file_path_scheme):
    table = local_catalog.create_table(
        "default.pathlike_metadata",
        Schema(NestedField(field_id=1, name="x", type=LongType())),
    )

    df = daft.from_pydict({"x": [1, 2, 3]})
    df.write_iceberg(table)
    table.refresh()

    parsed = urlparse(table.metadata_location)
    assert parsed.scheme == "file"

    metadata_path = Path(unquote(parsed.path))
    read_back = daft.read_iceberg(metadata_path)

    assert df.to_arrow() == read_back.to_arrow().sort_by("x")


@pytest.fixture(
    scope="function",
    params=[
        pytest.param((UNPARTITIONED_PARTITION_SPEC, 1), id="unpartitioned"),
        pytest.param(
            (
                PartitionSpec(
                    PartitionField(
                        source_id=1, field_id=1000, transform=IdentityTransform(), name="x_identity_partitioned"
                    )
                ),
                5,
            ),
            id="identity_partitioned",
        ),
        pytest.param(
            (
                PartitionSpec(
                    PartitionField(
                        source_id=1, field_id=1000, transform=BucketTransform(4), name="x_bucket_partitioned"
                    )
                ),
                3,
            ),
            id="bucket_partitioned",
        ),
        pytest.param(
            (
                PartitionSpec(
                    PartitionField(
                        source_id=1, field_id=1000, transform=TruncateTransform(2), name="x_truncate_partitioned"
                    )
                ),
                3,
            ),
            id="truncate_partitioned",
        ),
    ],
)
def simple_local_table(request, local_catalog):
    partition_spec, num_partitions = request.param

    schema = Schema(
        NestedField(field_id=1, name="x", type=LongType()),
    )

    table = local_catalog.create_table("default.test", schema, partition_spec=partition_spec)
    return table, num_partitions


def test_read_after_write_append(simple_local_table):
    table, num_partitions = simple_local_table

    df = daft.from_pydict({"x": [1, 2, 3, 4, 5]})
    as_arrow = df.to_arrow()
    result = df.write_iceberg(table)
    as_dict = result.to_pydict()
    assert all(op == "ADD" for op in as_dict["operation"]), as_dict["operation"]
    assert sum(as_dict["rows"]) == 5, as_dict["rows"]
    assert len(as_dict["operation"]) == num_partitions
    read_back = daft.read_iceberg(table)
    assert as_arrow == read_back.to_arrow().sort_by("x")


def test_read_after_write_overwrite(simple_local_table):
    table, num_partitions = simple_local_table

    df = daft.from_pydict({"x": [1, 2, 3, 4, 5]})
    as_arrow = df.to_arrow()
    result = df.write_iceberg(table)
    as_dict = result.to_pydict()
    assert all(op == "ADD" for op in as_dict["operation"]), as_dict["operation"]
    assert sum(as_dict["rows"]) == 5, as_dict["rows"]
    assert len(as_dict["operation"]) == num_partitions

    # write again (in append)
    result = df.write_iceberg(table)
    as_dict = result.to_pydict()
    assert all(op == "ADD" for op in as_dict["operation"]), as_dict["operation"]
    assert sum(as_dict["rows"]) == 5, as_dict["rows"]
    assert len(as_dict["operation"]) == num_partitions

    read_back = daft.read_iceberg(table)
    assert pa.concat_tables([as_arrow, as_arrow]).sort_by("x") == read_back.to_arrow().sort_by("x")

    # write again (in overwrite)
    result = df.write_iceberg(table, mode="overwrite")
    as_dict = result.to_pydict()
    assert len(as_dict["operation"]) == 3 * num_partitions
    assert all(op == "ADD" for op in as_dict["operation"][:num_partitions]), as_dict["operation"][:num_partitions]
    assert sum(as_dict["rows"][:num_partitions]) == 5, as_dict["rows"][:num_partitions]
    assert all(op == "DELETE" for op in as_dict["operation"][num_partitions:]), as_dict["operation"][num_partitions:]
    assert sum(as_dict["rows"][num_partitions : 2 * num_partitions]) == 5, as_dict["rows"][
        num_partitions : 2 * num_partitions
    ]
    assert sum(as_dict["rows"][2 * num_partitions :]) == 5, as_dict["rows"][2 * num_partitions :]

    read_back = daft.read_iceberg(table)
    assert as_arrow == read_back.to_arrow().sort_by("x")


def test_read_and_overwrite(simple_local_table):
    table, num_partitions = simple_local_table

    df = daft.from_pydict({"x": [1, 2, 3, 4, 5]})
    result = df.write_iceberg(table)
    as_dict = result.to_pydict()
    assert all(op == "ADD" for op in as_dict["operation"]), as_dict["operation"]
    assert sum(as_dict["rows"]) == 5, as_dict["rows"]
    assert len(as_dict["operation"]) == num_partitions

    df = daft.from_pydict({"x": [1, 1, 1, 1, 1]})
    result = df.write_iceberg(table, mode="overwrite")
    as_dict = result.to_pydict()
    assert len(as_dict["operation"]) == num_partitions + 1
    assert as_dict["operation"][0] == "ADD"
    assert as_dict["rows"][0] == 5
    assert all(op == "DELETE" for op in as_dict["operation"][1:]), as_dict["operation"][1:]
    assert sum(as_dict["rows"][1:]) == 5, as_dict["rows"][1:]

    read_back = daft.read_iceberg(table)
    assert df.to_arrow() == read_back.to_arrow().sort_by("x")


def _get_snapshot_property(table, key: str) -> str | None:
    table.refresh()
    current_snapshot = table.current_snapshot()
    assert current_snapshot is not None

    val = current_snapshot.summary.get(key)
    return str(val) if val is not None else None


def test_write_append_with_snapshot_properties(simple_local_table):
    table, _ = simple_local_table

    snapshot_properties = {"mypath.myproperty": "my-property-value"}
    df = daft.from_pydict({"x": [1, 2, 3]})
    df.write_iceberg(table, mode="append", snapshot_properties=snapshot_properties)

    assert _get_snapshot_property(table, "mypath.myproperty") == "my-property-value"


def test_write_overwrite_with_snapshot_properties(simple_local_table):
    table, _ = simple_local_table

    first_df = daft.from_pydict({"x": [1, 2, 3]})
    first_df.write_iceberg(table, mode="append")
    assert _get_snapshot_property(table, "mypath.myproperty") is None

    snapshot_properties = {"mypath.myproperty": "my-property-value"}
    overwrite_df = daft.from_pydict({"x": [4, 5, 6]})
    overwrite_df.write_iceberg(table, mode="overwrite", snapshot_properties=snapshot_properties)

    assert _get_snapshot_property(table, "mypath.myproperty") == "my-property-value"


def test_missing_columns_write(simple_local_table):
    table, _ = simple_local_table

    df = daft.from_pydict({"x": [1, 2, 3, 4, 5]})

    df = daft.from_pydict({"y": [1, 2, 3, 4, 5]})
    result = df.write_iceberg(table)
    as_dict = result.to_pydict()
    assert as_dict["operation"] == ["ADD"]
    assert as_dict["rows"] == [5]
    read_back = daft.read_iceberg(table)
    assert read_back.to_pydict() == {"x": [None] * 5}


def test_too_many_columns_write(simple_local_table):
    table, num_partitions = simple_local_table

    df = daft.from_pydict({"x": [1, 2, 3, 4, 5]})
    as_arrow = df.to_arrow()

    df = daft.from_pydict({"x": [1, 2, 3, 4, 5], "y": [6, 7, 8, 9, 10]})
    result = df.write_iceberg(table)
    as_dict = result.to_pydict()
    assert len(as_dict["operation"]) == num_partitions
    assert all(op == "ADD" for op in as_dict["operation"]), as_dict["operation"]
    assert sum(as_dict["rows"]) == 5, as_dict["rows"]
    read_back = daft.read_iceberg(table)
    assert as_arrow == read_back.to_arrow().sort_by("x")


def test_read_after_write_nested_fields(local_catalog):
    # We need to cast Large Types such as LargeList and LargeString to the i32 variants
    df = daft.from_pydict({"x": [["a", "b"], ["c", "d", "e"]]})
    as_arrow = df.to_arrow()
    table = local_catalog.create_table("default.test", as_arrow.schema)
    result = df.write_iceberg(table)
    as_dict = result.to_pydict()
    assert as_dict["operation"] == ["ADD"]
    assert as_dict["rows"] == [2]
    read_back = daft.read_iceberg(table)
    assert as_arrow == read_back.to_arrow()


@pytest.mark.skipif(
    get_tests_daft_runner_name() == "native",
    reason="Native executor does not support into_partitions",
)
def test_read_after_write_with_empty_partition(local_catalog):
    df = daft.from_pydict({"x": [1, 2, 3]}).into_partitions(4)
    as_arrow = df.to_arrow()
    table = local_catalog.create_table("default.test", as_arrow.schema)
    result = df.write_iceberg(table)
    as_dict = result.to_pydict()
    assert as_dict["operation"] == ["ADD", "ADD", "ADD"]
    assert as_dict["rows"] == [1, 1, 1]
    read_back = daft.read_iceberg(table)
    assert as_arrow == read_back.to_arrow()


def test_iceberg_writer_empty_micropartition(tmp_path):
    iceberg_schema = Schema(_NestedField(field_id=1, name="x", field_type=LongType(), required=False))
    empty = MicroPartition.from_arrow(pa.table({"x": pa.array([], type=pa.int64())}))
    writer = IcebergWriter(
        root_dir=str(tmp_path),
        file_idx=0,
        schema=iceberg_schema,
        properties={},
        partition_spec_id=UNPARTITIONED_PARTITION_SPEC.spec_id,
    )
    bytes_written = writer.write(empty)
    assert bytes_written == 0
    # Must not IndexError on metadata_collector[0] when no file was opened.
    result = writer.close()
    assert len(result) == 0
    assert "data_file" in result.schema().column_names()
    assert not (tmp_path / writer.file_name).exists()


@pytest.fixture
def complex_table() -> tuple[pa.Table, Schema]:
    table = pa.table(
        {
            "int": [1, 2, 3],
            "float": [1.1, 2.2, 3.3],
            "string": ["foo", "bar", "baz"],
            "binary": [b"foo", b"bar", b"baz"],
            "boolean": [True, False, True],
            "timestamp": [
                datetime.datetime(2024, 2, 10, 12, 1, 40),
                datetime.datetime(2024, 2, 11, 12, 2, 41),
                datetime.datetime(2024, 2, 12, 12, 3, 42),
            ],
            "date": [datetime.date(2024, 2, 10), datetime.date(2024, 2, 11), datetime.date(2024, 2, 12)],
            "decimal": pa.array(
                [decimal.Decimal("1234.567"), decimal.Decimal("1233.456"), decimal.Decimal("1232.345")],
                type=pa.decimal128(7, 3),
            ),
            "list": [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
            "struct": [{"x": 1, "y": False}, {"y": True, "z": "foo"}, {"x": 5, "z": "bar"}],
            "map": pa.array(
                [[("x", 1), ("y", 0)], [("a", 2), ("b", 45)], [("c", 4), ("d", 18)]],
                type=pa.map_(pa.string(), pa.int64()),
            ),
        }
    )

    schema = Schema(
        NestedField(field_id=1, name="int", type=LongType()),
        NestedField(field_id=2, name="float", type=DoubleType()),
        NestedField(field_id=3, name="string", type=StringType()),
        NestedField(field_id=4, name="binary", type=BinaryType()),
        NestedField(field_id=5, name="boolean", type=BooleanType()),
        NestedField(field_id=6, name="timestamp", type=TimestampType()),
        NestedField(field_id=7, name="date", type=DateType()),
        NestedField(field_id=8, name="decimal", type=DecimalType(7, 3)),
        NestedField(field_id=9, name="list", type=ListType(element_id=20, element=LongType())),
        NestedField(
            field_id=10,
            name="struct",
            type=StructType(
                NestedField(field_id=11, name="x", type=LongType()),
                NestedField(field_id=12, name="y", type=BooleanType()),
                NestedField(field_id=13, name="z", type=StringType()),
            ),
        ),
        NestedField(
            field_id=14,
            name="map",
            type=MapType(key_id=21, key_type=StringType(), value_id=22, value_type=LongType()),
        ),
    )

    return table, schema


@pytest.mark.parametrize(
    "partition_spec,num_partitions",
    [
        pytest.param(UNPARTITIONED_PARTITION_SPEC, 1, id="unpartitioned"),
        pytest.param(
            PartitionSpec(
                PartitionField(
                    source_id=1, field_id=1000, transform=IdentityTransform(), name="int_identity_partitioned"
                )
            ),
            3,
            id="int_identity_partitioned",
        ),
        pytest.param(
            PartitionSpec(
                PartitionField(source_id=1, field_id=1000, transform=BucketTransform(2), name="int_bucket_partitioned")
            ),
            2,
            id="int_bucket_partitioned",
        ),
        pytest.param(
            PartitionSpec(
                PartitionField(
                    source_id=1, field_id=1000, transform=TruncateTransform(2), name="int_truncate_partitioned"
                )
            ),
            2,
            id="int_truncate_partitioned",
        ),
        pytest.param(
            PartitionSpec(
                PartitionField(
                    source_id=2, field_id=1000, transform=IdentityTransform(), name="float_identity_partitioned"
                )
            ),
            3,
            id="float_identity_partitioned",
        ),
        pytest.param(
            PartitionSpec(
                PartitionField(
                    source_id=3, field_id=1000, transform=IdentityTransform(), name="string_identity_partitioned"
                )
            ),
            3,
            id="string_identity_partitioned",
        ),
        pytest.param(
            PartitionSpec(
                PartitionField(
                    source_id=3, field_id=1000, transform=BucketTransform(2), name="string_bucket_partitioned"
                )
            ),
            2,
            id="string_bucket_partitioned",
        ),
        pytest.param(
            PartitionSpec(
                PartitionField(
                    source_id=3, field_id=1000, transform=TruncateTransform(2), name="string_truncate_partitioned"
                )
            ),
            2,
            id="string_truncate_partitioned",
        ),
        pytest.param(
            PartitionSpec(
                PartitionField(
                    source_id=4, field_id=1000, transform=IdentityTransform(), name="binary_identity_partitioned"
                )
            ),
            3,
            id="binary_identity_partitioned",
        ),
        pytest.param(
            PartitionSpec(
                PartitionField(
                    source_id=4, field_id=1000, transform=BucketTransform(2), name="binary_bucket_partitioned"
                )
            ),
            2,
            id="binary_bucket_partitioned",
        ),
        pytest.param(
            PartitionSpec(
                PartitionField(
                    source_id=4, field_id=1000, transform=TruncateTransform(2), name="binary_truncate_partitioned"
                )
            ),
            2,
            id="binary_truncate_partitioned",
        ),
        pytest.param(
            PartitionSpec(
                PartitionField(
                    source_id=5, field_id=1000, transform=IdentityTransform(), name="bool_identity_partitioned"
                )
            ),
            2,
            id="bool_identity_partitioned",
        ),
        pytest.param(
            PartitionSpec(
                PartitionField(
                    source_id=6, field_id=1000, transform=IdentityTransform(), name="datetime_identity_partitioned"
                )
            ),
            3,
            id="datetime_identity_partitioned",
        ),
        pytest.param(
            PartitionSpec(
                PartitionField(
                    source_id=6, field_id=1000, transform=BucketTransform(2), name="datetime_bucket_partitioned"
                )
            ),
            2,
            id="datetime_bucket_partitioned",
        ),
        pytest.param(
            PartitionSpec(
                PartitionField(source_id=6, field_id=1000, transform=YearTransform(), name="datetime_year_partitioned")
            ),
            1,
            id="datetime_year_partitioned",
        ),
        pytest.param(
            PartitionSpec(
                PartitionField(
                    source_id=6, field_id=1000, transform=MonthTransform(), name="datetime_month_partitioned"
                )
            ),
            1,
            id="datetime_month_partitioned",
        ),
        pytest.param(
            PartitionSpec(
                PartitionField(source_id=6, field_id=1000, transform=DayTransform(), name="datetime_day_partitioned")
            ),
            3,
            id="datetime_day_partitioned",
        ),
        pytest.param(
            PartitionSpec(
                PartitionField(source_id=6, field_id=1000, transform=HourTransform(), name="datetime_hour_partitioned")
            ),
            3,
            id="datetime_hour_partitioned",
        ),
        pytest.param(
            PartitionSpec(
                PartitionField(
                    source_id=7, field_id=1000, transform=IdentityTransform(), name="date_identity_partitioned"
                )
            ),
            3,
            id="date_identity_partitioned",
        ),
        pytest.param(
            PartitionSpec(
                PartitionField(source_id=7, field_id=1000, transform=BucketTransform(2), name="date_bucket_partitioned")
            ),
            2,
            id="date_bucket_partitioned",
        ),
        pytest.param(
            PartitionSpec(
                PartitionField(source_id=7, field_id=1000, transform=YearTransform(), name="date_year_partitioned")
            ),
            1,
            id="date_year_partitioned",
        ),
        pytest.param(
            PartitionSpec(
                PartitionField(source_id=7, field_id=1000, transform=MonthTransform(), name="date_month_partitioned")
            ),
            1,
            id="date_month_partitioned",
        ),
        pytest.param(
            PartitionSpec(
                PartitionField(source_id=7, field_id=1000, transform=DayTransform(), name="date_day_partitioned")
            ),
            3,
            id="date_day_partitioned",
        ),
        pytest.param(
            PartitionSpec(
                PartitionField(
                    source_id=8, field_id=1000, transform=IdentityTransform(), name="decimal_identity_partitioned"
                )
            ),
            3,
            id="decimal_identity_partitioned",
        ),
        pytest.param(
            PartitionSpec(
                PartitionField(
                    source_id=8, field_id=1000, transform=BucketTransform(2), name="decimal_bucket_partitioned"
                )
            ),
            1,
            id="decimal_bucket_partitioned",
        ),
        pytest.param(
            PartitionSpec(
                PartitionField(
                    source_id=8,
                    field_id=1000,
                    transform=TruncateTransform(2),
                    name="decimal_truncate_partitioned",
                )
            ),
            3,
            id="decimal_truncate_partitioned",
        ),
        pytest.param(
            PartitionSpec(
                PartitionField(source_id=1, field_id=1000, transform=BucketTransform(2), name="int_bucket_partitioned"),
                PartitionField(
                    source_id=3, field_id=1000, transform=TruncateTransform(2), name="string_truncate_partitioned"
                ),
            ),
            3,
            id="double_partitioned",
        ),
    ],
)
def test_complex_table_write_read(
    local_catalog, complex_table, partition_spec, num_partitions, patch_scan_task_file_path_scheme
):
    pa_table, schema = complex_table
    table = local_catalog.create_table("default.test", schema, partition_spec=partition_spec)
    df = daft.from_arrow(pa_table)
    result = df.write_iceberg(table)
    as_dict = result.to_pydict()
    assert len(as_dict["operation"]) == num_partitions
    assert all(op == "ADD" for op in as_dict["operation"]), as_dict["operation"]
    assert sum(as_dict["rows"]) == 3, as_dict["rows"]
    read_back = daft.read_iceberg(table)
    assert df.to_arrow() == read_back.to_arrow().sort_by("int")


# ---------------------------------------------------------------------------
# Protocol-alias tests
#
# These tests verify that when io_config.protocol_aliases maps a custom URI
# scheme to a known one (e.g. "myscheme" -> "file"), Daft:
#   1. Can write Iceberg data files through that alias.
#   2. Records the ORIGINAL custom scheme (not the alias target) in the
#      DataFile.file_path entries that end up in manifest avro files.
#   3. Can read the written data back through the same alias.
#
# This is the fix for the "foo://" class of bugs where a downstream
# system uses a custom S3-compatible URI scheme but manifest avro entries
# were recording the underlying scheme (s3://) instead.
# ---------------------------------------------------------------------------

CUSTOM_SCHEME = "myscheme"


@pytest.fixture
def catalog(tmp_path):
    """SqlCatalog backed by local storage with a separate directory for the aliased write root."""
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    catalog = SqlCatalog(
        "default",
        uri=f"sqlite:///{tmp_path}/pyiceberg_catalog.db",
        warehouse=f"file://{warehouse}",
    )
    catalog.create_namespace("default")
    yield catalog
    catalog.engine.dispose()


def _make_alias_io_config() -> IOConfig:
    """Return an IOConfig that maps CUSTOM_SCHEME -> file."""
    return IOConfig(protocol_aliases={CUSTOM_SCHEME: "file"})


def _table_location_with_custom_scheme(table) -> str:
    """Return the table's real file:// location rewritten with CUSTOM_SCHEME."""
    return str(table.location()).replace("file://", f"{CUSTOM_SCHEME}://", 1)


@pytest.mark.parametrize(
    "partition_spec",
    [
        pytest.param(UNPARTITIONED_PARTITION_SPEC, id="unpartitioned"),
        pytest.param(
            PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="x_identity")),
            id="identity_partitioned",
        ),
        pytest.param(
            PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=BucketTransform(2), name="x_bucket")),
            id="bucket_partitioned",
        ),
    ],
)
def test_protocol_alias_scheme_preserved_in_manifest_paths(catalog, partition_spec):
    """DataFile.file_path entries in Iceberg manifest avro files must use the original custom URI scheme.

    Regression test for the bug where tables with a custom S3-compatible scheme
    (e.g. foo://) would have manifest avro file_path entries recorded with
    the underlying scheme (s3://) instead.
    """
    schema = Schema(NestedField(field_id=1, name="x", type=LongType()))
    table = catalog.create_table("default.test", schema, partition_spec=partition_spec)

    # Override write.data.path to use the custom scheme while keeping the
    # physical location the same (the alias maps it back to file://).
    custom_data_path = _table_location_with_custom_scheme(table) + "/data"
    with table.transaction() as tx:
        tx.set_properties(**{"write.data.path": custom_data_path})
    table.refresh()

    io_config = _make_alias_io_config()
    df = daft.from_pydict({"x": [1, 2, 3, 4, 5]})
    result = df.write_iceberg(table, io_config=io_config)
    result_dict = result.to_pydict()

    assert result_dict["operation"] == ["ADD"] * len(result_dict["operation"])
    assert sum(result_dict["rows"]) == 5

    # The written file paths (== DataFile.file_path, written into the manifest
    # avro) must use the CUSTOM scheme, not "file".
    for file_name in result_dict["file_name"]:
        assert file_name.startswith(f"{CUSTOM_SCHEME}://"), (
            f"Expected manifest entry to use '{CUSTOM_SCHEME}://' scheme but got: {file_name!r}"
        )

    # Round-trip: Daft must be able to read the data back using the same alias.
    read_back = daft.read_iceberg(table, io_config=io_config)
    assert df.to_arrow() == read_back.to_arrow().sort_by("x")

    # Verify all file_path metadata with pyiceberg
    for file_path in table.inspect.files()["file_path"]:
        assert file_path.as_py().startswith(f"{CUSTOM_SCHEME}://"), (
            f"Expected manifest entry to use '{CUSTOM_SCHEME}://' scheme but got: {file_path!r}"
        )


def test_protocol_alias_resolves_to_local_filesystem(tmp_path):
    """A custom scheme aliased to file:// resolves to LocalFileSystem with bare local paths."""
    target_dir = tmp_path / "data"
    custom_path = f"{CUSTOM_SCHEME}://{str(target_dir).lstrip('/')}"
    io_config = _make_alias_io_config()

    resolved_paths, fs = _resolve_paths_and_filesystem(custom_path, io_config=io_config)

    assert isinstance(fs, pafs.LocalFileSystem), f"Expected LocalFileSystem, got {type(fs)}"
    # The resolved path is a bare local path -- no scheme prefix.
    assert not any(p.startswith(f"{CUSTOM_SCHEME}://") for p in resolved_paths)
    assert not any(p.startswith("file://") for p in resolved_paths)


def test_protocol_alias_unknown_scheme_raises_without_alias():
    """Without a matching protocol alias, Daft raises NotImplementedError for an unknown URI scheme."""
    with pytest.raises(NotImplementedError, match="Cannot infer PyArrow filesystem"):
        _resolve_paths_and_filesystem("unknownscheme://bucket/path")


@pytest.fixture(scope="function")
def dt_partitioned_table(local_catalog):
    """A table partitioned by identity(dt), pre-loaded with two partitions."""
    schema = Schema(
        NestedField(field_id=1, name="dt", type=StringType(), required=False),
        NestedField(field_id=2, name="x", type=LongType(), required=False),
    )
    partition_spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="dt"))
    table = local_catalog.create_table("default.overwrite_filter", schema, partition_spec=partition_spec)

    daft.from_pydict({"dt": ["2024-01-01", "2024-01-01", "2024-01-02"], "x": [1, 2, 3]}).write_iceberg(table)
    table.refresh()
    return table


def _rows(table):
    table.refresh()
    as_dict = daft.read_iceberg(table).to_pydict()
    return sorted(zip(as_dict["dt"], as_dict["x"]))


@pytest.mark.parametrize("filter_kind", ["string", "daft_expression", "iceberg_expression"])
def test_overwrite_filter_replaces_only_matching_partition(dt_partitioned_table, filter_kind):
    from pyiceberg.expressions import EqualTo

    table = dt_partitioned_table
    overwrite_filter = {
        "string": "dt = '2024-01-01'",
        "daft_expression": daft.col("dt") == "2024-01-01",
        "iceberg_expression": EqualTo("dt", "2024-01-01"),
    }[filter_kind]

    daft.from_pydict({"dt": ["2024-01-01"], "x": [99]}).write_iceberg(
        table, mode="overwrite", overwrite_filter=overwrite_filter
    )

    # Only the 2024-01-01 partition is replaced; 2024-01-02 survives untouched.
    assert _rows(table) == [("2024-01-01", 99), ("2024-01-02", 3)]


def test_overwrite_filter_reports_only_matching_deletes(dt_partitioned_table):
    table = dt_partitioned_table

    result = daft.from_pydict({"dt": ["2024-01-01"], "x": [99]}).write_iceberg(
        table, mode="overwrite", overwrite_filter="dt = '2024-01-01'"
    )
    as_dict = result.to_pydict()

    # One ADD for the new file, one DELETE for the single file in the matched partition.
    # The 2024-01-02 data file must not be reported as deleted.
    assert as_dict["operation"] == ["ADD", "DELETE"]
    assert as_dict["rows"] == [1, 2]
    assert all("dt=2024-01-01" in file_name for file_name in as_dict["file_name"]), as_dict["file_name"]


def test_overwrite_without_filter_still_replaces_whole_table(dt_partitioned_table):
    table = dt_partitioned_table

    daft.from_pydict({"dt": ["2024-01-03"], "x": [7]}).write_iceberg(table, mode="overwrite")

    assert _rows(table) == [("2024-01-03", 7)]


def test_overwrite_filter_matching_nothing_only_appends(dt_partitioned_table):
    table = dt_partitioned_table

    daft.from_pydict({"dt": ["2024-01-03"], "x": [7]}).write_iceberg(
        table, mode="overwrite", overwrite_filter="dt = '2024-01-03'"
    )

    assert _rows(table) == [("2024-01-01", 1), ("2024-01-01", 2), ("2024-01-02", 3), ("2024-01-03", 7)]


def test_overwrite_filter_below_partition_granularity_rewrites_survivors(dt_partitioned_table):
    """A filter finer than the partition boundary falls back to copy-on-write."""
    table = dt_partitioned_table

    # x = 99 falls outside the `x = 1` filter, so the write needs the check disabled.
    daft.from_pydict({"dt": ["2024-01-01"], "x": [99]}).write_iceberg(
        table, mode="overwrite", overwrite_filter="x = 1", validate_overwrite_filter=False
    )

    # Only the x = 1 row is dropped; x = 2 is rewritten into a new file and kept.
    assert _rows(table) == [("2024-01-01", 2), ("2024-01-01", 99), ("2024-01-02", 3)]


def test_overwrite_filter_rejected_in_append_mode(dt_partitioned_table):
    table = dt_partitioned_table
    snapshots_before = len(table.metadata.snapshots)

    with pytest.raises(ValueError, match="only supported with mode='overwrite'"):
        daft.from_pydict({"dt": ["2024-01-01"], "x": [99]}).write_iceberg(
            table, mode="append", overwrite_filter="dt = '2024-01-01'"
        )

    table.refresh()
    assert len(table.metadata.snapshots) == snapshots_before


def test_overwrite_filter_unknown_column_fails_before_writing(dt_partitioned_table):
    table = dt_partitioned_table
    snapshots_before = len(table.metadata.snapshots)

    with pytest.raises(ValueError, match="Could not find field with name nope"):
        daft.from_pydict({"dt": ["2024-01-01"], "x": [99]}).write_iceberg(
            table, mode="overwrite", overwrite_filter="nope = 1"
        )

    # The predicate is validated before the write executes, so the table is unchanged.
    table.refresh()
    assert len(table.metadata.snapshots) == snapshots_before
    assert _rows(table) == [("2024-01-01", 1), ("2024-01-01", 2), ("2024-01-02", 3)]


def test_overwrite_filter_unconvertible_expression_raises(dt_partitioned_table):
    """An expression Iceberg cannot represent raises instead of widening to a full overwrite."""
    table = dt_partitioned_table

    with pytest.raises(ValueError, match="Iceberg does not support function"):
        daft.from_pydict({"dt": ["2024-01-01"], "x": [99]}).write_iceberg(
            table, mode="overwrite", overwrite_filter=(daft.col("x") + 1) > 3
        )

    assert _rows(table) == [("2024-01-01", 1), ("2024-01-01", 2), ("2024-01-02", 3)]


def test_overwrite_filter_bad_type_raises(dt_partitioned_table):
    table = dt_partitioned_table

    with pytest.raises(TypeError, match="overwrite_filter must be"):
        daft.from_pydict({"dt": ["2024-01-01"], "x": [99]}).write_iceberg(table, mode="overwrite", overwrite_filter=123)


def test_catalog_table_overwrite_accepts_overwrite_filter(dt_partitioned_table):
    from daft.catalog import Table

    table = dt_partitioned_table
    catalog_table = Table.from_iceberg(table)

    catalog_table.overwrite(daft.from_pydict({"dt": ["2024-01-01"], "x": [99]}), overwrite_filter="dt = '2024-01-01'")

    assert _rows(table) == [("2024-01-01", 99), ("2024-01-02", 3)]


def test_catalog_table_write_dispatches_overwrite_filter(dt_partitioned_table):
    """`Table.write` is what `Catalog.write_table` forwards to, so options must survive the dispatch."""
    from daft.catalog import Table

    table = dt_partitioned_table
    catalog_table = Table.from_iceberg(table)

    catalog_table.write(
        daft.from_pydict({"dt": ["2024-01-01"], "x": [99]}),
        mode="overwrite",
        overwrite_filter="dt = '2024-01-01'",
    )

    assert _rows(table) == [("2024-01-01", 99), ("2024-01-02", 3)]


def test_catalog_table_append_rejects_overwrite_filter(dt_partitioned_table):
    from daft.catalog import Table

    catalog_table = Table.from_iceberg(dt_partitioned_table)

    with pytest.raises(ValueError, match="Unsupported option"):
        catalog_table.append(daft.from_pydict({"dt": ["2024-01-01"], "x": [99]}), overwrite_filter="dt = '2024-01-01'")


def test_overwrite_filter_rejects_rows_outside_filter(dt_partitioned_table):
    table = dt_partitioned_table
    snapshots_before = len(table.metadata.snapshots)

    # The 2024-01-02 row falls outside the filter, so the delete would not cover it.
    with pytest.raises(ValueError, match="Cannot write rows that do not match overwrite_filter"):
        daft.from_pydict({"dt": ["2024-01-01", "2024-01-02"], "x": [99, 100]}).write_iceberg(
            table, mode="overwrite", overwrite_filter="dt = '2024-01-01'"
        )

    # Nothing was committed, since validation runs before the transaction opens.
    table.refresh()
    assert len(table.metadata.snapshots) == snapshots_before
    assert _rows(table) == [("2024-01-01", 1), ("2024-01-01", 2), ("2024-01-02", 3)]


def test_overwrite_filter_validation_can_be_disabled(dt_partitioned_table):
    table = dt_partitioned_table

    daft.from_pydict({"dt": ["2024-01-01", "2024-01-02"], "x": [99, 100]}).write_iceberg(
        table, mode="overwrite", overwrite_filter="dt = '2024-01-01'", validate_overwrite_filter=False
    )

    # The straddling row lands in a partition the delete never touched.
    assert _rows(table) == [("2024-01-01", 99), ("2024-01-02", 3), ("2024-01-02", 100)]


def test_overwrite_filter_validation_accepts_non_partition_predicate(dt_partitioned_table):
    """A predicate over a non-partition column is proven from the file's column statistics."""
    table = dt_partitioned_table

    daft.from_pydict({"dt": ["2024-01-01", "2024-01-02"], "x": [1, 1]}).write_iceberg(
        table, mode="overwrite", overwrite_filter="x = 1"
    )

    assert _rows(table) == [("2024-01-01", 1), ("2024-01-01", 2), ("2024-01-02", 1), ("2024-01-02", 3)]


def test_overwrite_filter_validation_rejects_non_partition_violation(dt_partitioned_table):
    table = dt_partitioned_table

    with pytest.raises(ValueError, match="Cannot write rows that do not match overwrite_filter"):
        daft.from_pydict({"dt": ["2024-01-01", "2024-01-01"], "x": [1, 5]}).write_iceberg(
            table, mode="overwrite", overwrite_filter="x = 1"
        )


def test_overwrite_filter_validation_accepts_range_over_partitions(dt_partitioned_table):
    """A range predicate spanning several partitions accepts files in every one of them."""
    table = dt_partitioned_table

    daft.from_pydict({"dt": ["2024-01-01", "2024-01-02"], "x": [99, 100]}).write_iceberg(
        table, mode="overwrite", overwrite_filter="dt >= '2024-01-01' and dt <= '2024-01-02'"
    )

    assert _rows(table) == [("2024-01-01", 99), ("2024-01-02", 100)]


def test_overwrite_filter_validation_with_hidden_day_partitioning(local_catalog):
    """Hidden partitioning: the predicate is over `ts`, but the table is partitioned by day(ts)."""
    schema = Schema(
        NestedField(field_id=1, name="ts", type=TimestampType(), required=False),
        NestedField(field_id=2, name="x", type=LongType(), required=False),
    )
    partition_spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=DayTransform(), name="ts_day"))
    table = local_catalog.create_table("default.hidden_day", schema, partition_spec=partition_spec)

    day_one = datetime.datetime(2024, 1, 1, 6, 0, 0)
    day_two = datetime.datetime(2024, 1, 2, 6, 0, 0)
    daft.from_pydict({"ts": [day_one, day_two], "x": [1, 2]}).write_iceberg(table)

    daft.from_pydict({"ts": [day_one], "x": [99]}).write_iceberg(
        table,
        mode="overwrite",
        overwrite_filter="ts >= '2024-01-01T00:00:00' and ts < '2024-01-02T00:00:00'",
    )

    table.refresh()
    as_dict = daft.read_iceberg(table).to_pydict()
    assert sorted(zip(as_dict["ts"], as_dict["x"])) == [(day_one, 99), (day_two, 2)]


def test_overwrite_filter_validation_on_unpartitioned_table(local_catalog):
    """With no partition spec to project onto, the check falls back to column statistics."""
    schema = Schema(
        NestedField(field_id=1, name="dt", type=StringType(), required=False),
        NestedField(field_id=2, name="x", type=LongType(), required=False),
    )
    table = local_catalog.create_table("default.unpartitioned", schema, partition_spec=UNPARTITIONED_PARTITION_SPEC)
    daft.from_pydict({"dt": ["2024-01-01", "2024-01-02"], "x": [1, 2]}).write_iceberg(table)

    with pytest.raises(ValueError, match="Cannot write rows that do not match overwrite_filter"):
        daft.from_pydict({"dt": ["2024-01-02"], "x": [99]}).write_iceberg(
            table, mode="overwrite", overwrite_filter="dt = '2024-01-01'"
        )

    daft.from_pydict({"dt": ["2024-01-01"], "x": [99]}).write_iceberg(
        table, mode="overwrite", overwrite_filter="dt = '2024-01-01'"
    )
    assert _rows(table) == [("2024-01-01", 99), ("2024-01-02", 2)]


def test_catalog_table_overwrite_forwards_validate_flag(dt_partitioned_table):
    from daft.catalog import Table

    table = dt_partitioned_table
    catalog_table = Table.from_iceberg(table)
    straddling = daft.from_pydict({"dt": ["2024-01-01", "2024-01-02"], "x": [99, 100]})

    with pytest.raises(ValueError, match="Cannot write rows that do not match overwrite_filter"):
        catalog_table.overwrite(straddling, overwrite_filter="dt = '2024-01-01'")

    catalog_table.overwrite(straddling, overwrite_filter="dt = '2024-01-01'", validate_overwrite_filter=False)

    assert _rows(table) == [("2024-01-01", 99), ("2024-01-02", 3), ("2024-01-02", 100)]


def test_overwrite_filter_validation_rejects_null_partition_values(dt_partitioned_table):
    """A null partition value does not satisfy `dt = ...` and must be rejected."""
    table = dt_partitioned_table

    with pytest.raises(ValueError, match="Cannot write rows that do not match overwrite_filter"):
        daft.from_pydict({"dt": [None], "x": [99]}).write_iceberg(
            table, mode="overwrite", overwrite_filter="dt = '2024-01-01'"
        )


def test_overwrite_filter_with_empty_dataframe_deletes_the_partition(dt_partitioned_table):
    """An empty DataFrame plus a filter drops a partition without writing anything."""
    table = dt_partitioned_table
    empty = daft.from_pydict({"dt": ["2024-01-01"], "x": [1]}).limit(0)

    result = empty.write_iceberg(table, mode="overwrite", overwrite_filter="dt = '2024-01-01'")

    assert result.to_pydict()["operation"] == ["DELETE"]
    assert _rows(table) == [("2024-01-02", 3)]


def test_write_result_reports_partition_values(local_catalog):
    """Regression: reported partition values used to be all-null.

    `data_file.partition` is a positional `Record`. pyiceberg 0.9 dropped the attribute
    access this code relied on, so every reported value became None.
    """
    schema = Schema(
        NestedField(field_id=1, name="dt", type=StringType(), required=False),
        NestedField(field_id=2, name="x", type=LongType(), required=False),
    )
    partition_spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="dt"))
    table = local_catalog.create_table("default.reported_partitions", schema, partition_spec=partition_spec)

    result = daft.from_pydict({"dt": ["2024-01-01", "2024-01-01", "2024-01-02"], "x": [1, 2, 3]}).write_iceberg(table)
    as_dict = result.to_pydict()

    assert sorted(row["dt"] for row in as_dict["partitioning"]) == ["2024-01-01", "2024-01-02"]


def test_overwrite_result_reports_partition_values_for_deleted_files(dt_partitioned_table):
    """DELETE rows describe files read back from a manifest rather than files just written."""
    table = dt_partitioned_table

    result = daft.from_pydict({"dt": ["2024-01-03"], "x": [9]}).write_iceberg(table, mode="overwrite")
    as_dict = result.to_pydict()

    reported = sorted(zip(as_dict["operation"], (row["dt"] for row in as_dict["partitioning"])))
    assert reported == [
        ("ADD", "2024-01-03"),
        ("DELETE", "2024-01-01"),
        ("DELETE", "2024-01-02"),
    ]


def test_write_result_keys_partitions_by_partition_field_name(local_catalog):
    """Non-identity transforms: the key is the partition field, not the source column."""
    schema = Schema(
        NestedField(field_id=1, name="dt", type=StringType(), required=False),
        NestedField(field_id=2, name="x", type=LongType(), required=False),
    )
    partition_spec = PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=BucketTransform(4), name="x_bucket_4")
    )
    table = local_catalog.create_table("default.bucketed_partitions", schema, partition_spec=partition_spec)

    values = [1, 2, 3, 4]
    result = daft.from_pydict({"dt": ["a"] * len(values), "x": values}).write_iceberg(table)
    reported = result.to_pydict()["partitioning"]

    # Keyed by the Iceberg partition field, matching the read side and the directory layout.
    assert all(set(row.keys()) == {"x_bucket_4"} for row in reported), reported

    bucket_of = BucketTransform(4).transform(LongType())
    assert sorted(row["x_bucket_4"] for row in reported) == sorted({bucket_of(v) for v in values})


def test_write_result_reports_every_field_of_a_multi_field_spec(local_catalog):
    """Two partition fields over one source column must not collapse into a single key."""
    schema = Schema(
        NestedField(field_id=1, name="dt", type=StringType(), required=False),
        NestedField(field_id=2, name="x", type=LongType(), required=False),
    )
    partition_spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="dt"),
        PartitionField(source_id=1, field_id=1001, transform=TruncateTransform(4), name="dt_year"),
    )
    table = local_catalog.create_table("default.multi_field_partitions", schema, partition_spec=partition_spec)

    result = daft.from_pydict({"dt": ["2024-01-01", "2025-02-02"], "x": [1, 2]}).write_iceberg(table)
    reported = sorted(result.to_pydict()["partitioning"], key=lambda row: row["dt"])

    assert reported == [
        {"dt": "2024-01-01", "dt_year": "2024"},
        {"dt": "2025-02-02", "dt_year": "2025"},
    ]


def test_write_result_has_no_partitioning_column_when_unpartitioned(local_catalog):
    schema = Schema(NestedField(field_id=1, name="x", type=LongType(), required=False))
    table = local_catalog.create_table("default.no_partitions", schema, partition_spec=UNPARTITIONED_PARTITION_SPEC)

    result = daft.from_pydict({"x": [1, 2, 3]}).write_iceberg(table)

    assert "partitioning" not in result.schema().column_names()


@pytest.fixture(scope="function")
def bounds_table_factory(local_catalog):
    """Builds identity(dt)-partitioned tables carrying a non-partition string column."""

    def _make(name, properties=None):
        schema = Schema(
            NestedField(field_id=1, name="dt", type=StringType(), required=False),
            NestedField(field_id=2, name="s", type=StringType(), required=False),
        )
        partition_spec = PartitionSpec(
            PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="dt")
        )
        table = local_catalog.create_table(
            f"default.{name}", schema, partition_spec=partition_spec, properties=properties or {}
        )
        daft.from_pydict({"dt": ["seed"], "s": ["seed"]}).write_iceberg(table)
        return table

    return _make


# Longer than Iceberg's default 16-byte bound truncation, and sharing a prefix so that
# truncated bounds cannot tell these two apart.
LONG_VALUE = "L" * 30 + "tail"
LONG_SIBLING = "L" * 30 + "other"


def test_validation_accepts_long_partition_values(bounds_table_factory):
    """Partition values are stored whole, so length does not defeat the partition-level check.

    Column bounds are truncated and partition values are not, which is why
    `validate_data_files_match_filter` needs both tests.
    """
    table = bounds_table_factory("long_partition_value")

    daft.from_pydict({"dt": [LONG_VALUE], "s": ["x"]}).write_iceberg(
        table, mode="overwrite", overwrite_filter=f"dt = '{LONG_VALUE}'"
    )

    assert sorted(daft.read_iceberg(table).to_pydict()["dt"]) == sorted(["seed", LONG_VALUE])


def test_validation_never_accepts_a_violating_write_despite_truncated_bounds(bounds_table_factory):
    """Truncation widens bounds, which can only make the proof harder, never easier.

    The written value shares its first 16 bytes with the one in the filter, so truncated
    bounds cannot distinguish them. The write must still be refused.
    """
    table = bounds_table_factory("truncation_safety")

    with pytest.raises(ValueError, match="Cannot write rows that do not match overwrite_filter"):
        daft.from_pydict({"dt": ["seed"], "s": [LONG_SIBLING]}).write_iceberg(
            table, mode="overwrite", overwrite_filter=f"s = '{LONG_VALUE}'"
        )


def test_validation_conservatively_rejects_equality_beyond_truncated_bounds(bounds_table_factory):
    """Known limitation: equality on a long non-partition value cannot be proven.

    The rows do satisfy the filter, but truncated bounds cannot show it, so the check
    refuses. Iceberg's own `StrictMetricsEvaluator` behaves the same way. If this becomes
    provable, update this test rather than widening the check.
    """
    table = bounds_table_factory("truncated_bounds")
    matching = daft.from_pydict({"dt": ["seed"], "s": [LONG_VALUE]})

    with pytest.raises(ValueError, match="Cannot write rows that do not match overwrite_filter"):
        matching.write_iceberg(table, mode="overwrite", overwrite_filter=f"s = '{LONG_VALUE}'")

    # A range predicate over the same rows stays provable: the bounds sit inside it.
    matching.write_iceberg(table, mode="overwrite", overwrite_filter="s >= 'L' and s < 'M'")
    assert sorted(daft.read_iceberg(table).to_pydict()["s"]) == sorted(["seed", LONG_VALUE])


def test_validation_rejects_when_column_metrics_are_disabled(bounds_table_factory):
    """Same conservative path when a table turns off statistics for the filtered column."""
    table = bounds_table_factory("metrics_off", {"write.metadata.metrics.column.s": "none"})

    with pytest.raises(ValueError, match="Cannot write rows that do not match overwrite_filter"):
        daft.from_pydict({"dt": ["seed"], "s": ["short"]}).write_iceberg(
            table, mode="overwrite", overwrite_filter="s = 'short'"
        )

    # Opting out of the check is the documented way through.
    daft.from_pydict({"dt": ["seed"], "s": ["short"]}).write_iceberg(
        table, mode="overwrite", overwrite_filter="s = 'short'", validate_overwrite_filter=False
    )
