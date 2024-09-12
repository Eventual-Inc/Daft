from __future__ import annotations

import datetime
import decimal

import pyarrow as pa
import pytest

pyiceberg = pytest.importorskip("pyiceberg")

PYARROW_LE_8_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) < (8, 0, 0)
pytestmark = pytest.mark.skipif(PYARROW_LE_8_0_0, reason="iceberg only supported if pyarrow >= 8.0.0")


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

import daft


@pytest.fixture(scope="function")
def local_catalog(tmpdir):
    catalog = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///{tmpdir}/pyiceberg_catalog.db",
            "warehouse": f"file://{tmpdir}",
        },
    )
    catalog.create_namespace("default")
    return catalog


@pytest.fixture(
    scope="function",
    params=[
        pytest.param(UNPARTITIONED_PARTITION_SPEC, id="unpartitioned"),
        pytest.param(
            PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="x")),
            id="identity_partitioned",
        ),
        pytest.param(
            PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=BucketTransform(4), name="x")),
            id="bucket_partitioned",
        ),
        pytest.param(
            PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=TruncateTransform(2), name="x")),
            id="truncate_partitioned",
        ),
    ],
)
def simple_local_table(request, local_catalog):
    schema = Schema(
        NestedField(field_id=1, name="x", type=LongType()),
    )

    table = local_catalog.create_table("default.test", schema, partition_spec=request.param)
    return table


def test_read_after_write_append(simple_local_table):
    df = daft.from_pydict({"x": [1, 2, 3, 4, 5]})
    as_arrow = df.to_arrow()
    result = df.write_iceberg(simple_local_table)
    as_dict = result.to_pydict()
    assert all(op == "ADD" for op in as_dict["operation"]), as_dict["operation"]
    assert sum(as_dict["rows"]) == 5, as_dict["rows"]
    read_back = daft.read_iceberg(simple_local_table)
    assert as_arrow == read_back.to_arrow().sort_by("x")


def test_read_after_write_overwrite(simple_local_table):
    df = daft.from_pydict({"x": [1, 2, 3, 4, 5]})
    as_arrow = df.to_arrow()
    result = df.write_iceberg(simple_local_table)
    as_dict = result.to_pydict()
    assert all(op == "ADD" for op in as_dict["operation"]), as_dict["operation"]
    assert sum(as_dict["rows"]) == 5, as_dict["rows"]
    num_adds_1 = len(as_dict["operation"])

    # write again (in append)
    result = df.write_iceberg(simple_local_table)
    as_dict = result.to_pydict()
    assert all(op == "ADD" for op in as_dict["operation"]), as_dict["operation"]
    assert sum(as_dict["rows"]) == 5, as_dict["rows"]
    num_adds_2 = len(as_dict["operation"])

    read_back = daft.read_iceberg(simple_local_table)
    assert pa.concat_tables([as_arrow, as_arrow]).sort_by("x") == read_back.to_arrow().sort_by("x")

    # write again (in overwrite)
    result = df.write_iceberg(simple_local_table, mode="overwrite")
    as_dict = result.to_pydict()
    total_num_adds = num_adds_1 + num_adds_2
    assert all(op == "ADD" for op in as_dict["operation"][:-total_num_adds]), as_dict["operation"][:-total_num_adds]
    assert sum(as_dict["rows"][:-total_num_adds]) == 5, as_dict["rows"][:-total_num_adds]
    assert all(op == "DELETE" for op in as_dict["operation"][-num_adds_1:]), as_dict["operation"][-num_adds_1:]
    assert sum(as_dict["rows"][-num_adds_1:]) == 5, as_dict["rows"][-num_adds_1:]
    assert all(op == "DELETE" for op in as_dict["operation"][-num_adds_2:]), as_dict["operation"][-num_adds_2:]
    assert sum(as_dict["rows"][-num_adds_2:]) == 5, as_dict["rows"][-num_adds_2:]

    read_back = daft.read_iceberg(simple_local_table)
    assert as_arrow == read_back.to_arrow().sort_by("x")


def test_read_and_overwrite(simple_local_table):
    df = daft.from_pydict({"x": [1, 2, 3, 4, 5]})
    result = df.write_iceberg(simple_local_table)
    as_dict = result.to_pydict()
    assert all(op == "ADD" for op in as_dict["operation"]), as_dict["operation"]
    assert sum(as_dict["rows"]) == 5, as_dict["rows"]
    num_adds = len(as_dict["operation"])

    df = daft.read_iceberg(simple_local_table).with_column("x", daft.col("x") + 1)
    result = df.write_iceberg(simple_local_table, mode="overwrite")
    as_dict = result.to_pydict()
    assert all(op == "ADD" for op in as_dict["operation"][:-num_adds]), as_dict["operation"][:-num_adds]
    assert sum(as_dict["rows"][:-num_adds]) == 5, as_dict["rows"][:-num_adds]
    assert all(op == "DELETE" for op in as_dict["operation"][-num_adds:]), as_dict["operation"][-num_adds:]
    assert sum(as_dict["rows"][-num_adds:]) == 5, as_dict["rows"][-num_adds:]

    read_back = daft.read_iceberg(simple_local_table)
    assert daft.from_pydict({"x": [2, 3, 4, 5, 6]}).to_arrow() == read_back.to_arrow().sort_by("x")


def test_missing_columns_write(simple_local_table):
    df = daft.from_pydict({"x": [1, 2, 3, 4, 5]})

    df = daft.from_pydict({"y": [1, 2, 3, 4, 5]})
    result = df.write_iceberg(simple_local_table)
    as_dict = result.to_pydict()
    assert all(op == "ADD" for op in as_dict["operation"]), as_dict["operation"]
    assert sum(as_dict["rows"]) == 5, as_dict["rows"]
    read_back = daft.read_iceberg(simple_local_table)
    print("as_dict", as_dict)
    assert read_back.to_pydict() == {"x": [None] * 5}


def test_too_many_columns_write(simple_local_table):
    df = daft.from_pydict({"x": [1, 2, 3, 4, 5]})
    as_arrow = df.to_arrow()

    df = daft.from_pydict({"x": [1, 2, 3, 4, 5], "y": [6, 7, 8, 9, 10]})
    result = df.write_iceberg(simple_local_table)
    as_dict = result.to_pydict()
    assert all(op == "ADD" for op in as_dict["operation"]), as_dict["operation"]
    assert sum(as_dict["rows"]) == 5, as_dict["rows"]
    read_back = daft.read_iceberg(simple_local_table)
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


@pytest.fixture
def complex_table() -> tuple[pa.Table, Schema]:
    table = pa.table(
        {
            "a": [1, 2, 3],
            "b": [1.1, 2.2, 3.3],
            "c": ["foo", "bar", "baz"],
            "d": [b"foo", b"bar", b"baz"],
            "e": [True, False, True],
            "f": [datetime.datetime(2024, 2, 10), datetime.datetime(2024, 2, 11), datetime.datetime(2024, 2, 12)],
            "g": [datetime.date(2024, 2, 10), datetime.date(2024, 2, 11), datetime.date(2024, 2, 12)],
            "h": pa.array(
                [decimal.Decimal("1234.567"), decimal.Decimal("1233.456"), decimal.Decimal("1232.345")],
                type=pa.decimal128(7, 3),
            ),
            "i": [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
            "j": [{"x": 1, "y": False}, {"y": True, "z": "foo"}, {"x": 5, "z": "bar"}],
            "k": pa.array(
                [[("x", 1), ("y", 0)], [("a", 2), ("b", 45)], [("c", 4), ("d", 18)]],
                type=pa.map_(pa.string(), pa.int64()),
            ),
        }
    )

    schema = Schema(
        NestedField(field_id=1, name="a", type=LongType()),
        NestedField(field_id=2, name="b", type=DoubleType()),
        NestedField(field_id=3, name="c", type=StringType()),
        NestedField(field_id=4, name="d", type=BinaryType()),
        NestedField(field_id=5, name="e", type=BooleanType()),
        NestedField(field_id=6, name="f", type=TimestampType()),
        NestedField(field_id=7, name="g", type=DateType()),
        NestedField(field_id=8, name="h", type=DecimalType(7, 3)),
        NestedField(field_id=9, name="i", type=ListType(element_id=20, element=LongType())),
        NestedField(
            field_id=10,
            name="j",
            type=StructType(
                NestedField(field_id=11, name="x", type=LongType()),
                NestedField(field_id=12, name="y", type=BooleanType()),
                NestedField(field_id=13, name="z", type=StringType()),
            ),
        ),
        NestedField(
            field_id=14,
            name="k",
            type=MapType(key_id=21, key_type=StringType(), value_id=22, value_type=LongType()),
        ),
    )

    return table, schema


@pytest.fixture(
    params=[
        pytest.param(UNPARTITIONED_PARTITION_SPEC, id="unpartitioned"),
        pytest.param(
            PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="a")),
            id="int_identity_partitioned",
        ),
        pytest.param(
            PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=BucketTransform(4), name="a")),
            id="int_bucket_partitioned",
        ),
        pytest.param(
            PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=TruncateTransform(2), name="a")),
            id="int_truncate_partitioned",
        ),
        pytest.param(
            PartitionSpec(PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="b")),
            id="float_identity_partitioned",
        ),
        pytest.param(
            PartitionSpec(PartitionField(source_id=3, field_id=1000, transform=IdentityTransform(), name="c")),
            id="string_identity_partitioned",
        ),
        pytest.param(
            PartitionSpec(PartitionField(source_id=3, field_id=1000, transform=BucketTransform(4), name="c")),
            id="string_bucket_partitioned",
        ),
        pytest.param(
            PartitionSpec(PartitionField(source_id=3, field_id=1000, transform=TruncateTransform(2), name="c")),
            id="string_truncate_partitioned",
        ),
        pytest.param(
            PartitionSpec(PartitionField(source_id=4, field_id=1000, transform=IdentityTransform(), name="d")),
            id="binary_identity_partitioned",
        ),
        pytest.param(
            PartitionSpec(PartitionField(source_id=4, field_id=1000, transform=BucketTransform(4), name="d")),
            id="binary_bucket_partitioned",
        ),
        pytest.param(
            PartitionSpec(PartitionField(source_id=4, field_id=1000, transform=TruncateTransform(2), name="d")),
            id="binary_truncate_partitioned",
        ),
        pytest.param(
            PartitionSpec(PartitionField(source_id=5, field_id=1000, transform=IdentityTransform(), name="e")),
            id="bool_identity_partitioned",
        ),
        pytest.param(
            PartitionSpec(PartitionField(source_id=6, field_id=1000, transform=IdentityTransform(), name="f")),
            id="datetime_identity_partitioned",
        ),
        pytest.param(
            PartitionSpec(PartitionField(source_id=6, field_id=1000, transform=BucketTransform(4), name="f")),
            id="datetime_bucket_partitioned",
        ),
        pytest.param(
            PartitionSpec(PartitionField(source_id=6, field_id=1000, transform=YearTransform(), name="f")),
            id="datetime_year_partitioned",
        ),
        pytest.param(
            PartitionSpec(PartitionField(source_id=6, field_id=1000, transform=MonthTransform(), name="f")),
            id="datetime_month_partitioned",
        ),
        pytest.param(
            PartitionSpec(PartitionField(source_id=6, field_id=1000, transform=DayTransform(), name="f")),
            id="datetime_day_partitioned",
        ),
        pytest.param(
            PartitionSpec(PartitionField(source_id=6, field_id=1000, transform=HourTransform(), name="f")),
            id="datetime_hour_partitioned",
        ),
        pytest.param(
            PartitionSpec(PartitionField(source_id=7, field_id=1000, transform=IdentityTransform(), name="g")),
            id="date_identity_partitioned",
        ),
        pytest.param(
            PartitionSpec(PartitionField(source_id=7, field_id=1000, transform=BucketTransform(4), name="g")),
            id="date_bucket_partitioned",
        ),
        pytest.param(
            PartitionSpec(PartitionField(source_id=7, field_id=1000, transform=YearTransform(), name="g")),
            id="date_year_partitioned",
        ),
        pytest.param(
            PartitionSpec(PartitionField(source_id=7, field_id=1000, transform=MonthTransform(), name="g")),
            id="date_month_partitioned",
        ),
        pytest.param(
            PartitionSpec(PartitionField(source_id=7, field_id=1000, transform=DayTransform(), name="g")),
            id="date_day_partitioned",
        ),
        pytest.param(
            PartitionSpec(PartitionField(source_id=8, field_id=1000, transform=IdentityTransform(), name="h")),
            id="decimal_identity_partitioned",
        ),
        pytest.param(
            PartitionSpec(PartitionField(source_id=8, field_id=1000, transform=BucketTransform(4), name="h")),
            id="decimal_bucket_partitioned",
        ),
        pytest.param(
            PartitionSpec(PartitionField(source_id=8, field_id=1000, transform=TruncateTransform(2), name="h")),
            id="decimal_truncate_partitioned",
        ),
    ]
)
def partition_spec(request) -> PartitionSpec:
    return request.param


def test_complex_table_write_read(local_catalog, complex_table, partition_spec):
    pa_table, schema = complex_table
    table = local_catalog.create_table("default.test", schema, partition_spec=partition_spec)
    df = daft.from_arrow(pa_table)
    result = df.write_iceberg(table)
    as_dict = result.to_pydict()
    assert all(op == "ADD" for op in as_dict["operation"]), as_dict["operation"]
    assert sum(as_dict["rows"]) == 3, as_dict["rows"]
    read_back = daft.read_iceberg(table)
    assert df.to_arrow() == read_back.to_arrow().sort_by("a")
