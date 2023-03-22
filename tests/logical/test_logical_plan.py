from __future__ import annotations

import pytest

from daft.datatype import DataType
from daft.expressions import ExpressionsProjection, col
from daft.logical.logical_plan import Filter, InMemoryScan, Projection
from daft.logical.schema import Schema
from daft.runners.partitioning import PartitionCacheEntry


@pytest.fixture(scope="function")
def schema():
    return Schema._from_field_name_and_types(
        list(
            map(
                lambda col_name: (col_name, DataType.int64()),
                ["a", "b", "c"],
            )
        )
    )


def test_projection_logical_plan(schema) -> None:
    scan = InMemoryScan(cache_entry=PartitionCacheEntry("", None), schema=schema)
    assert scan.schema() == schema

    full_project = Projection(scan, ExpressionsProjection([col("a"), col("b"), col("c")]), custom_resource_request=None)

    assert full_project.schema().column_names() == ["a", "b", "c"]

    project = Projection(scan, ExpressionsProjection([col("b")]), custom_resource_request=None)
    assert project.schema().column_names() == ["b"]

    project = Projection(scan, ExpressionsProjection([col("b") * 2]), custom_resource_request=None)

    assert project.schema().column_names() == ["b"]


def test_projection_logical_plan_bad_input(schema) -> None:
    scan = InMemoryScan(cache_entry=PartitionCacheEntry("", None), schema=schema)
    assert scan.schema() == schema

    with pytest.raises(ValueError):
        Projection(scan, ExpressionsProjection([col("d")]), custom_resource_request=None)


def test_filter_logical_plan(schema) -> None:
    scan = InMemoryScan(cache_entry=PartitionCacheEntry("", None), schema=schema)
    assert scan.schema() == schema

    full_filter = Filter(scan, ExpressionsProjection([col("a") == 1, col("b") < 10, col("c") > 10]))
    assert full_filter.schema() == schema

    project = Filter(scan, ExpressionsProjection([col("b") < 10]))
    assert project.schema() == schema


def test_filter_logical_plan_bad_input(schema) -> None:
    scan = InMemoryScan(cache_entry=PartitionCacheEntry("", None), schema=schema)
    assert scan.schema() == schema

    with pytest.raises(ValueError):
        filter = Filter(scan, ExpressionsProjection([col("d") == 1]))


def test_projection_new_columns_logical_plan(schema) -> None:
    scan = InMemoryScan(cache_entry=PartitionCacheEntry("", None), schema=schema)
    assert scan.schema() == schema

    Projection(
        scan,
        ExpressionsProjection.from_schema(schema).union(ExpressionsProjection([(col("a") + col("b")).alias("d")])),
        custom_resource_request=None,
    )
    projection = Projection(scan, ExpressionsProjection([col("b")]), custom_resource_request=None)
    proj_schema = projection.schema()
    hstacked_on_proj = Projection(
        projection,
        ExpressionsProjection.from_schema(proj_schema).union(
            ExpressionsProjection([(col("b") + 1).alias("a"), (col("b") + 2).alias("c")])
        ),
        custom_resource_request=None,
    )

    assert hstacked_on_proj.schema().column_names() == ["b", "a", "c"]

    projection_reorder = Projection(
        hstacked_on_proj, ExpressionsProjection([col("a"), col("b"), col("c")]), custom_resource_request=None
    )
    assert projection_reorder.schema().column_names() == ["a", "b", "c"]


def test_filter_logical_plan_bad_input(schema) -> None:
    scan = InMemoryScan(cache_entry=PartitionCacheEntry("", None), schema=schema)
    assert scan.schema() == schema

    with pytest.raises(ValueError):
        scan = Projection(
            scan, ExpressionsProjection([col("a"), (col("b") + 1).alias("a")]), custom_resource_request=None
        )


def test_scan_projection_filter_projection_chain(schema) -> None:
    scan = InMemoryScan(cache_entry=PartitionCacheEntry("", None), schema=schema)
    assert scan.schema() == schema

    hstacked = Projection(
        scan,
        ExpressionsProjection.from_schema(schema).union(ExpressionsProjection([(col("a") + col("b")).alias("d")])),
        custom_resource_request=None,
    )
    assert hstacked.schema().column_names() == ["a", "b", "c", "d"]

    filtered = Filter(hstacked, ExpressionsProjection([col("d") < 20, col("a") > 10]))
    assert filtered.schema().column_names() == ["a", "b", "c", "d"]
    assert filtered.schema() == filtered.schema()

    projection_alias = Projection(
        filtered, ExpressionsProjection([col("b").alias("out"), col("d")]), custom_resource_request=None
    )

    projection = Projection(projection_alias, ExpressionsProjection([col("out")]), custom_resource_request=None)

    assert projection.schema().column_names() == ["out"]
