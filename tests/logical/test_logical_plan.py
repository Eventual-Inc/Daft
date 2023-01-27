from __future__ import annotations

import pytest

from daft.expressions import ExpressionList, col
from daft.logical.field import Field
from daft.logical.logical_plan import Filter, InMemoryScan, Projection
from daft.logical.schema import Schema
from daft.runners.partitioning import PartitionCacheEntry
from daft.types import ExpressionType


@pytest.fixture(scope="function")
def schema():
    return Schema(
        list(
            map(
                lambda col_name: Field(col_name, ExpressionType.from_py_type(int)),
                ["a", "b", "c"],
            )
        )
    )


def test_projection_logical_plan(schema) -> None:
    scan = InMemoryScan(cache_entry=PartitionCacheEntry("", None), schema=schema)
    assert scan.schema() == schema

    full_project = Projection(scan, ExpressionList([col("a"), col("b"), col("c")]))

    assert full_project.schema().column_names() == ["a", "b", "c"]

    project = Projection(scan, ExpressionList([col("b")]))
    assert project.schema().column_names() == ["b"]

    project = Projection(scan, ExpressionList([col("b") * 2]))

    assert project.schema().column_names() == ["b"]


def test_projection_logical_plan_bad_input(schema) -> None:
    scan = InMemoryScan(cache_entry=PartitionCacheEntry("", None), schema=schema)
    assert scan.schema() == schema

    with pytest.raises(ValueError):
        Projection(scan, ExpressionList([col("d")]))


def test_filter_logical_plan(schema) -> None:
    scan = InMemoryScan(cache_entry=PartitionCacheEntry("", None), schema=schema)
    assert scan.schema() == schema

    full_filter = Filter(scan, ExpressionList([col("a") == 1, col("b") < 10, col("c") > 10]))
    assert full_filter.schema() == schema

    project = Filter(scan, ExpressionList([col("b") < 10]))
    assert project.schema() == schema


def test_filter_logical_plan_bad_input(schema) -> None:
    scan = InMemoryScan(cache_entry=PartitionCacheEntry("", None), schema=schema)
    assert scan.schema() == schema

    with pytest.raises(ValueError):
        filter = Filter(scan, ExpressionList([col("d") == 1]))


def test_projection_new_columns_logical_plan(schema) -> None:
    scan = InMemoryScan(cache_entry=PartitionCacheEntry("", None), schema=schema)
    assert scan.schema() == schema

    Projection(scan, schema.to_column_expressions().union(ExpressionList([(col("a") + col("b")).alias("d")])))
    projection = Projection(scan, ExpressionList([col("b")]))
    proj_schema = projection.schema()
    hstacked_on_proj = Projection(
        projection,
        proj_schema.to_column_expressions().union(
            ExpressionList([(col("b") + 1).alias("a"), (col("b") + 2).alias("c")])
        ),
    )

    assert hstacked_on_proj.schema().column_names() == ["b", "a", "c"]

    projection_reorder = Projection(hstacked_on_proj, ExpressionList([col("a"), col("b"), col("c")]))
    assert projection_reorder.schema().column_names() == ["a", "b", "c"]


def test_filter_logical_plan_bad_input(schema) -> None:
    scan = InMemoryScan(cache_entry=PartitionCacheEntry("", None), schema=schema)
    assert scan.schema() == schema

    with pytest.raises(ValueError):
        scan = Projection(scan, ExpressionList([col("a"), (col("b") + 1).alias("a")]))


def test_scan_projection_filter_projection_chain(schema) -> None:
    scan = InMemoryScan(cache_entry=PartitionCacheEntry("", None), schema=schema)
    assert scan.schema() == schema

    hstacked = Projection(
        scan, schema.to_column_expressions().union(ExpressionList([(col("a") + col("b")).alias("d")]))
    )
    assert hstacked.schema().column_names() == ["a", "b", "c", "d"]

    filtered = Filter(hstacked, ExpressionList([col("d") < 20, col("a") > 10]))
    assert filtered.schema().column_names() == ["a", "b", "c", "d"]
    assert filtered.schema() == filtered.schema()

    projection_alias = Projection(filtered, ExpressionList([col("b").alias("out"), col("d")]))

    projection = Projection(projection_alias, ExpressionList([col("out")]))

    assert projection.schema().column_names() == ["out"]
