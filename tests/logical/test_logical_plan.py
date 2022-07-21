import pytest

from daft.expressions import col
from daft.logical.logical_plan import Projection, Scan, Selection
from daft.logical.schema import ExpressionList


@pytest.fixture(scope="function")
def schema():
    return ExpressionList(list(map(col, ["a", "b", "c"])))


def test_scan_predicates(schema) -> None:
    scan = Scan(schema)
    assert scan.schema() == schema

    proj_scan = Scan(schema, columns=["a"])
    assert proj_scan.schema().names == ["a"]

    select_scan = Scan(schema, selections=ExpressionList([col("a") < 10]))
    assert select_scan.schema().names == schema.names

    both_scan = Scan(schema, selections=ExpressionList([col("a") < 10]), columns=["b", "c"])
    assert both_scan.schema().names == schema.keep(["b", "c"]).names


def test_projection_logical_plan(schema) -> None:
    scan = Scan(schema)
    assert scan.schema() == schema

    full_project = Projection(scan, ExpressionList([col("a"), col("b"), col("c")]))
    # Projection(full_project, ExpressionList([col("a"), col("b"), col("c")]))
    assert full_project.schema().names == schema.names

    project = Projection(scan, ExpressionList([col("b")]))
    assert project.schema().names == schema.keep(["b"]).names


def test_projection_logical_plan_bad_input(schema) -> None:
    scan = Scan(schema)
    assert scan.schema() == schema

    with pytest.raises(ValueError):
        Projection(scan, ExpressionList([col("d")]))


def test_selection_logical_plan(schema) -> None:
    scan = Scan(schema)
    assert scan.schema().names == schema.names

    full_select = Selection(scan, ExpressionList([col("a") == 1, col("b") < 10, col("c") > 10]))
    assert full_select.schema().names == schema.names

    project = Selection(scan, ExpressionList([col("b") < 10]))
    assert project.schema().names == schema.names


def test_selection_logical_plan_bad_input(schema) -> None:
    scan = Scan(schema)
    assert scan.schema() == schema

    with pytest.raises(ValueError):
        select = Selection(scan, ExpressionList([col("d") == 1]))


def test_projection_new_columns_logical_plan(schema) -> None:
    scan = Scan(schema)
    assert scan.schema().names == schema.names

    hstacked = Projection(scan, schema.union(ExpressionList([(col("a") + col("b")).alias("d")])))
    assert hstacked.schema().names == ["a", "b", "c", "d"]

    projection = Projection(scan, ExpressionList([col("b")]))
    proj_schema = projection.schema()
    hstacked_on_proj = Projection(
        projection, proj_schema.union(ExpressionList([(col("b") + 1).alias("a"), (col("b") + 2).alias("c")]))
    )
    assert hstacked_on_proj.schema().names == ["b", "a", "c"]

    projection_reorder = Projection(hstacked_on_proj, ExpressionList([col("a"), col("b"), col("c")]))
    assert projection_reorder.schema().names == ["a", "b", "c"]


def test_selection_logical_plan_bad_input(schema) -> None:
    scan = Scan(schema)
    assert scan.schema().names == schema.names

    with pytest.raises(ValueError):
        scan = Projection(scan, ExpressionList([col("a"), (col("b") + 1).alias("a")]))


def test_scan_projection_filter_projection_chain(schema) -> None:
    scan = Scan(schema)
    assert scan.schema().names == schema.names

    hstacked = Projection(scan, schema.union(ExpressionList([(col("a") + col("b")).alias("d")])))
    assert hstacked.schema().names == ["a", "b", "c", "d"]

    filtered = Selection(hstacked, ExpressionList([col("d") < 20, col("a") > 10]))
    assert filtered.schema().names == ["a", "b", "c", "d"]

    projection_alias = Projection(filtered, ExpressionList([col("b").alias("out"), col("d")]))

    projection = Projection(projection_alias, ExpressionList([col("out")]))
    assert projection.schema().names == ["out"]
    print(projection.to_dot())
