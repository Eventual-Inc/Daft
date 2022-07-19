import pytest

from daft.expressions import col
from daft.logical.logical_plan import HStack, Projection, Scan, Selection
from daft.logical.schema import PlanSchema


def test_scan_predicates() -> None:
    schema = PlanSchema(["a", "b", "c"])
    scan = Scan(schema)
    assert scan.schema() == schema

    proj_scan = Scan(schema, columns=["a"])
    assert proj_scan.schema() == PlanSchema(["a"])

    select_scan = Scan(schema, selections=[col("a") < 10])
    assert select_scan.schema() == schema

    both_scan = Scan(schema, selections=[col("a") < 10], columns=["b", "c"])
    assert both_scan.schema() == PlanSchema(["b", "c"])


def test_projection_logical_plan() -> None:
    schema = PlanSchema(["a", "b", "c"])
    scan = Scan(schema)
    assert scan.schema() == schema

    full_project = Projection(scan, [col("a"), col("b"), col("c")])
    assert full_project.schema() == schema

    project = Projection(scan, [col("b")])
    assert project.schema() == PlanSchema(["b"])


def test_projection_logical_plan_bad_input() -> None:
    schema = PlanSchema(["a", "b", "c"])
    scan = Scan(schema)
    assert scan.schema() == schema

    with pytest.raises(ValueError):
        Projection(scan, [col("d")])


def test_selection_logical_plan() -> None:
    schema = PlanSchema(["a", "b", "c"])
    scan = Scan(schema)
    assert scan.schema() == schema

    full_select = Selection(scan, [col("a") == 1, col("b") < 10, col("c") > 10])
    assert full_select.schema() == schema

    project = Selection(scan, [col("b") < 10])
    assert project.schema() == schema


def test_selection_logical_plan_bad_input() -> None:
    schema = PlanSchema(["a", "b", "c"])
    scan = Scan(schema)
    assert scan.schema() == schema

    with pytest.raises(ValueError):
        select = Selection(scan, [col("d") == 1])


def test_hstack_logical_plan() -> None:
    schema = PlanSchema(["a", "b", "c"])
    scan = Scan(schema)
    assert scan.schema() == schema

    hstacked = HStack(scan, [(col("a") + col("b")).alias("d")])
    assert hstacked.schema() == schema.add_columns(["d"])

    projection = Projection(scan, [col("b")])

    hstacked_on_proj = HStack(projection, [(col("b") + 1).alias("a"), (col("b") + 2).alias("c")])
    assert hstacked_on_proj.schema() == PlanSchema(["b", "a", "c"])

    projection_reorder = Projection(hstacked_on_proj, [col("a"), col("b"), col("c")])
    assert projection_reorder.schema() == schema


def test_selection_logical_plan_bad_input() -> None:
    schema = PlanSchema(["a", "b", "c"])
    scan = Scan(schema)
    assert scan.schema() == schema

    with pytest.raises(ValueError):
        scan = HStack(scan, [(col("b") + 1).alias("a")])
