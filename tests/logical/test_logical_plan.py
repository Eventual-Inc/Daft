import pytest

from daft.expressions import col
from daft.logical.logical_plan import Projection, Scan
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

    full_project = Projection(scan, ExpressionList([col("a"), col("b") * 2, col("c")]))
    Projection(full_project, ExpressionList([col("a"), col("b"), col("c")]))
    import ipdb

    ipdb.set_trace()
    assert full_project.schema() == schema

    project = Projection(scan, ExpressionList([col("b")]))
    assert project.schema() == ExpressionList.keep(["b"])


# def test_projection_logical_plan_bad_input(schema) -> None:
#     scan = Scan(schema)
#     assert scan.schema() == schema

#     with pytest.raises(ValueError):
#         Projection(scan, [col("d")])


# def test_selection_logical_plan(schema) -> None:
#     scan = Scan(schema)
#     assert scan.schema() == schema

#     full_select = Selection(scan, [col("a") == 1, col("b") < 10, col("c") > 10])
#     assert full_select.schema() == schema

#     project = Selection(scan, [col("b") < 10])
#     assert project.schema() == schema


# def test_selection_logical_plan_bad_input(schema) -> None:
#     scan = Scan(schema)
#     assert scan.schema() == schema

#     with pytest.raises(ValueError):
#         select = Selection(scan, [col("d") == 1])


# def test_hstack_logical_plan(schema) -> None:
#     scan = Scan(schema)
#     assert scan.schema() == schema

#     hstacked = HStack(scan, [(col("a") + col("b")).alias("d")])
#     assert hstacked.schema() == schema.add_columns(["d"])

#     projection = Projection(scan, [col("b")])

#     hstacked_on_proj = HStack(projection, [(col("b") + 1).alias("a"), (col("b") + 2).alias("c")])
#     assert hstacked_on_proj.schema() == ExpressionList(["b", "a", "c"])

#     projection_reorder = Projection(hstacked_on_proj, [col("a"), col("b"), col("c")])
#     assert projection_reorder.schema() == schema


# def test_selection_logical_plan_bad_input(schema) -> None:
#     scan = Scan(schema)
#     assert scan.schema() == schema

#     with pytest.raises(ValueError):
#         scan = HStack(scan, [(col("b") + 1).alias("a")])
