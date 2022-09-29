import pytest

from daft.datasources import InMemorySourceInfo
from daft.expressions import ColumnExpression, col
from daft.logical.logical_plan import Filter, Projection, Scan
from daft.logical.schema import ExpressionList
from daft.types import ExpressionType


@pytest.fixture(scope="function")
def schema():
    return ExpressionList(
        list(
            map(
                lambda col_name: ColumnExpression(col_name, expr_type=ExpressionType.from_py_type(int)),
                ["a", "b", "c"],
            )
        )
    )


@pytest.fixture(scope="function")
def source_info():
    return InMemorySourceInfo(data={})


def test_scan_predicates(schema, source_info) -> None:
    scan = Scan(schema=schema, source_info=source_info)
    assert scan.schema() == schema

    proj_scan = Scan(schema=schema, source_info=source_info, columns=["a"])
    assert proj_scan.schema().names == ["a"]

    filter_scan = Scan(schema=schema, source_info=source_info, predicate=ExpressionList([col("a") < 10]))
    assert filter_scan.schema() == schema

    both_scan = Scan(
        schema=schema, source_info=source_info, predicate=ExpressionList([col("a") < 10]), columns=["b", "c"]
    )
    assert both_scan.schema() == schema.keep(["b", "c"])


def test_projection_logical_plan(schema, source_info) -> None:
    scan = Scan(schema=schema, source_info=source_info)
    assert scan.schema() == schema

    full_project = Projection(scan, ExpressionList([col("a"), col("b"), col("c")]))
    # Projection(full_project, ExpressionList([col("a"), col("b"), col("c")]))
    assert full_project.schema() == schema

    project = Projection(scan, ExpressionList([col("b")]))
    assert project.schema() == schema.keep(["b"])

    project = Projection(scan, ExpressionList([col("b") * 2]))
    assert project.schema() != schema.keep(["b"])
    assert (
        project.schema().get_expression_by_name("b").required_columns()[0].is_same(schema.get_expression_by_name("b"))
    )


def test_projection_logical_plan_bad_input(schema, source_info) -> None:
    scan = Scan(schema=schema, source_info=source_info)
    assert scan.schema() == schema

    with pytest.raises(ValueError):
        Projection(scan, ExpressionList([col("d")]))


def test_filter_logical_plan(schema, source_info) -> None:
    scan = Scan(schema=schema, source_info=source_info)
    assert scan.schema() == schema

    full_filter = Filter(scan, ExpressionList([col("a") == 1, col("b") < 10, col("c") > 10]))
    assert full_filter.schema() == schema

    project = Filter(scan, ExpressionList([col("b") < 10]))
    assert project.schema() == schema


def test_filter_logical_plan_bad_input(schema, source_info) -> None:
    scan = Scan(schema=schema, source_info=source_info)
    assert scan.schema() == schema

    with pytest.raises(ValueError):
        filter = Filter(scan, ExpressionList([col("d") == 1]))


def test_projection_new_columns_logical_plan(schema, source_info) -> None:
    scan = Scan(schema=schema, source_info=source_info)
    assert scan.schema() == schema

    hstacked = Projection(scan, schema.union(ExpressionList([(col("a") + col("b")).alias("d")])))
    assert hstacked.schema().keep(["a", "b", "c"]) == schema
    old_ids = {schema.get_expression_by_name(n).get_id() for n in schema.names}

    assert hstacked.schema().get_expression_by_name("d").get_id() not in old_ids

    projection = Projection(scan, ExpressionList([col("b")]))
    proj_schema = projection.schema()
    hstacked_on_proj = Projection(
        projection, proj_schema.union(ExpressionList([(col("b") + 1).alias("a"), (col("b") + 2).alias("c")]))
    )

    assert hstacked_on_proj.schema().names == ["b", "a", "c"]
    assert hstacked_on_proj.schema().get_expression_by_name("b").is_same(schema.get_expression_by_name("b"))

    assert not hstacked_on_proj.schema().get_expression_by_name("a").is_same(schema.get_expression_by_name("a"))
    assert (
        hstacked_on_proj.schema()
        .get_expression_by_name("a")
        .required_columns()[0]
        .is_same(schema.get_expression_by_name("b"))
    )

    assert not hstacked_on_proj.schema().get_expression_by_name("c").is_same(schema.get_expression_by_name("c"))
    assert (
        hstacked_on_proj.schema()
        .get_expression_by_name("c")
        .required_columns()[0]
        .is_same(schema.get_expression_by_name("b"))
    )

    projection_reorder = Projection(hstacked_on_proj, ExpressionList([col("a"), col("b"), col("c")]))
    assert projection_reorder.schema().names == ["a", "b", "c"]
    assert projection_reorder.schema() == hstacked_on_proj.schema().keep(["a", "b", "c"])


def test_filter_logical_plan_bad_input(schema, source_info) -> None:
    scan = Scan(schema=schema, source_info=source_info)
    assert scan.schema() == schema

    with pytest.raises(ValueError):
        scan = Projection(scan, ExpressionList([col("a"), (col("b") + 1).alias("a")]))


def test_scan_projection_filter_projection_chain(schema, source_info) -> None:
    scan = Scan(schema=schema, source_info=source_info)
    assert scan.schema() == schema

    hstacked = Projection(scan, schema.union(ExpressionList([(col("a") + col("b")).alias("d")])))
    assert hstacked.schema().names == ["a", "b", "c", "d"]
    assert hstacked.schema().keep(["a", "b", "c"]) == schema

    filtered = Filter(hstacked, ExpressionList([col("d") < 20, col("a") > 10]))
    assert filtered.schema().names == ["a", "b", "c", "d"]
    assert filtered.schema() == hstacked.schema()

    projection_alias = Projection(filtered, ExpressionList([col("b").alias("out"), col("d")]))
    projection_alias.schema().get_expression_by_name("out") == schema.get_expression_by_name("b")
    projection_alias.schema().get_expression_by_name("d") == filtered.schema().get_expression_by_name("d")

    projection = Projection(projection_alias, ExpressionList([col("out")]))

    projection_alias.schema().get_expression_by_name("out") == schema.get_expression_by_name("b")

    assert projection.schema().names == ["out"]
