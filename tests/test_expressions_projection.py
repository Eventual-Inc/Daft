from __future__ import annotations

import pytest

from daft.errors import ExpressionTypeError
from daft.expressions import Expression, ExpressionsProjection, col
from daft.table import Table


def test_expressions_projection_error_dup_name():
    with pytest.raises(ValueError):
        ep = ExpressionsProjection(
            [
                col("x"),
                col("y").alias("x"),
            ]
        )


def test_expressions_projection_empty():
    ep = ExpressionsProjection([])

    # Test len
    assert len(ep) == 0

    # Test iter
    assert list(ep) == []

    # Test eq
    assert ep == ep
    assert ep != ExpressionsProjection([col("x")])

    # Test required_columns
    assert ep.required_columns() == set()

    # Test to_name_set()
    assert ep.to_name_set() == set()

    # Test to_column_expression()
    assert ep.to_column_expressions() == ExpressionsProjection([])


def test_expressions_projection():
    exprs = [
        col("x"),
        col("y") + 1,
        col("z").alias("a"),
    ]
    ep = ExpressionsProjection(exprs)

    # Test len
    assert len(ep) == 3

    # Test iter
    for e1, e2 in zip(ep, exprs):
        assert e1.name() == e2.name()

    # Test eq
    # TODO: [RUST-INT] Needs Expression._is_eq
    # assert ep == ep
    # assert ep != ExpressionsProjection([])

    # Test required_columns
    # TODO: [RUST-INT] Needs Expression._required_columns
    # assert ep.required_columns() == ["x", "y", "z"]

    # Test to_name_set()
    assert ep.to_name_set() == {"x", "y", "a"}

    # Test to_column_expression()
    # TODO: [RUST-INT] Needs Expression._is_eq
    # assert ep.to_column_expressions() == ExpressionsProjection([col("x"), col("y"), col("a")])


def test_expressions_union():
    exprs1 = [
        col("x"),
        col("y"),
    ]
    ep1 = ExpressionsProjection(exprs1)

    exprs2 = [
        col("z"),
    ]
    ep2 = ExpressionsProjection(exprs2)

    assert ep1.union(ep2).to_name_set() == {"x", "y", "z"}


def test_expressions_union_dup_err():
    exprs1 = [
        col("x"),
        col("y"),
    ]
    ep1 = ExpressionsProjection(exprs1)

    exprs2 = [
        col("x"),
    ]
    ep2 = ExpressionsProjection(exprs2)

    with pytest.raises(ValueError):
        assert ep1.union(ep2)


def test_expressions_union_dup_rename():
    exprs1 = [
        col("x"),
        col("y"),
    ]
    ep1 = ExpressionsProjection(exprs1)

    exprs2 = [
        col("x"),
    ]
    ep2 = ExpressionsProjection(exprs2)
    assert ep1.union(ep2, rename_dup="foo.").union(ep2, rename_dup="foo.").to_name_set() == {
        "x",
        "y",
        "foo.x",
        "foo.foo.x",
    }


@pytest.mark.skip(reason="[RUST-INT] Needs Expression._input_mapping")
def test_input_mapping():
    exprs = [
        col("x"),
        col("y") + 1,
        col("z").alias("a"),
    ]
    ep = ExpressionsProjection(exprs)
    assert ep.input_mapping() == {
        "x": "x",
        "a": "z",
    }


def test_get_expression_by_name():
    exprs = [col("x")]
    ep = ExpressionsProjection(exprs)
    assert ep.get_expression_by_name("x").name() == "x"


def test_expressions_projection_indexing():
    exprs = [
        col("x"),
        col("y") + 1,
        col("z").alias("a"),
    ]
    ep = ExpressionsProjection(exprs)
    assert isinstance(ep[0], Expression)
    assert ep[0].name() == "x"
    assert isinstance(ep[:2], list)
    assert [e.name() for e in ep[:2]] == ["x", "y"]

    # TODO: [RUST-INT] enable once we have Expression._is_eq
    # assert ep[0]._is_eq(col("x"))
    # assert all([result._is_eq(expected) for result, expected in zip(ep[:2], list(col("x"), col("y") + 1))])


def test_resolve_schema():
    tbl = Table.from_pydict(
        {
            "foo": [1, 2, 3],
        }
    )
    ep = ExpressionsProjection([col("foo"), (col("foo") + 1).alias("foo_plus")])
    resolved_schema = ep.resolve_schema(tbl.schema())
    assert resolved_schema.to_name_set() == {"foo", "foo_plus"}


@pytest.mark.skip(reason="[RUST-INT] throw error on bad types during schema resolving")
def test_resolve_schema_invalid_type():
    tbl = Table.from_pydict(
        {
            "foo": ["a", "b", "c"],
        }
    )
    ep = ExpressionsProjection([(col("foo") + 1).alias("invalid")])
    with pytest.raises(ExpressionTypeError):
        ep.resolve_schema(tbl.schema())


def test_resolve_schema_missing_col():
    tbl = Table.from_pydict(
        {
            "foo": ["a", "b", "c"],
        }
    )
    ep = ExpressionsProjection([col("bar")])
    with pytest.raises(ValueError):
        ep.resolve_schema(tbl.schema())
