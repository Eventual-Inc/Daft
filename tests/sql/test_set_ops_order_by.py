from __future__ import annotations

import pytest

import daft


@pytest.mark.parametrize(
    ("operation", "expected"),
    [
        ("UNION ALL", [1, 1, 2, 2, 2, 3, 4, None, None]),
        ("UNION", [1, 2, 3, 4, None]),
        ("INTERSECT", [1, 2, None]),
        ("INTERSECT ALL", [1, 2, None]),
    ],
)
def test_set_operation_order_by_output_column(operation, expected):
    lhs = daft.from_pydict({"id": [4, 1, 2, 2, None]})
    rhs = daft.from_pydict({"id": [2, 3, 1, None]})
    result = daft.sql(
        f"SELECT id AS value FROM lhs {operation} SELECT id AS value FROM rhs ORDER BY value",
        lhs=lhs,
        rhs=rhs,
    )
    assert result.to_pydict() == {"value": expected}


def test_union_order_by_position_and_multiple_columns():
    result = daft.sql("""
        SELECT 'b' AS label, 1 AS value
        UNION ALL SELECT 'a' AS label, 2 AS value
        UNION ALL SELECT 'a' AS label, 1 AS value
        ORDER BY 2 DESC, 1 ASC
    """)
    assert result.to_pydict() == {"label": ["a", "a", "b"], "value": [2, 1, 1]}


def test_union_order_by_desc_nulls_first():
    lhs = daft.from_pydict({"id": [2, None]})
    rhs = daft.from_pydict({"id": [3, 1]})
    result = daft.sql(
        "SELECT id FROM lhs UNION ALL SELECT id FROM rhs ORDER BY id DESC NULLS FIRST",
        lhs=lhs,
        rhs=rhs,
    )
    assert result.to_pydict() == {"id": [None, 3, 2, 1]}


def test_union_order_by_limit_offset():
    result = daft.sql("""
        SELECT 3 AS id UNION ALL SELECT 1 AS id UNION ALL SELECT 2 AS id
        ORDER BY id LIMIT 1 OFFSET 1
    """)
    assert result.to_pydict() == {"id": [2]}


def test_union_limit_without_order_by():
    result = daft.sql("SELECT 2 AS id UNION ALL SELECT 1 AS id LIMIT 1")
    assert len(result.to_pydict()["id"]) == 1


def test_union_limit_zero():
    result = daft.sql("SELECT 2 AS id UNION ALL SELECT 1 AS id ORDER BY id LIMIT 0")
    assert result.to_pydict() == {"id": []}


@pytest.mark.parametrize("position", [0, 2])
def test_union_order_by_invalid_position(position):
    with pytest.raises(Exception, match="ORDER BY position"):
        daft.sql(f"SELECT 2 AS id UNION ALL SELECT 1 AS id ORDER BY {position}")


@pytest.mark.parametrize(("clause", "message"), [("LIMIT -1", "LIMIT <n>"), ("LIMIT 2 OFFSET -1", "OFFSET <n>")])
def test_union_rejects_negative_limit_or_offset(clause, message):
    with pytest.raises(Exception, match=message):
        daft.sql(f"SELECT 2 AS id UNION ALL SELECT 1 AS id {clause}")


def test_set_operation_with_cte_and_order_by():
    result = daft.sql("""
        WITH input AS (SELECT 2 AS id)
        SELECT id FROM input UNION ALL SELECT 1 AS id ORDER BY id
    """)
    assert result.to_pydict() == {"id": [1, 2]}
