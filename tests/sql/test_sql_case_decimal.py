"""SQL CASE should accept a numeric literal beside a DECIMAL branch.

Regression for https://github.com/Eventual-Inc/Daft/issues/7602.
"""

from __future__ import annotations

import decimal

import pytest

import daft


def _prices() -> daft.DataFrame:
    df = daft.from_pydict(
        {
            "shipmode": ["AIR", "RAIL"],
            "price": [decimal.Decimal("10.50"), decimal.Decimal("3.25")],
        }
    )
    return df.with_column("price", daft.col("price").cast(daft.DataType.decimal128(15, 2)))


@pytest.mark.parametrize("else_expr", ["0", "0.0", "CAST(0 AS DECIMAL(15, 2))"])
def test_sql_case_decimal_with_numeric_literal(else_expr: str) -> None:
    result = daft.sql(
        f"""
        SELECT
            shipmode,
            CASE WHEN shipmode = 'AIR' THEN price ELSE {else_expr} END AS amount
        FROM lineitem
        """,
        lineitem=_prices(),
    ).collect()

    assert result.schema()["amount"].dtype == daft.DataType.decimal128(15, 2)
    assert result.to_pydict() == {
        "shipmode": ["AIR", "RAIL"],
        "amount": [decimal.Decimal("10.50"), decimal.Decimal("0.00")],
    }


def _shipments() -> daft.DataFrame:
    df = daft.from_pydict(
        {
            "shipmode": ["AIR", "RAIL", "TRUCK"],
            "price": [decimal.Decimal("10.50"), decimal.Decimal("3.25"), decimal.Decimal("7.00")],
            "qty": [1, 2, 3],
        }
    )
    return df.with_column("price", daft.col("price").cast(daft.DataType.decimal128(15, 2)))


@pytest.mark.parametrize("then_expr", ["0", "0.0"])
def test_sql_case_decimal_literal_on_then_branch(then_expr: str) -> None:
    result = daft.sql(
        f"""
        SELECT
            shipmode,
            CASE WHEN shipmode = 'AIR' THEN {then_expr} ELSE price END AS amount
        FROM lineitem
        """,
        lineitem=_prices(),
    ).collect()

    assert result.schema()["amount"].dtype == daft.DataType.decimal128(15, 2)
    assert result.to_pydict() == {
        "shipmode": ["AIR", "RAIL"],
        "amount": [decimal.Decimal("0.00"), decimal.Decimal("3.25")],
    }


def test_sql_case_decimal_multiple_when_clauses() -> None:
    result = daft.sql(
        """
        SELECT
            shipmode,
            CASE
                WHEN shipmode = 'AIR' THEN price
                WHEN shipmode = 'RAIL' THEN 1
                ELSE 0.0
            END AS amount
        FROM lineitem
        """,
        lineitem=_shipments(),
    ).collect()

    assert result.schema()["amount"].dtype == daft.DataType.decimal128(15, 2)
    assert result.to_pydict() == {
        "shipmode": ["AIR", "RAIL", "TRUCK"],
        "amount": [
            decimal.Decimal("10.50"),
            decimal.Decimal("1.00"),
            decimal.Decimal("0.00"),
        ],
    }


@pytest.mark.parametrize(
    ("else_expr", "rail_amount"),
    [
        ("-2", decimal.Decimal("-2.00")),
        ("0.5", decimal.Decimal("0.50")),
        ("0.1", decimal.Decimal("0.10")),
        ("0.01", decimal.Decimal("0.01")),
        ("1.10", decimal.Decimal("1.10")),
        ("-0.1", decimal.Decimal("-0.10")),
        ("+0.1", decimal.Decimal("0.10")),
        ("-(0.01)", decimal.Decimal("-0.01")),
        ("1e-1", decimal.Decimal("0.10")),
        ("1E+2", decimal.Decimal("100.00")),
        ("1.25e1", decimal.Decimal("12.50")),
        (".5", decimal.Decimal("0.50")),
    ],
)
def test_sql_case_decimal_exact_signed_and_fractional_literals(else_expr: str, rail_amount: decimal.Decimal) -> None:
    result = daft.sql(
        f"""
        SELECT CASE WHEN shipmode = 'AIR' THEN price ELSE {else_expr} END AS amount
        FROM lineitem
        """,
        lineitem=_prices(),
    ).collect()

    assert result.schema()["amount"].dtype == daft.DataType.decimal128(15, 2)
    assert result.to_pydict()["amount"] == [decimal.Decimal("10.50"), rail_amount]


def test_sql_case_without_decimal_keeps_integer_result() -> None:
    result = daft.sql(
        """
        SELECT CASE WHEN shipmode = 'AIR' THEN 1 ELSE 0 END AS amount
        FROM lineitem
        """,
        lineitem=_prices(),
    ).collect()

    assert result.schema()["amount"].dtype == daft.DataType.int64()
    assert result.to_pydict()["amount"] == [1, 0]


def test_sql_case_mixed_decimal_scales() -> None:
    df = daft.from_pydict(
        {
            "shipmode": ["AIR", "RAIL", "TRUCK"],
            "price": [decimal.Decimal("10.50"), decimal.Decimal("3.25"), decimal.Decimal("7.00")],
            "fee": [decimal.Decimal("1.0000"), decimal.Decimal("2.5000"), decimal.Decimal("0.0100")],
        }
    )
    df = df.with_column("price", daft.col("price").cast(daft.DataType.decimal128(15, 2)))
    df = df.with_column("fee", daft.col("fee").cast(daft.DataType.decimal128(10, 4)))
    result = daft.sql(
        """
        SELECT
            CASE
                WHEN shipmode = 'AIR' THEN price
                WHEN shipmode = 'RAIL' THEN fee
                ELSE 0.1
            END AS amount
        FROM lineitem
        """,
        lineitem=df,
    ).collect()

    assert result.schema()["amount"].dtype == daft.DataType.decimal128(15, 4)
    assert result.to_pydict()["amount"] == [
        decimal.Decimal("10.5000"),
        decimal.Decimal("2.5000"),
        decimal.Decimal("0.1000"),
    ]


@pytest.mark.parametrize(
    ("else_expr", "rejected_type"),
    [
        ("0.125", "Float64"),
        ("1e-3", "Float64"),
        ("10000000000000", "Int64"),
        ("qty", "Int64"),
        ("FALSE", "Bool"),
    ],
)
def test_sql_case_decimal_rejects_inexact_or_non_literal_branches(else_expr: str, rejected_type: str) -> None:
    # `0.125` and `1e-3` need another decimal place, `10000000000000` does not fit
    # Decimal(15, 2), `qty` is an integer column, and `FALSE` has no decimal supertype.
    with pytest.raises(Exception, match="castable to the same supertype") as exc_info:
        daft.sql(
            f"""
            SELECT CASE WHEN shipmode = 'AIR' THEN price ELSE {else_expr} END AS amount
            FROM lineitem
            """,
            lineitem=_shipments(),
        ).collect()

    assert exc_info.type.__name__ == "InvalidSQLException"
    assert rejected_type in str(exc_info.value)
