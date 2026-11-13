from __future__ import annotations

import decimal
import itertools
import tempfile

import pyarrow as pa
import pytest

import daft


def test_decimal_parquet_roundtrip() -> None:
    python_decimals = [decimal.Decimal("-2.010"), decimal.Decimal("0.000"), decimal.Decimal("2.010")]
    data = {
        "decimal128": pa.array(python_decimals),
        # Not supported yet.
        # "decimal256": pa.array([decimal.Decimal('1234567890.1234567890123456789012345678901234567890')]),
    }

    df = daft.from_pydict(data)

    with tempfile.TemporaryDirectory() as dirname:
        df.write_parquet(dirname)
        df_readback = daft.read_parquet(dirname).collect()

        assert str(df.to_pydict()["decimal128"]) == str(df_readback.to_pydict()["decimal128"])


def test_arrow_decimal() -> None:
    # Test roundtrip of Arrow decimals.
    pa_table = pa.Table.from_pydict(
        {"decimal128": pa.array([decimal.Decimal("-1.010"), decimal.Decimal("0.000"), decimal.Decimal("1.010")])}
    )

    df = daft.from_arrow(pa_table)

    assert df.to_arrow() == pa_table


def test_python_decimal() -> None:
    # Test roundtrip of Python decimals.
    python_decimals = [decimal.Decimal("-1.010"), decimal.Decimal("0.000"), decimal.Decimal("1.010")]
    df = daft.from_pydict({"decimal128": python_decimals})

    res = df.to_pydict()["decimal128"]
    assert str(res) == str(python_decimals)


@pytest.mark.parametrize("prec, partitions", itertools.product([5, 30], [1, 2]))
def test_decimal_sum(prec, partitions) -> None:
    python_decimals = [decimal.Decimal("-1.010"), decimal.Decimal("99.001"), decimal.Decimal("10.010")]
    df = daft.from_pydict({"decimal128": python_decimals}).repartition(partitions)
    df = df.with_column("decimal128", df["decimal128"].cast(daft.DataType.decimal128(prec, 3)))
    res = df.sum().collect()
    assert res.to_pydict()["decimal128"] == [decimal.Decimal("108.001")]

    schema = res.schema()
    assert schema["decimal128"].dtype == daft.DataType.decimal128(38, 3)


@pytest.mark.parametrize("prec, partitions", itertools.product([5, 30], [1, 2]))
def test_decimal_mean(prec, partitions) -> None:
    python_decimals = [decimal.Decimal("-1.010"), decimal.Decimal("99.001"), decimal.Decimal("10.010")]
    df = daft.from_pydict({"decimal128": python_decimals}).repartition(partitions)
    df = df.with_column("decimal128", df["decimal128"].cast(daft.DataType.decimal128(prec, 3)))
    res = df.mean().collect()
    assert res.to_pydict()["decimal128"] == [decimal.Decimal("36.0003333")]

    schema = res.schema()
    assert schema["decimal128"].dtype == daft.DataType.decimal128(38, 7)


@pytest.mark.parametrize("prec, partitions", itertools.product([5, 30], [1, 2]))
def test_decimal_stddev(prec, partitions) -> None:
    python_decimals = [decimal.Decimal("-1.010"), decimal.Decimal("99.001"), decimal.Decimal("10.010")]
    df = daft.from_pydict({"decimal128": python_decimals}).repartition(partitions)
    df = df.with_column("decimal128", df["decimal128"].cast(daft.DataType.decimal128(prec, 3)))
    res = df.agg(df["decimal128"].stddev())  ## TODO: we can't do `select(df['x'].stddev())` make ticket
    assert pytest.approx(res.to_pydict()["decimal128"][0]) == 54.83769780300165

    schema = res.schema()
    assert schema["decimal128"].dtype == daft.DataType.float64()


@pytest.mark.parametrize("prec, partitions", itertools.product([5, 30], [1, 2]))
def test_decimal_grouped_sum(prec, partitions) -> None:
    python_decimals = [decimal.Decimal("-1.010"), decimal.Decimal("99.001"), decimal.Decimal("10.010"), None]
    group = [0, 1, 0, 1]

    df = daft.from_pydict({"decimal128": python_decimals, "group": group}).repartition(partitions)
    df = df.with_column("decimal128", df["decimal128"].cast(daft.DataType.decimal128(prec, 3)))
    res = df.groupby("group").sum().sort("group").collect()
    assert res.to_pydict() == {"group": [0, 1], "decimal128": [decimal.Decimal("9.000"), decimal.Decimal("99.001")]}
    schema = res.schema()
    assert schema["decimal128"].dtype == daft.DataType.decimal128(38, 3)


@pytest.mark.parametrize("prec, partitions", itertools.product([5, 30], [1, 2]))
def test_decimal_grouped_mean(prec, partitions) -> None:
    python_decimals = [decimal.Decimal("-1.010"), decimal.Decimal("99.001"), decimal.Decimal("10.010"), None]
    group = [0, 1, 0, 1]

    df = daft.from_pydict({"decimal128": python_decimals, "group": group}).repartition(partitions)
    df = df.with_column("decimal128", df["decimal128"].cast(daft.DataType.decimal128(prec, 3)))
    res = df.groupby("group").mean().sort("group").collect()
    assert res.to_pydict() == {"group": [0, 1], "decimal128": [decimal.Decimal("4.500"), decimal.Decimal("99.001")]}
    schema = res.schema()
    assert schema["decimal128"].dtype == daft.DataType.decimal128(38, 7)


@pytest.mark.parametrize("prec, partitions", itertools.product([5, 30], [1, 2]))
def test_decimal_grouped_stddev(prec, partitions) -> None:
    python_decimals = [decimal.Decimal("-1.010"), decimal.Decimal("99.001"), decimal.Decimal("10.010"), None]
    group = [0, 1, 0, 1]

    df = daft.from_pydict({"decimal128": python_decimals, "group": group}).repartition(partitions)
    df = df.with_column("decimal128", df["decimal128"].cast(daft.DataType.decimal128(prec, 3)))
    res = df.groupby("group").stddev().sort("group").collect()
    assert res.to_pydict() == {"group": [0, 1], "decimal128": [pytest.approx(7.792316), None]}
    schema = res.schema()
    assert schema["decimal128"].dtype == daft.DataType.float64()


def test_decimal_multiply_precision_overflow_is_bounded() -> None:
    # Multiplying two Decimal(20, 2) columns needs precision 40 (> 38). This previously failed to
    # plan with "result precision: 40 exceed bounds of [1, 38]"; it should now bound the result to
    # Decimal(38, 4) and compute correctly. Regression test for #7601 (TPC-H Q1 shape).
    df = daft.from_pydict(
        {
            "a": [decimal.Decimal("100.00"), decimal.Decimal("200.00")],
            "b": [decimal.Decimal("3.00"), decimal.Decimal("1.50")],
        }
    )
    df = df.with_column("a", df["a"].cast(daft.DataType.decimal128(20, 2)))
    df = df.with_column("b", df["b"].cast(daft.DataType.decimal128(20, 2)))
    res = df.select((df["a"] * df["b"]).alias("product")).collect()

    # 100.00 * 3.00 = 300.00 ; 200.00 * 1.50 = 300.00
    assert res.to_pydict()["product"] == [decimal.Decimal("300.0000"), decimal.Decimal("300.0000")]
    assert res.schema()["product"].dtype == daft.DataType.decimal128(38, 4)


def test_decimal_subtract_precision_overflow_is_bounded() -> None:
    # Decimal(38, 4) - Decimal(30, 4) needs precision 39 (> 38); it should bound to Decimal(38, 4)
    # rather than failing to plan. Regression test for #7601 (TPC-H Q9 shape).
    df = daft.from_pydict(
        {
            "a": [decimal.Decimal("100.0000"), decimal.Decimal("250.5000")],
            "b": [decimal.Decimal("0.5000"), decimal.Decimal("50.2500")],
        }
    )
    df = df.with_column("a", df["a"].cast(daft.DataType.decimal128(38, 4)))
    df = df.with_column("b", df["b"].cast(daft.DataType.decimal128(30, 4)))
    res = df.select((df["a"] - df["b"]).alias("diff")).collect()

    assert res.to_pydict()["diff"] == [decimal.Decimal("99.5000"), decimal.Decimal("200.2500")]
    assert res.schema()["diff"].dtype == daft.DataType.decimal128(38, 4)
