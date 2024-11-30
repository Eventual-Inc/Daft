from __future__ import annotations

import decimal
import itertools
import tempfile

import pyarrow as pa
import pytest

import daft

PYARROW_GE_7_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) >= (7, 0, 0)


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
    expected_prec = min(38, prec + 19)  # see agg_ops.rs
    assert schema["decimal128"].dtype == daft.DataType.decimal128(expected_prec, 3)


@pytest.mark.parametrize("prec, partitions", itertools.product([5, 30], [1, 2]))
def test_decimal_mean(prec, partitions) -> None:
    python_decimals = [decimal.Decimal("-1.010"), decimal.Decimal("99.001"), decimal.Decimal("10.010")]
    df = daft.from_pydict({"decimal128": python_decimals}).repartition(partitions)
    df = df.with_column("decimal128", df["decimal128"].cast(daft.DataType.decimal128(prec, 3)))
    res = df.mean().collect()
    assert res.to_pydict()["decimal128"] == [decimal.Decimal("36.0003333")]

    schema = res.schema()
    expected_prec = min(38, prec + 19)  # see agg_ops.rs
    assert schema["decimal128"].dtype == daft.DataType.decimal128(expected_prec, 7)


@pytest.mark.parametrize("prec, partitions", itertools.product([5, 30], [1, 2]))
def test_decimal_stddev(prec, partitions) -> None:
    python_decimals = [decimal.Decimal("-1.010"), decimal.Decimal("99.001"), decimal.Decimal("10.010")]
    df = daft.from_pydict({"decimal128": python_decimals}).repartition(partitions)
    df = df.with_column("decimal128", df["decimal128"].cast(daft.DataType.decimal128(prec, 3)))
    res = df.agg(df["decimal128"].stddev())  ## TODO: we can't do `select(df['x'].stddev())` make ticket
    assert pytest.approx(res.to_pydict()["decimal128"][0]) == 44.774792762098734

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
    expected_prec = min(38, prec + 19)  # see agg_ops.rs
    assert schema["decimal128"].dtype == daft.DataType.decimal128(expected_prec, 3)


@pytest.mark.parametrize("prec, partitions", itertools.product([5, 30], [1, 2]))
def test_decimal_grouped_mean(prec, partitions) -> None:
    python_decimals = [decimal.Decimal("-1.010"), decimal.Decimal("99.001"), decimal.Decimal("10.010"), None]
    group = [0, 1, 0, 1]

    df = daft.from_pydict({"decimal128": python_decimals, "group": group}).repartition(partitions)
    df = df.with_column("decimal128", df["decimal128"].cast(daft.DataType.decimal128(prec, 3)))
    res = df.groupby("group").mean().sort("group").collect()
    assert res.to_pydict() == {"group": [0, 1], "decimal128": [decimal.Decimal("4.500"), decimal.Decimal("99.001")]}
    schema = res.schema()
    expected_prec = min(38, prec + 19)  # see agg_ops.rs
    assert schema["decimal128"].dtype == daft.DataType.decimal128(expected_prec, 7)


@pytest.mark.parametrize("prec, partitions", itertools.product([5, 30], [1, 2]))
def test_decimal_grouped_stddev(prec, partitions) -> None:
    python_decimals = [decimal.Decimal("-1.010"), decimal.Decimal("99.001"), decimal.Decimal("10.010"), None]
    group = [0, 1, 0, 1]

    df = daft.from_pydict({"decimal128": python_decimals, "group": group}).repartition(partitions)
    df = df.with_column("decimal128", df["decimal128"].cast(daft.DataType.decimal128(prec, 3)))
    res = df.groupby("group").stddev().sort("group").collect()
    assert res.to_pydict() == {"group": [0, 1], "decimal128": [pytest.approx(5.51), pytest.approx(0)]}
    schema = res.schema()
    assert schema["decimal128"].dtype == daft.DataType.float64()
