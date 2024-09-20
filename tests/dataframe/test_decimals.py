from __future__ import annotations

import decimal
import tempfile

import pyarrow as pa
import pytest

import daft
from daft import context

pytestmark = pytest.mark.skipif(
    context.get_context().daft_execution_config.enable_native_executor is True,
    reason="Native executor fails for these tests",
)

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


@pytest.mark.parametrize("prec", [5, 30])
def test_decimal_sum(prec) -> None:
    python_decimals = [decimal.Decimal("-1.010"), decimal.Decimal("99.001"), decimal.Decimal("10.010")]
    df = daft.from_pydict({"decimal128": python_decimals})
    df = df.with_column("decimal128", df["decimal128"].cast(daft.DataType.decimal128(prec, 3)))
    print(df.collect())
    res = df.sum().collect()
    assert res.to_pydict()["decimal128"] == [decimal.Decimal("108.001")]

    schema = res.schema()
    expected_prec = min(38, prec + 19)  # see agg_ops.rs
    assert schema["decimal128"].dtype == daft.DataType.decimal128(expected_prec, 3)
