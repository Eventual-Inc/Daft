from __future__ import annotations

import pytest

pyiceberg = pytest.importorskip("pyiceberg")
import itertools
from datetime import date, datetime

import pandas as pd
import pytz

import daft
from daft.expressions import Expression
from tests.conftest import assert_df_equals


@pytest.mark.integration()
def test_daft_iceberg_table_predicate_pushdown_days(local_iceberg_catalog):
    tab = local_iceberg_catalog.load_table("default.test_partitioned_by_days")
    df = daft.read_iceberg(tab)
    df = df.where(df["ts"] < date(2023, 3, 6))
    df.collect()
    daft_pandas = df.to_pandas()
    iceberg_pandas = tab.scan().to_arrow().to_pandas()
    # need to use datetime here
    iceberg_pandas = iceberg_pandas[iceberg_pandas["ts"] < datetime(2023, 3, 6, tzinfo=pytz.utc)]

    assert_df_equals(daft_pandas, iceberg_pandas, sort_key=[])


def is_null(obj):
    if isinstance(obj, Expression):
        return obj.is_null()
    elif isinstance(obj, pd.Series):
        return obj.isnull()
    else:
        raise NotImplementedError()


def udf_func(obj):
    if isinstance(obj, Expression):
        return obj.apply(lambda x: str(x)[:1] == "1", return_dtype=daft.DataType.bool())
    elif isinstance(obj, pd.Series):
        return obj.apply(lambda x: str(x)[:1] == "1")
    else:
        raise NotImplementedError()


@pytest.mark.integration()
@pytest.mark.parametrize(
    "predicate, table, limit",
    itertools.product(
        [
            lambda x: x < date(2023, 3, 6),
            lambda x: x == date(2023, 3, 6),
            lambda x: x > date(2023, 3, 6),
            lambda x: x != date(2023, 3, 6),
            lambda x: date(2023, 3, 6) > x,
            lambda x: date(2023, 3, 6) == x,
            lambda x: date(2023, 3, 6) < x,
            lambda x: date(2023, 3, 6) != x,
            is_null,
            udf_func,
        ],
        [
            "test_partitioned_by_months",
            "test_partitioned_by_years",
        ],
        [None, 1, 2, 1000],
    ),
)
def test_daft_iceberg_table_predicate_pushdown_on_date_column(predicate, table, limit, local_iceberg_catalog):
    tab = local_iceberg_catalog.load_table(f"default.{table}")
    df = daft.read_iceberg(tab)
    df = df.where(predicate(df["dt"]))
    if limit:
        df = df.limit(limit)
    df.collect()

    daft_pandas = df.to_pandas()
    iceberg_pandas = tab.scan().to_arrow().to_pandas()
    iceberg_pandas = iceberg_pandas[predicate(iceberg_pandas["dt"])]
    if limit:
        iceberg_pandas = iceberg_pandas[:limit]
    assert_df_equals(daft_pandas, iceberg_pandas, sort_key=[])


@pytest.mark.integration()
@pytest.mark.parametrize(
    "predicate, table, limit",
    itertools.product(
        [
            lambda x: x < datetime(2023, 3, 6, tzinfo=pytz.utc),
            lambda x: x == datetime(2023, 3, 6, tzinfo=pytz.utc),
            lambda x: x > datetime(2023, 3, 6, tzinfo=pytz.utc),
            lambda x: x != datetime(2023, 3, 6, tzinfo=pytz.utc),
            lambda x: datetime(2023, 3, 6, tzinfo=pytz.utc) > x,
            lambda x: datetime(2023, 3, 6, tzinfo=pytz.utc) == x,
            lambda x: datetime(2023, 3, 6, tzinfo=pytz.utc) < x,
            lambda x: datetime(2023, 3, 6, tzinfo=pytz.utc) != x,
            is_null,
            udf_func,
        ],
        [
            "test_partitioned_by_days",
            "test_partitioned_by_hours",
            "test_partitioned_by_identity",
        ],
        [None, 1, 2, 1000],
    ),
)
def test_daft_iceberg_table_predicate_pushdown_on_timestamp_column(predicate, table, limit, local_iceberg_catalog):
    tab = local_iceberg_catalog.load_table(f"default.{table}")
    df = daft.read_iceberg(tab)
    df = df.where(predicate(df["ts"]))
    if limit:
        df = df.limit(limit)
    df.collect()

    daft_pandas = df.to_pandas()
    iceberg_pandas = tab.scan().to_arrow().to_pandas()
    iceberg_pandas = iceberg_pandas[predicate(iceberg_pandas["ts"])]
    if limit:
        iceberg_pandas = iceberg_pandas[:limit]

    assert_df_equals(daft_pandas, iceberg_pandas, sort_key=[])


@pytest.mark.integration()
@pytest.mark.parametrize(
    "predicate, table, limit",
    itertools.product(
        [
            lambda x: x < "d",
            lambda x: x == "d",
            lambda x: x > "d",
            lambda x: x != "d",
            lambda x: x == "z",
            lambda x: "d" > x,
            lambda x: "d" == x,
            lambda x: "d" < x,
            lambda x: "d" != x,
            lambda x: "z" == x,
            is_null,
            udf_func,
        ],
        [
            "test_partitioned_by_truncate",
        ],
        [None, 1, 2, 1000],
    ),
)
def test_daft_iceberg_table_predicate_pushdown_on_letter(predicate, table, limit, local_iceberg_catalog):
    tab = local_iceberg_catalog.load_table(f"default.{table}")
    df = daft.read_iceberg(tab)
    df = df.where(predicate(df["letter"]))
    if limit:
        df = df.limit(limit)
    df.collect()
    daft_pandas = df.to_pandas()
    iceberg_pandas = tab.scan().to_arrow().to_pandas()
    iceberg_pandas = iceberg_pandas[predicate(iceberg_pandas["letter"])]
    if limit:
        iceberg_pandas = iceberg_pandas[:limit]

    assert_df_equals(daft_pandas, iceberg_pandas, sort_key=[])


@pytest.mark.integration()
@pytest.mark.parametrize(
    "predicate, table, limit",
    itertools.product(
        [
            lambda x: x < 4,
            lambda x: x == 4,
            lambda x: x > 4,
            lambda x: x != 4,
            lambda x: x == 100,
            lambda x: 4 > x,
            lambda x: 4 == x,
            lambda x: 4 < x,
            lambda x: 4 != x,
            lambda x: 100 == x,
            is_null,
            udf_func,
        ],
        [
            "test_partitioned_by_bucket",
        ],
        [None, 1, 2, 1000],
    ),
)
def test_daft_iceberg_table_predicate_pushdown_on_number(predicate, table, limit, local_iceberg_catalog):
    tab = local_iceberg_catalog.load_table(f"default.{table}")
    df = daft.read_iceberg(tab)
    df = df.where(predicate(df["number"]))
    if limit:
        df = df.limit(limit)
    df.collect()
    daft_pandas = df.to_pandas()
    iceberg_pandas = tab.scan().to_arrow().to_pandas()
    iceberg_pandas = iceberg_pandas[predicate(iceberg_pandas["number"])]
    if limit:
        iceberg_pandas = iceberg_pandas[:limit]

    assert_df_equals(daft_pandas, iceberg_pandas, sort_key=[])


@pytest.mark.integration()
def test_daft_iceberg_table_predicate_pushdown_empty_scan(local_iceberg_catalog):
    tab = local_iceberg_catalog.load_table("default.test_partitioned_by_months")
    df = daft.read_iceberg(tab)
    df = df.where(df["dt"] > date(2030, 1, 1))
    df.collect()
    values = df.to_arrow()
    assert len(values) == 0
