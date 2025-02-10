from __future__ import annotations

import os

import pytest

import daft


def test_write_csv_basic(make_spark_df, spark_session, tmp_path):
    df = make_spark_df({"id": [1, 2, 3]})
    csv_dir = os.path.join(tmp_path, "csv")
    df.write.csv(csv_dir)


def test_write_csv_with_header(make_df, make_spark_df, spark_session, tmp_path):
    df = make_spark_df({"id": [1, 2, 3]})
    csv_dir = os.path.join(tmp_path, "csv")
    df.write.option("header", False).csv(csv_dir)

    expected_df = make_df({"column_1": [1, 2, 3]})
    actual_df = daft.read_csv(csv_dir, has_headers=False, schema={"column_1": daft.DataType.int64()})
    assert actual_df.to_pydict() == expected_df.to_pydict()


@pytest.mark.skip(reason="https://github.com/Eventual-Inc/Daft/issues/3786")
def test_write_csv_with_delimiter(make_df, make_spark_df, spark_session, tmp_path):
    pass


@pytest.mark.skip(reason="https://github.com/Eventual-Inc/Daft/issues/3787")
def test_write_csv_with_quote(spark_session, tmp_path):
    pass


@pytest.mark.skip(
    reason="https://github.com/Eventual-Inc/Daft/issues/3609: CSV null value handling not yet implemented"
)
def test_write_csv_with_null_value(spark_session, tmp_path):
    pass


def test_write_csv_with_compression(spark_session, tmp_path):
    pass


@pytest.mark.skip(reason="TODO: investigate why this occasionally fails in CI")
def test_write_parquet(spark_session, tmp_path):
    df = spark_session.range(10)
    parquet_dir = os.path.join(tmp_path, "test.parquet")
    df.write.parquet(parquet_dir)

    df_read = spark_session.read.parquet(parquet_dir)
    df_pandas = df.toPandas()
    df_read_pandas = df_read.toPandas()
    assert df_pandas["id"].equals(df_read_pandas["id"]), "Data should be unchanged after write/read"
