from __future__ import annotations

import os

import pytest

import daft

# ------
# CSV
# ------


def test_csv_basic_roundtrip(make_spark_df, assert_spark_equals, spark_session, tmp_path):
    df = make_spark_df({"id": [1, 2, 3]})
    csv_dir = os.path.join(tmp_path, "csv")
    df.write.csv(csv_dir)

    spark_df_read = spark_session.read.option("header", True).csv(csv_dir)
    df_read = daft.read_csv(csv_dir)
    assert_spark_equals(df_read, spark_df_read)


def test_csv_overwrite(make_spark_df, assert_spark_equals, spark_session, tmp_path):
    df = make_spark_df({"id": [1, 2, 3]})
    csv_dir = os.path.join(tmp_path, "csv")
    df.write.csv(csv_dir)

    df = make_spark_df({"id": [4, 5, 6]})
    df.write.csv(csv_dir, mode="overwrite")

    spark_df_read = spark_session.read.option("header", True).csv(csv_dir)
    df_read = daft.read_csv(csv_dir)
    assert spark_df_read.count() == 3
    assert_spark_equals(df_read, spark_df_read)


@pytest.mark.skip(reason="https://github.com/Eventual-Inc/Daft/issues/3775")
def test_write_csv_without_header(make_df, make_spark_df, spark_session, tmp_path):
    pass


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


# -------
# Parquet
# -------


@pytest.mark.skip(reason="TODO: investigate why this occasionally fails in CI")
def test_write_parquet(spark_session, tmp_path):
    df = spark_session.range(10)
    parquet_dir = os.path.join(tmp_path, "test.parquet")
    df.write.parquet(parquet_dir)

    df_read = spark_session.read.parquet(parquet_dir)
    df_pandas = df.toPandas()
    df_read_pandas = df_read.toPandas()
    assert df_pandas["id"].equals(df_read_pandas["id"]), "Data should be unchanged after write/read"


@pytest.mark.skip(reason="TODO: investigate why this occasionally fails in CI")
def test_read_parquet(spark_session, make_spark_df, assert_spark_equals, tmp_path):
    df = daft.from_pydict({"id": [1, 2, 3]})
    parquet_dir = os.path.join(tmp_path, "test.parquet")
    df.write_parquet(parquet_dir)

    df_read = spark_session.read.format("parquet").load(parquet_dir)
    assert_spark_equals(df, df_read)


def test_unknown_options(spark_session, make_spark_df, assert_spark_equals, tmp_path):
    parquet_dir = os.path.join(tmp_path, "test.parquet")

    try:
        spark_session.read.option("something", "idk").format("parquet").load(parquet_dir).collect()
    except Exception as e:
        assert "something" in str(e)
