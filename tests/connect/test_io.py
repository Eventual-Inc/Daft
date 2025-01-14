from __future__ import annotations

import os
import tempfile

import pytest


def test_write_csv_basic(spark_session, tmp_path):
    df = spark_session.range(10)
    csv_dir = os.path.join(tmp_path, "csv")
    df.write.csv(csv_dir)

    csv_files = [f for f in os.listdir(csv_dir) if f.endswith(".csv")]
    assert len(csv_files) > 0, "Expected at least one CSV file to be written"

    df_read = spark_session.read.csv(str(csv_dir))
    df_pandas = df.toPandas()
    df_read_pandas = df_read.toPandas()
    assert df_pandas["id"].equals(df_read_pandas["id"]), "Data should be unchanged after write/read"


def test_write_csv_with_header(spark_session, tmp_path):
    df = spark_session.range(10)
    csv_dir = os.path.join(tmp_path, "csv")
    df.write.option("header", True).csv(csv_dir)

    df_read = spark_session.read.option("header", True).csv(str(csv_dir))
    df_pandas = df.toPandas()
    df_read_pandas = df_read.toPandas()
    assert df_pandas["id"].equals(df_read_pandas["id"])


def test_write_csv_with_delimiter(spark_session, tmp_path):
    df = spark_session.range(10)
    csv_dir = os.path.join(tmp_path, "csv")
    df.write.option("sep", "|").csv(csv_dir)

    df_read = spark_session.read.option("sep", "|").csv(str(csv_dir))
    df_pandas = df.toPandas()
    df_read_pandas = df_read.toPandas()
    assert df_pandas["id"].equals(df_read_pandas["id"])


def test_write_csv_with_quote(spark_session, tmp_path):
    df = spark_session.createDataFrame([("a,b",), ("c'd",)], ["text"])
    csv_dir = os.path.join(tmp_path, "csv")
    df.write.option("quote", "'").csv(csv_dir)

    df_read = spark_session.read.option("quote", "'").csv(str(csv_dir))
    df_pandas = df.toPandas()
    df_read_pandas = df_read.toPandas()
    assert df_pandas["text"].equals(df_read_pandas["text"])


def test_write_csv_with_escape(spark_session, tmp_path):
    df = spark_session.createDataFrame([("a'b",), ("c'd",)], ["text"])
    csv_dir = os.path.join(tmp_path, "csv")
    df.write.option("escape", "\\").csv(csv_dir)

    df_read = spark_session.read.option("escape", "\\").csv(str(csv_dir))
    df_pandas = df.toPandas()
    df_read_pandas = df_read.toPandas()
    assert df_pandas["text"].equals(df_read_pandas["text"])


@pytest.mark.skip(
    reason="https://github.com/Eventual-Inc/Daft/issues/3609: CSV null value handling not yet implemented"
)
def test_write_csv_with_null_value(spark_session, tmp_path):
    df = spark_session.createDataFrame([(1, None), (2, "test")], ["id", "value"])
    csv_dir = os.path.join(tmp_path, "csv")
    df.write.option("nullValue", "NULL").csv(csv_dir)

    df_read = spark_session.read.option("nullValue", "NULL").csv(str(csv_dir))
    df_pandas = df.toPandas()
    df_read_pandas = df_read.toPandas()
    assert df_pandas["value"].isna().equals(df_read_pandas["value"].isna())


def test_write_csv_with_compression(spark_session, tmp_path):
    df = spark_session.range(10)
    csv_dir = os.path.join(tmp_path, "csv")
    df.write.option("compression", "gzip").csv(csv_dir)

    df_read = spark_session.read.csv(str(csv_dir))
    df_pandas = df.toPandas()
    df_read_pandas = df_read.toPandas()
    assert df_pandas["id"].equals(df_read_pandas["id"])


def test_write_parquet(spark_session):
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create DataFrame from range(10)
        df = spark_session.range(10)

        # Write DataFrame to parquet directory
        parquet_dir = os.path.join(temp_dir, "test.parquet")
        df.write.parquet(parquet_dir)

        # List all files in the parquet directory
        parquet_files = [f for f in os.listdir(parquet_dir) if f.endswith(".parquet")]

        # Assert there is at least one parquet file
        assert len(parquet_files) > 0, "Expected at least one parquet file to be written"

        # Read back from the parquet directory (not specific file)
        df_read = spark_session.read.parquet(parquet_dir)

        # Verify the data is unchanged
        df_pandas = df.toPandas()
        df_read_pandas = df_read.toPandas()
        assert df_pandas["id"].equals(df_read_pandas["id"]), "Data should be unchanged after write/read"
