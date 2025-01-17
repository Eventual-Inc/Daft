from __future__ import annotations

import pytest


def test_create_or_replace_temp_view(spark_session):
    df = spark_session.createDataFrame([(1, "foo")], ["id", "name"])
    try:
        df.createOrReplaceTempView("test_view")
    except Exception as e:
        pytest.fail(f"createOrReplaceTempView failed: {e}")


def test_sql(spark_session):
    df = spark_session.createDataFrame([(1, "foo")], ["id", "name"])
    df.createOrReplaceTempView("test_view")
    try:
        result = spark_session.sql("SELECT * FROM test_view")
    except Exception as e:
        pytest.fail(f"sql failed: {e}")
    assert result.collect() == [(1, "foo")]
