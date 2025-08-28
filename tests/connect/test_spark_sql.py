from __future__ import annotations

import random

import pytest


def test_create_or_replace_temp_view(spark_session):
    df = spark_session.createDataFrame([(1, "foo")], ["id", "name"])
    try:
        df.createOrReplaceTempView("test_view")
    except Exception as e:
        pytest.fail(f"createOrReplaceTempView failed: {e}")


def test_select(spark_session):
    df = spark_session.createDataFrame([(1, "foo")], ["id", "name"])
    df.createOrReplaceTempView("test_view")
    try:
        result = spark_session.sql("SELECT * FROM test_view")
        assert result.collect() == [(1, "foo")]
    except Exception as e:
        pytest.fail(f"sql failed: {e}")


def test_limit_offset(spark_session):
    ids = [i for i in range(1024)]
    random.shuffle(ids)
    df = spark_session.createDataFrame([(id, f"user_{id}") for id in ids], ["id", "name"])
    df.createOrReplaceTempView("test_view")

    try:
        result = spark_session.sql("SELECT * FROM test_view ORDER BY id OFFSET 100 LIMIT 10")
        assert result.collect() == [(i, f"user_{i}") for i in range(100, 110)]

        result = spark_session.sql("SELECT * FROM test_view ORDER BY id LIMIT 10 OFFSET 100")
        assert result.collect() == [(i, f"user_{i}") for i in range(100, 110)]
    except Exception as e:
        pytest.fail(f"sql failed: {e}")
