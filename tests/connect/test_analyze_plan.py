from __future__ import annotations


def test_analyze_plan(spark_session):
    data = [[1000, 99]]
    df1 = spark_session.createDataFrame(data, schema="Value int, Total int")
    df1.collect()
