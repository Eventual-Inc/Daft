from __future__ import annotations


def test_analyze_plan(spark_session):
    data = [[1000, 99]]
    df1 = spark_session.createDataFrame(data, schema="Value int, Total int")
    s = df1.schema

    # todo: this is INCORRECT but it is an issue with pyspark client
    # ideally should be StructType([StructField('Value', IntegerType(), True), StructField('Total', IntegerType(), True)])
    assert str(s) == "StructType([StructField('_1', LongType(), True), StructField('_2', LongType(), True)])"
