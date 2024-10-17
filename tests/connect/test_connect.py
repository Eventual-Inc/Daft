# def test_apply_lambda
# def test_apply_module_func
# def test_apply_inline_func
# def test_apply_lambda_pyobj

from __future__ import annotations
import pytest

import dataclasses


from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.dataframe import DataFrame

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import numpy as np



from daft.daft import connect_start
import time




def test_abc():
    print("start of func")
    # connect_start()

    # sleep 2 seconds
    time.sleep(1)

    print("Start of main function")

    # Create a Spark session using Spark Connect
    spark: SparkSession = SparkSession.builder \
        .appName("SparkConnectExample") \
        .remote("sc://localhost:50051") \
        .getOrCreate()
    print("Spark session created")

    # Read the Parquet file back into a DataFrame
    df: DataFrame = spark.read.parquet("/Users/andrewgazelka/Projects/simple-spark-connect/increasing_id_data.parquet")
    print("DataFrame read from Parquet file")
    # The DataFrame remains unchanged:
    # +---+
    # | id|
    # +---+
    # |  0|
    # |  1|
    # |  2|
    # |  3|
    # |  4|
    # +---+

    print("DataFrame schema:")
    df.printSchema()
    # root
    #  |-- id: long (nullable = false)

    print("\nDataFrame content:")
    df.show()

    print("done showing")

    # Perform operations on the DataFrame
    # 1. filter(col("id") > 2): Select only rows where 'id' is greater than 2
    # 2. withColumn("id2", col("id") + 2): Add a new column 'id2' that is 'id' plus 2
    result: DataFrame = df.filter(col("id") > 2).withColumn("id2", col("id") + 2)


    print("\nFiltered and transformed DataFrame:")
    result.show()

    result_pandas = result.toPandas()
    # The resulting DataFrame looks like this:
    # +---+---+
    # | id|id2|
    # +---+---+
    # |  3|  5|
    # |  4|  6|
    # +---+---+
    # Explanation:
    # 1. Only rows with id > 2 are kept (3 and 4)
    # 2. A new column 'id2' is added with values id + 2

    # Stop the Spark session
    spark.sql("select * from increasing_id_data").show()

    spark.stop()
    print("Spark session stopped")

    # Waiting for 10 seconds
    time.sleep(2)

    print("End of main function")
# from daft.
#
#
# def add_1(x):
#     return x + 1
#
#
# def test_apply_module_func():
#     df = daft.from_pydict({"a": [1, 2, 3]})
#     df = df.with_column("a_plus_1", df["a"].apply(add_1, return_dtype=DataType.int32()))
#     assert df.to_pydict() == {"a": [1, 2, 3], "a_plus_1": [2, 3, 4]}
#
#
# def test_apply_lambda():
#     df = daft.from_pydict({"a": [1, 2, 3]})
#     df = df.with_column("a_plus_1", df["a"].apply(lambda x: x + 1, return_dtype=DataType.int32()))
#     assert df.to_pydict() == {"a": [1, 2, 3], "a_plus_1": [2, 3, 4]}
#
#
# def test_apply_inline_func():
#     def inline_add_1(x):
#         return x + 1
#
#     df = daft.from_pydict({"a": [1, 2, 3]})
#     df = df.with_column("a_plus_1", df["a"].apply(inline_add_1, return_dtype=DataType.int32()))
#     assert df.to_pydict() == {"a": [1, 2, 3], "a_plus_1": [2, 3, 4]}
#
#
# @dataclasses.dataclass
# class MyObj:
#     x: int
#
#
# def test_apply_obj():
#     df = daft.from_pydict({"obj": [MyObj(x=0), MyObj(x=0), MyObj(x=0)]})
#
#     def inline_mutate_obj(obj):
#         obj.x = 1
#         return obj
#
#     df = df.with_column("mut_obj", df["obj"].apply(inline_mutate_obj, return_dtype=DataType.python()))
#     result = df.to_pydict()
#     for mut_obj in result["mut_obj"]:
#         assert mut_obj.x == 1
