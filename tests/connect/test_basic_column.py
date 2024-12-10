from __future__ import annotations

from pyspark.sql.functions import col
from pyspark.sql.types import StringType


def test_column_alias(spark_session):
    df = spark_session.range(10)
    df_alias = df.select(col("id").alias("my_number"))
    assert "my_number" in df_alias.columns, "alias should rename column"
    assert df_alias.toPandas()["my_number"].equals(df.toPandas()["id"]), "data should be unchanged"


def test_column_cast(spark_session):
    df = spark_session.range(10)
    df_cast = df.select(col("id").cast(StringType()))
    assert df_cast.schema.fields[0].dataType == StringType(), "cast should change data type"
    assert df_cast.toPandas()["id"].dtype == "object", "cast should change pandas dtype to object/string"


def test_column_null_checks(spark_session):
    df = spark_session.range(10)
    df_null = df.select(col("id").isNotNull().alias("not_null"), col("id").isNull().alias("is_null"))
    assert df_null.toPandas()["not_null"].iloc[0], "isNotNull should be True for non-null values"
    assert not df_null.toPandas()["is_null"].iloc[0], "isNull should be False for non-null values"


def test_column_name(spark_session):
    df = spark_session.range(10)
    df_name = df.select(col("id").name("renamed_id"))
    assert "renamed_id" in df_name.columns, "name should rename column"
    assert df_name.toPandas()["renamed_id"].equals(df.toPandas()["id"]), "data should be unchanged"


# TODO: Uncomment when https://github.com/Eventual-Inc/Daft/issues/3433 is fixed
# def test_column_desc(spark_session):
#     df = spark_session.range(10)
#     df_attr = df.select(col("id").desc())
#     assert df_attr.toPandas()["id"].iloc[0] == 9, "desc should sort in descending order"


# TODO: Add test when extract value is implemented
# def test_column_getitem(spark_session):
#     df = spark_session.range(10)
#     df_item = df.select(col("id")[0])
#     assert df_item.toPandas()["id"].iloc[0] == 0, "getitem should return first element"
