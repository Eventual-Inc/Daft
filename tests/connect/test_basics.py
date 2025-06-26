from __future__ import annotations

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import LongType, StringType, StructField, StructType


def test_alias(spark_session):
    # Create DataFrame from range(10)
    df = spark_session.range(10)

    # Simply rename the 'id' column to 'my_number'
    df_renamed = df.select(col("id").alias("my_number"))

    # Verify the alias was set correctly
    assert df_renamed.schema != df.schema, "Schema should be changed after alias"

    # Verify the data is unchanged but column name is different
    df_rows = df.collect()
    df_renamed_rows = df_renamed.collect()
    assert [row.id for row in df_rows] == [
        row.my_number for row in df_renamed_rows
    ], "Data should be unchanged after alias"


@pytest.mark.skip(
    reason="Currently an issue in the spark connect code. It always passes the inferred schema instead of the supplied schema. see: https://issues.apache.org/jira/browse/SPARK-50627"
)
def test_analyze_plan(spark_session):
    data = [[1000, 99]]
    df1 = spark_session.createDataFrame(data, schema="Value int, Total int")
    s = df1.schema

    # todo: this is INCORRECT but it is an issue with pyspark client
    # right now it is assert str(s) == "StructType([StructField('_1', LongType(), True), StructField('_2', LongType(), True)])"
    assert (
        str(s) == "StructType([StructField('Value', IntegerType(), True), StructField('Total', IntegerType(), True)])"
    )


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


def test_range_operation(spark_session):
    # Create a range using Spark
    # For example, creating a range from 0 to 9
    spark_range = spark_session.range(10)  # Creates DataFrame with numbers 0 to 9

    # Convert to Pandas DataFrame
    pandas_df = spark_range.toPandas()

    # Verify the DataFrame has expected values
    assert len(pandas_df) == 10, "DataFrame should have 10 rows"
    assert list(pandas_df["id"]) == list(range(10)), "DataFrame should contain values 0-9"


def test_range_collect(spark_session):
    # Create a range using Spark
    # For example, creating a range from 0 to 9
    spark_range = spark_session.range(10)  # Creates DataFrame with numbers 0 to 9

    # Collect the data
    collected_rows = spark_range.collect()

    # Verify the collected data has expected values
    assert len(collected_rows) == 10, "Should have 10 rows"
    assert [row["id"] for row in collected_rows] == list(range(10)), "Should contain values 0-9"


def test_drop(spark_session):
    # Create DataFrame from range(10)
    df = spark_session.range(10)

    # Drop the 'id' column
    df_dropped = df.drop("id")

    # Verify the drop was successful
    assert "id" not in df_dropped.columns, "Column 'id' should be dropped"
    assert len(df_dropped.columns) == len(df.columns) - 1, "Should have one less column after drop"

    # Verify the DataFrame has no columns after dropping all columns"
    assert len(df_dropped.columns) == 0, "DataFrame should have no columns after dropping 'id'"


def test_range_first(spark_session):
    spark_range = spark_session.range(10)
    first_row = spark_range.first()
    assert first_row["id"] == 0, "First row should have id=0"


def test_range_limit(spark_session):
    spark_range = spark_session.range(10)
    limited_df = spark_range.limit(5).toPandas()
    assert len(limited_df) == 5, "Limited DataFrame should have 5 rows"
    assert list(limited_df["id"]) == list(range(5)), "Limited DataFrame should contain values 0-4"


def test_filter(spark_session):
    # Create DataFrame from range(10)
    df = spark_session.range(10)

    # Filter for values less than 5
    df_filtered = df.filter(col("id") < 5)

    # Verify the schema is unchanged after filter
    assert df_filtered.schema == df.schema, "Schema should be unchanged after filter"

    # Verify the filtered data is correct
    df_filtered_pandas = df_filtered.toPandas()
    assert len(df_filtered_pandas) == 5, "Should have 5 rows after filtering < 5"
    assert all(df_filtered_pandas["id"] < 5), "All values should be less than 5"


def test_get_attr(spark_session):
    # Create DataFrame from range(10)
    df = spark_session.range(10)

    # Get column using df[...]
    # df.get_attr("id") is equivalent to df["id"]
    df_col = df["id"]

    # Check that column values match expected range
    values = df.select(df_col).collect()  # Changed to select column first
    assert len(values) == 10
    assert [row[0] for row in values] == list(range(10))  # Need to extract values from Row objects


def test_group_by(spark_session):
    # Create DataFrame from range(10)
    df = spark_session.range(10)

    # Add a column that will have repeated values for grouping
    df = df.withColumn("group", col("id") % 3)

    # Group by the new column and sum the ids in each group
    df_grouped = df.groupBy("group").sum("id")

    # Convert to pandas to verify the sums
    df_grouped_pandas = df_grouped.toPandas()

    # Sort by group to ensure consistent order for comparison
    df_grouped_pandas = df_grouped_pandas.sort_values("group").reset_index(drop=True)

    # Verify the expected sums for each group
    #    group  id
    # 0      2  15
    # 1      1  12
    # 2      0  18
    expected = {
        "group": [0, 1, 2],
        "id": [18, 12, 15],  # todo(correctness): should this be "id" for value here?
    }

    assert df_grouped_pandas["group"].tolist() == expected["group"]
    assert df_grouped_pandas["id"].tolist() == expected["id"]


def test_schema(spark_session):
    # Create DataFrame from range(10)
    df = spark_session.range(10)

    # Define the expected schema
    # in reality should be nullable=False, but daft has all our structs as nullable=True
    expected_schema = StructType([StructField("id", LongType(), nullable=True)])

    # Verify the schema is as expected
    assert df.schema == expected_schema, "Schema should match the expected schema"


def test_select(spark_session):
    # Create DataFrame from range(10)
    df = spark_session.range(10)

    # Select just the 'id' column
    df_selected = df.select(col("id"))

    # Verify the schema is unchanged since we selected same column
    assert df_selected.schema == df.schema, "Schema should be unchanged after selecting same column"
    assert len(df_selected.collect()) == 10, "Row count should be unchanged after select"

    # Verify the data is unchanged
    df_data = [row["id"] for row in df.collect()]
    df_selected_data = [row["id"] for row in df_selected.collect()]
    assert df_data == df_selected_data, "Data should be unchanged after select"


def test_show(spark_session, capsys):
    df = spark_session.range(10)
    df.show()
    captured = capsys.readouterr()
    expected = (
        "╭───────╮\n"
        "│ id    │\n"
        "│ ---   │\n"
        "│ Int64 │\n"
        "╞═══════╡\n"
        "│ 0     │\n"
        "├╌╌╌╌╌╌╌┤\n"
        "│ 1     │\n"
        "├╌╌╌╌╌╌╌┤\n"
        "│ 2     │\n"
        "├╌╌╌╌╌╌╌┤\n"
        "│ 3     │\n"
        "├╌╌╌╌╌╌╌┤\n"
        "│ 4     │\n"
        "├╌╌╌╌╌╌╌┤\n"
        "│ 5     │\n"
        "├╌╌╌╌╌╌╌┤\n"
        "│ 6     │\n"
        "├╌╌╌╌╌╌╌┤\n"
        "│ 7     │\n"
        "├╌╌╌╌╌╌╌┤\n"
        "│ 8     │\n"
        "├╌╌╌╌╌╌╌┤\n"
        "│ 9     │\n"
        "╰───────╯\n"
    )
    # This may fail locally depending on your terminal and how you run the test.
    # The show() command uses ANSI escape sequences to make the header bold.
    assert captured.out == expected


def test_take(spark_session):
    # Create DataFrame with 10 rows
    df = spark_session.range(10)

    # Take first 5 rows and collect
    result = df.take(5)

    # Verify the expected values
    expected = df.limit(5).collect()

    assert result == expected

    # Test take with more rows than exist
    result_large = df.take(20)
    expected_large = df.collect()
    assert result_large == expected_large  # Should return all existing rows


def test_numeric_equals(spark_session):
    """Test numeric equality comparison with NULL handling."""
    data = [(1, 10), (2, None)]
    df = spark_session.createDataFrame(data, ["id", "value"])

    result = df.withColumn("equals_20", F.col("value") == F.lit(20)).collect()

    assert result[0].equals_20 is False  # 10 == 20
    assert result[1].equals_20 is None  # NULL == 20


def test_string_equals(spark_session):
    """Test string equality comparison with NULL handling."""
    data = [(1, "apple"), (2, None)]
    df = spark_session.createDataFrame(data, ["id", "text"])

    result = df.withColumn("equals_banana", F.col("text") == F.lit("banana")).collect()

    assert result[0].equals_banana is False  # apple == banana
    assert result[1].equals_banana is None  # NULL == banana


@pytest.mark.skip(reason="We believe null-safe equals are not yet implemented")
def test_null_safe_equals(spark_session):
    """Test null-safe equality comparison."""
    data = [(1, 10), (2, None)]
    df = spark_session.createDataFrame(data, ["id", "value"])

    result = df.withColumn("null_safe_equals", F.col("value").eqNullSafe(F.lit(10))).collect()

    assert result[0].null_safe_equals is True  # 10 <=> 10
    assert result[1].null_safe_equals is False  # NULL <=> 10


def test_not(spark_session):
    """Test logical NOT operation with NULL handling."""
    data = [(True,), (False,), (None,)]
    df = spark_session.createDataFrame(data, ["value"])

    result = df.withColumn("not_value", ~F.col("value")).collect()

    assert result[0].not_value is False  # NOT True
    assert result[1].not_value is True  # NOT False
    assert result[2].not_value is None  # NOT NULL


def test_with_column(spark_session):
    # Create DataFrame from range(10)
    df = spark_session.range(10)

    # Add a new column that's a boolean indicating if id > 2
    df_with_col = df.withColumn("double_id", col("id") > 2)

    # Verify the schema has both columns
    assert "id" in df_with_col.schema.names, "Original column should still exist"
    assert "double_id" in df_with_col.schema.names, "New column should be added"

    # Verify the data is correct
    df_pandas = df_with_col.toPandas()
    assert (df_pandas["double_id"] == (df_pandas["id"] > 2)).all(), "New column should be greater than 2 comparison"


def test_with_columns_renamed(spark_session):
    # Test withColumnRenamed
    df = spark_session.range(5)
    renamed_df = df.withColumnRenamed("id", "number")

    collected = renamed_df.collect()
    assert len(collected) == 5
    assert "number" in renamed_df.columns
    assert "id" not in renamed_df.columns
    assert [row["number"] for row in collected] == list(range(5))

    # todo: this edge case is a spark connect bug; it will only send rename of id -> character over protobuf
    # # Test withColumnsRenamed
    # df = spark_session.range(2)
    # renamed_df = df.withColumnsRenamed({"id": "number", "id": "character"})
    #
    # collected = renamed_df.collect()
    # assert len(collected) == 2
    # assert set(renamed_df.columns) == {"number", "character"}
    # assert "id" not in renamed_df.columns
    # assert [(row["number"], row["character"]) for row in collected] == [(0, 0), (1, 1)]


def test_with_column_renamed_preserves_other_columns(spark_session):
    """Test that withColumnRenamed preserves non-renamed columns."""
    # Create a DataFrame with multiple columns
    data = [(1, "alice", 25), (2, "bob", 30), (3, "charlie", 35)]
    df = spark_session.createDataFrame(data, ["id", "name", "age"])

    # Rename only the 'name' column to 'full_name'
    renamed_df = df.withColumnRenamed("name", "full_name")

    # Verify the schema has all columns
    assert "id" in renamed_df.columns, "Original 'id' column should be preserved"
    assert "age" in renamed_df.columns, "Original 'age' column should be preserved"
    assert "full_name" in renamed_df.columns, "Renamed column should exist"
    assert "name" not in renamed_df.columns, "Original 'name' column should be gone"
    assert len(renamed_df.columns) == 3, "Should have same number of columns"

    # Verify the data is correct
    collected = renamed_df.collect()
    assert len(collected) == 3
    assert collected[0]["id"] == 1
    assert collected[0]["full_name"] == "alice"
    assert collected[0]["age"] == 25


def test_with_column_renamed_chained(spark_session):
    """Test that multiple withColumnRenamed calls work correctly."""
    # Create a DataFrame with multiple columns
    data = [(1, "alice", 25), (2, "bob", 30), (3, "charlie", 35)]
    df = spark_session.createDataFrame(data, ["id", "name", "age"])

    # Chain multiple renames
    renamed_df = df.withColumnRenamed("name", "full_name").withColumnRenamed("age", "years")

    # Verify the schema has all columns with correct names
    assert "id" in renamed_df.columns, "Original 'id' column should be preserved"
    assert "full_name" in renamed_df.columns, "First renamed column should exist"
    assert "years" in renamed_df.columns, "Second renamed column should exist"
    assert "name" not in renamed_df.columns, "Original 'name' column should be gone"
    assert "age" not in renamed_df.columns, "Original 'age' column should be gone"
    assert len(renamed_df.columns) == 3, "Should have same number of columns"

    # Verify the data is correct
    collected = renamed_df.collect()
    assert len(collected) == 3
    assert collected[0]["id"] == 1
    assert collected[0]["full_name"] == "alice"
    assert collected[0]["years"] == 25


def test_explain(spark_session, capsys):
    df = spark_session.createDataFrame([(1, 2), (2, 2)], ["a", "b"])
    df.explain()
    actual = capsys.readouterr()

    expected = """* Project: col(a), col(b)
|
* Source:
|   Number of partitions = 1
|   Output schema = a#Int64, b#Int64

"""
    assert actual.out == expected


def test_when(spark_session):
    df = spark_session.createDataFrame([(1), (None), (10)], ["a"])
    result = df.select(F.when(col("a").isNull(), True)).collect()
    assert result == [Row(a=None), Row(a=True), Row(a=None)]


def test_when_otherwise(spark_session):
    df = spark_session.createDataFrame([(1), (None), (10)], ["a"])

    result = df.select(F.when(col("a").isNull(), True).otherwise(False)).collect()

    assert result == [Row(a=True), Row(a=False), Row(a=True)]
