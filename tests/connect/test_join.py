from __future__ import annotations

from pyspark.sql.functions import col


def test_join(spark_session):
    # Create two DataFrames with overlapping IDs
    df1 = spark_session.range(5)
    df2 = spark_session.range(3, 7)

    # Perform inner join on 'id' column
    joined_df = df1.join(df2, "id", "inner")

    # Verify join results using collect()
    joined_ids = {row.id for row in joined_df.select("id").collect()}
    assert joined_ids == {3, 4}, "Inner join should only contain IDs 3 and 4"

    # Test left outer join
    left_joined_df = df1.join(df2, "id", "left")
    left_joined_ids = {row.id for row in left_joined_df.select("id").collect()}
    assert left_joined_ids == {0, 1, 2, 3, 4}, "Left join should keep all rows from left DataFrame"

    # Test right outer join
    right_joined_df = df1.join(df2, "id", "right")
    right_joined_ids = {row.id for row in right_joined_df.select("id").collect()}
    assert right_joined_ids == {3, 4, 5, 6}, "Right join should keep all rows from right DataFrame"



def test_cross_join(spark_session):
    # Create two small DataFrames to demonstrate cross join
    #   df_left:  [0, 1]
    #   df_right: [10, 11]
    #   Expected result will be all combinations:
    #   id1  id2
    #    0   10
    #    0   11  
    #    1   10
    #    1   11
    df_left = spark_session.range(2)
    df_right = spark_session.range(10, 12).withColumnRenamed("id", "id2")

    # Perform cross join - this creates cartesian product of both DataFrames
    cross_joined_df = df_left.crossJoin(df_right)

    # Convert to pandas for easier verification
    result_df = cross_joined_df.toPandas()

    # Verify we get all 4 combinations (2 x 2 = 4 rows)
    assert len(result_df) == 4, "Cross join should produce 4 rows (2x2 cartesian product)"

    # Verify all expected combinations exist
    expected_combinations = {(0, 10), (0, 11), (1, 10), (1, 11)}
    actual_combinations = {(row["id"], row["id2"]) for _, row in result_df.iterrows()}
    assert actual_combinations == expected_combinations, "Cross join should contain all possible combinations"


