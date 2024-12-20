from __future__ import annotations

from pyspark.sql.functions import col


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
