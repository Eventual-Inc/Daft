from __future__ import annotations

from pyspark.sql import Row


def test_distinct(spark_session):
    # Create simple DataFrame with single column
    data = [(1,), (2,), (1,)]
    df = spark_session.createDataFrame(data, ["id"]).distinct()

    assert df.count() == 2, "DataFrame should have 2 rows"

    assert df.sort().collect() == [Row(id=1), Row(id=2)], "DataFrame should contain expected values"
