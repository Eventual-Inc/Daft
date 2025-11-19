from __future__ import annotations

from datetime import datetime

import daft


def test_partitioning_exprs():
    df = daft.from_pydict(
        {
            "id": [1, 2, 3, 4, 5],
            "date": [
                datetime(2024, 1, 1),
                datetime(2024, 2, 1),
                datetime(2024, 3, 1),
                datetime(2024, 4, 1),
                datetime(2024, 5, 1),
            ],
        }
    )
    bindings = {"test": df}
    expected = (
        df.select(
            daft.col("date").partition_days().alias("date_days"),
            daft.col("date").partition_hours().alias("date_hours"),
            daft.col("date").partition_months().alias("date_months"),
            daft.col("date").partition_years().alias("date_years"),
            daft.col("id").partition_iceberg_bucket(10).alias("id_bucket"),
            daft.col("id").partition_iceberg_truncate(10).alias("id_truncate"),
        )
        .collect()
        .to_pydict()
    )
    sql = """
    SELECT
        partitioning_days(date) AS date_days,
        partitioning_hours(date) AS date_hours,
        partitioning_months(date) AS date_months,
        partitioning_years(date) AS date_years,
        partitioning_iceberg_bucket(id, 10) AS id_bucket,
        partitioning_iceberg_truncate(id, 10) AS id_truncate
    FROM test
    """
    actual = daft.sql(sql, **bindings).collect().to_pydict()

    assert actual == expected
