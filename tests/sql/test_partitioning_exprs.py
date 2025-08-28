from __future__ import annotations

from datetime import datetime

import daft
from daft.sql.sql import SQLCatalog


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
    catalog = SQLCatalog({"test": df})
    expected = (
        df.select(
            daft.col("date").partitioning.days().alias("date_days"),
            daft.col("date").partitioning.hours().alias("date_hours"),
            daft.col("date").partitioning.months().alias("date_months"),
            daft.col("date").partitioning.years().alias("date_years"),
            daft.col("id").partitioning.iceberg_bucket(10).alias("id_bucket"),
            daft.col("id").partitioning.iceberg_truncate(10).alias("id_truncate"),
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
    actual = daft.sql(sql, catalog).collect().to_pydict()

    assert actual == expected
