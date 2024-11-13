from __future__ import annotations

import time

import pytest
from pyspark.sql import SparkSession

from daft.daft import connect_start


@pytest.fixture
def spark_session():
    """Fixture to create and clean up a Spark session."""

    # Start Daft Connect server
    server = connect_start("sc://localhost:50051")

    # Initialize Spark Connect session
    session = SparkSession.builder.appName("DaftConfigTest").remote("sc://localhost:50051").getOrCreate()

    yield session

    # Cleanup
    server.shutdown()
    session.stop()
    time.sleep(2)  # Allow time for session cleanup


def test_range_operation(spark_session, tmpdir):
    # Read the parquet file
    # df = spark_session.read.parquet(
    #     "/Users/andrewgazelka/Projects/Daft/docs/source/user_guide/fotw/data/sample_taxi.parquet"
    # )
    df = spark_session.range(10)

    # Write to a temporary parquet file using tmpdir
    # temp_path = str(tmpdir / "test_parquet_output.parquet")
    temp_path = "/Users/andrewgazelka/Projects/Daft/lol"
    df.write.parquet(temp_path)

    # # Read back the written file
    df2 = spark_session.read.parquet(temp_path)

    # # Compare the two dataframes
    # assert df.collect() == df2.collect()
