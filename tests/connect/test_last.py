from __future__ import annotations

# import time
import pytest
from pyspark.sql import SparkSession


@pytest.fixture
def spark_session():
    """Fixture to create and clean up a Spark session."""
    from daft.daft import connect_start

    # Start Daft Connect server
    server = connect_start("sc://localhost:50051")

    # Initialize Spark Connect session
    session = SparkSession.builder.appName("DaftConfigTest").remote("sc://localhost:50051").getOrCreate()

    yield session

    # Cleanup
    server.shutdown()
    session.stop()
    # time.sleep(2)  # Allow time for session cleanup


def test_last_operation(spark_session):
    # Create a range using Spark
    # For example, creating a range from 0 to 9
    spark_range = spark_session.range(10)  # Creates DataFrame with numbers 0 to 9

    # Get the last row using last()
    last_row = spark_range.tail(1)

    # Verify the last row value is 9
    assert last_row[0] == 9
