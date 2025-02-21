from __future__ import annotations

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    """Fixture to create and clean up a Spark session.

    This fixture is available to all test files and creates a single
    Spark session for the entire test suite run.
    """
    from daft.daft import connect_start

    # Start Daft Connect server
    server = connect_start()

    url = f"sc://localhost:{server.port()}"

    # Initialize Spark Connect session
    session = SparkSession.builder.appName("DaftConfigTest").remote(url).getOrCreate()

    yield session

    # Cleanup
    server.shutdown()
    session.stop()


@pytest.fixture(scope="function")
def make_spark_df(spark_session):
    def make_df(data):
        fields = [name for name in data]
        rows = list(zip(*[data[name] for name in fields]))
        return spark_session.createDataFrame(rows, fields)

    yield make_df
