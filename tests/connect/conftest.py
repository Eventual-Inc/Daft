from __future__ import annotations

from typing import Any

import pytest
from pyspark.sql import SparkSession

import daft


@pytest.fixture(params=["local_spark", "ray_spark"])
def spark_session(request):
    return request.getfixturevalue(request.param)


@pytest.fixture(scope="session")
def local_spark():
    """Fixture to create and clean up a Spark session.

    This fixture is available to all test files and creates a single
    Spark session for the entire test suite run.
    """
    import daft

    # Start Daft Connect server
    server = daft.spark.connect_start()

    url = server.spark_remote_url()

    # Initialize Spark Connect session
    session = SparkSession.builder.appName("DaftConfigTest").remote(url).getOrCreate()

    yield session

    # Cleanup
    server.shutdown()
    session.stop()


@pytest.fixture(scope="session")
def ray_spark():
    """Fixture to create and clean up a Spark session.

    This fixture is available to all test files and creates a single
    Spark session for the entire test suite run.
    """
    import daft

    # Start Daft Connect server
    server = daft.spark.connect_start_ray()

    url = server.spark_remote_url()

    # Initialize Spark Connect session
    session = SparkSession.builder.appName("DaftConfigTest").remote(url).getOrCreate()

    yield session

    # Cleanup
    server.shutdown()
    session.stop()


@pytest.fixture(scope="function")
def make_spark_df(spark_session):
    def _make_spark_df(data: dict[str, Any]):
        fields = [name for name in data]
        rows = list(zip(*[data[name] for name in fields]))
        return spark_session.createDataFrame(rows, fields)

    yield _make_spark_df


@pytest.fixture(scope="function")
def assert_spark_equals(spark_session):
    def _assert_spark_dfs_eq(df1, df2):
        if isinstance(df1, daft.DataFrame):
            df1 = df1.to_pandas()
        else:
            df1 = df1.toPandas()
        if isinstance(df2, daft.DataFrame):
            df2 = df2.to_pandas()
        else:
            df2 = df2.toPandas()

        assert df1.equals(df2)

    yield _assert_spark_dfs_eq
