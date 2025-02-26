from __future__ import annotations

from typing import Any

import pytest

import daft


@pytest.fixture(params=["local_spark", "ray_spark"], scope="session")
def spark_session(request):
    return request.getfixturevalue(request.param)


@pytest.fixture(scope="session")
def local_spark():
    """Fixture to create and clean up a Spark session.

    This fixture is available to all test files and creates a single
    Spark session for the entire test suite run.
    """
    from daft.pyspark import SparkSession

    # Initialize Spark Connect session
    session = SparkSession.builder.appName("DaftConfigTest").local().getOrCreate()

    yield session


@pytest.fixture(scope="session")
def ray_spark():
    """Fixture to create and clean up a Spark session.

    This fixture is available to all test files and creates a single
    Spark session for the entire test suite run.
    """
    from daft.pyspark import SparkSession

    # Initialize Spark Connect session

    session = SparkSession.builder.appName("DaftConfigTest").remote("ray://localhost:10001").getOrCreate()

    yield session


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
