from __future__ import annotations

import pytest


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
