from __future__ import annotations

import pytest

import daft
from daft.pyspark import SparkSession


@pytest.fixture(params=["local_spark", "ray_spark"], scope="session")
def spark_session(request):
    return request.getfixturevalue(request.param)


@pytest.fixture(scope="session")
def local_spark():
    session = SparkSession.builder.appName("DaftConfigTest").local().getOrCreate()
    yield session
    session.stop()


@pytest.fixture(scope="session")
def ray_spark():
    session = SparkSession.builder.appName("DaftConfigTest").remote("ray://localhost:10001").getOrCreate()
    yield session
    session.stop()


@pytest.fixture(scope="function")
def make_spark_df(spark_session):
    def make_df(data):
        fields = [name for name in data]
        rows = list(zip(*[data[name] for name in fields]))
        return spark_session.createDataFrame(rows, fields)

    yield make_df


@pytest.fixture(scope="function")
def spark_to_daft():
    def inner(spark_df):
        return daft.from_pandas(spark_df.toPandas())

    yield inner
