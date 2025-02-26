from daft.daft import connect_start
from pyspark.sql import SparkSession as PySparkSession


class Builder:
    def __init__(self):
        self._builder = PySparkSession.builder

    def local(self):
        self._connection = connect_start()
        url = f"sc://0.0.0.0:{self._connection.port()}"
        return PySparkSession.builder.remote(url)

    def remote(self, url):
        if url.startswith("ray://"):
            import daft

            daft.context.set_runner_ray(address=url)
            self._connection = connect_start()
            url = f"sc://0.0.0.0:{self._connection.port()}"
            return PySparkSession.builder.remote(url)
        else:
            return PySparkSession.builder.remote(url)

    def getOrCreate(self):
        return SparkSession(self._builder.getOrCreate(), self._connection)


class SparkSession:
    builder = Builder()

    def __init__(self, spark_session, connection=None):
        self._spark_session = spark_session
        self._connection = connection

    def __repr__(self):
        return self._spark_session.__repr__()

    def __getattr__(self, name):
        return getattr(self._spark_session, name)

    def stop(self):
        self._spark_session.stop()
        self._connection.shutdown()

    __doc__ = property(lambda self: self._spark_session.__doc__)
