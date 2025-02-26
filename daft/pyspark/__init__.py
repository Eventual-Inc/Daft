"""# PySpark.

The `daft.pyspark` module provides a way to create a PySpark session that can be run locally or backed by a ray cluster.

This serves as a way to run the daft query engine, but with a spark compatible API.


## Example

```py
from daft.pyspark import SparkSession
from pyspark.sql.functions import col

# create a local spark session
spark = SparkSession.builder.local().getOrCreate()

# alternatively, connect to a ray cluster
spark = SparkSession.builder.remote("ray://<ray-ip>:10001").getOrCreate()

# use spark as you would with the native spark library, but with a daft backend!

spark.createDataFrame([{"hello": "world"}]).select(col("hello")).show()

# stop the spark session
spark.stop()
```
"""

from daft.daft import connect_start
from pyspark.sql import SparkSession as PySparkSession


class Builder:
    def __init__(self):
        self._builder = PySparkSession.builder

    def local(self):
        self._connection = connect_start()
        url = f"sc://0.0.0.0:{self._connection.port()}"
        self._builder = PySparkSession.builder.remote(url)
        return self

    def remote(self, url):
        if url.startswith("ray://"):
            import daft

            if url.startswith("ray://localhost"):
                daft.context.set_runner_ray(noop_if_initialized=True)
            else:
                daft.context.set_runner_ray(address=url, noop_if_initialized=True)
            self._connection = connect_start()
            url = f"sc://0.0.0.0:{self._connection.port()}"
            self._builder = PySparkSession.builder.remote(url)
            return self
        else:
            self._builder = PySparkSession.builder.remote(url)
            return self

    def getOrCreate(self):
        return SparkSession(self._builder.getOrCreate(), self._connection)

    def __getattr__(self, name):
        attr = getattr(self._builder, name)
        if callable(attr):

            def wrapped(*args, **kwargs):
                result = attr(*args, **kwargs)
                # If result is the original builder, return self instead
                return self if result == self._builder else result

            return wrapped
        return attr

    __doc__ = property(lambda self: self._spark_session.__doc__)


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
