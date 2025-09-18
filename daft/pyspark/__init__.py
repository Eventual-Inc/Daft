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

spark = SparkSession.builder.remote("ray://<HEAD_IP>:10001").getOrCreate()
# you can use `ray get-head-ip <cluster_config.yaml>` to get the head ip!
# use spark as you would with the native spark library, but with a daft backend!

spark.createDataFrame([{"hello": "world"}]).select(col("hello")).show()

# stop the spark session
spark.stop()
```
"""

from typing import Any
from daft.daft import ConnectionHandle, connect_start
from pyspark.sql import SparkSession as PySparkSession


class Builder:
    def __init__(self) -> None:
        self._builder = PySparkSession.builder

    def local(self) -> "Builder":
        self._connection = connect_start()
        url = f"sc://localhost:{self._connection.port()}"
        self._builder = PySparkSession.builder.remote(url)
        return self

    def remote(self, url: str) -> "Builder":
        if url.startswith("ray://"):
            import daft

            if url.startswith("ray://localhost") or url.startswith("ray://127.0.0.1"):
                daft.context.set_runner_ray(noop_if_initialized=True)
            else:
                daft.context.set_runner_ray(address=url, noop_if_initialized=True)
            self._connection = connect_start()
            url = f"sc://localhost:{self._connection.port()}"
            self._builder = PySparkSession.builder.remote(url)
            return self
        else:
            self._builder = PySparkSession.builder.remote(url)
            return self

    def getOrCreate(self) -> "SparkSession":
        return SparkSession(self._builder.getOrCreate(), self._connection)

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._builder, name)
        if callable(attr):

            def wrapped(*args: Any, **kwargs: Any) -> Any:
                result = attr(*args, **kwargs)
                # If result is the original builder, return self instead
                return self if result == self._builder else result

            return wrapped
        return attr

    __doc__ = property(lambda self: self._spark_session.__doc__)  # type: ignore


class SparkSession:
    builder = Builder()

    def __init__(self, spark_session: PySparkSession, connection: ConnectionHandle) -> None:
        self._spark_session = spark_session
        self._connection = connection

    def __repr__(self) -> str:
        return self._spark_session.__repr__()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._spark_session, name)

    def stop(self) -> None:
        self._spark_session.stop()
        self._connection.shutdown()

    __doc__ = property(lambda self: self._spark_session.__doc__)  # type: ignore
