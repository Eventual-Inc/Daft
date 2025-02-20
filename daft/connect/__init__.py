from daft.daft import connect_start

try:
    import ray

    @ray.remote
    class DaftConnectRayAdaptor:
        """A Ray remote class that wraps the the daft.daft.connect_start function.

        Usage:

            ```py
            import ray
            from daft.connect import DaftConnectRayAdaptor
            from pyspark.sql import SparkSession

            server = DaftConnectRayAdaptor.options(lifetime="detached").remote()
            url = ray.get(server.spark_remote_url.remote())
            spark = SparkSession.builder.remote(url).getOrCreate()

            spark.createDataFrame([("hello", 1), ("world", 2)], ["word", "count"]).show()
            ```
        """

        def __init__(self):
            self._server = connect_start()

        def spark_remote_url(self):
            """Returns the remote url to connect to the spark service."""
            ip = ray._private.services.get_node_ip_address()
            return f"sc://{ip}:{self._server.port()}"

        def shutdown(self):
            """Shuts down the server."""
            self._server.shutdown()

    __all__ = ["DaftConnectRayAdaptor", "connect_start"]
except ImportError:
    __all__ = ["connect_start"]
