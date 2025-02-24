"""Daft Connect provides a simple way to start a Spark server and connect to it from a client.

Additionally, Daft Connect provides a Ray remote class that wraps the the daft.daft.connect_start function.

Usage:
```py
import daft

server = daft.spark.connect_start()  # optionally pass in a port `connect_start(port=1234)`

# Get the url to connect to the spark service
url = server.spark_remote_url()

# Connect to the spark service
spark = SparkSession.builder.remote(url).getOrCreate()

spark.createDataFrame([("hello", 1), ("world", 2)], ["word", "count"]).show()

# Shutdown the server
server.shutdown()
```

Ray Usage:


For ray, we provide an Actor class that can be used to start a daft connect server on your ray cluster.


```py
import ray
import daft
from pyspark.sql import SparkSession

# we recommend using the detached lifetime for the server
# this will keep the server running even if the client disconnects
server = daft.spark.connect_start_ray(lifetime="detached")

# Get the url to connect to the spark service
url = server.spark_remote_url()

# Connect to the spark service
spark = SparkSession.builder.remote(url).getOrCreate()

spark.createDataFrame([("hello", 1), ("world", 2)], ["word", "count"]).show()
```


Tip:
You can force the server to start on the head node. This is useful if you want the spark service to be accessible using the same address as the ray cluster.

```py
# my_script.py
# submit this code to the head node
#
import ray
import daft

daft.spark.connect_start_ray(
    port=9999,
    run_on_head_node=True,
    lifetime="detached",
    name="daft-connect-server",
    get_if_exists=True,
)
```

```sh
ray job submit -- python my_script.py --head
```


Then elsewhere, you can connect to the server using the same address as the ray cluster. (Note: this assumes that the specified port is open on the head node)

```py
from pyspark.sql import SparkSession

RAY_ADDRESS='http://127.0.0.1'

url = f"sc://{RAY_ADDRESS}:9999"
spark = SparkSession.builder.remote(url).getOrCreate()
```
"""

from daft.daft import connect_start

try:
    import ray

    @ray.remote
    class DaftConnectRayAdaptorActor:
        """A Ray remote class that wraps the the daft.daft.connect_start function."""

        def __init__(self, port: int | None = None):
            import daft

            daft.context.set_runner_ray()
            self._server = connect_start(port)

        def spark_remote_url(self):
            """Returns the remote url to connect to the spark service."""
            ip = ray._private.services.get_node_ip_address()
            return f"sc://{ip}:{self._server.port()}"

        def port(self):
            """Returns the port that the server is running on."""
            return self._server.port()

        def shutdown(self):
            """Shuts down the server."""
            self._server.shutdown()

    class DaftConnectRayAdaptor:
        def __init__(self, actor: DaftConnectRayAdaptorActor):
            self._actor = actor

        def spark_remote_url(self):
            """Returns the remote url to connect to the spark service."""
            return ray.get(self._actor.spark_remote_url.remote())

        def port(self):
            """Returns the port that the server is running on."""
            return ray.get(self._actor.port.remote())

        def shutdown(self):
            """Shuts down the server."""
            ray.get(self._actor.shutdown.remote())

    def connect_start_ray(port: int | None = None, run_on_head_node: bool = False, **kwargs) -> DaftConnectRayAdaptor:
        """Starts a Daft Connect server on the ray cluster.

        Args:
            port: The port to start the server on. If not provided, a random available port will be chosen.
            run_on_head_node: If True, the server will be started on the head node of the ray cluster. You must submit the script to the head node (via `ray job submit -- python my_script.py --head`)
            **kwargs: Additional arguments to pass to the ray actor
        """
        if run_on_head_node:
            kwargs["scheduling_strategy"] = ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                node_id=ray.get_runtime_context().get_node_id(),
                soft=False,
            )
        return DaftConnectRayAdaptor(DaftConnectRayAdaptorActor.options(**kwargs).remote(port))

    __all__ = ["DaftConnectRayAdaptor", "DaftConnectRayAdaptorActor", "connect_start", "connect_start_ray"]
except ImportError:
    __all__ = ["connect_start"]
