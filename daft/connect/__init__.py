"""Daft Connect provides a simple way to start a Spark server and connect to it from a client.

Additionally, Daft Connect provides a Ray remote class that wraps the the daft.daft.connect_start function.

Usage:
```py
from daft.daft import connect_start

connect_server = connect_start()  # optionally pass in a port `connect_start(port=1234)`

# Get the url to connect to the spark service
port = connect_server.port()
url = f"sc://localhost:{port}"

# Connect to the spark service
spark = SparkSession.builder.remote(url).getOrCreate()

spark.createDataFrame([("hello", 1), ("world", 2)], ["word", "count"]).show()

# Shutdown the server
connect_server.shutdown()
```

Ray Usage:


For ray, we provide an Actor class that can be used to start a daft connect server on your ray cluster.


```py
import ray
from daft.connect import DaftConnectRayAdaptor
from pyspark.sql import SparkSession

# we recommend using the detached lifetime for the server
# this will keep the server running even if the client disconnects
server = DaftConnectRayAdaptor.options(lifetime="detached").remote()

# Get the url to connect to the spark service
url = ray.get(server.spark_remote_url.remote())

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
from daft.connect import DaftConnectRayAdaptor

DaftConnectRayAdaptor.options(
  lifetime="detached",
  scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
    node_id=ray.get_runtime_context().get_node_id(),
    soft=False,
  )
  name = "daft-connect-server",
  get_if_exists=True,
).remote(port=9999)

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
    class DaftConnectRayAdaptor:
        """A Ray remote class that wraps the the daft.daft.connect_start function."""

        def __init__(self, port: int | None = None):
            import daft

            daft.context.set_runner_ray()
            self._server = connect_start(port)

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
