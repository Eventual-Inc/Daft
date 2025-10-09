# Running on Ray

You can run Daft on Ray in multiple ways:

### Simple Local Setup

If you want to start a single node ray cluster on your local machine, you can do the following:

```bash
pip install "daft[ray]"
ray start --head
```

This should output something like:

```
Usage stats collection is enabled. To disable this, add `--disable-usage-stats` to the command that starts the cluster, or run the following command: `ray disable-usage-stats` before starting the cluster. See https://docs.ray.io/en/master/cluster/usage-stats.html for more details.

Local node IP: 127.0.0.1

--------------------
Ray runtime started.
--------------------

...
```

You can take the IP address and port and pass it to Daft with [`set_runner_ray`][daft.context.set_runner_ray]:

```python
>>> import daft
>>> daft.context.set_runner_ray("ray://127.0.0.1:10001")
DaftContext(_daft_execution_config=<daft.daft.PyDaftExecutionConfig object at 0x100fbd1f0>, _daft_planning_config=<daft.daft.PyDaftPlanningConfig object at 0x100fbd270>, _runner_config=_RayRunnerConfig(address='127.0.0.1:10001', max_task_backlog=None), _disallow_set_runner=True, _runner=None)

>>> df = daft.from_pydict({
...   'text': ['hello', 'world']
... })
2024-07-29 15:49:26,610 INFO worker.py:1567 -- Connecting to existing Ray cluster at address: 127.0.0.1:10001...
2024-07-29 15:49:26,622 INFO worker.py:1752 -- Connected to Ray cluster.

>>> print(df)
╭───────╮
│ text  │
│ ---   │
│ Utf8  │
╞═══════╡
│ hello │
├╌╌╌╌╌╌╌┤
│ world │
╰───────╯

(Showing first 2 of 2 rows)
```

By default, if no address is specified, Daft will spin up a Ray cluster locally on your machine. If you are running Daft on a powerful machine (such as an AWS P3 machine which is equipped with multiple GPUs) this is already very useful because Daft can parallelize its execution of computation across your CPUs and GPUs.

!!! tip "Deploy on Kubernetes"

    Looking to try out Daft with Ray on Kubernetes? Check out our [Kubernetes quickstart](kubernetes.md).

### Connecting to Remote Ray Clusters

If you already have your own Ray cluster running remotely, you can connect Daft to it by supplying an address with [`set_runner_ray`][daft.context.set_runner_ray]:

=== "🐍 Python"

    ```python
    daft.context.set_runner_ray(address="ray://url-to-mycluster")
    ```

For more information about the `address` keyword argument, please see the [Ray documentation on initialization](https://docs.ray.io/en/latest/ray-core/api/doc/ray.init.html).

### Using Ray Client

The Ray client is a quick way to get started with running tasks and retrieving their results on Ray using Python.

!!! warning "Warning"

    To run tasks using the Ray client, the version of Daft and the minor version (eg. 3.9, 3.10) of Python must match between client and server.

```python
import daft
import ray

# Refer to the note under "Ray Job" for details on "runtime_env"
ray.init("ray://<head_node_host>:10001", runtime_env={"pip": ["daft"]})

# Starts the Ray client and tells Daft to use Ray to execute queries
# If ray.init() has already been called, it uses the existing client
daft.context.set_runner_ray("ray://<head_node_host>:10001")

df = daft.from_pydict({
    "a": [3, 2, 5, 6, 1, 4],
    "b": [True, False, False, True, True, False]
})
df = df.where(df["b"]).sort(df["a"])

# Daft executes the query remotely and returns a preview to the client
df.collect()
```

```{title="Output"}
╭───────┬─────────╮
│ a     ┆ b       │
│ ---   ┆ ---     │
│ Int64 ┆ Boolean │
╞═══════╪═════════╡
│ 1     ┆ true    │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ 3     ┆ true    │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ 6     ┆ true    │
╰───────┴─────────╯

(Showing first 3 of 3 rows)
```

### Using Ray Jobs

Ray jobs allow for more control and observability over using the Ray client. In addition, your entire code runs on Ray, which means it is not constrained by the compute, network, library versions, or availability of your local machine.

```python
# wd/job.py

import daft

def main():
    # call without any arguments to connect to Ray from the head node
    daft.context.set_runner_ray()

    # ... Run Daft commands here ...

if __name__ == "__main__":
    main()
```

To submit this script as a job, use the Ray CLI, which can be installed with `pip install "ray[default]"`.

```bash
ray job submit \
    --working-dir wd \
    --address "http://<head_node_host>:8265" \
    --runtime-env-json '{"pip": ["daft"]}' \
    -- python job.py
```

!!! note "Note"

    The runtime env parameter specifies that Daft should be installed on the Ray workers. Alternative methods of including Daft in the worker dependencies can be found [here](https://docs.ray.io/en/latest/ray-core/handling-dependencies.html).

For more information about Ray jobs, see [Ray docs -> Ray Jobs Overview](https://docs.ray.io/en/latest/cluster/running-applications/job-submission/index.html).
