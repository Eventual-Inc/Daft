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

### Dynamic Autoscaling

Daft on Ray can automatically adjust cluster size to match workload pressure.

- Scale-up: When the scheduler detects backlog pressure beyond capacity, Daft asks Ray to provision more workers.
- Scale-down (simplified): When `DAFT_AUTOSCALING_DOWNSCALE_ENABLED` is true and there are idle RaySwordfishActor, Daft retires eligible idle RaySwordfishActor. Idle candidacy is controlled by `DAFT_AUTOSCALING_DOWNSCALE_IDLE_SECONDS`. Scaling down here refers to reclaiming idle RaySwordfishActors. The release of Ray worker nodes is managed by its built-in autoscaling mechanism. For example, on Kubernetes, worker nodes are released after being idle for 1 minute.

#### Environment Variables

Use these environment variables to tune autoscaling behavior. You can set them in your shell or via `os.environ` in Python.

-   `DAFT_AUTOSCALING_DOWNSCALE_ENABLED`
    -   **Meaning**: Enables or disables the downscaling logic in the scheduler.
    -   **Default**: `true`
    -   **Accepted Values**: `"1"/"true"` or `"0"/"false"`
    -   **Effect**: When disabled, only scale-up will occur. The final downscale on job completion is also skipped.

-   `DAFT_AUTOSCALING_DOWNSCALE_IDLE_SECONDS`
    -   **Meaning**: The minimum time a worker must be idle to be considered a candidate for retirement.
    -   **Default**: `60`
    -   **Accepted Values**: An `integer` representing seconds.
    -   **Effect**: Prevents recently used workers from being retired too quickly.

-   `DAFT_AUTOSCALING_THRESHOLD`
    -   **Meaning**: The scale-up threshold used by the `DefaultScheduler`.
    -   **Default**: `1.25`
    -   **Accepted Values**: A `float` greater than or equal to `1.0`.
    -   **Effect**: If the backlog-to-capacity ratio exceeds this threshold, the scheduler will request more workers.

-   `DAFT_SCHEDULER_LINEAR` (Optional)
    -   **Meaning**: Selects the `LinearScheduler` instead of the default scheduler.
    -   **Default**: `false`
    -   **Accepted Values**: `"1"/"true"` or `"0"/"false"`
    -   **Effect**: Switches the scheduling algorithm used by the `SchedulerActor`.

!!! note
    Autoscaling requires a Ray cluster configured with the Ray Autoscaler (e.g., in your cluster YAML, define node types with `min_workers` and `max_workers`). A simple head-only local cluster started with `ray start --head` will not automatically provision new nodes.

#### Usage Examples

**Example A: Local Cluster with Autoscaling**

First, start a Ray cluster. For a true autoscaling experience, you would typically use a cluster configuration file. For local testing, you can simulate this by ensuring your environment is correctly set up.

```bash
# Configure downscaling aggressively
export DAFT_AUTOSCALING_DOWNSCALE_ENABLED=1
export DAFT_AUTOSCALING_MIN_SURVIVOR_WORKERS=1
export DAFT_AUTOSCALING_DOWNSCALE_IDLE_SECONDS=0

# Tune scale-up sensitivity
export DAFT_AUTOSCALING_THRESHOLD=1.25
```

Now, run a Daft script that generates a significant backlog.

```python
import time
import daft
import ray
from daft import col, udf
from daft.datatype import DataType

ray.init(address="auto")
daft.context.set_runner_ray()

@udf(return_dtype=DataType.int64())
class SleepyUdf:
    def __init__(self):
        self._sleep = 0.2

    def __call__(self, x):
        time.sleep(self._sleep)
        return x

sleepy = SleepyUdf.with_concurrency(4)
df = daft.from_pydict({"x": list(range(400))}).repartition(100, "x")
_ = df.select(sleepy(col("x"))).collect()

print("Workload finished. Downscaling should occur if cluster was idle.")
```

**Expected Behavior**: The Daft scheduler will detect a large backlog relative to available capacity, triggering a scale-up request to Ray. After the workload completes and workers become idle, the scheduler retires idle workers.
