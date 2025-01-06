# Distributed Computing

By default, Daft runs using your local machine's resources and your operations are thus limited by the CPUs, memory and GPUs available to you in your single local development machine.

However, Daft has strong integrations with [Ray](https://www.ray.io) which is a distributed computing framework for distributing computations across a cluster of machines. Here is a snippet showing how you can connect Daft to a Ray cluster:

=== "🐍 Python"

    ```python
    import daft

    daft.context.set_runner_ray()
    ```

By default, if no address is specified Daft will spin up a Ray cluster locally on your machine. If you are running Daft on a powerful machine (such as an AWS P3 machine which is equipped with multiple GPUs) this is already very useful because Daft can parallelize its execution of computation across your CPUs and GPUs. However, if instead you already have your own Ray cluster running remotely, you can connect Daft to it by supplying an address:

=== "🐍 Python"

    ```python
    daft.context.set_runner_ray(address="ray://url-to-mycluster")
    ```

For more information about the `address` keyword argument, please see the [Ray documentation on initialization](https://docs.ray.io/en/latest/ray-core/api/doc/ray.init.html).


If you want to start a single node ray cluster on your local machine, you can do the following:

```bash
> pip install ray[default]
> ray start --head --port=6379
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

You can take the IP address and port and pass it to Daft:

=== "🐍 Python"

    ```python
    >>> import daft
    >>> daft.context.set_runner_ray("127.0.0.1:6379")
    DaftContext(_daft_execution_config=<daft.daft.PyDaftExecutionConfig object at 0x100fbd1f0>, _daft_planning_config=<daft.daft.PyDaftPlanningConfig object at 0x100fbd270>, _runner_config=_RayRunnerConfig(address='127.0.0.1:6379', max_task_backlog=None), _disallow_set_runner=True, _runner=None)
    >>> df = daft.from_pydict({
    ...   'text': ['hello', 'world']
    ... })
    2024-07-29 15:49:26,610	INFO worker.py:1567 -- Connecting to existing Ray cluster at address: 127.0.0.1:6379...
    2024-07-29 15:49:26,622	INFO worker.py:1752 -- Connected to Ray cluster.
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
