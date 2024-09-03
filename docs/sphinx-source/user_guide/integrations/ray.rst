Ray
===

`Ray <https://docs.ray.io/en/latest/ray-overview/index.html>`_ is an open-source framework for distributed computing.

Daft's native support for Ray enables you to run distributed DataFrame workloads at scale.

Usage
-----

You can run Daft on Ray in two ways: by using the `Ray Client <https://docs.ray.io/en/latest/cluster/running-applications/job-submission/ray-client.html>`_ or by submitting a Ray job.

Ray Client
**********
The Ray client is a quick way to get started with running tasks and retrieving their results on Ray using Python.

.. WARNING::
    To run tasks using the Ray client, the version of Daft and the minor version (eg. 3.9, 3.10) of Python must match between client and server.

Here's an example of how you can use the Ray client with Daft:

.. code:: python

    >>> import daft
    >>> import ray
    >>>
    >>> # Refer to the note under "Ray Job" for details on "runtime_env"
    >>> ray.init("ray://<head_node_host>:10001", runtime_env={"pip": ["getdaft"]})
    >>>
    >>> # Starts the Ray client and tells Daft to use Ray to execute queries
    >>> # If ray.init() has already been called, it uses the existing client
    >>> daft.context.set_runner_ray("ray://<head_node_host>:10001")
    >>>
    >>> df = daft.from_pydict({
    >>>     "a": [3, 2, 5, 6, 1, 4],
    >>>     "b": [True, False, False, True, True, False]
    >>> })
    >>> df = df.where(df["b"]).sort(df["a"])
    >>>
    >>> # Daft executes the query remotely and returns a preview to the client
    >>> df.collect()
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

Ray Job
*******
Ray jobs allow for more control and observability over using the Ray client. In addition, your entire code runs on Ray, which means it is not constrained by the compute, network, library versions, or availability of your local machine.

.. code:: python

    # wd/job.py

    import daft

    def main():
        # call without any arguments to connect to Ray from the head node
        daft.context.set_runner_ray()

        # ... Run Daft commands here ...

    if __name__ == "__main__":
        main()

To submit this script as a job, use the Ray CLI, which can be installed with `pip install "ray[default]"`.

.. code:: sh

    ray job submit \
        --working-dir wd \
        --address "http://<head_node_host>:8265" \
        --runtime-env-json '{"pip": ["getdaft"]}' \
        -- python job.py

.. NOTE::

    The runtime env parameter specifies that Daft should be installed on the Ray workers. Alternative methods of including Daft in the worker dependencies can be found `here <https://docs.ray.io/en/latest/ray-core/handling-dependencies.html>`_.


For more information about Ray jobs, see `Ray docs -> Ray Jobs Overview  <https://docs.ray.io/en/latest/cluster/running-applications/job-submission/index.html>`_.
