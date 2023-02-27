Distributed Computing
=====================

By default, Daft runs using your local machine's resources and your operations are thus limited by the CPUs, memory and GPUs available to you in your single local development machine.

However, Daft has strong integrations with `Ray <https://www.ray.io>`_ which is a distributed computing framework for distributing computations across a cluster of machines. Here is a snippet showing how you can connect Daft to a Ray cluster:

.. code:: python

    import daft

    daft.context.set_runner_ray()

By default, if no address is specified Daft will spin up a Ray cluster locally on your machine. If you are running Daft on a powerful machine (such as an AWS P3 machine which is equipped with multiple GPUs) this is already very useful because Daft can parallelize its execution of computation across your CPUs and GPUs. However, if instead you already have your own Ray cluster running remotely, you can connect Daft to it by supplying an address:

.. code:: python

    daft.context.set_runner_ray(address="ray://url-to-mycluster")

For more information about the ``address`` keyword argument, please see the `Ray documentation on initialization <https://docs.ray.io/en/latest/ray-core/api/doc/ray.init.html>`_.
