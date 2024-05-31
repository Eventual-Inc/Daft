Configuration
=============

Setting the Runner
******************

Control the execution backend that Daft will run on by calling these functions once at the start of your application.

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/configuration_functions

    daft.context.set_runner_py
    daft.context.set_runner_ray

Setting configurations
**********************

Configure Daft in various ways during execution.

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/configuration_functions

    daft.set_planning_config
    daft.set_execution_config

I/O Configurations
******************

Configure behavior when Daft interacts with storage (e.g. credentials, retry policies and various other knobs to control performance/resource usage)

These configurations are most often used as inputs to Daft DataFrame reading I/O functions such as in :doc:`creation`.

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/io_configs

    daft.io.IOConfig
    daft.io.S3Config
    daft.io.S3Credentials
    daft.io.GCSConfig
    daft.io.AzureConfig
