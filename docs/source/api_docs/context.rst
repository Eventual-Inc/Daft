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

    daft.context.set_planning_config
    daft.context.set_execution_config
