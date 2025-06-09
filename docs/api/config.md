# Configuration

Configure the execution backend, Daft in various ways during execution, and how Daft interacts with storage.

## Setting the Runner

Control the execution backend that Daft will run on by calling these functions once at the start of your application.

::: daft.context.set_runner_native
    options:
        heading_level: 3

::: daft.context.set_runner_ray
    options:
        heading_level: 3

## Setting Configurations

Configure Daft in various ways during execution.

::: daft.context.set_planning_config
    options:
        heading_level: 3

::: daft.context.planning_config_ctx
    options:
        heading_level: 3

::: daft.context.set_execution_config
    options:
        heading_level: 3

::: daft.context.execution_config_ctx
    options:
        heading_level: 3

## I/O Configurations

Configure behavior when Daft interacts with storage (e.g. credentials, retry policies and various other knobs to control performance/resource usage)

These configurations are most often used as inputs to Daft when reading I/O functions such as in [I/O](io.md).

::: daft.daft.IOConfig
    options:
        filters: ["!^_"]

::: daft.io.S3Config
    options:
        filters: ["!^_"]

::: daft.io.S3Credentials
    options:
        filters: ["!^_"]

::: daft.io.GCSConfig
    options:
        filters: ["!^_"]

::: daft.io.AzureConfig
    options:
        filters: ["!^_"]
