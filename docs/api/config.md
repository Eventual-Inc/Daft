# Configuration

Configure Daft's execution backend, runtime behavior, and storage interactions for optimal performance.

<div class="grid cards api" markdown>

* [**Setting the Runner**](#setting-the-runner)

    Control which execution backend (native or Ray) Daft uses for distributed processing.

* [**Setting Configurations**](#setting-configurations)

    Configure Daft's planning and execution behavior during runtime.

* [**I/O Configuration**s](#io-configurations)

    Configure storage credentials, retry policies, and performance settings.

</div>

## Setting the Runner

Control the execution backend that Daft will run on by calling these functions once at the start of your application.

::: daft.context.set_runner_native
::: daft.context.set_runner_ray

## Setting Configurations

Configure Daft in various ways during execution.

::: daft.context.set_planning_config
::: daft.context.planning_config_ctx
::: daft.context.set_execution_config
::: daft.context.execution_config_ctx

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
