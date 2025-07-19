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

<!-- BEGIN GENERATED TABLE -->
| Method | Description |
|--------|-------------|
| [`set_runner_native`][daft.context.set_runner_native] | Configure Daft to execute dataframes using native multi-threaded processing. |
| [`set_runner_ray`][daft.context.set_runner_ray] | Configure Daft to execute dataframes using the Ray distributed computing framework. |
<!-- END GENERATED TABLE -->

::: daft.context.set_runner_native
::: daft.context.set_runner_ray

## Setting Configurations

Configure Daft in various ways during execution.

<!-- BEGIN GENERATED TABLE -->
| Method | Description |
|--------|-------------|
| [`execution_config_ctx`][daft.context.execution_config_ctx] | Context manager that wraps set_execution_config to reset the config to its original setting afternwards. |
| [`planning_config_ctx`][daft.context.planning_config_ctx] | Context manager that wraps set_planning_config to reset the config to its original setting afternwards. |
| [`set_execution_config`][daft.context.set_execution_config] | Globally sets various configuration parameters which control various aspects of Daft execution. |
| [`set_planning_config`][daft.context.set_planning_config] | Globally sets various configuration parameters which control Daft plan construction behavior. |
<!-- END GENERATED TABLE -->

::: daft.context.execution_config_ctx
::: daft.context.planning_config_ctx
::: daft.context.set_execution_config
::: daft.context.set_planning_config

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
