# Logging

Daft uses Python's standard `logging` module to emit logs. This allows for flexible configuration of logging levels and output formats, depending on whether you are running Daft locally or in a distributed environment with Ray.

## Native Execution

When running Daft in its default local mode, you can directly use Python's `logging` module to control the verbosity of logs from different Daft modules. This is useful for debugging specific components of your query.

To set logging levels, you can get the logger for a specific Daft module and set its level. For example, to enable `DEBUG` level logging for Daft's distributed components, you can do the following:

```python
import logging

logging.getLogger("daft.distributed").setLevel(logging.DEBUG)
```

## Remote Execution

When running Daft on a Ray cluster, logging is more complex due to the distributed nature of the system. Logs can be emitted from the driver process (where you call `ray.init()`) or from the worker processes (where the actual data processing happens).

### Driver vs. Worker Logging

- **Driver Logs**: These are logs from the main process that orchestrates the query. They provide high-level information about the query plan and execution progress.
- **Worker Logs**: These are logs from the individual Ray actors that process data in parallel. They provide detailed information about the execution of specific tasks.

### Configuration Options

Here are a few ways to configure logging when using Daft with Ray:

1.  **Suppressing Worker Logs on the Driver**: By default, Ray forwards logs from all worker processes to the driver, which can be overwhelming. If you only want to see driver logs, you can disable this behavior by setting `log_to_driver=False` in `ray.init()`:

    ```python
    import ray

    ray.init(log_to_driver=False)
    ```

2.  **Per-Worker Logging Configuration**: If you need to set different logging levels for different modules on each worker, you can use a `worker_process_setup_hook`. This is a function that runs at the start of each worker process, allowing you to configure logging before any Daft code is executed.

    Here is an example of how to define a setup hook to configure logging on each worker:

    ```python
    def configure_logging():
        import logging
        logging.getLogger("daft.core").setLevel(logging.DEBUG)
        logging.getLogger("daft.execution").setLevel(logging.INFO)

    ray.init(
        runtime_env={
            "worker_process_setup_hook": configure_logging,
        }
    )
    ```

3.  **Uniform Logging Configuration**: If you want to set the same logging level for both the driver and all workers, you can use the `logging_config` parameter in `ray.init()`. This is a convenient way to apply a consistent logging policy across the entire cluster.

    ```python
    from ray.job_config import LoggingConfig

    ray.init(
        logging_config=LoggingConfig(log_level="DEBUG"),
    )
    ```

### Complete Example

Here is a complete example of how to initialize Ray with a combination of these logging configurations, along with other useful settings for debugging Daft:

```python
import ray
from ray.job_config import LoggingConfig

# A setup hook to configure logging on each worker
def configure_logging():
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    # Example of setting a specific Daft module to DEBUG
    logging.getLogger("daft.distributed").setLevel(logging.DEBUG)


# Initialize Ray with advanced logging and environment settings
ray.init(
    dashboard_host="0.0.0.0",
    runtime_env={
        "env_vars": {
            "DAFT_PROGRESS_BAR": "0",
            "DAFT_DEBUG_DISPATCH": "1"
        },
        "worker_process_setup_hook": configure_logging,
    },
    logging_config=LoggingConfig(
        log_level="DEBUG",
    ),
    log_to_driver=False,
)
```

!!! note "A Note on Rust Tracing"
    While Daft's Python side uses the standard `logging` module, the core Rust components use the `tracing` library for structured, high-performance logging. When debugging performance-critical sections of Daft, you may find it useful to enable `tracing` logs, which can provide more detailed insights into the execution of the underlying query engine.
