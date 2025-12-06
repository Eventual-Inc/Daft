# Logging

Daft uses Python's standard `logging` module to emit logs. This allows for flexible configuration of logging levels and output formats, depending on whether you are running Daft locally or in a distributed environment with Ray.

Daft also provides utility functions to simplify logging configuration, particularly for debugging purposes.

## Native Execution

When running Daft in its default local mode, you can directly use Python's `logging` module to control the verbosity of logs from different Daft modules. This is useful for debugging specific components of your query.

To set logging levels, you can get the logger for a specific Daft module and set its level. For example, to enable `DEBUG` level logging for Daft's distributed components, you can do the following:

```python
import logging

# Configure logging BEFORE importing daft
logging.getLogger("daft.distributed").setLevel(logging.DEBUG)

# Import daft AFTER setting up logging
import daft
```

Daft also provides a convenience function `setup_logger()` that can be used to quickly enable debug logging:

```python
# Import daft first to access the utility function
import daft
from daft.logging import setup_logger

# Configure debug logging
setup_logger()
```

The `setup_logger()` function accepts three optional parameters:

- `level` (str, optional): The log level to use. Valid options are `DEBUG`, `INFO`, `WARNING`, `ERROR`. Defaults to `DEBUG`.
- `daft_only` (bool, optional): When True, only logs from Daft modules will be shown. Defaults to True.
- `exclude_prefix` (list[str], optional): A list of module prefixes to exclude from logging. Defaults to [].

By default, `setup_logger()` sets the root logger to `DEBUG` level and applies a filter to only show logs from Daft modules. This makes it easier to focus on Daft-specific logs while debugging.

For most use cases, simply using `logging.getLogger(__name__)` is the common first approach. The example above with a specific logger like `"daft.distributed"` is for targeting specific internal Daft components when debugging particular subsystems.

!!! note
    - It's important to set the logging level before importing Daft, as the underlying Rust components will not pick up level changes that occur after the import.

    - After each logger setup,` refresh_logger()` is called to synchronize the configuration with the Rust backend.
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
        # Configure logging BEFORE importing daft
        logging.getLogger("daft.execution").setLevel(logging.INFO)
        # Import daft AFTER setting up logging
        import daft

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
import logging
import ray
from ray.job_config import LoggingConfig

# Configure logging BEFORE importing daft
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
# Example of setting a specific Daft module to DEBUG
logging.getLogger("daft.distributed").setLevel(logging.DEBUG)

# Import daft AFTER setting up logging
import daft

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
    log_to_driver=True,
)
```

Note that in the above example, we configure logging before importing Daft to ensure the Rust components pick up the logging level changes.

!!! note "A Note on Rust Tracing"
    While Daft's Python side uses the standard `logging` module, the core Rust components use the `tracing` library for structured, high-performance logging. When debugging performance-critical sections of Daft, you may find it useful to enable `tracing` logs, which can provide more detailed insights into the execution of the underlying query engine.
