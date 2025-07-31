# Observability

When executing a long-running query, it can be useful to have some observability tooling to monitor the current status and metrics used to predict the amount of work left. After execution, these metrics can also aid in additional profiling and performance tweaking.

This page will discuss tooling Daft provides or integrates with to help with your observability needs.

## Progress Indicators

When running in a shell (like Bash) or in a Jupyter Notebook, Daft will display a progress indicator with statistics of the executing query so far. These statistics differ between the local and remote execution.

!!! note "Note"

    When running a Python script outside of a user-facing shell, the progress indicator is disabled. If you want to disable the progress indicator explicitly, set the environment variable `DAFT_PROGRESS_BAR=0`. This can be useful for benchmarking as progress tracking can add overhead in local execution.


### Local Execution
In local or single-node execution, the progress indicator shows:

* The time spent since the start of an execution operator. This is only the "wall clock" time, not the total time spent across all threads.
* The number of rows received and emitted per operator. This can be useful to see if a particular operation is currently waiting for more rows or processing output
* Additional metrics depending on the operator. For example, we show the number of bytes written by write operators

### Remote Execution
In remote execution on Ray, the progress indicator shows:

* The number of partitions processed by a pipeline of tasks
* The time spent per pipeline so far
* An approximation of the time left for the known remaining partitions

#### Why is the progress indicator different?

Ray automatically manages a different progress indicator based on tasks and actors. This progress indicator tracks different statistics and is not as customizable.
