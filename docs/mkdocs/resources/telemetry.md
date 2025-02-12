# Telemetry

To help core developers improve Daft, we collect non-identifiable statistics on Daft usage in order to better understand how Daft is used, common bugs and performance bottlenecks. Data is collected from a combination of our own analytics and [Scarf](https://scarf.sh).

We take the privacy of our users extremely seriously, and telemetry in Daft is built to be:

1. Easy to opt-out: To disable telemetry, set the following environment variables:

    • `DAFT_ANALYTICS_ENABLED=0`

    • `SCARF_NO_ANALYTICS=true` or `DO_NOT_TRACK=true`

2. Non-identifiable: Events are keyed by a session ID which is generated on import of Daft
3. Metadata-only: We do not collect any of our users' proprietary code or data

We **do not** sell or buy any of the data that is collected in telemetry.

!!! info "*Daft telemetry is enabled in versions >= v0.0.21*"

## What data do we collect?

To audit what data is collected, please see the implementation of `AnalyticsClient` in the `daft.analytics` module as well as `scarf_telemetry.py`.

In short, we collect the following:

1. On import, we track system information such as the runner being used, version of Daft, OS, Python version, etc.
2. On calls of public methods on the DataFrame object, we track metadata about the execution: the name of the method, the walltime for execution and the class of error raised (if any). Function parameters and stacktraces are not logged, ensuring that user data remains private.
