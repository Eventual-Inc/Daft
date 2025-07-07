# Telemetry

To help core developers improve Daft, we collect non-identifiable statistics on Daft usage in order to better understand how Daft is used, common bugs and performance bottlenecks. Data is collected from [Scarf](https://scarf.sh).

We take the privacy of our users extremely seriously, and telemetry in Daft is built to be:

1. Easy to opt-out: To disable telemetry, set the environment variable: `DO_NOT_TRACK=true`

2. Non-identifiable: Events are keyed by a session ID which is generated on import of Daft
3. Metadata-only: We do not collect any of our users' proprietary code or data

We **do not** sell or buy any of the data that is collected in telemetry.

!!! info "*Daft telemetry is enabled in versions >= v0.0.21*"

## What data do we collect?

To audit what data is collected, please see `scarf_telemetry.py`.

In short, we collect the following:

1. On import, we track the version of Daft, OS, Python version, and system architecture.
2. On DataFrame executions, we track the same system information as we do on imports, in addition to the runner being used, i.e. native or Ray.
