Daft Telemetry
==============

To help core developers improve Daft, we collect non-identifiable statistics on Daft usage in order to better understand how Daft is used, common bugs and performance bottlenecks.

We take the privacy of our users extremely seriously, and telemetry in Daft is built to be:

1. Easy to opt-out: to disable telemetry, set the following environment variable: ``DAFT_ANALYTICS_ENABLED=0``
2. Non-identifiable: events are keyed by a session ID which is generated on import of Daft
3. Metadata-only: we do not collect any of our users' proprietary code or data
4. Easy to review: please get in touch with us at telemetry@eventualcomputing.com with specific session IDs that you would like to review or delete

We **do not** sell or buy any of the data that is collected in telemetry.

*Daft telemetry is enabled in versions >= v0.0.21*

What data do we collect?
------------------------

To audit what data is collected, please see the implementation of ``AnalyticsClient`` in the ``daft.analytics`` module.

In short, we collect the following:

1. On import, we track system information such as the runner being used, version of Daft, OS, Python version, etc.
2. On calls of public methods on the DataFrame object, we track metadata about the execution: the name of the method, the walltime for execution and the class of error raised (if any). Function parameters and stacktraces are not logged, ensuring that user data remains private.

Collected data can be found as JSON files in your temporary directory under ``$TMPDIR/daft/<session_id>/analytics.log``. You can retrieve the temporary directory that Daft uses by running ``python -c 'import tempfile; print(tempfile.gettempdir())'`` in your terminal.
