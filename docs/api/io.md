# I/O

Daft supports diverse input sources and output sinks which are covered in [DataFrame Creation](dataframe_creation.md) —
this page covers lower-level APIs which we are evolving for more advanced usage.

!!! warning "Warning"

    These APIs are considered experimental.

::: daft.io.scan.ScanOperator
    options:
        filters: ["!^_"]

## Pushdowns

Daft supports predicate, projection, and limit pushdowns.

<!-- Learn more about [Pushdowns](../advanced/pushdowns.md) in the Daft User Guide. -->

::: daft.io.pushdowns.Pushdowns
    options:
        filters: ["!^_"]
