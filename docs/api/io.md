# I/O

Daft supports diverse input sources and output sinks which are covered in [DataFrame Creation](dataframe_creation.md) —
this page covers lower-level APIs which we are evolving for more advanced usage.

!!! warning "Warning"

    These APIs are considered experimental.

## Sources

::: daft.io.read_parquet
    options:
        heading_level: 3

::: daft.io.read_csv
    options:
        heading_level: 3

::: daft.io.read_json
    options:
        heading_level: 3

::: daft.io.read_warc
    options:
        heading_level: 3

::: daft.io.read_iceberg
    options:
        heading_level: 3

::: daft.io.read_deltalake
    options:
        heading_level: 3

::: daft.io.read_hudi
    options:
        heading_level: 3

::: daft.io.read_sql
    options:
        heading_level: 3

::: daft.io.read_lance
    options:
        heading_level: 3


## Interfaces

::: daft.io.source.DataSource
    options:
        filters: ["!^_"]
        heading_level: 3

::: daft.io.source.DataSourceTask
    options:
        filters: ["!^_"]
        heading_level: 3

## Pushdowns

Daft has predicate, projection, and limit pushdowns with expressions being represented by *Terms*. Learn more about [Pushdowns](../advanced/pushdowns.md) in the Daft User Guide.

::: daft.io.pushdowns.Pushdowns
    options:
        filters: ["!^_"]
        heading_level: 3

::: daft.io.pushdowns.Term
    options:
        filters: ["!^_"]
        heading_level: 3

::: daft.io.pushdowns.Reference
    options:
        filters: ["!^_"]
        heading_level: 3

::: daft.io.pushdowns.Literal
    options:
        filters: ["!^_"]
        heading_level: 3

::: daft.io.pushdowns.Expr
    options:
        filters: ["!^_"]
        heading_level: 3

::: daft.io.pushdowns.Arg
    options:
        filters: ["!^_"]
        heading_level: 3

::: daft.io.pushdowns.TermVisitor
    options:
        filters: ["!^_"]
        heading_level: 3
