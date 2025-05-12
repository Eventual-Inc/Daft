# I/O

## Input

<!-- from_ -->

::: daft.from_arrow
    options:
        heading_level: 3

::: daft.from_dask_dataframe
    options:
        heading_level: 3

::: daft.from_pandas
    options:
        heading_level: 3

::: daft.from_pydict
    options:
        heading_level: 3

::: daft.from_pylist
    options:
        heading_level: 3

::: daft.from_ray_dataset
    options:
        heading_level: 3

<!-- read_ -->

::: daft.read_csv
    options:
        heading_level: 3

::: daft.read_deltalake
    options:
        heading_level: 3

::: daft.read_hudi
    options:
        heading_level: 3

::: daft.read_iceberg
    options:
        heading_level: 3

::: daft.read_json
    options:
        heading_level: 3

::: daft.read_lance
    options:
        heading_level: 3

::: daft.read_parquet
    options:
        heading_level: 3

::: daft.read_sql
    options:
        heading_level: 3

::: daft.read_warc
    options:
        heading_level: 3

## Output

<!-- write_ -->

::: daft.dataframe.DataFrame.write_csv
    options:
        heading_level: 3

::: daft.dataframe.DataFrame.write_deltalake
    options:
        heading_level: 3

::: daft.dataframe.DataFrame.write_iceberg
    options:
        heading_level: 3

::: daft.dataframe.DataFrame.write_lance
    options:
        heading_level: 3

::: daft.dataframe.DataFrame.write_parquet
    options:
        heading_level: 3

## User-Defined

!!! warning "Warning"

    These APIs are considered experimental.

::: daft.io.source.DataSource
    options:
        filters: ["!^_"]
        heading_level: 3

::: daft.io.source.DataSourceTask
    options:
        filters: ["!^_"]
        heading_level: 3

## Pushdowns

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
