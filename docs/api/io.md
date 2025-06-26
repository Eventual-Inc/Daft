# I/O

Daft offers a variety of approaches to creating a DataFrame from reading various data sources (in-memory data, files, data catalogs, and integrations) and writing to various data sources. See more about [I/O](../io/index.md) in Daft User Guide.

## Input

<!-- from_ -->

::: daft.from_arrow
    options:
        heading_level: 3

::: daft.from_dask_dataframe
    options:
        heading_level: 3

::: daft.from_glob_path
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

::: daft.sql.sql.sql
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

Daft supports diverse input sources and output sinks, this section covers lower-level APIs which we are evolving for more advanced usage.

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

::: daft.io.sink.DataSink
    options:
        filters: ["!^_"]
        heading_level: 3

::: daft.io.sink.WriteResult
    options:
        filters: ["!^_"]
        heading_level: 3

## Pushdowns

Daft supports predicate, projection, and limit pushdowns.

::: daft.io.pushdowns.Pushdowns
    options:
        filters: ["!^_"]
        heading_level: 3

::: daft.io.scan.ScanOperator
    options:
        filters: ["!^_"]
