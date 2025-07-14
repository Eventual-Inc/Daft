# I/O

Daft offers a variety of approaches to creating a DataFrame from reading various data sources (in-memory data, files, data catalogs, and integrations) and writing to various data sources. See more about [I/O](../io/index.md) in Daft User Guide.

<div class="grid cards api" markdown>

* [**Input**](#input)

    Create DataFrames from files, databases, and other data sources.

* [**Output**](#output)

    Write DataFrames to various file formats and storage systems.

* [**User-Defined** (experimental)](#user-defined)

    Create custom data sources and sinks for advanced I/O scenarios.

* [**Pushdowns**](#pushdowns)

    Optimize queries by pushing operations down to the storage layer.

</div>

## Input

::: daft.from_arrow
::: daft.from_dask_dataframe
::: daft.from_glob_path
::: daft.from_pandas
::: daft.from_pydict
::: daft.from_pylist
::: daft.from_ray_dataset
::: daft.read_csv
::: daft.read_deltalake
::: daft.read_hudi
::: daft.read_iceberg
::: daft.read_json
::: daft.read_lance
::: daft.read_parquet
::: daft.read_sql
::: daft.read_warc
::: daft.sql.sql.sql

## Output

::: daft.DataFrame.write_csv
::: daft.DataFrame.write_deltalake
::: daft.DataFrame.write_iceberg
::: daft.DataFrame.write_json
::: daft.DataFrame.write_lance
::: daft.DataFrame.write_parquet
::: daft.DataFrame.write_sink

## User-Defined

Daft supports diverse input sources and output sinks, this section covers lower-level APIs which we are evolving for more advanced usage.

!!! warning "Warning"

    These APIs are considered experimental.

::: daft.io.source.DataSource
    options:
        filters: ["!^_"]


::: daft.io.source.DataSourceTask
    options:
        filters: ["!^_"]


::: daft.io.sink.DataSink
    options:
        filters: ["!^_"]


::: daft.io.sink.WriteResult
    options:
        filters: ["!^_"]


## Pushdowns

Daft supports predicate, projection, and limit pushdowns.

::: daft.io.pushdowns.Pushdowns
    options:
        filters: ["!^_"]


::: daft.io.scan.ScanOperator
    options:
        filters: ["!^_"]
