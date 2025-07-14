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

<!-- BEGIN GENERATED TABLE -->
| Method | Description |
|--------|-------------|
| [`from_arrow`][daft.from_arrow] | Creates a DataFrame from a pyarrow Table. |
| [`from_dask_dataframe`][daft.from_dask_dataframe] | Creates a Daft DataFrame from a Dask DataFrame. |
| [`from_glob_path`][daft.from_glob_path] | Creates a DataFrame of file paths and other metadata from a glob path. |
| [`from_pandas`][daft.from_pandas] | Creates a Daft DataFrame from a pandas DataFrame. |
| [`from_pydict`][daft.from_pydict] | Creates a DataFrame from a Python dictionary. |
| [`from_pylist`][daft.from_pylist] | Creates a DataFrame from a list of dictionaries. |
| [`from_ray_dataset`][daft.from_ray_dataset] | Creates a DataFrame from a Ray Dataset. |
| [`read_csv`][daft.read_csv] | Creates a DataFrame from CSV file(s). |
| [`read_deltalake`][daft.read_deltalake] | Create a DataFrame from a Delta Lake table. |
| [`read_hudi`][daft.read_hudi] | Create a DataFrame from a Hudi table. |
| [`read_iceberg`][daft.read_iceberg] | Create a DataFrame from an Iceberg table. |
| [`read_json`][daft.read_json] | Creates a DataFrame from line-delimited JSON file(s). |
| [`read_lance`][daft.read_lance] | Create a DataFrame from a LanceDB table. |
| [`read_parquet`][daft.read_parquet] | Creates a DataFrame from Parquet file(s). |
| [`read_sql`][daft.read_sql] | Create a DataFrame from the results of a SQL query. |
| [`read_warc`][daft.read_warc] | Creates a DataFrame from WARC or gzipped WARC file(s). This is an experimental feature and the API may change in the future. |
| [`sql`][daft.sql.sql.sql] | Run a SQL query, returning the results as a DataFrame. |
<!-- END GENERATED TABLE -->

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

<!-- BEGIN GENERATED TABLE -->
| Method | Description |
|--------|-------------|
| [`write_csv`][daft.DataFrame.write_csv] | Writes the DataFrame as CSV files, returning a new DataFrame with paths to the files that were written. |
| [`write_deltalake`][daft.DataFrame.write_deltalake] | Writes the DataFrame to a [Delta Lake](https://docs.delta.io/latest/index.html) table, returning a new DataFrame with the operations that occurred. |
| [`write_iceberg`][daft.DataFrame.write_iceberg] | Writes the DataFrame to an [Iceberg](https://iceberg.apache.org/docs/nightly/) table, returning a new DataFrame with the operations that occurred. |
| [`write_json`][daft.DataFrame.write_json] | Writes the DataFrame as JSON files, returning a new DataFrame with paths to the files that were written. |
| [`write_lance`][daft.DataFrame.write_lance] | Writes the DataFrame to a Lance table. |
| [`write_parquet`][daft.DataFrame.write_parquet] | Writes the DataFrame as parquet files, returning a new DataFrame with paths to the files that were written. |
| [`write_sink`][daft.DataFrame.write_sink] | Writes the DataFrame to the given DataSink. |
<!-- END GENERATED TABLE -->

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
