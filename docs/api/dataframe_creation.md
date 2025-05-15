# DataFrame Creation

Daft offers a variety of approaches to creating a DataFrame from various data sources like in-memory data, files, data catalogs, and integrations. Learn more about [Creating DataFrames](../core_concepts.md#creating-a-dataframe) in Daft User Guide.

## SQL

::: daft.sql.sql.sql
    options:
        heading_level: 3

## In-Memory Data

::: daft.from_pylist
    options:
        heading_level: 3

::: daft.from_pydict
    options:
        heading_level: 3

::: daft.from_arrow
    options:
        heading_level: 3

::: daft.from_pandas
    options:
        heading_level: 3

## Files

::: daft.read_parquet
    options:
        heading_level: 3

::: daft.read_csv
    options:
        heading_level: 3

::: daft.read_json
    options:
        heading_level: 3

::: daft.read_warc
    options:
        heading_level: 3

::: daft.from_glob_path
    options:
        heading_level: 3

## Data Catalogs

::: daft.read_iceberg
    options:
        heading_level: 3

::: daft.read_deltalake
    options:
        heading_level: 3

::: daft.read_hudi
    options:
        heading_level: 3

## Integrations

::: daft.from_ray_dataset
    options:
        heading_level: 3

::: daft.from_dask_dataframe
    options:
        heading_level: 3

::: daft.read_sql
    options:
        heading_level: 3

::: daft.read_lance
    options:
        heading_level: 3
