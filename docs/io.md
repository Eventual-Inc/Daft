# I/O

Please see [Daft I/O API docs](./api/io.md) for API details.

## In-Memory

| Function                                          | Description                                             |
|---------------------------------------------------|---------------------------------------------------------|
| [`from_arrow`][daft.from_arrow]                   | Create a DataFrame from PyArrow Tables or RecordBatches |
| [`from_dask_dataframe`][daft.from_dask_dataframe] | Create a DataFrame from a Dask DataFrame                |
| [`from_pandas`][daft.from_pandas]                 | Create a DataFrame from a Pandas DataFrame              |
| [`from_pydict`][daft.from_pydict]                 | Create a DataFrame from a python dictionary             |
| [`from_pylist`][daft.from_pylist]                 | Create a DataFrame from a python list                   |
| [`from_ray_dataset`][daft.from_ray_dataset]       | Create a DataFrame from a Ray Dataset                   |


## CSV

| Function                                          | Description                                            |
|---------------------------------------------------|--------------------------------------------------------|
| [`read_csv`][daft.io.read_csv]                    | Read a CSV file or multiple CSV files into a DataFrame |
| [`write_csv`][daft.dataframe.DataFrame.write_csv] | Write a DataFrame to CSV files                         |


## Delta Lake

| Function                                                      | Description                              |
|---------------------------------------------------------------|------------------------------------------|
| [`read_deltalake`][daft.io.read_deltalake]                    | Read a Delta Lake table into a DataFrame |
| [`write_deltalake`][daft.dataframe.DataFrame.write_deltalake] | Write a DataFrame to a Delta Lake table  |


## Hudi

| Function                         | Description                        |
|----------------------------------|------------------------------------|
| [`read_hudi`][daft.io.read_hudi] | Read a Hudi table into a DataFrame |


## Iceberg

| Function                                                  | Description                            |
|-----------------------------------------------------------|----------------------------------------|
| [`read_iceberg`][daft.io.read_iceberg]                    | Read an Iceberg table into a DataFrame |
| [`write_iceberg`][daft.dataframe.DataFrame.write_iceberg] | Write a DataFrame to an Iceberg table  |

## JSON

| Function                         | Description                                              |
|----------------------------------|----------------------------------------------------------|
| [`read_json`][daft.io.read_json] | Read a JSON file or multiple JSON files into a DataFrame |


## Lance

| Function                                              | Description                           |
|-------------------------------------------------------|---------------------------------------|
| [`read_lance`][daft.io.read_lance]                    | Read a Lance dataset into a DataFrame |
| [`write_lance`][daft.dataframe.DataFrame.write_lance] | Write a DataFrame to a Lance dataset  |


## Parquet

| Function                                                  | Description                                                    |
|-----------------------------------------------------------|----------------------------------------------------------------|
| [`read_parquet`][daft.io.read_parquet]                    | Read a Parquet file or multiple Parquet files into a DataFrame |
| [`write_parquet`][daft.dataframe.DataFrame.write_parquet] | Write a DataFrame to Parquet files                             |


## SQL

| Function                       | Description                                    |
|--------------------------------|------------------------------------------------|
| [`read_sql`][daft.io.read_sql] | Read data from a SQL database into a DataFrame |


## WARC

| Function                         | Description                                              |
|----------------------------------|----------------------------------------------------------|
| [`read_warc`][daft.io.read_warc] | Read a WARC file or multiple WARC files into a DataFrame |


## User-Defined

| Function                                                    | Description                                                        |
|-------------------------------------------------------------|--------------------------------------------------------------------|
| [`DataSource`][daft.io.source.DataSource]         | Interface for reading data into DataFrames                         |
| [`DataSourceTask`][daft.io.source.DataSourceTask] | Represents a partition of data that can be processed independently |
