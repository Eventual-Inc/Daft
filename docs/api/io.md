# I/O

Daft offers a variety of approaches to creating a DataFrame from reading various data sources (in-memory data, files, data catalogs, and integrations) and writing to various data sources. See more about other [Connectors](../connectors/index.md) in Daft User Guide.

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

::: daft.read_video_frames
    options:
        heading_level: 3

::: daft.read_warc
    options:
        heading_level: 3

::: daft.read_huggingface
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

## FilenameProvider

Daft allows customizing filenames generated during writes via [`daft.io.FilenameProvider`][daft.io.FilenameProvider].

The `filename_provider` argument can be used in the following scenarios:

- `DataFrame.write_parquet(..., filename_provider=...)`
- `DataFrame.write_csv(..., filename_provider=...)`
- `daft.functions.url.upload(..., filename_provider=...)`

`FilenameProvider` receives a `write_uuid` at the start of a logical write operation, and all files generated in that operation share this UUID. Depending on the write mode, Daft calls:

- `get_filename_for_block(write_uuid, task_index, block_index, file_idx, ext)`: For block-based writes, such as `write_parquet` / `write_csv`.
- `get_filename_for_row(row, write_uuid, task_index, block_index, row_index, ext)`: For row-based writes, such as `functions.url.upload` when uploading to a single directory.

Daft's built-in default implementation [`_DefaultFilenameProvider`][daft.io.filename_provider._DefaultFilenameProvider] uses the following pattern to generate filenames:

```text
<write_uuid>_<task_index>_<block_index>_<row_or_file_index>.<ext>
```

### Example: Custom Parquet Filenames

```python
import daft
from daft.io import FilenameProvider


class LabelledParquetProvider(FilenameProvider):
    def get_filename_for_block(self, write_uuid, task_index, block_index, file_idx, ext):
        return f"part-{task_index}-{block_index}-{file_idx}.{ext}"

    def get_filename_for_row(self, row, write_uuid, task_index, block_index, row_index, ext):
        # Not used for block-based writes; can raise an error
        raise NotImplementedError


df = daft.from_pydict({"x": [1, 2, 3]})
# Generates filenames like "part-0-0-0.parquet"
df.write_parquet("output_dir", filename_provider=LabelledParquetProvider())
```

### Example: Custom Filenames for Uploads

```python
import os

import daft
from daft.io import FilenameProvider


class ImageUploadProvider(FilenameProvider):
    def get_filename_for_block(self, *args, **kwargs):  # pragma: no cover - unused
        raise NotImplementedError

    def get_filename_for_row(self, row, write_uuid, task_index, block_index, row_index, ext):
        return f"image-{row_index}.bin"


df = daft.from_pydict({"bytes": [b"a", b"b", b"c"]})
folder = "./uploads"

# Resulting local paths will look like "uploads/image-0.bin", "uploads/image-1.bin", etc.
df = df.with_column("path", df["bytes"].upload(folder, filename_provider=ImageUploadProvider()))
result = df.collect()
print(result.to_pydict()["path"])
```

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
