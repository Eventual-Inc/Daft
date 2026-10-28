# ORC Files

Daft reads ORC files from local storage and remote URLs using [`daft.read_orc`][daft.read_orc]. This can be used to process raw ORC files produced by tools such as Apache Spark and Hive.

## Read ORC Files

```python
import daft

df = daft.read_orc("/path/to/file.orc")
df = daft.read_orc("/path/to/directory")
df = daft.read_orc("/path/to/files-*.orc")
df = daft.read_orc(["/path/to/first.orc", "/path/to/second.orc"])
```

Directories are searched recursively for files matching `*.orc`. An explicit file path can have a different extension. Overlapping paths and glob patterns are deduplicated. Empty inputs, unmatched paths, unreadable files, and corrupt ORC files raise errors.

Paths follow Daft's native glob syntax, including `*`, `?`, `[...]`, and `{...}`. A path containing unescaped glob metacharacters is treated as a pattern even when a file with that name exists. For example, `a[1].orc` matches `a1.orc`; use `a[[]1[]].orc` to match the literal filename `a[1].orc`.

By default, schema inference occurs when the DataFrame is created. With `infer_schema=False`, planning uses the supplied schema without inspecting an ORC schema; the reader still accesses each file's physical schema at execution time. Rows are read when an action such as `collect`, `show`, or `write_parquet` executes. Each file is processed by one task, so multiple files can be read in parallel. Stripes within one file are not scheduled as separate distributed tasks.

## Remote Storage

Use the same [`IOConfig`][daft.io.IOConfig] as other Daft file readers. If `io_config` is omitted, the reader uses the planning context's default configuration.

```python
from daft.io import IOConfig, S3Config

io_config = IOConfig(s3=S3Config(region_name="us-east-1", anonymous=True))
df = daft.read_orc("s3://my-public-bucket/data/*.orc", io_config=io_config)
```

## Query and Batch Size

Select columns and filter rows using the DataFrame API:

```python
df = daft.read_orc("/path/to/data/*.orc", batch_size=65536)
result = df.where(daft.col("score") > 0.5).select("id", "label").limit(100)
```

Column projection reduces the fields requested from the reader. Row filters and limits preserve Daft query semantics; ORC-native predicate pruning and footer-based aggregation are not supported.

`limit()` restricts the returned rows but does not guarantee early termination of file reads. The shared Python source bridge can continue reading and buffering batches after the returned-row limit is reached, so even `.limit(1)` can read beyond the first batch and retain multiple batches in memory.

`batch_size` controls the maximum number of rows in each emitted batch and defaults to 131072. It does not set a memory limit: field sizes, decoder buffers, and queued batches also affect memory usage.

## Schema and Compatibility

The output schema remains fixed for each DataFrame. By default, it is inferred from the first matched file. Use `schema` and `infer_schema` to control it:

- With `infer_schema=False`, `schema` is required and defines the output fields, types, and order. Fields outside this schema are excluded.
- With `infer_schema=True` (the default), `schema` provides hints: existing fields keep their inferred positions but use the specified types. Fields absent from the inferred schema are appended in the order provided. An appended field is read from later files where it exists.

```python
schema = {"event_id": daft.DataType.int64(), "label": daft.DataType.string()}

# Use a definitive schema, without planning-time schema inference.
df = daft.read_orc("events/*.orc", schema=schema, infer_schema=False)

# Override event_id's type and include label even if it is absent from the first file.
df_with_hints = daft.read_orc("events/*.orc", schema=schema)
```

Every file is aligned to the output schema before row filters are evaluated: missing fields become nulls, extra fields are excluded, and fields are cast using Daft's conversion rules, as with Parquet reads. Unsupported type conversions raise an error; some value conversions, such as an invalid numeric string, can produce null. Files with zero rows and a valid schema are supported. Empty hints leave the inferred schema unchanged.

The reader uses the ORC types supported by PyArrow and Daft's Arrow conversion, including numeric values, strings, binary values, dates, timestamps, decimals, lists, structs, and maps. It does not automatically merge schemas from every file. Supply hints or a definitive schema to include expected fields that are absent from the first file.

This API reads raw ORC files. It does not interpret Hive ACID layouts or table transactions, and does not add ORC support to `read_iceberg` or provide an ORC writer.
