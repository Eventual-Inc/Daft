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

Schema inference occurs when the DataFrame is created. Rows are read when an action such as `collect`, `show`, or `write_parquet` executes. Each file is processed by one task, so multiple files can be read in parallel. Stripes within one file are not scheduled as separate distributed tasks.

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

Column projection reduces the fields requested from the reader. Row filters and limits preserve Daft query semantics. Hive partition filters can exclude files when directory partitioning is enabled; ORC-native stripe predicate pruning and footer-based aggregation are not supported.

`limit()` restricts the returned rows but does not guarantee early termination of file reads. The shared Python source bridge can continue reading and buffering batches after the returned-row limit is reached, so even `.limit(1)` can read beyond the first batch and retain multiple batches in memory.

`batch_size` controls the maximum number of rows in each emitted batch and defaults to 131072. It does not set a memory limit: field sizes, decoder buffers, and queued batches also affect memory usage.

## Hive Directory Partitions

Set `hive_partitioning=True` to read partition columns from `key=value` directories. Partition discovery is disabled by default.

```text
data/
  year=2024/region=east/part.orc
  year=2025/region=west/part.orc
```

```python
df = daft.read_orc("/path/to/data", hive_partitioning=True)
result = df.where(daft.col("year") == 2025).select("region")
```

The first matched file determines the partition keys and their types. Later files use those types, and new directory keys are ignored. Directory parsing, type inference, and value conversion use the same Rust Hive helpers as native file scans. Values are inferred as booleans, signed 64-bit integers, floating-point numbers, dates, times, timestamps, or strings. Time and timestamp precision is preserved, including nanoseconds and fixed timestamp offsets. Values that cannot be parsed as the inferred type become null. No partition schema merging or explicit partition type override is provided.

Invalid UTF-8 directory text or invalid timezone metadata raises an error. The current Hive inference produces invalid timezone metadata when a partition's first timestamp value has a negative offset with nonzero minutes; these reads raise an error.

Directory keys and values are URI decoded once, after splitting path components. Encode a slash or equals sign inside a value as `%2F` or `%3D`; a plus sign remains a plus sign. For repeated keys, the last directory value is used. Empty values and `__HIVE_DEFAULT_PARTITION__` represent null. A null value in the first matched path gives that partition column a string type.

For every declared partition key, a missing directory value becomes a typed null. Directory partition columns override physical columns with the same name, including when the directory value is missing or null. This rule applies to full reads, column projections, and filters. These two rules are explicit ORC behavior; they do not imply that every Parquet reader path handles these edge cases identically.

Predicates resolved against partition values can exclude files before their data is scanned. Directory listing and schema inspection I/O can still occur for excluded files. Predicates involving physical data continue to use Daft's existing row filtering. Selecting only partition columns preserves the number of rows in each ORC file.

## Schema and Compatibility

The schema is inferred from the first matched file and remains fixed for that DataFrame. Later files are aligned to this schema: missing fields become nulls, extra fields are excluded, and fields are cast using Daft's conversion rules, as with Parquet reads. Unsupported type conversions raise an error; some value conversions, such as an invalid numeric string, can produce null. Files with zero rows and a valid schema are supported.

The reader uses the ORC types supported by PyArrow and Daft's Arrow conversion, including numeric values, strings, binary values, dates, timestamps, decimals, lists, structs, and maps. It does not provide full schema merging or an explicit schema override.

This API reads raw ORC files. It does not interpret Hive ACID layouts or table transactions, and does not add ORC support to `read_iceberg` or provide an ORC writer.
