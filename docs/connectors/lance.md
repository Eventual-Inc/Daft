# Reading from and Writing to Lance

[Lance](https://lance.org/) is a next-generation columnar storage format for multimodal datasets (images, video, audio, and general columnar data). It supports local POSIX filesystems and cloud object stores (e.g., S3/GCS). Lance is known for extremely fast random access, zero-copy reads, deep integration with PyArrow/DuckDB, and strong performance for vector retrieval workloads.

Daft currently supports:

1. **Parallel and distributed reads**: Daft parallelizes reads on the default multithreading runner or on the [distributed Ray runner](../distributed/index.md)

2. **Skipping filtered data (Data Skipping)**: Daft uses [`df.where()`][daft.DataFrame.where] predicates and file/fragment statistics to skip non-matching data

3. **Multi-cloud and local access**: Read from S3, GCS, Azure Blob Storage, and local filesystems with unified IO configuration

4. **Version/time-slice reads**: Use `version` and `asof` parameters to read a specific version or the latest version as of a given timestamp

5. **Scan optimization**: Configure `fragment_group_size` to group fragments and improve scan efficiency

6. **Cache tuning**: Configure `index_cache_size` and `metadata_cache_size_bytes` to optimize index page caching and metadata retrieval for large datasets

## Installing Daft with Lance Support

Daft integrates Lance through an optional dependency:

```bash
pip install -U "daft[lance]"
```

## Reading a Table

Use [`daft.read_lance`][daft.read_lance] to read a Lance dataset. You can pass either a local path or a cloud object store URI.

=== "🐍 Python"

    ```python
    # Read a Lance dataset from local or object storage
    import daft

    # Local path example
    df_local = daft.read_lance("/data/my_lance_dataset")

    # Read a specific version or a time slice
    df_v1 = daft.read_lance("/data/my_lance_dataset", version=1)
    ```

To access public S3/GCS buckets, configure IO options for authentication and endpoints:

=== "🐍 Python"

    ```python
    import daft
    from daft.io import IOConfig, S3Config, GCSConfig

    # Public S3 example (anonymous access)
    s3_config = S3Config(region_name="us-west-2", anonymous=True)
    gcs_config = GCSConfig(credentials="/path/to/gcp-service-account.json")
    io_config = IOConfig(s3=s3_config, gcs=gcs_config)
    df_s3 = daft.read_lance("s3://daft-public-data/lance/words-test-dataset", io_config=io_config)

    # GCS example (service account credentials)
    df_gcs = daft.read_lance("gs://my-bucket/lance/my-dataset", io_config=io_config)
    ```

For S3-compatible services (e.g. Volcengine TOS), configure IO options for authentication and endpoints:

=== "🐍 Python"

    ```python
    import daft
    from daft.io import IOConfig, S3Config

    io_config = IOConfig(
        s3=S3Config(
            endpoint_url="https://tos-s3-{region}.ivolces.com",
            region_name="{region}",
            force_virtual_addressing=True,
            verify_ssl=True,
            key_id="your-access-key-id",
            access_key="your-secret-access-key",
        )
    )
    df = daft.read_lance("s3://bucket-name/lance-path", io_config=io_config)
    ```

For datasets with many fragments, group fragments to reduce scheduling and metadata overhead:

=== "🐍 Python"

    ```python
    # Process multiple fragments in a single scan task
    df_grouped = daft.read_lance("/data/my_lance_dataset", fragment_group_size=8)
    ```

Filter operations on the Daft `df` DataFrame object will be pushed down to the Lance data source for efficient data skipping.

=== "🐍 Python"

```python
# Enable strict filter pushdown for more efficient data skipping
daft.context.set_planning_config(enable_strict_filter_pushdown=True)

filtered = df_local.where(df_local["score"] >= 0.8)
filtered.show()
```

## Writing to Lance

Use [`df.write_lance()`][daft.dataframe.DataFrame.write_lance] to write a DataFrame to a Lance dataset. Supported modes include `create`, `append`, and `overwrite`. Additional write parameters (e.g., maximum file size) are passed through to the underlying writer.

=== "🐍 Python"

```python
import daft

df = daft.from_pydict({"a": [1, 2, 3, 4]})
meta = df.write_lance("/tmp/lance/my_table.lance")
meta.show()  # Contains metadata such as num_fragments / num_deleted_rows / num_small_files / version

# Overwrite existing table with extra parameters (passed to lance.write_fragments)
meta2 = df.write_lance("/tmp/lance/my_table.lance", mode="overwrite", max_bytes_per_file=1024)
meta2.show()

# Append rows (must be compatible with the existing table schema)
meta3 = df.write_lance("/tmp/lance/my_table.lance", mode="append")
```

To access public S3/GCS buckets, configure IO options for authentication and endpoints. For S3-compatible services (e.g. Volcengine TOS), configure IO options for authentication and endpoints:

=== "🐍 Python"

    ```python
    import daft
    from daft.io import IOConfig, S3Config

    df = daft.from_pydict({"user_id": [1, 2, 3], "score": [0.5, 0.8, 0.9]})

    io_config = IOConfig(
        s3=S3Config(
            endpoint_url="https://tos-s3-{region}.ivolces.com",
            region_name="{region}",
            force_virtual_addressing=True,
            verify_ssl=True,
            key_id="your-access-key-id",
            access_key="your-secret-access-key",
        )
    )

    meta = df.write_lance("s3://my-tos-bucket/lance/my_table.lance", io_config=io_config)
    ```

Note: the `{region}` placeholder in the `endpoint_url` should match the region of your TOS bucket. E.g. if your bucket is in `cn-beijing`, you should set `region_name="cn-beijing"` and `endpoint_url="https://tos-s3-cn-beijing.ivolces.com"`.

### Writing with a specific schema

You can provide a `pyarrow.Schema` to control the on-disk Lance schema. Daft will align the DataFrame to this schema (column order, types, and nullability) before writing:

=== "🐍 Python"

    ```python
    import daft
    import pyarrow as pa

    df = daft.from_pydict(
        {
            "id": [1, 2, 3],
            "score": [0.5, 0.8, 0.9],
        }
    )

    target_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("score", pa.float32(), nullable=True),
        ]
    )

    meta = df.write_lance("/tmp/lance/typed_table.lance", schema=target_schema)
    ```

This ensures that the resulting Lance table uses the exact schema you specify, even if the in-memory DataFrame has compatible but different types or column ordering.

!!! note "Write Schema Control"

    - If `schema` is not provided, Daft uses the current DataFrame schema.
    - If a `pyarrow.Schema` is provided, data will be aligned to that schema before writing (type/order/nullability).
    - If the target dataset already exists and the write is not an overwrite, data is converted to the existing table schema for compatibility.

## Advanced Usage


### Data Evolution

If you need to add derived columns in-place to an existing Lance dataset (e.g., apply a UDF across batches and persist the result), use `daft.io.lance.merge_columns`:

=== "🐍 Python"

```python
from daft.io.lance import merge_columns

# Example: double the values in column c and write to a new column new_column
import pyarrow.compute as pc

def double_score(batch):
    return batch.append_column("new_column", pc.multiply(batch["c"], 2))

merge_columns(
    "/tmp/lance/my_table.lance",
    transform=double_score,
    read_columns=["c"],
)
```

### Compaction

Compaction is the process of rewriting a Lance dataset to optimize its structure for query performance. This operation can:

- **Merge small files**: Combine multiple small data fragments into fewer, larger fragments to reduce metadata overhead and improve read throughput.
- **Materialize deletions**: Physically remove rows that have been marked for deletion, which can reduce storage and accelerate scans.
- **Improve data layout**: Reorganize data to improve compression and read efficiency.

Use compaction when a dataset has undergone many small appends, has a high number of deleted rows, or contains a large number of small files.


Daft provides a distributed implementation of compaction through [`daft.io.lance.compact_files`][daft.io.lance.compact_files].

```python
daft.io.lance.compact_files(
    uri,
    compaction_options=None,
    io_config=None,
    # --- other options ---
    index_cache_size=None,
    block_size=None,
    default_scan_options=None,
    metadata_cache_size_bytes=None,
    partition_num=None,
    concurrency=None,
)
```

- **`uri`**: Path to the Lance dataset.
- **`compaction_options`**: A dictionary of options to control compaction behavior (e.g., `target_rows_per_fragment`, `materialize_deletions`), see [Lance documentation](https://lance-format.github.io/lance-python-doc/all-modules.html#lance.dataset.DatasetOptimizer.compact_files) for more details.
- **Returns**: A `CompactionMetrics` object with statistics if compaction was performed, or `None` if no action was needed. The `CompactionMetrics` object contains the following fields:
    - `fragments_removed`: Number of fragments removed during compaction.
    - `fragments_added`: Number of fragments added during compaction.
    - `files_removed`: Number of files removed during compaction.
    - `files_added`: Number of files added during compaction.
- **`partition_num`**: On the Ray Runner, this controls the number of parallel compaction tasks. Defaults to 1. On the native runner, this option is ignored.
- **`concurrency`**: The maximum number of concurrent compaction task instances to run. This controls how many compaction tasks can be processed simultaneously. Defaults to None, which means no explicit concurrency limit.


This example compacts a dataset with multiple fragments into a single, larger fragment, and uses `partition_num` to control the number of parallel compaction tasks.

=== "🐍 Python"

    ```python
    import daft
    import lance
    import pandas as pd
    import pyarrow as pa

    daft.set_runner_ray()

    # Create a dataset and delete some rows
    data = pa.table({"a": range(800), "b": range(800)})
    dataset_path = "/tmp/dataset.lance"
    dataset = lance.write_dataset(data, dataset_path, max_rows_per_file=200)

    # The initial row count is 800, and the initial fragment count is 4
    print(f"Initial row count: {dataset.count_rows()}, and initial fragment count: {len(dataset.get_fragments())}")

    metrics = daft.io.lance.compact_files(
        dataset_path,
        compaction_options={
            "target_rows_per_fragment": 400,
        },
        partition_num=2,
    )

    dataset = lance.dataset(dataset_path)
    # The final row count is 800, and the final fragment count is 2
    print(f"Final row count: {dataset.count_rows()}, and final fragment count: {len(dataset.get_fragments())}")
    ```


### Creating Scalar Indexes

Daft provides a distributed scalar indexing API on top of Lance through [`daft.io.lance.create_scalar_index`][daft.io.lance.create_scalar_index]. This allows you to build full-text and scalar indexes across all fragments in a Lance dataset.

The `index_type` parameter controls the type of scalar index and supports INVERTED, FTS, and BTREE. For INVERTED and FTS indexes, the indexed column must be a string column or a large_string column. BTREE indexes support numeric and string columns. If the column has an incompatible type for the chosen `index_type`, Daft raises a `TypeError`.

=== "🐍 Python"

    ```python
    import daft

    # Build an inverted index on a text column
    daft.io.lance.create_scalar_index(
        "/tmp/lance/my_table.lance",
        column="text",
        index_type="INVERTED",
    )

    # Build a full-text search index
    daft.io.lance.create_scalar_index(
        "/tmp/lance/my_table.lance",
        column="text",
        index_type="FTS",
    )

    # Build a BTREE index
    daft.io.lance.create_scalar_index(
        "/tmp/lance/my_table.lance",
        column="text",
        index_type="BTREE",
    )
    ```

#### Concurrency and partitioning

`daft.io.lance.create_scalar_index` builds the index in parallel by splitting the dataset into fragment batches and running workers across them.

- **`concurrency`** controls the maximum number of workers that build fragment-level indexes in parallel. If `concurrency` is `None`, Daft defaults to `4`. Concurrency must be a positive integer; values larger than the number of fragments are clipped down to the fragment count.
- **`partition_num`** On the Ray Runner, controls how many logical batches of fragment IDs are processed when dispatching work through Daft. If `partition_num` is `None` or less than or equal to `1`, Daft processes the fragment batches without additional repartitioning. On the native runner, this option is ignored.

=== "🐍 Python"

    ```python
    import daft

    uri = "/tmp/lance/my_table.lance"

    # Use a higher concurrency to speed up indexing on larger tables
    daft.io.lance.create_scalar_index(
        uri,
        column="text",
        index_type="INVERTED",
        concurrency=8,
    )

    # Explicitly control how fragment batches are repartitioned
    daft.io.lance.create_scalar_index(
        uri,
        column="text",
        index_type="INVERTED",
        concurrency=8,
        partition_num=4,
    )
    ```
