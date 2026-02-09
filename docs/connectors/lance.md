# Reading from and Writing to Lance

[Lance](https://lance.org/) is a next-generation columnar storage format for multimodal datasets (images, video, audio, and general columnar data). It supports local POSIX filesystems and cloud object stores (e.g., S3/GCS). Lance is known for extremely fast random access, zero-copy reads, deep integration with PyArrow/DuckDB, and strong performance for vector retrieval workloads.

Daft currently supports:

1. **Parallel and distributed reads**: Daft parallelizes reads on the default multithreading runner or on the [distributed Ray runner](../distributed/index.md)

2. **Skipping filtered data (Data Skipping)**: Daft uses [`df.where()`][daft.DataFrame.where] predicates and file/fragment statistics to skip non-matching data

3. **Multi-cloud and local access**: Read from S3, GCS, Azure Blob Storage, and local filesystems with unified IO configuration

4. **Version/time-slice reads**: Use `version` and `asof` parameters to read a specific version or the latest version as of a given timestamp

5. **Scan optimization**: Configure `fragment_group_size` to group fragments and improve scan efficiency

6. **Cache tuning**: Configure `index_cache_size` and `metadata_cache_size_bytes` to optimize index page caching and metadata retrieval for large datasets

7. **REST API support**: Connect to Lance tables managed by REST-compliant services like LanceDB Cloud, Apache Gravitino, and other catalog systems using the Lance REST Namespace specification

## Installing Daft with Lance Support

Daft integrates Lance through an optional dependency:

```bash
pip install -U "daft[lance]"
```

For REST API support, you'll also need the Lance namespace packages:

```bash
pip install lance-namespace lance-namespace-urllib3-client
```

## Reading a Table

Use [`daft.read_lance`][daft.read_lance] to read a Lance dataset. You can pass either a local path, a cloud object store URI, or a REST endpoint.

### File-based Reading

=== "🐍 Python"

    ```python
    # Read a Lance dataset from local or object storage
    import daft

    # Local path example
    df_local = daft.read_lance("/data/my_lance_dataset")

    # Read a specific version or a time slice
    df_v1 = daft.read_lance("/data/my_lance_dataset", version=1)
    ```

### REST-based Reading

=== "🐍 Python"

    ```python
    # Read a Lance table via REST API
    import daft
    from daft.io.lance import LanceRestConfig

    # Configure REST connection
    rest_config = LanceRestConfig(
        base_url="https://api.lancedb.com",
        api_key="your-api-key"
    )

    # Read from REST endpoint
    df_rest = daft.read_lance(
        "rest://my_namespace/my_table",
        rest_config=rest_config
    )

    # Read from root namespace
    df_root = daft.read_lance(
        "rest:///my_table",
        rest_config=rest_config
    )
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

    region = "cn-beijing" # Region of your TOS bucket
    bucket_name = "my-tos-bucket" # Name of your TOS bucket
    io_config = IOConfig(
        s3=S3Config(
            endpoint_url=f"https://tos-s3-{region}.ivolces.com",
            region_name=region,
            force_virtual_addressing=True,
            verify_ssl=True,
            key_id="your-access-key-id",
            access_key="your-secret-access-key",
        )
    )
    df = daft.read_lance(f"s3://{bucket_name}/lance-path", io_config=io_config)
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

Use [`df.write_lance()`][daft.dataframe.DataFrame.write_lance] to write a DataFrame to a Lance dataset. Supported modes include `create`, `append`, and `overwrite`. You can write to both file-based and REST-based Lance tables.

### File-based Writing

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

### REST-based Writing

=== "🐍 Python"

```python
import daft
from daft.io.lance import LanceRestConfig

# Configure REST connection
rest_config = LanceRestConfig(
    base_url="https://api.lancedb.com",
    api_key="your-api-key"
)

df = daft.from_pydict({"a": [1, 2, 3, 4]})

# Create a new table via REST
meta = df.write_lance(
    "rest://my_namespace/my_table",
    rest_config=rest_config,
    mode="create"
)
meta.show()

# Append to existing table
df2 = daft.from_pydict({"a": [5, 6, 7, 8]})
meta2 = df2.write_lance(
    "rest://my_namespace/my_table",
    rest_config=rest_config,
    mode="append"
)

# Overwrite table
df3 = daft.from_pydict({"a": [10, 20], "b": ["x", "y"]})
meta3 = df3.write_lance(
    "rest://my_namespace/my_table",
    rest_config=rest_config,
    mode="overwrite"
)
```

### For S3-compatible services (e.g. Volcengine TOS), configure IO options for authentication and endpoints:

=== "🐍 Python"

    ```python
    import daft
    from daft.io import IOConfig, S3Config

    df = daft.from_pydict({"user_id": [1, 2, 3], "score": [0.5, 0.8, 0.9]})

    region = "cn-beijing" # Region of your TOS bucket
    bucket_name = "my-tos-bucket" # Name of your TOS bucket
    io_config = IOConfig(
        s3=S3Config(
            endpoint_url=f"https://tos-s3-{region}.ivolces.com",
            region_name=region,
            force_virtual_addressing=True,
            verify_ssl=True,
            key_id="your-access-key-id",
            access_key="your-secret-access-key",
        )
    )

    meta = df.write_lance(f"s3://{bucket_name}/lance/my_table.lance", io_config=io_config)
    ```

Note: the `{region}` should match the region of your TOS bucket. e.g. if your bucket is in `cn-beijing`, you should set `region_name="cn-beijing"` and `endpoint_url="https://tos-s3-cn-beijing.ivolces.com"`.

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

## REST Configuration and Catalog Management

When working with Lance tables via REST APIs, you can configure authentication and manage table catalogs:

=== "🐍 Python"

```python
from daft.io.lance import LanceRestConfig, create_lance_table_rest, register_lance_table_rest
import pyarrow as pa

# Configure REST connection with custom headers
rest_config = LanceRestConfig(
    base_url="https://my-lance-service.com",
    api_key="your-api-key",
    timeout=60,
    headers={"Custom-Header": "value"}
)

# Create a table in the catalog without data
schema = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("name", pa.string()),
    pa.field("embedding", pa.list_(pa.float32()))
])

create_lance_table_rest(
    rest_config=rest_config,
    namespace="my_namespace",
    table_name="my_table",
    schema=schema
)

# Register an existing Lance table with the catalog
register_lance_table_rest(
    rest_config=rest_config,
    namespace="my_namespace",
    table_name="existing_table",
    table_uri="s3://my-bucket/lance-data/existing_table"
)
```

### Supported REST Services

The Lance REST implementation is compatible with any service that implements the [Lance REST Namespace specification](https://lance.org/format/namespace/rest/catalog-spec/), including:

- **LanceDB Cloud**: Managed Lance service with enterprise features
- **LanceDB Enterprise**: Self-hosted enterprise deployment
- **Apache Gravitino**: Multi-modal metadata service with Lance support
- **Custom implementations**: Any service implementing the Lance REST Namespace OpenAPI spec

## Advanced Usage


### Vector Search

Daft can push Lance's vector search options through `default_scan_options`. This lets
you express nearest-neighbor queries at scan time while keeping the rest of your
pipeline in Daft.

=== "🐍 Python"

    ```python
    import daft
    import pyarrow as pa

    # Example: dataset with a fixed-size list or embedding column named "vector"
    query = pa.array([0.0, 0.0], type=pa.float32())

    df = daft.read_lance(
        "/data/my_lance_vectors",
        # Forward Lance's `nearest` options into the underlying scanner
        default_scan_options={
            "nearest": {
                "column": "vector",  # Lance vector column name
                "q": query,           # Query vector (any Lance-compatible QueryVectorLike)
                "k": 5,               # Top-K neighbors to return
            },
        },
    )

    # The resulting DataFrame contains the K nearest rows according to Lance,
    # and you can continue working with it using normal Daft APIs.
    df.select("vector").show()
    ```

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
    io_config=None,
    *,
    storage_options=None,
    version=None,
    asof=None,
    block_size=None,
    commit_lock=None,
    index_cache_size=None,
    default_scan_options=None,
    metadata_cache_size_bytes=None,
    compaction_options=None,
    num_partitions=None,
    concurrency=None,
    micro_commit_batch_size=None,
)
```

- **`uri`**: Path to the Lance dataset.
- **`compaction_options`**: A dictionary of options to control compaction behavior. Common keys include:
    - `target_rows_per_fragment`: Target number of rows per fragment after compaction.
    - `materialize_deletions`: Whether to materialize soft deletions into physical deletes.
    - `materialize_deletions_threadhold`: The fraction of original rows that are soft deleted in a fragment before it becomes a candidate for compaction.
- **`num_partitions`**: Optional number of partitions to use when repartitioning fragment batches before execution. Only values greater than 1 enable additional parallelism on distributed runners; values <= 1 or None will use the default partitioning.
- **`concurrency`**: Maximum number of `CompactionTask` instances executed concurrently per worker.
- **`micro_commit_batch_size`**: Number of compaction tasks to commit in a single micro-batch.

    - If `micro_commit_batch_size` is `None` (the default), all tasks are committed in a single batch and the function returns a `CompactionMetrics` object.
    - Values <= 0 are treated the same as `None` (single-batch commit; no micro-batching).
    - If `0 < micro_commit_batch_size < plan.num_tasks()`, compaction tasks are committed in multiple micro-batches and the function returns `None` (per-batch metrics are not aggregated).
    - If `micro_commit_batch_size` is greater than or equal to `plan.num_tasks()`, the behavior is equivalent to committing in a single batch and the function returns a `CompactionMetrics` object.
- **Returns**: A `CompactionMetrics` object with statistics if compaction was performed and committed in a single batch, or `None` if no action was needed or when micro-batch commits are used. The `CompactionMetrics` object contains the following fields:
    - `fragments_removed`: Number of fragments removed during compaction.
    - `fragments_added`: Number of fragments added during compaction.
    - `files_removed`: Number of files removed during compaction.
    - `files_added`: Number of files added during compaction.

This example compacts a dataset with multiple fragments into a single, larger fragment, and uses `num_partitions` to control the number of parallel compaction tasks.

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
            "materialize_deletions": True,
            "materialize_deletions_threadhold": 0.5,
        },
        num_partitions=2,
        concurrency=2,
    )

    dataset = lance.dataset(dataset_path)
    # The final row count is 800, and the final fragment count is 2
    print(f"Final row count: {dataset.count_rows()}, and final fragment count: {len(dataset.get_fragments())}")
    ```
