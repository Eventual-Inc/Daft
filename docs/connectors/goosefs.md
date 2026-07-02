# Reading from and Writing to GooseFS

Daft supports reading and writing data to [GooseFS](https://cloud.tencent.com/document/product/1424) — a distributed caching file system — and understands natively the URL protocol `goosefs://` as referring to data that resides in a GooseFS cluster.

Under the hood, Daft speaks GooseFS's native gRPC protocol via [OpenDAL](https://opendal.apache.org/)'s `services-goosefs` backend. This means that no HDFS/JVM gateway is required — Daft talks to GooseFS masters and workers directly.

## URL Format

URLs to data in GooseFS come in the form:

```text
goosefs://{MASTER_HOST}:{MASTER_PORT}/{PATH}
```

- `MASTER_HOST:MASTER_PORT` identifies a GooseFS master. If no explicit `master_addr` is provided in the config, Daft will use this URL authority as the master address.
- `PATH` is the absolute path to the file or directory inside the GooseFS namespace.

!!! note "High Availability (HA) masters"

    For a GooseFS deployment with multiple masters, encode all masters as a comma-separated list in the [`GooseFSConfig`](#configuration-options) `master_addr` field, e.g. `"10.0.0.1:9200,10.0.0.2:9200,10.0.0.3:9200"`. When `master_addr` is set, the URL authority is ignored — you can then use a symbolic host in the URL, for example `goosefs://ha/path/to/data`.

## Authorization/Authentication

GooseFS supports two authentication modes: `simple` (username-based, the default) and `nosasl` (no authentication). Anonymous access forces `nosasl`.

### Manually specify credentials

You can pass a [`GooseFSConfig`](#configuration-options) into your Daft I/O function calls using a `daft.io.IOConfig` config object.

`daft.set_planning_config` is a convenient way to set your `daft.io.IOConfig` as the default config to use on any subsequent Daft method calls.

=== "Using Simple Authentication"

    ```python
    import daft
    from daft.io import IOConfig
    from daft.daft import GooseFSConfig

    io_config = IOConfig(
        goosefs=GooseFSConfig(
            master_addr="10.0.0.1:9200",
            auth_type="simple",
            auth_username="alice",
        )
    )

    # Globally set the default IOConfig for any subsequent I/O calls
    daft.set_planning_config(default_io_config=io_config)

    # Perform some I/O operation
    df = daft.read_parquet("goosefs://10.0.0.1:9200/my_path/**/*")
    ```

=== "Using HA Masters"

    ```python
    import daft
    from daft.io import IOConfig
    from daft.daft import GooseFSConfig

    io_config = IOConfig(
        goosefs=GooseFSConfig(
            # Comma-separated HA master list; the URL authority is ignored.
            master_addr="10.0.0.1:9200,10.0.0.2:9200,10.0.0.3:9200",
            auth_type="simple",
            auth_username="alice",
        )
    )

    df = daft.read_parquet("goosefs://ha/my_path/**/*", io_config=io_config)
    ```

=== "Anonymous Access"

    ```python
    import daft
    from daft.io import IOConfig
    from daft.daft import GooseFSConfig

    # Anonymous mode forces `nosasl` and skips credential forwarding.
    io_config = IOConfig(goosefs=GooseFSConfig(anonymous=True))

    df = daft.read_parquet(
        "goosefs://10.0.0.1:9200/public/**/*",
        io_config=io_config,
    )
    ```

=== "Custom Root and Write Type"

    ```python
    import daft
    from daft.io import IOConfig
    from daft.daft import GooseFSConfig

    io_config = IOConfig(
        goosefs=GooseFSConfig(
            master_addr="10.0.0.1:9200",
            root="/warehouse",
            # One of: must_cache, cache_through, through, async_through
            write_type="cache_through",
        )
    )

    df = daft.read_parquet("goosefs://10.0.0.1:9200/table/**/*", io_config=io_config)
    ```

Alternatively, Daft supports overriding the default `IOConfig` per-operation by passing it into the `io_config=` keyword argument. This is extremely flexible as you can pass a different `GooseFSConfig` per function call if you wish!

=== "Per-Operation Config"

    ```python
    import daft
    from daft.io import IOConfig
    from daft.daft import GooseFSConfig

    io_config = IOConfig(
        goosefs=GooseFSConfig(
            master_addr="10.0.0.1:9200",
            auth_username="alice",
        )
    )

    # Perform some I/O operation but override the IOConfig
    df2 = daft.read_csv(
        "goosefs://10.0.0.1:9200/my_other_path/**/*",
        io_config=io_config,
    )
    ```

## Writing Data

Daft supports writing data to GooseFS using the same `GooseFSConfig`:

```python
import daft
from daft.io import IOConfig
from daft.daft import GooseFSConfig

io_config = IOConfig(
    goosefs=GooseFSConfig(
        master_addr="10.0.0.1:9200",
        auth_username="alice",
        # Persist writes through to the underlying UFS.
        write_type="cache_through",
    )
)

df = daft.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"]})

# Write to GooseFS as Parquet
df.write_parquet("goosefs://10.0.0.1:9200/output/", io_config=io_config)
```

## Configuration Options

The `GooseFSConfig` object supports the following options:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `root` | `str` | `None` (`"/"`) | Root path of the backend. All operations happen under this root. |
| `master_addr` | `str` | `None` | Master address(es) in `host:port` format. Comma-separated for HA, e.g. `"10.0.0.1:9200,10.0.0.2:9200"`. If not set, the URL authority is used. |
| `block_size` | `int` | `None` (64 MiB) | Block size in bytes for new files. |
| `chunk_size` | `int` | `None` (1 MiB) | Chunk size in bytes for streaming RPCs. |
| `write_type` | `str` | `None` (`"cache_through"`) | Default write type for new files. One of `"must_cache"`, `"cache_through"`, `"through"`, `"async_through"`. |
| `auth_type` | `str` | `None` (`"simple"`) | Authentication type. One of `"nosasl"`, `"simple"`. |
| `auth_username` | `str` | `None` (current OS user) | Authentication username used in SIMPLE mode. |
| `auth_password` | `str` | `None` | Optional authentication password. |
| `anonymous` | `bool` | `False` | Whether to use anonymous access. Forces `auth_type="nosasl"` and skips credential forwarding. |
| `max_retries` | `int` | `3` | Maximum number of retries for failed requests. |
| `retry_timeout_ms` | `int` | `30000` | Timeout duration for retry attempts in milliseconds. |
| `connect_timeout_ms` | `int` | `10000` | Connection timeout in milliseconds. |
| `read_timeout_ms` | `int` | `30000` | Read timeout in milliseconds. |
| `max_concurrent_requests` | `int` | `50` | Maximum number of concurrent requests. |
| `max_connections` | `int` | `50` | Maximum number of connections per IO thread. |

!!! tip "Master address resolution"

    You do not have to duplicate the master address in both the URL and the config:

    - If only the URL authority (`goosefs://host:port/...`) is provided, it is used as `master_addr`.
    - If `master_addr` is set explicitly, it takes precedence over the URL authority (this is required for HA setups).
    - If neither is provided, the operation will fail because there is no master to talk to.

!!! tip "Choosing a `write_type`"

    - `must_cache` — write only to workers' cache; not persisted to UFS.
    - `cache_through` — write to workers' cache and synchronously to UFS. This is the safest default for durability.
    - `through` — bypass workers' cache and write directly to UFS.
    - `async_through` — write to workers' cache and asynchronously to UFS.

## Supported Operations

Daft supports the following operations with GooseFS:

- **Read**: `read_parquet`, `read_csv`, `read_json`, and other file readers
- **Write**: `write_parquet`, `write_csv`, `write_json`
- **List**: Listing objects with glob pattern matching
- **Delete**: Deleting objects
