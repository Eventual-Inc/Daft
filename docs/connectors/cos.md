# Reading from and Writing to Tencent Cloud COS

Daft supports reading and writing data to Tencent Cloud COS (Cloud Object Storage), and understands natively the URL protocols `cos://` and `cosn://` (Hadoop CosN compatible) as referring to data that resides in COS.

## Authorization/Authentication

In Tencent Cloud COS, data is stored under the hierarchy of:

1. Bucket: The container for data storage, which is the top-level namespace for data storage in COS.
2. Object Key: The unique identifier for a piece of data within a bucket.

URLs to data in COS come in the form: `cos://{BUCKET}/{OBJECT_KEY}`.

!!! note "Hadoop CosN Compatibility"

    Daft also supports the `cosn://` URL scheme for compatibility with Hadoop CosN. Both `cos://` and `cosn://` are treated identically.

### Rely on Environment

You can configure Daft to automatically discover credentials from environment variables. Daft supports the following environment variable prefixes:

| Environment Variable | Description |
|---|---|
| `COS_ENDPOINT` | Endpoint of the COS service |
| `COS_REGION` or `TENCENTCLOUD_REGION` | Region of the COS service |
| `COS_SECRET_ID` or `TENCENTCLOUD_SECRET_ID` | SecretId for COS authentication |
| `COS_SECRET_KEY` or `TENCENTCLOUD_SECRET_KEY` | SecretKey for COS authentication |
| `COS_SECURITY_TOKEN` or `TENCENTCLOUD_SECURITY_TOKEN` | Security token for temporary credentials (STS) |

Please be aware that when doing so in a distributed environment such as Ray, Daft will pick these credentials up from worker machines and thus each worker machine needs to be appropriately provisioned.

If instead you wish to have Daft use credentials from the "driver", you may wish to manually specify your credentials.

### Manually specify credentials

You may also choose to pass these values into your Daft I/O function calls using a [`daft.io.CosConfig`][daft.io.CosConfig] config object.

[`daft.set_planning_config`][daft.context.set_planning_config] is a convenient way to set your [`daft.io.IOConfig`][daft.io.IOConfig] as the default config to use on any subsequent Daft method calls.

=== "Using Permanent Keys"

    ```python
    from daft.io import IOConfig, CosConfig

    io_config = IOConfig(
        cos=CosConfig(
            region="ap-guangzhou",
            secret_id="your-secret-id",
            secret_key="your-secret-key",
        )
    )

    # Globally set the default IOConfig for any subsequent I/O calls
    daft.set_planning_config(default_io_config=io_config)

    # Perform some I/O operation
    df = daft.read_parquet("cos://my-bucket/my_path/**/*")
    ```

=== "Using STS Temporary Credentials"

    ```python
    from daft.io import IOConfig, CosConfig

    io_config = IOConfig(
        cos=CosConfig(
            region="ap-guangzhou",
            secret_id="your-tmp-secret-id",
            secret_key="your-tmp-secret-key",
            security_token="your-security-token",
        )
    )

    # Perform some I/O operation
    df = daft.read_parquet("cos://my-bucket/my_path/**/*", io_config=io_config)
    ```

=== "Using Custom Endpoint"

    ```python
    from daft.io import IOConfig, CosConfig

    # Use a custom endpoint (region will be auto-derived from the endpoint)
    io_config = IOConfig(
        cos=CosConfig(
            endpoint="https://cos.ap-beijing.myqcloud.com",
            secret_id="your-secret-id",
            secret_key="your-secret-key",
        )
    )

    df = daft.read_parquet("cos://my-bucket/data/**/*", io_config=io_config)
    ```

=== "Anonymous Access"

    ```python
    from daft.io import IOConfig, CosConfig

    # Access public buckets without credentials
    io_config = IOConfig(cos=CosConfig(anonymous=True))

    # Read from a public bucket
    df = daft.read_parquet("cos://public-bucket/data/**/*", io_config=io_config)
    ```

=== "Using CosN URL Scheme"

    ```python
    from daft.io import IOConfig, CosConfig

    io_config = IOConfig(
        cos=CosConfig(
            region="ap-guangzhou",
            secret_id="your-secret-id",
            secret_key="your-secret-key",
        )
    )

    # Use cosn:// scheme (Hadoop CosN compatible)
    df = daft.read_parquet("cosn://my-bucket/my_path/**/*", io_config=io_config)
    ```

Alternatively, Daft supports overriding the default IOConfig per-operation by passing it into the `io_config=` keyword argument. This is extremely flexible as you can pass a different [`daft.io.CosConfig`][daft.io.CosConfig] per function call if you wish!

=== "Per-Operation Config"

    ```python
    from daft.io import IOConfig, CosConfig

    io_config = IOConfig(
        cos=CosConfig(
            region="ap-guangzhou",
            secret_id="your-secret-id",
            secret_key="your-secret-key",
        )
    )

    # Perform some I/O operation but override the IOConfig
    df2 = daft.read_csv("cos://my-bucket/my_other_path/**/*", io_config=io_config)
    ```

## Writing Data

Daft supports writing data to COS using the same `CosConfig`:

```python
import daft
from daft.io import IOConfig, CosConfig

io_config = IOConfig(
    cos=CosConfig(
        region="ap-guangzhou",
        secret_id="your-secret-id",
        secret_key="your-secret-key",
    )
)

df = daft.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"]})

# Write to COS as Parquet
df.write_parquet("cos://my-bucket/output/", io_config=io_config)
```

## Configuration Options

The [`daft.io.CosConfig`][daft.io.CosConfig] object supports the following options:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `region` | `str` | `None` | Region name, e.g. `"ap-guangzhou"`, `"ap-beijing"`, `"ap-shanghai"` |
| `endpoint` | `str` | `None` | Custom endpoint URL, e.g. `"https://cos.ap-guangzhou.myqcloud.com"`. If not provided, it will be derived from the region. |
| `secret_id` | `str` | `None` | Tencent Cloud SecretId |
| `secret_key` | `str` | `None` | Tencent Cloud SecretKey |
| `security_token` | `str` | `None` | Security token for temporary credentials (STS) |
| `anonymous` | `bool` | `False` | Whether to use anonymous access (for public buckets) |
| `max_retries` | `int` | `3` | Maximum number of retries for failed requests |
| `retry_timeout_ms` | `int` | `30000` | Timeout duration for retry attempts in milliseconds |
| `connect_timeout_ms` | `int` | `10000` | Connection timeout in milliseconds |
| `read_timeout_ms` | `int` | `30000` | Read timeout in milliseconds |
| `max_concurrent_requests` | `int` | `50` | Maximum number of concurrent requests |
| `max_connections` | `int` | `50` | Maximum number of connections per IO thread |

!!! tip "Region and Endpoint Auto-Derivation"

    You only need to specify either `region` or `endpoint` — Daft will automatically derive the other:

    - If only `region` is provided, the endpoint is derived as `https://cos.{region}.myqcloud.com`
    - If only `endpoint` is provided, the region is extracted from the endpoint URL
    - If both are provided, both values are used as-is

## Supported Operations

Daft supports the following operations with COS:

- **Read**: `read_parquet`, `read_csv`, `read_json`, and other file readers
- **Write**: `write_parquet`, `write_csv`, `write_json` (including multipart uploads)
- **List**: Listing objects with glob pattern matching
- **Delete**: Deleting objects
