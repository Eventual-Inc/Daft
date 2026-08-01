# io

## AzureConfig

```python
AzureConfig(storage_account: str | None=None, access_key: str | None=None, sas_token: str | None=None, bearer_token: str | None=None, tenant_id: str | None=None, client_id: str | None=None, client_secret: str | None=None, use_fabric_endpoint: bool | None=None, anonymous: bool | None=None, endpoint_url: str | None=None, use_ssl: bool | None=None, max_connections: int | None=None)
```

I/O configuration for accessing Azure Blob Storage.

## CosConfig

```python
CosConfig(region: str | None=None, endpoint: str | None=None, secret_id: str | None=None, secret_key: str | None=None, security_token: str | None=None, anonymous: bool | None=None, max_retries: int | None=None, retry_timeout_ms: int | None=None, connect_timeout_ms: int | None=None, read_timeout_ms: int | None=None, max_concurrent_requests: int | None=None, max_connections: int | None=None)
```

I/O configuration for accessing Tencent Cloud COS (Cloud Object Storage).

Args:
    region (str, optional): Name of the region, e.g. "ap-guangzhou", "ap-beijing". Defaults to None.
    endpoint (str, optional): URL to the COS endpoint. Defaults to None, will be inferred from region.
    secret_id (str, optional): Tencent Cloud SecretId. Defaults to None.
    secret_key (str, optional): Tencent Cloud SecretKey. Defaults to None.
    security_token (str, optional): Security token for temporary credentials (STS). Defaults to None.
    anonymous (bool, optional): Whether to use anonymous access. Defaults to False.
    max_retries (int, optional): Maximum number of retries for failed requests. Defaults to 3.
    retry_timeout_ms (int, optional): Timeout duration for retry attempts in milliseconds. Defaults to 30000ms.
    connect_timeout_ms (int, optional): Timeout duration to make a connection in milliseconds. Defaults to 10000ms.
    read_timeout_ms (int, optional): Timeout duration to read the first byte in milliseconds. Defaults to 30000ms.
    max_concurrent_requests (int, optional): Maximum number of concurrent requests. Defaults to 50.
    max_connections (int, optional): Maximum number of connections per IO thread. Defaults to 50.

## DataSink

```python
DataSink()
```

Interface for writing data to a sink that is not built-in.

When a DataFrame is written using the `.write_sink()` method, the following sequence occurs:

1. The sink's `.start()` method is called once at the beginning of the write process.
2. The DataFrame is executed, and its output is split into micropartitions.
3. The sink's `.write()` method is invoked on each micropartition, potentially in parallel
   and distributed across multiple tasks or workers.
4. After all writes complete, the resulting `WriteOutput` objects are gathered on a single node.
5. The `.finalize()` method is then called with all write outputs to produce a final `MicroPartition`.

Warning:
    This API is early in its development and is subject to change.

## DataSource

```python
DataSource()
```

DataSource is a low-level interface for reading data into DataFrames.

When a DataSource is read, it is split into multiple tasks which can be distributed
for parallel processing. Each task is responsible for reading a specific portion of
the data (e.g., a file partition, a range of rows, or a subset of a database table)
and converting it into RecordBatches. Implementations should ensure that tasks
are appropriately sized to balance parallelism.

Warning:
    This API is early in its development and is subject to change.

## DataSourceTask

```python
DataSourceTask()
```

DataSourceTask represents a partition of data that can be processed independently.

Warning:
    This API is early in its development and is subject to change.

## GCSConfig

```python
GCSConfig(project_id: str | None=None, credentials: str | None=None, token: str | None=None, anonymous: bool | None=None, max_connections: int | None=None, retry_initial_backoff_ms: int | None=None, connect_timeout_ms: int | None=None, read_timeout_ms: int | None=None, num_tries: int | None=None)
```

I/O configuration for accessing Google Cloud Storage.

## GooseFSConfig

```python
GooseFSConfig(root: str | None=None, master_addr: str | None=None, block_size: int | None=None, chunk_size: int | None=None, write_type: str | None=None, auth_type: str | None=None, auth_username: str | None=None, auth_password: str | None=None, anonymous: bool | None=None, max_retries: int | None=None, retry_timeout_ms: int | None=None, connect_timeout_ms: int | None=None, read_timeout_ms: int | None=None, max_concurrent_requests: int | None=None, max_connections: int | None=None)
```

I/O configuration for accessing GooseFS (distributed caching file system) via native gRPC.

Args:
    root (str, optional): Root path of the backend. All operations happen under this root. Defaults to None ("/").
    master_addr (str, optional): Master address(es) in ``host:port`` format. Comma-separated for HA, e.g. ``"10.0.0.1:9200,10.0.0.2:9200"``. Defaults to None (uses the URL authority).
    block_size (int, optional): Block size in bytes for new files. Defaults to None (64 MiB).
    chunk_size (int, optional): Chunk size in bytes for streaming RPCs. Defaults to None (1 MiB).
    write_type (str, optional): Default write type for new files. One of ``"must_cache"``, ``"cache_through"``, ``"through"``, ``"async_through"``. Defaults to None (``"cache_through"``).
    auth_type (str, optional): Authentication type. One of ``"nosasl"``, ``"simple"``. Defaults to None (``"simple"``).
    auth_username (str, optional): Authentication username used in SIMPLE mode. Defaults to None (current OS user).
    auth_password (str, optional): Optional authentication password. Defaults to None.
    anonymous (bool, optional): Whether to use anonymous access. Forces ``auth_type="nosasl"`` and skips credential forwarding. Defaults to False.
    max_retries (int, optional): Maximum number of retries for failed requests. Defaults to 3.
    retry_timeout_ms (int, optional): Timeout duration for retry attempts in milliseconds. Defaults to 30000ms.
    connect_timeout_ms (int, optional): Timeout duration to make a connection in milliseconds. Defaults to 10000ms.
    read_timeout_ms (int, optional): Timeout duration to read the first byte in milliseconds. Defaults to 30000ms.
    max_concurrent_requests (int, optional): Maximum number of concurrent requests. Defaults to 50.
    max_connections (int, optional): Maximum number of connections per IO thread. Defaults to 50.

## GravitinoConfig

```python
GravitinoConfig(endpoint: str | None, metalake_name: str | None, auth_type: str | None, username: str | None, password: str | None, token: str | None)
```

I/O configuration for Gravitino filesets.

## HTTPConfig

```python
HTTPConfig(bearer_token: str | None=None, retry_initial_backoff_ms: int | None=None, connect_timeout_ms: int | None=None, read_timeout_ms: int | None=None, num_tries: int | None=None)
```

I/O configuration for accessing HTTP systems.

## HuggingFaceConfig

```python
HuggingFaceConfig(token: str | None=None, anonymous: bool | None=None, use_xet: bool | None=None, use_content_defined_chunking: bool | None=None, row_group_size: int | None=None, target_filesize: int | None=None, max_operations_per_commit: int | None=None)
```

I/O configuration for accessing Hugging Face datasets.

Args:
    token (str, optional): Your Hugging Face access token, generated from https://huggingface.co/settings/tokens.
    anonymous (bool, optional): Whether or not to use "anonymous mode", which will access Hugging Face without any credentials. Defaults to False.
    use_xet (bool, optional): When True, attempt to read Xet-backed files via the Xet protocol before falling back to HTTP. Defaults to True.
    use_content_defined_chunking (bool, optional): Set the `use_content_defined_chunking` parameter when creating a `pyarrow.parquet.ParquetWriter`. Only available with pyarrow>=21. Defaults to true if available.
    row_group_size (int, optional): Row group size when writing Parquet files. Defaults to the default `pyarrow.parquet.ParquetWriter` row group size.
    target_filesize (int, optional): Target size in bytes for each written Parquet file. Defaults to 512 MB.
    max_operations_per_commit (int, optional): Maximum number of files to add/copy/delete per commit. Defaults to 100.

## S3Config

```python
S3Config(region_name: str | None=None, endpoint_url: str | None=None, key_id: str | None=None, session_token: str | None=None, access_key: str | None=None, credentials_provider: Callable[[], S3Credentials] | None=None, buffer_time: int | None=None, max_connections: int | None=None, retry_initial_backoff_ms: int | None=None, connect_timeout_ms: int | None=None, read_timeout_ms: int | None=None, num_tries: int | None=None, retry_mode: str | None=None, anonymous: bool | None=None, use_ssl: bool | None=None, verify_ssl: bool | None=None, check_hostname_ssl: bool | None=None, requester_pays: bool | None=None, force_virtual_addressing: bool | None=None, profile_name: str | None=None, multipart_size: int | None=None, multipart_max_concurrency: int | None=None, custom_retry_msgs: list[str] | None=None)
```

I/O configuration for accessing an S3-compatible system.

Args:
    region_name (str, optional): Name of the region to be used (used when accessing AWS S3), defaults to "us-east-1".
        If wrongly provided, Daft will attempt to auto-detect the buckets' region at the cost of extra S3 requests.
    endpoint_url (str, optional): URL to the S3 endpoint, defaults to endpoints to AWS
    key_id (str, optional): AWS Access Key ID, defaults to auto-detection from the current environment
    access_key (str, optional): AWS Secret Access Key, defaults to auto-detection from the current environment
    credentials_provider (Callable[[], S3Credentials], optional): Custom credentials provider function, should return a `S3Credentials` object
    buffer_time (int, optional): Amount of time in seconds before the actual credential expiration time where credentials given by `credentials_provider` are considered expired, defaults to 10s
    max_connections (int, optional): Maximum number of connections to S3 at any time per io thread, defaults to 8
    session_token (str, optional): AWS Session Token, required only if `key_id` and `access_key` are temporary credentials
    retry_initial_backoff_ms (int, optional): Initial backoff duration in milliseconds for an S3 retry, defaults to 1000ms
    connect_timeout_ms (int, optional): Timeout duration to wait to make a connection to S3 in milliseconds, defaults to 30 seconds
    read_timeout_ms (int, optional): Timeout duration to wait to read the first byte from S3 in milliseconds, defaults to 30 seconds
    num_tries (int, optional): Number of attempts to make a connection, defaults to 25
    retry_mode (str, optional): Retry Mode when a request fails, current supported values are `standard` and `adaptive`, defaults to `adaptive`
    anonymous (bool, optional): Whether or not to use "anonymous mode", which will access S3 without any credentials
    use_ssl (bool, optional): Whether or not to use SSL, which require accessing S3 over HTTPS rather than HTTP, defaults to True
    verify_ssl (bool, optional): Whether or not to verify ssl certificates, which will access S3 without checking if the certs are valid, defaults to True
    check_hostname_ssl (bool, optional): Whether or not to verify the hostname when verifying ssl certificates, this was the legacy behavior for openssl, defaults to True
    requester_pays (bool, optional): Whether or not the authenticated user will assume transfer costs, which is required by some providers of bulk data, defaults to False
    force_virtual_addressing (bool, optional): Force S3 client to use virtual addressing in all cases. If False, virtual addressing will only be used if `endpoint_url` is empty, defaults to False
    profile_name (str, optional): Name of AWS_PROFILE to load, defaults to None which will then check the Environment Variable `AWS_PROFILE` then fall back to `default`
    multipart_size (int, optional): The size of multipart part (bytes), the size range should be 5MB to 5GB, defaults to 8MB.
    multipart_max_concurrency (int, optional): The max concurrency of upload part per object, defaults to 100.
    custom_retry_msgs (list[str], optional): Will retry the request if any custom retry message appeared in the error message of response, defaults to None.

## S3Credentials

```python
S3Credentials(key_id: str, access_key: str, session_token: str | None=None, expiry: datetime.datetime | None=None)
```

_(no docstring)_

## TosConfig

```python
TosConfig(region: str | None=None, endpoint: str | None=None, access_key: str | None=None, secret_key: str | None=None, security_token: str | None=None, anonymous: bool | None=None, max_retries: int | None=None, retry_timeout_ms: int | None=None, connect_timeout_ms: int | None=None, read_timeout_ms: int | None=None, max_concurrent_requests: int | None=None, max_connections_per_io_thread: int | None=None)
```

I/O configuration for accessing Volcengine TOS (Torch Object Storage).

Args:
    region (str, optional): Name of the region to be used, defaults to None, it can be detected automatically from endpoint if standard endpoint is set.
    endpoint (str, optional): URL to the TOS endpoint, defaults to None for Volcengine TOS, it can be inferred from region.
    access_key (str, optional): TOS Access Key, defaults to None.
    secret_key (str, optional): TOS Secret Key, defaults to None.
    security_token (str, optional): TOS Security Token, required for temporary credentials, defaults to None.
    anonymous (bool, optional): Whether to use "anonymous mode" or not, which will access TOS without any credentials. Defaults to False.
    max_retries (int, optional): Maximum number of retries for failed requests, defaults to 3.
    retry_timeout_ms (int, optional): Timeout duration for retry attempts in milliseconds, defaults to 30000ms.
    connect_timeout_ms (int, optional): Timeout duration to wait to make a connection to TOS in milliseconds, defaults to 10000ms.
    read_timeout_ms (int, optional): Timeout duration to wait to read the first byte from TOS in milliseconds, defaults to 30000ms.
    max_concurrent_requests (int, optional): Maximum number of concurrent requests to TOS at any time, defaults to 50.
    max_connections_per_io_thread (int, optional): Maximum number of connections to TOS per IO thread, defaults to 50.

## UnityConfig

```python
UnityConfig(endpoint: str | None, token: str | None)
```

I/O configuration for Unity Catalog volumes.
