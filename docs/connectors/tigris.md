# Reading from and Writing to Tigris

[Tigris](https://www.tigrisdata.com/docs/overview/) is an S3-compatible object storage service. Daft connects to it through its native S3 support: use `s3://` URLs together with a [`daft.io.S3Config`][daft.io.S3Config] that points `endpoint_url` at Tigris.

Two properties of the service are relevant when running Daft against it:

- Tigris buckets are globally distributed: objects are served from the region closest to the requester, so a Daft cluster does not need to be co-located with the bucket's region to get local-read latency. See [Tigris architecture](https://www.tigrisdata.com/docs/concepts/architecture/) for details.
- Tigris does not bill for data transfer (egress), so repeated reads of the same dataset from compute in any cloud or region do not incur transfer costs. See [Tigris pricing](https://www.tigrisdata.com/pricing/) for details.

## Authorization/Authentication

In Tigris, data is stored under the hierarchy of:

1. Bucket: The container for data storage, which is the top-level namespace for data storage.
2. Object Key: The unique identifier for a piece of data within a bucket.

URLs to data in Tigris come in the form: `s3://{BUCKET}/{OBJECT_KEY}`.

Credentials are access key pairs created in the [Tigris console](https://console.storage.dev): an Access Key ID (prefixed `tid_`) and a Secret Access Key (prefixed `tsec_`). Tigris authenticates with access key pairs only — STS and temporary session credentials are not supported, so `session_token` is not used.

All requests must be directed at the Tigris endpoint `https://t3.storage.dev`. Set it explicitly via `endpoint_url` in the [`daft.io.S3Config`][daft.io.S3Config]: Daft only switches to path-style addressing when `endpoint_url` is set on the config itself, so an endpoint configured elsewhere (e.g. environment variables or an AWS profile) is not sufficient. Use `region_name="auto"` — Tigris routes requests to the nearest region automatically.

### Rely on Environment

Daft auto-detects the access key pair from the standard AWS environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, or from an AWS profile (via `profile_name` or the `AWS_PROFILE` environment variable):

```bash
export AWS_ACCESS_KEY_ID="tid_your_access_key"
export AWS_SECRET_ACCESS_KEY="tsec_your_secret_key"
```

=== "🐍 Python"

    ```python
    import daft
    from daft.io import IOConfig, S3Config

    # Credentials are picked up from the environment; only the endpoint is Tigris-specific
    io_config = IOConfig(
        s3=S3Config(
            endpoint_url="https://t3.storage.dev",
            region_name="auto",
        )
    )

    df = daft.read_parquet("s3://my-tigris-bucket/my_path/**/*", io_config=io_config)
    ```

Please be aware that when doing so in a distributed environment such as Ray, Daft will pick these credentials up from worker machines and thus each worker machine needs to be appropriately provisioned.

If instead you wish to have Daft use credentials from the "driver", you may wish to manually specify your credentials.

### Manually specify credentials

You may also choose to pass these values into your Daft I/O function calls using a [`daft.io.S3Config`][daft.io.S3Config] config object.

[`daft.set_planning_config`][daft.context.set_planning_config] is a convenient way to set your [`daft.io.IOConfig`][daft.io.IOConfig] as the default config to use on any subsequent Daft method calls.

=== "🐍 Python"

    ```python
    import daft
    from daft.io import IOConfig, S3Config

    io_config = IOConfig(
        s3=S3Config(
            endpoint_url="https://t3.storage.dev",
            region_name="auto",
            key_id="tid_your_access_key",
            access_key="tsec_your_secret_key",
        )
    )

    # Globally set the default IOConfig for any subsequent I/O calls
    daft.set_planning_config(default_io_config=io_config)

    # Perform some I/O operation
    df = daft.read_parquet("s3://my-tigris-bucket/my_path/**/*")
    ```

Alternatively, Daft supports overriding the default IOConfig per-operation by passing it into the `io_config=` keyword argument. This is extremely flexible as you can pass a different [`daft.io.S3Config`][daft.io.S3Config] per function call if you wish!

=== "🐍 Python"

    ```python
    # Perform some I/O operation but override the IOConfig
    df2 = daft.read_csv("s3://my-tigris-bucket/my_other_path/**/*", io_config=io_config)
    ```

## Writing Data

Writes use the same configuration, including multipart uploads for large files:

=== "🐍 Python"

    ```python
    import daft
    from daft.io import IOConfig, S3Config

    io_config = IOConfig(
        s3=S3Config(
            endpoint_url="https://t3.storage.dev",
            region_name="auto",
            key_id="tid_your_access_key",
            access_key="tsec_your_secret_key",
        )
    )

    df = daft.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"]})

    # Write to Tigris as Parquet
    df.write_parquet("s3://my-tigris-bucket/output/", io_config=io_config)
    ```

## Notes

- Tigris supports both path-style and virtual-hosted-style addressing. When `endpoint_url` is set, Daft defaults to path-style; set `force_virtual_addressing=True` to use virtual-hosted-style requests (`{bucket}.t3.storage.dev`) instead. Both work.
- `session_token`, `requester_pays`, and STS-based `credentials_provider` flows do not apply to Tigris.
- All other [`daft.io.S3Config`][daft.io.S3Config] tuning options (`max_connections`, `num_tries`, `retry_mode`, timeouts, `multipart_size`, `multipart_max_concurrency`) behave the same as against AWS S3.
