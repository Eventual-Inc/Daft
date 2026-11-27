# Reading from and Writing to AWS S3

Daft has [record-setting](https://www.daft.ai/blog/announcing-daft-02) performance when reading from and writing to AWS S3, and understands natively the URL protocol `s3://` as referring to data that resides in S3.

## Authorization/Authentication

In AWS S3, data is stored under the hierarchy of:

1. Bucket: The container for data storage, which is the top-level namespace for data storage in S3.
2. Object Key: The unique identifier for a piece of data within a bucket.

URLs to data in S3 come in the form: `s3://{BUCKET}/{OBJECT_KEY}`.

### Rely on Environment

You can configure the AWS [CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html) to have Daft automatically discover credentials. Alternatively, you may specify your credentials in [environment variables](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html): `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN`.

Please be aware that when doing so in a distributed environment such as Ray, Daft will pick these credentials up from worker machines and thus each worker machine needs to be appropriately provisioned.

If instead you wish to have Daft use credentials from the "driver", you may wish to manually specify your credentials.

### Manually specify credentials

You may also choose to pass these values into your Daft I/O function calls using an [`daft.io.S3Config`][daft.io.S3Config] config object.

[`daft.set_planning_config`][daft.context.set_planning_config] is a convenient way to set your [`daft.io.IOConfig`][daft.io.IOConfig] as the default config to use on any subsequent Daft method calls.

=== "🐍 Python"

    ```python
    from daft.io import IOConfig, S3Config

    # Supply actual values: key_id is the AWS Access Key ID, access_key is the AWS Secret Access Key
    io_config = IOConfig(s3=S3Config(key_id="key_id", session_token="session_token", access_key="access_key"))

    # Globally set the default IOConfig for any subsequent I/O calls
    daft.set_planning_config(default_io_config=io_config)

    # Perform some I/O operation
    df = daft.read_parquet("s3://my_bucket/my_path/**/*")
    ```

Alternatively, Daft supports overriding the default IOConfig per-operation by passing it into the `io_config=` keyword argument. This is extremely flexible as you can pass a different [`daft.io.S3Config`][daft.io.S3Config] per function call if you wish!

=== "🐍 Python"

    ```python
    # Perform some I/O operation but override the IOConfig
    df2 = daft.read_csv("s3://my_bucket/my_other_path/**/*", io_config=io_config)
    ```

## S3-Compatible Object Storage

The same `s3://` support works with S3-compatible object storage services, such as Backblaze B2, Cloudflare R2, and MinIO. Point [`daft.io.S3Config`][daft.io.S3Config] at the service with `endpoint_url` and set `region_name` to the region that the service expects:

=== "🐍 Python"

    ```python
    import daft
    from daft.io import IOConfig, S3Config

    io_config = IOConfig(
        s3=S3Config(
            endpoint_url="https://s3.example-region.example.com",
            region_name="example-region",
            key_id="your-access-key-id",
            access_key="your-secret-access-key",
        )
    )

    df = daft.read_parquet("s3://my-bucket/my_path/**/*.parquet", io_config=io_config)
    df.write_parquet("s3://my-bucket/my_output_path", io_config=io_config)
    ```

When `endpoint_url` is set, Daft uses path-style requests (`https://<endpoint>/<bucket>/<key>`). If the service requires virtual-hosted-style requests (`https://<bucket>.<endpoint>/<key>`), also set `force_virtual_addressing=True`.

Daft also reads the endpoint from the `AWS_ENDPOINT_URL` environment variable, together with `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_REGION`, so you can configure an S3-compatible service without changing code. In that case Daft sends virtual-hosted-style requests, so the service must support them; set `endpoint_url` in `S3Config` if it needs path-style requests. As with other environment-based credentials, each worker in a distributed environment needs these variables set.
