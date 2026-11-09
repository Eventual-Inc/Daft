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

## S3-compatible services

Daft can read from and write to any S3-compatible service through the same `s3://` protocol. Set `endpoint_url` on [`daft.io.S3Config`][daft.io.S3Config] to the service's S3 endpoint. When `endpoint_url` is set, Daft uses path-style requests (`{endpoint}/{bucket}/{key}`) unless `force_virtual_addressing=True`.

### Neon Object Storage

For [Neon Object Storage](https://neon.com/docs/storage/overview), set `endpoint_url` to the branch's S3 endpoint (`https://{BRANCH_ID}.storage.{CELL}.{REGION}.aws.neon.tech`), `key_id` to the credential's `token_id` and `access_key` to its `s3_secret_access_key`. Daft also reads the endpoint from `AWS_ENDPOINT_URL_S3` or `AWS_ENDPOINT_URL`. Daft always uses path-style requests for Neon endpoints, so `force_virtual_addressing` has no effect on them.

=== "🐍 Python"

    ```python
    from daft.io import IOConfig, S3Config

    io_config = IOConfig(
        s3=S3Config(
            endpoint_url="https://br-example-123456.storage.c-7.us-east-2.aws.neon.tech",
            key_id="nak_live_...",
            access_key="nsk_live_...",
        )
    )

    df = daft.read_parquet("s3://my-bucket/path/*.parquet", io_config=io_config)
    ```
