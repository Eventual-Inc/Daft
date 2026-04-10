# Reading from and Writing to Google Cloud Storage

Daft is able to read/write data to/from Google Cloud Storage (GCS), and understands natively the URL protocols `gs://` and `gcs://` as referring to data that resides in GCS.

## Authorization/Authentication

In GCS, data is stored under the hierarchy of:

1. Project: The Google Cloud project that owns the storage resources.
2. Bucket: The container for data storage.
3. Object Key: The unique identifier for a piece of data within a bucket.

URLs to data in GCS come in the form: `gs://{BUCKET}/{OBJECT_KEY}`.

### Rely on Environment

You can configure [Application Default Credentials (ADC)](https://cloud.google.com/docs/authentication/application-default-credentials) to have Daft automatically discover credentials. Common methods include:

- Setting the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to point to a service account key file
- Running `gcloud auth application-default login` for local development
- Using the default service account when running on Google Cloud (GCE, GKE, Cloud Run, etc.)

Please be aware that when doing so in a distributed environment such as Ray, Daft will pick these credentials up from worker machines and thus each worker machine needs to be appropriately provisioned.

If instead you wish to have Daft use credentials from the "driver", you may wish to manually specify your credentials.

### Manually specify credentials

You may also choose to pass these values into your Daft I/O function calls using a [`daft.io.GCSConfig`][daft.io.GCSConfig] config object.

[`daft.set_planning_config`][daft.context.set_planning_config] is a convenient way to set your [`daft.io.IOConfig`][daft.io.IOConfig] as the default config to use on any subsequent Daft method calls.

=== "Using Service Account Credentials"

    ```python
    from daft.io import IOConfig, GCSConfig

    # Path to your service account JSON key file
    io_config = IOConfig(gcs=GCSConfig(credentials="/path/to/service-account-key.json"))

    # Globally set the default IOConfig for any subsequent I/O calls
    daft.set_planning_config(default_io_config=io_config)

    # Perform some I/O operation
    df = daft.read_parquet("gs://my_bucket/my_path/**/*")
    ```

=== "Using OAuth2 Token"

    ```python
    from daft.io import IOConfig, GCSConfig

    # Use an OAuth2 access token directly
    io_config = IOConfig(gcs=GCSConfig(token="your-oauth2-access-token"))

    # Perform some I/O operation
    df = daft.read_parquet("gs://my_bucket/my_path/**/*", io_config=io_config)
    ```

=== "Anonymous Access"

    ```python
    from daft.io import IOConfig, GCSConfig

    # Access public buckets without credentials
    io_config = IOConfig(gcs=GCSConfig(anonymous=True))

    # Read from a public bucket
    df = daft.read_parquet("gs://public_bucket/data/**/*", io_config=io_config)
    ```

Alternatively, Daft supports overriding the default IOConfig per-operation by passing it into the `io_config=` keyword argument. This is extremely flexible as you can pass a different [`daft.io.GCSConfig`][daft.io.GCSConfig] per function call if you wish!

=== "Per-Operation Config"

    ```python
    from daft.io import IOConfig, GCSConfig

    io_config = IOConfig(gcs=GCSConfig(credentials="/path/to/service-account-key.json"))

    # Perform some I/O operation but override the IOConfig
    df2 = daft.read_csv("gs://my_bucket/my_other_path/**/*", io_config=io_config)
    ```

## Configuration Options

The [`daft.io.GCSConfig`][daft.io.GCSConfig] object supports the following options:

| Parameter | Type | Description |
|-----------|------|-------------|
| `project_id` | `str` | Google Cloud project ID |
| `credentials` | `str` | Path to service account JSON key file |
| `token` | `str` | OAuth2 access token |
| `anonymous` | `bool` | Whether to use anonymous access (for public buckets) |
| `max_connections` | `int` | Maximum number of concurrent connections |
| `retry_initial_backoff_ms` | `int` | Initial backoff time in milliseconds for retries |
| `connect_timeout_ms` | `int` | Connection timeout in milliseconds |
| `read_timeout_ms` | `int` | Read timeout in milliseconds |
| `num_tries` | `int` | Number of retry attempts |
