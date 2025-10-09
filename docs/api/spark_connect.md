# PySpark

The `daft.pyspark` module provides a way to create a PySpark session that can be run locally or backed by a ray cluster. This serves as a way to run the Daft query engine with a Spark compatible API.

For the full PySpark SQL API documentation, see the [official PySpark documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/index.html#spark-sql).

## Installing Daft with Spark Connect support

Daft supports Spark Connect through the optional `spark` dependency.

```bash
pip install -U "daft[spark]"
```

## Example

=== "🐍 Python"

```python
from daft.pyspark import SparkSession
from pyspark.sql.functions import col

# Create a local spark session
spark = SparkSession.builder.local().getOrCreate()

# Alternatively, connect to a Ray cluster
# You can use `ray get-head-ip <cluster_config.yaml>` to get the head ip!
spark = SparkSession.builder.remote("ray://<HEAD_IP>:10001").getOrCreate()

# Use spark as you would with the native spark library, but with a daft backend!
spark.createDataFrame([{"hello": "world"}]).select(col("hello")).show()

# Stop the Spark session
spark.stop()
```

## Object Store Configuration (S3, Azure, GCS)

For reading and writing remote files such as s3 buckets, you will need to pass in the credentials using daft's io config.

Here's a list of supported config values that you can set via
`spark.conf.set("<key>", "<value>")`

### S3 Configuration Options

| Configuration Key | Type |
|-------------------|------|
| daft.io.s3.region_name | String |
| daft.io.s3.endpoint_url | String |
| daft.io.s3.key_id | String |
| daft.io.s3.session_token | String |
| daft.io.s3.access_key | String |
| daft.io.s3.buffer_time | Integer |
| daft.io.s3.max_connections_per_io_thread | Integer |
| daft.io.s3.retry_initial_backoff_ms | Integer |
| daft.io.s3.connect_timeout_ms | Integer |
| daft.io.s3.read_timeout_ms | Integer |
| daft.io.s3.num_tries | Integer |
| daft.io.s3.retry_mode | String |
| daft.io.s3.anonymous | Boolean |
| daft.io.s3.use_ssl | Boolean |
| daft.io.s3.verify_ssl | Boolean |
| daft.io.s3.check_hostname_ssl | Boolean |
| daft.io.s3.requester_pays | Boolean |
| daft.io.s3.force_virtual_addressing | Boolean |
| daft.io.s3.profile_name | String |

### Azure Storage Configuration Options

| Configuration Key | Type |
|-------------------|------|
| daft.io.azure.storage_account | String |
| daft.io.azure.access_key | String |
| daft.io.azure.sas_token | String |
| daft.io.azure.bearer_token | String |
| daft.io.azure.tenant_id | String |
| daft.io.azure.client_id | String |
| daft.io.azure.client_secret | String |
| daft.io.azure.use_fabric_endpoint | Boolean |
| daft.io.azure.anonymous | Boolean |
| daft.io.azure.endpoint_url | String |
| daft.io.azure.use_ssl | Boolean |

### GCS Configuration Options

| Configuration Key | Type |
|-------------------|------|
| daft.io.gcs.project_id | String |
| daft.io.gcs.credentials | String |
| daft.io.gcs.token | String |
| daft.io.gcs.anonymous | Boolean |
| daft.io.gcs.max_connections_per_io_thread | Integer |
| daft.io.gcs.retry_initial_backoff_ms | Integer |
| daft.io.gcs.connect_timeout_ms | Integer |
| daft.io.gcs.read_timeout_ms | Integer |
| daft.io.gcs.num_tries | Integer |

### HTTP Configuration Options

| Configuration Key | Type |
|-------------------|------|
| daft.io.http.user_agent | String |
| daft.io.http.bearer_token | String |


## Notable Differences

A few methods do have some notable differences compared to PySpark.

### explain

The `df.explain()` method will output non-Spark compatible `explain` and instead will be the same as calling `explain` on a DataFrame.

### show

Similarly, `df.show()` will output Daft's dataframe output instead of native Spark's.
