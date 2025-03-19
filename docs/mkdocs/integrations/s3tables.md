# S3 Tables

Daft integrates with [S3 Tables](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables.html) using its [Catalog](../catalogs.md) interface which supports both reading and writing S3 Tables via Iceberg.

## Example

```python
from daft import Catalog

catalog = Catalog.from_arn("arn:aws:s3tables:<region>:<account>:bucket/<bucket>")

# verify we are connected
catalog.list_tables()

# read some table
catalog.read_table("my_namespace.my_table").show()

# write dataframe to table
catalog.write_table("my_namespace.my_table", daft.read_csv("/path/to/file.csv"))
```

### S3 Tables AWS API

You can use the S3 Tables AWS API via a `boto3` client or `boto3` session.

### S3 Tables Iceberg REST API

The S3 Tables service supports

> You can connect your Iceberg REST client to the Amazon S3 Tables Iceberg REST endpoint and make REST API calls to create, update, or query tables in S3 table buckets. The endpoint implements a set of standardized Iceberg REST APIs specified in the Apache Iceberg REST Catalog Open API specification . The endpoint works by translating Iceberg REST API operations into corresponding S3 Tables operations.

### Glue and LakeFormation

You may also use the AWS Glue Iceberg REST endpoint, which provides unified table management, centralized governance, and fine-grained access control (table level). However this requires service enabling integrations in
