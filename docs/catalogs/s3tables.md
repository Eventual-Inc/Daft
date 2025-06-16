# S3 Tables

Daft integrates with [S3 Tables](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables.html) using its [Catalog](index.md) interface which supports both reading and writing S3 Tables via Iceberg.

## Example

```python
from daft import Catalog

# ensure your aws credentials are configure, for example:
# import os
# os.environ["AWS_ACCESS_KEY_ID"] = "<access-id>"
# os.environ["AWS_SECRET_ACCESS_KEY"] = "<access-key>"
# os.environ["AWS_DEFAULT_REGION"] = "<region>"

catalog = Catalog.from_s3tables("arn:aws:s3tables:<region>:<account>:bucket/<bucket>")

# verify we are connected
catalog.list_tables("demo")
"""
['demo.points']
"""

# read some table
catalog.read_table("my_namespace.my_table").show()
"""
╭─────────┬───────┬──────╮
│ x       ┆ y     ┆ z    │
│ ---     ┆ ---   ┆ ---  │
│ Boolean ┆ Int64 ┆ Utf8 │
╞═════════╪═══════╪══════╡
│ true    ┆ 1     ┆ a    │
├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
│ true    ┆ 2     ┆ b    │
├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
│ false   ┆ 3     ┆ c    │
╰─────────┴───────┴──────╯

(Showing first 3 of 3 rows)
"""

# write dataframe to table
catalog.write_table(
    "demo.points",
    daft.from_pydict(
        {
            "x": [True],
            "y": [4],
            "z": ["d"],
        }
    ),
)

# check that the data was written
catalog.read_table("my_namespace.my_table").show()
"""
╭─────────┬───────┬──────╮
│ x       ┆ y     ┆ z    │
│ ---     ┆ ---   ┆ ---  │
│ Boolean ┆ Int64 ┆ Utf8 │
╞═════════╪═══════╪══════╡
│ true    ┆ 4     ┆ d    │
├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
│ true    ┆ 1     ┆ a    │
├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
│ true    ┆ 2     ┆ b    │
├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
│ false   ┆ 3     ┆ c    │
╰─────────┴───────┴──────╯

(Showing first 4 of 4 rows)
"""
```

### S3 Tables AWS API

You can use the S3 Tables AWS API via a `boto3` client or `boto3` session.

### S3 Tables Iceberg REST API

S3 Tables offers an [Iceberg REST compatible endpoint](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-open-source.html) which Daft uses when you create a catalog via [`from_s3tables`][daft.Catalog.from_s3tables].

> You can connect your Iceberg REST client to the Amazon S3 Tables Iceberg REST endpoint and make REST API calls to create, update, or query tables in S3 table buckets. The endpoint implements a set of standardized Iceberg REST APIs specified in the Apache Iceberg REST Catalog Open API specification. The endpoint works by translating Iceberg REST API operations into corresponding S3 Tables operations.

### Glue and LakeFormation

You may also use the AWS Glue Iceberg REST endpoint, which provides unified table management, centralized governance, and fine-grained access control (table level). However this requires service enabling integrations in AWS Glue and LakeFormation.
