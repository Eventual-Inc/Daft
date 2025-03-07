import pytest

from daft import Catalog

# https://github.com/Eventual-Inc/Daft/issues/3925


@pytest.mark.skip("S3 Tables will required integration tests, for now verify manually.")
def test_sanity():
    table_bucket_arn = "placeholder"
    s3tb = Catalog.from_s3tables(table_bucket_arn)
    s3tb.read_table("demo.points").show()
