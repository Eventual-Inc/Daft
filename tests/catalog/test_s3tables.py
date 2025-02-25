import pytest

from daft.catalog.__s3tables import S3Catalog


@pytest.skip()
def test_sanity():
    cat = S3Catalog.from_arn(arn)
    tbl = cat.get_table("demo.points")
    tbl.read().show()
