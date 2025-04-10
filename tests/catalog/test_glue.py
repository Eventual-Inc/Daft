import pytest

from daft.catalog import Catalog
from daft.catalog.__glue import GlueCatalog
from daft.session import Session


@pytest.mark.skip("local glue testing for the moment")
def test_glue_files():
    catalog = GlueCatalog.from_database("placeholder", region_name="us-west-2")
    for table in catalog.list_tables():
        print(table)

    table = catalog.get_table("taxi_parquet")
    print("read_table (parquet)")
    print(table)
    table.show()

    table = catalog.get_table("taxi_csv")
    print("read_table (csv)")
    print(table)
    table.show()


@pytest.mark.skip("Glue create_table, read, write, sql demo")
def test_iceberg_glue():
    import daft

    # Create a Daft Catalog which is implemented via PyIceberg's GlueCatalog
    catalog = Catalog.from_iceberg_glue(name="glue", region="us-west-2", warehouse="s3://rchowell")

    # Load some test data
    df = daft.read_parquet("tests/assets/parquet-data/mvp.parquet")
    df.show()

    # Create a Glue Database
    # if not catalog.has_namespace("rchowell_demo"):
    # catalog.create_namespace("rchowell_demo")

    # Create and write a Glue Table
    # catalog.create_table("rchowell_demo.mvp_1", df)

    # We can also get and read the table
    table = catalog.get_table("rchowell_demo.mvp_1")
    table.show()

    # Execute Daft-SQL
    sess = Session()
    sess.attach(catalog)
    sess.set_namespace("glue_iceberg_test")
    sess.sql("SELECT file_name, height, width FROM coco_images WHERE width > 500").show()
