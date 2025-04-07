import pytest

from daft.catalog.__glue import GlueCatalog

@pytest.mark.skip("local glue testing for the moment")
def test_glue_iceberg():
    catalog: GlueCatalog = GlueCatalog.from_database("placeholder", region_name="us-west-2")
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
