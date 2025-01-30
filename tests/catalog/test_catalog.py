import daft
from daft import DataType as dt
from daft import Schema
from daft.logical.schema import Field


def schema(*columns) -> Schema:
    """Create a Schema with concise syntax."""
    return Schema._from_fields([Field.create(c[0], c[1]) for c in columns])


def test_catalog():
    catalog = daft.create_catalog("default")
    table_t = catalog.create_table("T", schema(("x", dt.int64())))
    table_s = catalog.create_table("S")

    # don't love this extra line..
    daft.attach_catalog(catalog)
