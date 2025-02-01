import daft


def test_create_table_from_schema():
    df = daft.from_pydict(
        {
            "first": ["john", "jane"],
            "last": ["deer", "doe"],
        }
    )
    table = daft.create_table("people", df.schema())
    print(table.schema())
