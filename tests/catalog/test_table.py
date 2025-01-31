import daft

def test_create_table():
    df = daft.from_pydict({ 
        "first": ["john", "jane"],
        "last": ["deer", "doe"],
    })
    table = daft.create_table("people", df)
    table.show()
