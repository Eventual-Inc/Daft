import daft


def test_from_pydict():
    # Create sample data dictionary
    data = {"id": [1, 2, 3, 4, 5], "name": ["Alice", "Bob", "Charlie", "David", "Eve"], "age": [25, 30, 35, 40, 45]}

    # Create Daft DataFrame from dictionary
    df = daft.from_pydict(data)

    # Perform select with filter
    result = df.select("name", "age").where(df["age"] > 30)
    print(result.collect())


test_from_pydict()
