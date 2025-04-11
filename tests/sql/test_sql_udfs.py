from __future__ import annotations

import pytest

import daft
from daft.sql import SQLCatalog


def test_sql_udf():
    df = daft.from_pydict({"a": [1, 2, 3]})

    @daft.udf(return_dtype=daft.DataType.int64())
    def multiply_by_n(data, n):
        return [i * n for i in data]

    cat = SQLCatalog({"df": df})

    expected = {"b": [2, 4, 6]}
    actual = daft.sql("select multiply_by_n(a, n:=2) as b from df", cat).to_pydict()

    assert actual == expected


def test_sql_udf_ambigious_name():
    df = daft.from_pydict({"a": [1, 2, 3]})
    cat = SQLCatalog({"df": df})

    @daft.udf(return_dtype=daft.DataType.int64())
    def multiply_by_n(data, n):
        return [i * n for i in data]

    @daft.udf(return_dtype=daft.DataType.int64())
    def MULTIPLY_BY_N(data, n):
        return [i * n for i in data]

    with pytest.raises(Exception, match="Invalid operation: Ambigiuous identifier for Function: found"):
        daft.sql("select multiply_by_n(a, n:=2) from df", cat).to_pydict()


def test_sql_udf_multi_column():
    df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
    cat = SQLCatalog({"df": df})

    @daft.udf(return_dtype=daft.DataType.int64())
    def multiply(a, b):
        return a * b

    expected = {"a": [4, 10, 18]}
    actual = daft.sql("select multiply(a, b) as a from df", cat).to_pydict()
    assert actual == expected


def test_sql_udf_multi_column_and_kwargs():
    df = daft.from_pydict({"first_name": ["Alice", "Bob", "Charlie"], "last_name": ["Smith", "Johnson", "Williams"]})
    cat = SQLCatalog({"df": df})

    @daft.udf(return_dtype=str)
    def make_greeting(a, b, greeting="hello"):
        return [f"{greeting}, {a} {b}" for a, b in zip(a, b)]

    expected = {
        "greeting1": ["hello, Alice Smith", "hello, Bob Johnson", "hello, Charlie Williams"],
        "greeting2": ["hi, Alice Smith", "hi, Bob Johnson", "hi, Charlie Williams"],
    }
    actual = daft.sql(
        """
        select
            make_greeting(first_name, last_name) as greeting1,
            make_greeting(first_name, last_name, greeting:='hi') as greeting2
        from df
    """,
        cat,
    ).to_pydict()
    assert actual == expected


# to differentiate between literals & arguments, the scalar arguments to udfs need to be passed as keyword arguments
@pytest.mark.skip(reason="doesn't work as expected because udf's don error if the args are incorrect")
def test_sql_udf_kwargs_dont_work_as_positional():
    pass
