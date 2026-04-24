from __future__ import annotations

import hello
import pytest
from hello import greet, string_count

import daft
from daft import col
from daft.session import Session


def test_greet():
    sess = Session()
    sess.load_extension(hello)

    df = daft.from_pydict({"name": ["John", "Paul"]})

    with sess:
        result = df.select(greet(col("name"))).collect().to_pydict()

    values = result["greet"]
    assert values[0] == "Hello, John!"
    assert values[1] == "Hello, Paul!"


def test_greet_null():
    sess = Session()
    sess.load_extension(hello)

    df = daft.from_pydict({"name": ["George", "Ringo", None]})

    with sess:
        result = df.select(greet(col("name"))).collect().to_pydict()

    values = result["greet"]
    assert values[0] == "Hello, George!"
    assert values[1] == "Hello, Ringo!"
    assert values[2] is None


def test_greet_show(capsys):
    """Verify .show() output for use in docs/extensions/index.md."""
    sess = Session()
    sess.load_extension(hello)

    df = daft.from_pydict({"name": ["John", "Paul"]})

    with sess:
        df = df.select(hello.greet(df["name"]))
        df.show()

    captured = capsys.readouterr().out
    # The output should contain our greeted values
    assert "Hello, John!" in captured
    assert "Hello, Paul!" in captured
    # Print it so the test runner shows the formatted table
    print(captured)


def test_string_count():
    sess = Session()
    sess.load_extension(hello)

    df = daft.from_pydict({"name": ["Alice", "Bob", "Carol"]})

    with sess:
        result = df.agg(string_count(col("name"))).collect().to_pydict()

    assert result["string_count"][0] == 3


def test_string_count_with_nulls():
    sess = Session()
    sess.load_extension(hello)

    df = daft.from_pydict({"name": ["Alice", None, "Carol", None]})

    with sess:
        result = df.agg(string_count(col("name"))).collect().to_pydict()

    assert result["string_count"][0] == 2


def test_aggregate_not_available_without_extension():
    sess = Session()

    df = daft.from_pydict({"name": ["Alice"]})

    with sess:
        with pytest.raises(Exception):
            df.agg(daft.get_aggregate_function("string_count", col("name"))).collect()
