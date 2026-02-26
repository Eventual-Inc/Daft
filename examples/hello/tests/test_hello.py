from __future__ import annotations

import hello
from hello import greet

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
