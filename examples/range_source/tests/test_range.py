from __future__ import annotations

import range_source
from range_source import range

import daft
from daft.session import Session


def test_range_basic():
    sess = Session()
    sess.load_extension(range_source)

    with sess:
        df = range(n=100, partitions=4)
        result = df.collect().to_pydict()

    values = sorted(result["value"])
    assert values == list(range(100)), f"expected 0..99, got {len(values)} values"


def test_range_default():
    sess = Session()
    sess.load_extension(range_source)

    with sess:
        df = range()
        result = df.collect().to_pydict()

    values = result["value"]
    assert len(values) == 1000


def test_range_with_limit():
    sess = Session()
    sess.load_extension(range_source)

    with sess:
        df = range(n=1000, partitions=4).limit(10)
        result = df.collect().to_pydict()

    values = result["value"]
    assert len(values) == 10


def test_range_with_select():
    sess = Session()
    sess.load_extension(range_source)

    with sess:
        df = range(n=100, partitions=2).select(daft.col("value"))
        result = df.collect().to_pydict()

    assert "value" in result
    assert len(result["value"]) == 100
