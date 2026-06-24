from __future__ import annotations

import pytest

import daft
from daft import col
from daft.functions import overlay


def test_overlay_basic_default_len():
    # overlay("AAAAAAAAAA", "BBB", 3): default len = len(replace) = 3
    # -> src[0..2] + "BBB" + src[5..] = "AA" + "BBB" + "AAAAA" = "AABBBAAAAA"
    df = daft.from_pydict({"src": ["AAAAAAAAAA"]})
    result = df.select(overlay(col("src"), "BBB", 3)).to_pydict()
    assert result["src"] == ["AABBBAAAAA"]


def test_overlay_explicit_len_matches_replace():
    df = daft.from_pydict({"src": ["AAAAAAAAAA"]})
    result = df.select(overlay(col("src"), "BBB", 3, 3)).to_pydict()
    assert result["src"] == ["AABBBAAAAA"]


def test_overlay_len_smaller_than_replace():
    # overlay("AAAAAAAAAA", "RRR", 3, 1): replace 1 char with "RRR"
    # -> src[0..2] + "RRR" + src[3..] = "AA" + "RRR" + "AAAAAAA" = "AARRRAAAAAAA"
    df = daft.from_pydict({"src": ["AAAAAAAAAA"]})
    result = df.select(overlay(col("src"), "RRR", 3, 1)).to_pydict()
    assert result["src"] == ["AARRRAAAAAAA"]


def test_overlay_len_larger_than_replace():
    # overlay("AAAAAAAAAA", "RR", 3, 4): replace 4 chars with "RR"
    # -> src[0..2] + "RR" + src[6..] = "AA" + "RR" + "AAAA" = "AARRAAAA"
    df = daft.from_pydict({"src": ["AAAAAAAAAA"]})
    result = df.select(overlay(col("src"), "RR", 3, 4)).to_pydict()
    assert result["src"] == ["AARRAAAA"]


def test_overlay_pos_at_start():
    df = daft.from_pydict({"src": ["hello"]})
    result = df.select(overlay(col("src"), "X", 1)).to_pydict()
    assert result["src"] == ["Xello"]


def test_overlay_pos_at_end():
    df = daft.from_pydict({"src": ["hello"]})
    result = df.select(overlay(col("src"), "X", 5)).to_pydict()
    assert result["src"] == ["hellX"]


def test_overlay_pos_beyond_end_appends():
    df = daft.from_pydict({"src": ["hi"]})
    result = df.select(overlay(col("src"), "X", 10)).to_pydict()
    assert result["src"] == ["hiX"]


def test_overlay_pos_zero_clamped_to_one():
    df = daft.from_pydict({"src": ["hello"]})
    result = df.select(overlay(col("src"), "X", 0)).to_pydict()
    assert result["src"] == ["Xello"]


def test_overlay_empty_replace():
    df = daft.from_pydict({"src": ["hello"]})
    result = df.select(overlay(col("src"), "", 2)).to_pydict()
    assert result["src"] == ["hello"]


def test_overlay_empty_src():
    df = daft.from_pydict({"src": [""]})
    result = df.select(overlay(col("src"), "X", 1)).to_pydict()
    assert result["src"] == ["X"]


def test_overlay_multiple_rows():
    df = daft.from_pydict({"src": ["AAAAAAAAAA", "hello world", "abcdef"]})
    result = df.select(overlay(col("src"), "X", 1)).to_pydict()
    assert result["src"] == ["XAAAAAAAAA", "Xello world", "Xbcdef"]


def test_overlay_null_pos_propagates_null():
    df = daft.from_pydict({"src": ["hello", "world"]})
    result = df.select(overlay(col("src"), "X", None)).to_pydict()
    assert result["src"] == [None, None]


def test_overlay_non_string_raises():
    df = daft.from_pydict({"src": [1, 2, 3]})
    with pytest.raises(Exception):
        df.select(overlay(col("src"), "X", 1)).collect()
