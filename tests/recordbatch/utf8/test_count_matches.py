from __future__ import annotations

import re

import pytest

from daft.expressions import col
from daft.recordbatch import MicroPartition


def py_count_matches(s, pat, whole_words, case_sensitive):
    if s is None:
        return None
    if isinstance(pat, list):
        pat = "|".join(pat)
    if whole_words:
        pat = f"\\b({pat})\\b"
    reg = re.compile(pat, re.IGNORECASE if not case_sensitive else 0)
    return len(reg.findall(s))


test_data = [
    "the quick brown fox jumped over the lazy dog",
    "the quick brown foe jumped o'er the lazy dot",
    "the fox fox fox jumped over over dog lazy dog",
    "the quick brown foxes hovered above the lazy dogs",
    "the quick brown-fox jumped over the 'lazy dog'",
    None,
    "thequickbrownfoxjumpedoverthelazydog",
    "THE QUICK BROWN FOX JUMPED over THE Lazy DOG",
    "    fox     dog        over    ",
    None,
]


@pytest.mark.parametrize("whole_words", [False, True])
@pytest.mark.parametrize("case_sensitive", [False, True])
def test_table_count_matches(whole_words, case_sensitive):
    pat = ["fox", "over", "lazy dog", "dog"]
    df = MicroPartition.from_pydict({"a": test_data})
    res = df.eval_expression_list(
        [col("a").str.count_matches(pat, whole_words=whole_words, case_sensitive=case_sensitive)]
    )
    assert res.to_pydict()["a"] == [py_count_matches(s, pat, whole_words, case_sensitive) for s in test_data]


@pytest.mark.parametrize("whole_words", [False, True])
@pytest.mark.parametrize("case_sensitive", [False, True])
@pytest.mark.parametrize("pat", ["fox", "over", "lazy dog", "dog"])
def test_table_count_matches_single_pattern(whole_words, case_sensitive, pat):
    df = MicroPartition.from_pydict({"a": test_data})
    res = df.eval_expression_list(
        [col("a").str.count_matches(pat, whole_words=whole_words, case_sensitive=case_sensitive)]
    )
    assert res.to_pydict()["a"] == [py_count_matches(s, pat, whole_words, case_sensitive) for s in test_data]


def py_regexp_count(s, pattern):
    if s is None:
        return None
    reg = re.compile(pattern)
    return len(reg.findall(s))


regex_test_data = [
    "hello world",
    "foo bar baz",
    "test123test456",
    "abc def ghi",
    "multiple spaces   here",
    None,
    "no matches here",
    "123 456 789",
    "special chars !@# $%^",
    None,
]


@pytest.mark.parametrize("pattern", [r"\w+", r"\d+", r"[a-z]+", r"\s+"])
def test_table_regexp_count(pattern):
    df = MicroPartition.from_pydict({"a": regex_test_data})
    res = df.eval_expression_list([col("a").regexp_count(pattern)])
    assert res.to_pydict()["a"] == [py_regexp_count(s, pattern) for s in regex_test_data]


def test_table_regexp_count_edge_cases():
    df = MicroPartition.from_pydict({"a": ["", "a", "aa", "aaa", None]})

    # Test empty string pattern - empty pattern matches every position
    res = df.eval_expression_list([col("a").regexp_count("")])
    assert res.to_pydict()["a"] == [1, 2, 3, 4, None]  # Empty pattern matches every position

    # Test single character pattern
    res = df.eval_expression_list([col("a").regexp_count("a")])
    assert res.to_pydict()["a"] == [0, 1, 2, 3, None]

    # Test word boundary pattern
    res = df.eval_expression_list([col("a").regexp_count(r"\ba\b")])
    assert res.to_pydict()["a"] == [0, 1, 0, 0, None]


def test_table_regexp_count_invalid_pattern():
    df = MicroPartition.from_pydict({"a": ["test"]})

    # Test invalid regex pattern
    with pytest.raises(Exception):  # Should raise an error for invalid regex
        df.eval_expression_list([col("a").regexp_count("[invalid")])


def test_table_regexp_count_complex_patterns():
    df = MicroPartition.from_pydict(
        {
            "a": [
                "hello@world.com",
                "test123@example.org",
                "user.name+tag@domain.co.uk",
                "invalid-email",
                "another@test.com",
                None,
            ]
        }
    )

    # Test email pattern
    email_pattern = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
    res = df.eval_expression_list([col("a").regexp_count(email_pattern)])
    expected = [1, 1, 1, 0, 1, None]
    assert res.to_pydict()["a"] == expected

    # Test digit pattern
    res = df.eval_expression_list([col("a").regexp_count(r"\d+")])
    expected = [0, 1, 0, 0, 0, None]
    assert res.to_pydict()["a"] == expected
