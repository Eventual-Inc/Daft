from __future__ import annotations

import re

import pytest

from daft.expressions import col
from daft.table import MicroPartition


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
    res = df.eval_expression_list([col("a").str.count_matches(pat, whole_words, case_sensitive)])
    assert res.to_pydict()["a"] == [py_count_matches(s, pat, whole_words, case_sensitive) for s in test_data]


@pytest.mark.parametrize("whole_words", [False, True])
@pytest.mark.parametrize("case_sensitive", [False, True])
@pytest.mark.parametrize("pat", ["fox", "over", "lazy dog", "dog"])
def test_table_count_matches_single_pattern(whole_words, case_sensitive, pat):
    df = MicroPartition.from_pydict({"a": test_data})
    res = df.eval_expression_list([col("a").str.count_matches(pat, whole_words, case_sensitive)])
    assert res.to_pydict()["a"] == [py_count_matches(s, pat, whole_words, case_sensitive) for s in test_data]
