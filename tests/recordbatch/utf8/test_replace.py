from __future__ import annotations

import pytest

import daft
from daft.expressions import col, lit
from daft.recordbatch import MicroPartition


@pytest.mark.parametrize(
    ["expr", "data", "expected"],
    [
        (col("col").replace("a", "b"), daft.Series.from_pylist([]).cast(daft.DataType.string()), []),
        (col("col").replace("a", "b"), ["a", "ab", "c"], ["b", "bb", "c"]),
        (col("col").replace(lit("a"), lit("b")), ["a", "ab", "c"], ["b", "bb", "c"]),
        (
            col("col").replace(col("emptystrings") + lit("a"), col("emptystrings") + lit("b")),
            ["a", "ab", "c"],
            ["b", "bb", "c"],
        ),
        # regex pattern
        (col("col").regexp_replace(r"a+", "b"), ["aaa", "ab", "c"], ["b", "bb", "c"]),
        (col("col").regexp_replace(lit(r"a+"), lit("b")), ["aaa", "ab", "c"], ["b", "bb", "c"]),
        (
            col("col").regexp_replace(col("emptystrings") + lit(r"a+"), col("emptystrings") + lit("b")),
            ["aaa", "ab", "c"],
            ["b", "bb", "c"],
        ),
    ],
)
def test_series_utf8_replace(expr, data, expected) -> None:
    table = MicroPartition.from_pydict({"col": data, "emptystrings": [""] * len(data)})
    result = table.eval_expression_list([expr])
    assert result.to_pydict() == {"col": expected}


@pytest.mark.parametrize(
    ["replacement", "expected"],
    [
        # `$n` and `\n` are both group references.
        ("[$1]", "a[b]c"),
        (r"[\1]", "a[b]c"),
        ("$1", "abc"),
        # Lone backslashes are preserved literally (not silently dropped).
        (r"a\b", r"aa\bc"),
        ("x\\", r"ax\c"),
        # `\\` (two backslashes) is an escaped backslash -> one backslash.
        (r"\\", r"a\c"),
        # Escaped backslash + literal `1`, not a group reference.
        (r"\\1", r"a\1c"),
        # Escaped backslash + group reference.
        (r"\\\1", r"a\bc"),
        ("$$", "a$c"),
    ],
)
def test_series_utf8_regexp_replace_backslashes(replacement, expected) -> None:
    # Regression test for https://github.com/Eventual-Inc/Daft/issues/7471.
    table = MicroPartition.from_pydict({"col": ["abc"]})
    result = table.eval_expression_list([col("col").regexp_replace("(b)", replacement)])
    assert result.to_pydict() == {"col": [expected]}
