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
        # `\n`/`\t` are a literal backslash plus a letter, never a newline/tab.
        (r"\n", r"a\nc"),
        (r"\t", r"a\tc"),
        # The regex crate's own braced syntax passes through untouched.
        ("${1}", "abc"),
        # A `$` the regex crate would not read as a reference stays literal.
        ("$", "a$c"),
        # A literal `$` in front of a group reference must not be swallowed.
        (r"$\1", "a$bc"),
        # Only one digit is consumed: `\10` is group 1 then a literal `0`.
        (r"\10", "ab0c"),
        # Empty replacement deletes the match.
        ("", "ac"),
        # Multi-byte characters survive around a literal backslash.
        (r"é\ü", r"aé\üc"),
    ],
)
def test_series_utf8_regexp_replace_backslashes(replacement, expected) -> None:
    # Regression test for https://github.com/Eventual-Inc/Daft/issues/7471.
    table = MicroPartition.from_pydict({"col": ["abc"]})
    result = table.eval_expression_list([col("col").regexp_replace("(b)", replacement)])
    assert result.to_pydict() == {"col": [expected]}


def test_series_utf8_replace_literal_keeps_template_chars() -> None:
    # `replace` (non-regex) has no template semantics: backslashes and `$` are
    # ordinary characters there.
    table = MicroPartition.from_pydict({"col": ["abc"]})
    result = table.eval_expression_list([col("col").replace("b", r"\1$1\\")])
    assert result.to_pydict() == {"col": [r"a\1$1\\c"]}


def test_series_utf8_regexp_replace_per_row_replacement() -> None:
    # Templates are translated per row when the replacement is a real column,
    # not just broadcast from a single scalar.
    table = MicroPartition.from_pydict({"col": ["abc", "abc"], "repl": [r"[\1]", "x\\"]})
    result = table.eval_expression_list([col("col").regexp_replace("(b)", col("repl"))])
    assert result.to_pydict() == {"col": ["a[b]c", r"ax\c"]}
