from __future__ import annotations

import pytest

from daft.expressions import col
from daft.recordbatch import MicroPartition


def test_strip_html_basic_tags():
    table = MicroPartition.from_pydict({"col": ["<p>Hello world</p>"]})
    result = table.eval_expression_list([col("col").strip_html()])
    assert result.to_pydict() == {"col": ["Hello world"]}


def test_strip_html_inline_tags():
    table = MicroPartition.from_pydict({"col": ["<p>Hello <b>world</b></p>"]})
    result = table.eval_expression_list([col("col").strip_html()])
    assert result.to_pydict() == {"col": ["Hello world"]}


def test_strip_html_script_removed():
    table = MicroPartition.from_pydict(
        {"col": ["<p>text</p><script>alert(1)</script>"]}
    )
    result = table.eval_expression_list([col("col").strip_html()])
    assert result.to_pydict() == {"col": ["text"]}


def test_strip_html_style_removed():
    table = MicroPartition.from_pydict(
        {"col": ["<style>body{color:red}</style><p>text</p>"]}
    )
    result = table.eval_expression_list([col("col").strip_html()])
    assert result.to_pydict() == {"col": ["text"]}


def test_strip_html_entities_decoded():
    table = MicroPartition.from_pydict({"col": ["<p>fish &amp; chips</p>"]})
    result = table.eval_expression_list([col("col").strip_html()])
    assert result.to_pydict() == {"col": ["fish & chips"]}


def test_strip_html_nbsp_decoded():
    table = MicroPartition.from_pydict({"col": ["<p>hello&nbsp;world</p>"]})
    result = table.eval_expression_list([col("col").strip_html()])
    # &nbsp; decodes to a non-breaking space (U+00A0)
    assert result.to_pydict() == {"col": ["hello\u00a0world"]}


def test_strip_html_block_elements_newline():
    table = MicroPartition.from_pydict({"col": ["<p>first</p><p>second</p>"]})
    result = table.eval_expression_list([col("col").strip_html()])
    text = result.to_pydict()["col"][0]
    assert "first" in text
    assert "second" in text
    assert "\n" in text


def test_strip_html_plain_text_unchanged():
    table = MicroPartition.from_pydict({"col": ["hello world"]})
    result = table.eval_expression_list([col("col").strip_html()])
    assert result.to_pydict() == {"col": ["hello world"]}


def test_strip_html_empty_string():
    table = MicroPartition.from_pydict({"col": [""]})
    result = table.eval_expression_list([col("col").strip_html()])
    assert result.to_pydict() == {"col": [""]}


def test_strip_html_null_passthrough():
    table = MicroPartition.from_pydict({"col": [None, "<p>text</p>", None]})
    result = table.eval_expression_list([col("col").strip_html()])
    assert result.to_pydict() == {"col": [None, "text", None]}


def test_strip_html_batch():
    inputs = [
        "<p>Hello <b>world</b></p>",
        "<div>foo<script>bad()</script>bar</div>",
        "<h1>Title</h1><p>Body &amp; more</p>",
        None,
        "plain text",
    ]
    table = MicroPartition.from_pydict({"col": inputs})
    result = table.eval_expression_list([col("col").strip_html()])
    out = result.to_pydict()["col"]

    assert out[0] == "Hello world"
    assert out[1] == "foobar"
    assert "Title" in out[2] and "Body & more" in out[2]
    assert out[3] is None
    assert out[4] == "plain text"


def test_strip_html_via_function_import():
    from daft.functions import strip_html

    table = MicroPartition.from_pydict({"col": ["<b>bold</b>"]})
    result = table.eval_expression_list([strip_html(col("col"))])
    assert result.to_pydict() == {"col": ["bold"]}


def test_strip_html_separator_space():
    from daft.functions import strip_html

    table = MicroPartition.from_pydict({"col": ["<p>first</p><p>second</p>"]})
    result = table.eval_expression_list([strip_html(col("col"), separator=" ")])
    assert result.to_pydict() == {"col": ["first second"]}


def test_strip_html_separator_empty():
    from daft.functions import strip_html

    table = MicroPartition.from_pydict({"col": ["<p>first</p><p>second</p>"]})
    result = table.eval_expression_list([strip_html(col("col"), separator="")])
    assert result.to_pydict() == {"col": ["firstsecond"]}


def test_strip_html_separator_via_method():
    table = MicroPartition.from_pydict({"col": ["<h1>Title</h1><p>Body</p>"]})
    result = table.eval_expression_list([col("col").strip_html(separator=" ")])
    assert result.to_pydict() == {"col": ["Title Body"]}
