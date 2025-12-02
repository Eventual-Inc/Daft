# fmt: off
# ^ ruff formatting breaks tests which are sensitive to format.
from __future__ import annotations

import sys
from io import StringIO

import daft
from daft.dataframe.preview import PreviewFormatter


def _capture_stdout(df_fn):
    old_stdout = sys.stdout
    sys.stdout = StringIO()
    try:
        df_fn()
        output = sys.stdout.getvalue()
    finally:
        sys.stdout = old_stdout
    return output

def _markdown_count_rows(markdown_output: str) -> int:
    # Count markdown table lines: header + data, exclude separator lines like "|---"
    lines = [line for line in markdown_output.split('\n') if line.strip()]
    table_lines = [line for line in lines if line.strip().startswith('|') and not line.strip().startswith('|---')]
    return len(table_lines)

def _html_count_tr(markup: str) -> int:
    return markup.count('<tr>')

def _count_table_data_rows(table_output: str) -> int:
    # Count data rows in table format (lines containing "item_" pattern)
    # This counts the actual data rows, not header/type rows
    return sum(1 for line in table_output.split('\n') if 'item_' in line)


def test_show_limit_n_markdown_counts_rows():
    df = daft.from_pydict({
        'id': list(range(20)),
        'value': [f'item_{i}' for i in range(20)],
    })

    # n < 10
    out = _capture_stdout(lambda: df.show(n=5, format='markdown'))
    assert _markdown_count_rows(out) == 6  # 1 header + 5 data

    # n = 10
    out = _capture_stdout(lambda: df.show(n=10, format='markdown'))
    assert _markdown_count_rows(out) == 11  # 1 header + 10 data

    # n > 10
    out = _capture_stdout(lambda: df.show(n=15, format='markdown'))
    assert _markdown_count_rows(out) == 16  # 1 header + 15 data


def test_show_limit_n_html_counts_rows_notebook():
    # Simulate notebook environment by asking PreviewFormatter for HTML
    df = daft.from_pydict({
        'id': list(range(20)),
        'value': [f'item_{i}' for i in range(20)],
    }).limit(20)

    # Build preview with n rows
    preview = df._construct_show_preview(15)
    html = PreviewFormatter(preview, df.schema(), format='html')._to_html()
    assert _html_count_tr(html) == 16  # 1 header + 15 data

    preview = df._construct_show_preview(10)
    html = PreviewFormatter(preview, df.schema(), format='html')._to_html()
    assert _html_count_tr(html) == 11  # 1 header + 10 data

    preview = df._construct_show_preview(5)
    html = PreviewFormatter(preview, df.schema(), format='html')._to_html()
    assert _html_count_tr(html) == 6  # 1 header + 5 data


def test_show_limit_n_plain_counts_rows():
    df = daft.from_pydict({
        'id': list(range(20)),
        'value': [f'item_{i}' for i in range(20)],
    })

    # Use PreviewFormatter._to_text() to avoid stdout, for plain format
    from daft.dataframe.preview import PreviewFormatter

    # n < 10
    preview = df._construct_show_preview(5)
    out = PreviewFormatter(preview, df.schema(), format='plain')._to_text()
    # Count data lines (ignore header lines)
    lines = [line for line in out.split('\n') if line.strip()]
    # Heuristic: in plain format, header line is first line, then rows
    assert len(lines) == 6 # 1 header + 5 data

    # n = 10
    preview = df._construct_show_preview(10)
    out = PreviewFormatter(preview, df.schema(), format='plain')._to_text()
    lines = [line for line in out.split('\n') if line.strip()]
    assert len(lines) == 11 # 1 header + 10 data

    # n > 10
    preview = df._construct_show_preview(15)
    out = PreviewFormatter(preview, df.schema(), format='plain')._to_text()
    lines = [line for line in out.split('\n') if line.strip()]
    assert len(lines) == 16 # 1 header + 15 data


def test_collect_num_preview_rows():
    df = daft.from_pydict({
        'id': list(range(20)),
        'value': [f'item_{i}' for i in range(20)],
    })

    # Test with num_preview_rows < 10
    df_collected = df.collect(num_preview_rows=5)
    out = _capture_stdout(lambda: print(df_collected))
    assert "(Showing first 5 of 20 rows)" in out
    assert _count_table_data_rows(out) == 5

    # Test with num_preview_rows = 10
    df_collected = df.collect(num_preview_rows=10)
    out = _capture_stdout(lambda: print(df_collected))
    assert "(Showing first 10 of 20 rows)" in out
    assert _count_table_data_rows(out) == 10

    # Test with num_preview_rows > 10
    df_collected = df.collect(num_preview_rows=15)
    out = _capture_stdout(lambda: print(df_collected))
    assert "(Showing first 15 of 20 rows)" in out
    assert _count_table_data_rows(out) == 15

    # Test with num_preview_rows > len(df)
    df_collected = df.collect(num_preview_rows=25)
    out = _capture_stdout(lambda: print(df_collected))
    assert "(Showing first 20 of 20 rows)" in out
    assert _count_table_data_rows(out) == 20
