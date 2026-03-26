from __future__ import annotations

import pytest

import daft


# Non-regression test for progress bar truncating UTF-8 pipeline names correctly.
# See: https://github.com/Eventual-Inc/Daft/actions/runs/21921434809
@pytest.mark.parametrize(
    "col_name",
    [
        "ñ" * 10,  # 20 bytes UTF-8 (each ñ = 2 bytes), truncation at byte 15 splits a char
        "日本語カラム名テスト",  # 30 bytes UTF-8 (each CJK char = 3 bytes)
        "🎉🎊🎈🎁🎂🎃",  # 24 bytes UTF-8 (each emoji = 4 bytes)
        "café_résumé_naïve",  # mixed ASCII and 2-byte chars
    ],
    ids=["two_byte_chars", "three_byte_cjk", "four_byte_emoji", "mixed_ascii_multibyte"],
)
def test_progress_bar_truncates_multibyte_utf8_pipeline_names(col_name):
    """Progress bar should not panic when truncating pipeline names with multi-byte UTF-8."""
    df = daft.from_pydict({col_name: [1.0, 2.0, 3.0]})
    # col + col is an "interesting" expression that survives optimizer constant-folding,
    # causing ProjectOperator to use the expression display name as the pipeline name.
    df = df.with_column(col_name, daft.col(col_name) + daft.col(col_name))
    result = df.collect()
    assert result.to_pydict()[col_name] == [2.0, 4.0, 6.0]
