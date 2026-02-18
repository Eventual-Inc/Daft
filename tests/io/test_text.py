from __future__ import annotations

from pathlib import Path

import daft


def test_read_text_basic(tmp_path: Path) -> None:
    text_file = tmp_path / "example.txt"
    text_file.write_text("line1\nline2\n")

    df = daft.read_text(str(text_file))

    assert df.to_pydict() == {"text": ["line1", "line2"]}


def test_read_text_drop_empty_lines(tmp_path: Path) -> None:
    text_file = tmp_path / "example.txt"
    text_file.write_text("line1\n\nline3\n")

    df_default = daft.read_text(str(text_file))
    df_keep_empty = daft.read_text(str(text_file), drop_empty_lines=False)

    assert df_default.to_pydict() == {"text": ["line1", "line3"]}
    assert df_keep_empty.to_pydict() == {"text": ["line1", "", "line3"]}


def test_read_text_include_paths(tmp_path: Path) -> None:
    text_file = tmp_path / "example.txt"
    text_file.write_text("a\nb\n")

    df = daft.read_text(str(text_file), include_paths=True)
    result = df.to_pydict()

    assert result["text"] == ["a", "b"]
    assert result["path"] == [str(text_file), str(text_file)]
