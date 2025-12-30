from __future__ import annotations

import os
import re

import pytest

import daft
from daft.io import FilenameProvider
from tests.conftest import get_tests_daft_runner_name


def _make_skippable_dir(tmp_path, files):
    d = tmp_path / "json-data-skippable"
    d.mkdir()
    for name, content in files.items():
        (d / name).write_text(content, encoding="utf-8")
    return d


def test_read_json_skip_empty_files(tmp_path):
    d = _make_skippable_dir(
        tmp_path,
        {
            "valid1.json": '[{"a": 1, "b": 2}]',
            "valid2.json": '[{"a": 3, "b": 4}]',
            "empty.json": "",
        },
    )

    df = daft.read_json(str(d), skip_empty_files=True)
    # Only empty files are skipped; directory contains one empty and two valid files.
    assert len(df.schema()) == 2
    assert df.count_rows() == 2


def test_read_json_no_skip_empty_files(tmp_path):
    d = _make_skippable_dir(tmp_path, {"empty.json": ""})

    with pytest.raises(Exception, match="Empty JSON file"):
        daft.read_json(str(d / "empty.json"), skip_empty_files=False)


def test_read_json_no_skip_whitespace_files(tmp_path):
    d = _make_skippable_dir(tmp_path, {"whitespace.json": "   \n\t  "})

    with pytest.raises(Exception, match="Invalid JSON format"):
        daft.read_json(str(d / "whitespace.json"), skip_empty_files=False)

    with pytest.raises(Exception, match="Invalid JSON format"):
        daft.read_json(str(d / "whitespace.json"), skip_empty_files=True)


def test_read_json_skip_multiple_empty_files_in_dir(tmp_path):
    d = _make_skippable_dir(
        tmp_path,
        {
            "empty1.json": "",
            "empty2.json": "",
            "valid.json": '[{"a": 10}]',
            "empty3.json": "",
        },
    )

    df = daft.read_json(str(d), skip_empty_files=True)
    # Multiple empties must be skipped; only valid.json contributes rows and schema.
    assert len(df.schema()) == 1
    assert df.count_rows() == 1


class RecordingBlockFilenameProvider(FilenameProvider):
    """Simple FilenameProvider used to test JSON block-oriented writes.

    Mirrors the parquet and CSV tests but uses the "json" extension.
    """

    def __init__(self) -> None:  # pragma: no cover - exercised via higher-level IO tests
        self.calls: list[tuple[str, int, int, int, str]] = []

    def get_filename_for_block(
        self,
        write_uuid: str,
        task_index: int,
        block_index: int,
        file_idx: int,
        ext: str,
    ) -> str:
        # Record the call so that tests can assert on the arguments.
        self.calls.append((write_uuid, task_index, block_index, file_idx, ext))
        # Deterministic pattern that makes it easy to assert on basename.
        return f"block-{write_uuid}-{task_index}-{block_index}-{file_idx}.{ext}"

    def get_filename_for_row(
        self,
        row: dict[str, object],
        write_uuid: str,
        task_index: int,
        block_index: int,
        row_index: int,
        ext: str,
    ) -> str:  # pragma: no cover - not used in these tests
        raise AssertionError("get_filename_for_row should not be called for block writes")


def _extract_basenames(paths: list[str]) -> list[str]:
    basenames: list[str] = []
    for path in paths:
        if path.startswith("file://"):
            path = path[len("file://") :]
        basenames.append(os.path.basename(path))
    return basenames


def test_filename_provider_json_local(tmp_path) -> None:
    data = {"a": [1, 2, 3]}
    df = daft.from_pydict(data).repartition(2)

    provider = RecordingBlockFilenameProvider()
    result_df = df.write_json(str(tmp_path), filename_provider=provider)
    paths = result_df.to_pydict()["path"]
    basenames = _extract_basenames(paths)

    # All basenames should follow the provider's pattern and use the json
    # extension. We do not rely on the exact indices, only that the pattern is
    # respected.
    assert basenames

    # Check that all filenames match the expected pattern including the UUID
    # Pattern: block-<uuid>-<task>-<block>-<file>.json
    uuid_pattern = r"[0-9a-f]{32}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
    pattern = re.compile(rf"block-({uuid_pattern})-\d+-\d+-\d+\.json")

    matches = [pattern.match(name) for name in basenames]
    assert all(matches)

    if get_tests_daft_runner_name() != "ray":
        assert len(provider.calls) == len(basenames)
        write_uuids = {call[0] for call in provider.calls}
        assert len(write_uuids) == 1
        exts = {call[4] for call in provider.calls}
        assert exts == {"json"}
