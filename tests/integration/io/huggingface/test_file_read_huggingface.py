from __future__ import annotations

import datetime

import pytest

import daft


@pytest.mark.integration()
def test_read_huggingface_file_doesnt_fail():
    test_file_path = "hf://datasets/Eventual-Inc/sample-files/README.md"

    file = daft.File(test_file_path)
    with file.open() as f:
        bytes = f.read()
    assert len(bytes) > 0


@pytest.mark.integration()
def test_read_blob_hf_populates_last_modified():
    """HF tree API with `expand=true` should populate `last_modified` from `lastCommit.date`."""
    df = daft.read_blob("hf://datasets/Eventual-Inc/sample-files/README.md")
    data = df.to_pydict()
    assert data["content"][0] is not None
    assert data["size"][0] is not None and data["size"][0] > 0
    ts = data["last_modified"][0]
    assert ts is not None
    assert isinstance(ts, datetime.datetime)
    assert ts.tzinfo is not None
