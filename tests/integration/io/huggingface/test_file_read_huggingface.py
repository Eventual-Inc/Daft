from __future__ import annotations

import pytest

import daft


@pytest.mark.integration()
def test_read_huggingface_file_doesnt_fail():
    test_file_path = "hf://datasets/Eventual-Inc/sample-files/README.md"

    file = daft.File(test_file_path)
    with file.open() as f:
        bytes = f.read()
    assert len(bytes) > 0
