from __future__ import annotations

import pytest

import daft
from tests._hf_retry import call_with_hf_retry


@pytest.mark.integration()
def test_read_huggingface_file_doesnt_fail():
    test_file_path = "hf://datasets/Eventual-Inc/sample-files/README.md"

    file = daft.File(test_file_path)
    # Opening the file streams it from HuggingFace Hub, which is occasionally
    # rate-limited (HTTP 429) on shared CI runners.
    with call_with_hf_retry(file.open) as f:
        bytes = f.read()
    assert len(bytes) > 0
