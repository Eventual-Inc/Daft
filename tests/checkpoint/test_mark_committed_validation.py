"""Python-boundary validation for CheckpointStore.mark_committed.

mark_committed accepts a list of checkpoint id strings. Malformed ids must
raise ValueError at the Python boundary — not panic the Rust runtime.
"""

from __future__ import annotations

import pytest

import daft


def test_mark_committed_rejects_empty_id():
    ckpt = daft.CheckpointStore("s3://dummy/ckpt")
    with pytest.raises(ValueError, match="invalid CheckpointId"):
        ckpt.mark_committed([""])


def test_mark_committed_rejects_slash_in_id():
    ckpt = daft.CheckpointStore("s3://dummy/ckpt")
    with pytest.raises(ValueError, match="invalid CheckpointId"):
        ckpt.mark_committed(["bad/id"])


def test_mark_committed_rejects_space_in_id():
    ckpt = daft.CheckpointStore("s3://dummy/ckpt")
    with pytest.raises(ValueError, match="invalid CheckpointId"):
        ckpt.mark_committed(["bad id"])


def test_mark_committed_rejects_mixed_valid_and_invalid():
    ckpt = daft.CheckpointStore("s3://dummy/ckpt")
    with pytest.raises(ValueError, match="invalid CheckpointId"):
        ckpt.mark_committed(["valid-id_1", "bad/id"])
