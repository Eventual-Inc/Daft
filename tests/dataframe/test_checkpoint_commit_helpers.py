"""Direct unit tests for ``daft.dataframe._checkpoint_commit`` helpers.

The helpers are sink-agnostic and load-bearing for both
``write_iceberg(checkpoint=)`` and ``write_deltalake(checkpoint=)``.
Integration coverage through those write paths is Ray-only and slow;
these unit tests pin the helper contracts directly so a future
refactor gets fast, runner-independent feedback.
"""

from __future__ import annotations

import io
from typing import Any

import pyarrow as pa
import pytest

import daft
from daft.dataframe._checkpoint_commit import decode_file_metadata, empty_write_result
from daft.recordbatch import MicroPartition


def _ipc_bytes(column_name: str, values: list[Any]) -> bytes:
    """Build the Arrow IPC stream shape that ``decode_file_metadata`` expects.

    Mirrors what ``BlockingSinkNode.spawn_finalize`` writes when staging file
    metadata: an IPC stream of a MicroPartition with a single python-typed
    column whose cells are pickled Python objects.
    """
    mp = MicroPartition.from_pydict({column_name: values})
    tbl = mp.to_arrow()
    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, tbl.schema) as writer:
        writer.write_table(tbl)
    return sink.getvalue()


class _MockFileMetadata:
    def __init__(self, data: bytes):
        self.data = data


class _MockStore:
    """Minimal duck-typed CheckpointStore exposing only ``get_checkpointed_files``."""

    def __init__(self, blobs: list[bytes]):
        self._blobs = blobs

    def get_checkpointed_files(self) -> list[_MockFileMetadata]:
        return [_MockFileMetadata(b) for b in self._blobs]


# ── empty_write_result ────────────────────────────────────────────────────────


def test_empty_write_result_returns_empty_dataframe_with_expected_schema():
    df = daft.from_pydict({"x": [1, 2, 3]})
    result = empty_write_result(df)
    rows = result.to_pydict()

    assert rows == {"operation": [], "rows": [], "file_size": [], "file_name": []}


def test_empty_write_result_preserves_metadata():
    df = daft.from_pydict({"x": [1]})
    sentinel = {"run_id": "test-2026-05-12"}
    df._metadata = sentinel

    result = empty_write_result(df)
    assert result._metadata is sentinel


# ── decode_file_metadata ──────────────────────────────────────────────────────


class _Payload:
    """Stand-in for ``pyiceberg.DataFile`` / ``deltalake.AddAction``."""

    def __init__(self, label: str):
        self.label = label

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _Payload) and self.label == other.label

    def __hash__(self) -> int:
        return hash(("_Payload", self.label))


def test_decode_file_metadata_empty_store_returns_empty_list():
    assert decode_file_metadata(_MockStore([]), "data_file") == []


def test_decode_file_metadata_happy_path_roundtrips_python_objects():
    payloads_a = [_Payload("a1"), _Payload("a2")]
    payloads_b = [_Payload("b1")]
    store = _MockStore(
        [
            _ipc_bytes("data_file", payloads_a),
            _ipc_bytes("data_file", payloads_b),
        ]
    )

    decoded = decode_file_metadata(store, "data_file")

    assert decoded == payloads_a + payloads_b


def test_decode_file_metadata_wrong_column_raises_runtime_error_with_column_in_message():
    store = _MockStore([_ipc_bytes("data_file", [_Payload("x")])])

    with pytest.raises(RuntimeError, match="'add_action'"):
        decode_file_metadata(store, "add_action")


def test_decode_file_metadata_corrupted_bytes_raises_runtime_error_with_context():
    store = _MockStore([b"not a valid Arrow IPC stream"])

    with pytest.raises(RuntimeError, match="failed to decode 'data_file' payloads"):
        decode_file_metadata(store, "data_file")
