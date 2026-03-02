from __future__ import annotations

import daft


def _as_bytes(v: object) -> bytes:
    if isinstance(v, memoryview):
        return v.tobytes()
    if isinstance(v, (bytes, bytearray)):
        return bytes(v)
    raise TypeError(f"Unexpected binary value type: {type(v)}")


def test_read_binary_files_bytes_only(tmp_path):
    (tmp_path / "a.bin").write_bytes(b"\x00\x01hello")
    (tmp_path / "b.dat").write_bytes(b"world")

    df = daft.read_binary_files(str(tmp_path))
    assert df.count_rows() == 2

    schema = {field.name: field.dtype for field in df.schema()}
    assert str(schema["bytes"]) == "Binary"

    values = [_as_bytes(v) for v in df.to_pydict()["bytes"]]
    assert set(values) == {b"\x00\x01hello", b"world"}


def test_read_binary_files_file_path_column(tmp_path):
    a_path = tmp_path / "a.bin"
    b_path = tmp_path / "b.dat"
    a_path.write_bytes(b"hello")
    b_path.write_bytes(b"world")

    df = daft.read_binary_files(str(tmp_path), file_path_column="path")
    assert df.count_rows() == 2

    schema = {field.name: field.dtype for field in df.schema()}
    assert "path" in schema
    assert str(schema["path"]) == "String"

    paths = df.to_pydict()["path"]
    assert any(p.endswith(str(a_path)) or p.endswith("a.bin") for p in paths)
    assert any(p.endswith(str(b_path)) or p.endswith("b.dat") for p in paths)

    # Ensure selecting only the path column works (no need to read bytes).
    df_paths_only = daft.read_binary_files(str(tmp_path), file_path_column="path").select("path")
    assert df_paths_only.count_rows() == 2
