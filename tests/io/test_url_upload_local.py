from __future__ import annotations

import daft


def test_upload_local(tmpdir):
    bytes_data = [b"a", b"b", b"c"]
    data = {"data": bytes_data}
    df = daft.from_pydict(data)
    df = df.with_column("files", df["data"].url.upload(str(tmpdir)))
    df.collect()

    results = df.to_pydict()
    assert results["data"] == bytes_data
    assert len(results["files"]) == len(bytes_data)
    for path, expected in zip(results["files"], bytes_data):
        assert path.startswith("file://")
        path = path[len("file://") :]
        with open(path, "rb") as f:
            assert f.read() == expected
