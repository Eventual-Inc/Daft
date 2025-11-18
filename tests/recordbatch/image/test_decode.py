from __future__ import annotations

import daft


def test_decode_all_empty():
    df = daft.from_pydict({"foo": [b"not an image", None]})
    df = df.with_column("image", daft.functions.decode_image(df["foo"], on_error="null"))
    df.collect()

    assert df.to_pydict() == {
        "foo": [b"not an image", None],
        "image": [None, None],
    }
