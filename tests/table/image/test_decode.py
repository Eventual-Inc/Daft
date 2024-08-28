from __future__ import annotations

import daft


def test_decode_all_empty():
    df = daft.from_pydict({"foo": [b"not an image", None]})
    df = df.with_column("image", df["foo"].image.decode(on_error="null"))
    df.collect()

    assert df.to_pydict() == {
        "foo": [b"not an image", None],
        "image": [None, None],
    }


def test_decode_sql():
    sql_expr = daft.sql_expr("image_decode(foo)")
    expr = daft.col("foo").image.decode()
    assert sql_expr == expr
    sql_expr = daft.sql_expr("image_decode(foo, on_error='null')")
    expr = daft.col("foo").image.decode(on_error="null")
    assert sql_expr == expr
    sql_expr = daft.sql_expr("image_decode(foo, on_error='null', mode='RGB')")
    expr = daft.col("foo").image.decode(on_error="null", mode="RGB")
    assert sql_expr == expr
