from __future__ import annotations

import pytest
from PIL import Image

from daft import DataType as dt


@pytest.mark.parametrize(
    "user_provided_type, expected_datatype",
    [
        (int, dt.int64()),
        (float, dt.float64()),
        (bytes, dt.binary()),
        (None, dt.null()),
        (bool, dt.bool()),
        (object, dt.python()),
        (dict[str, str], dt.map(dt.string(), dt.string())),
        ({"foo": str}, dt.struct({"foo": dt.string()})),
        ({"_1": str, "_2": str}, dt.struct({"_1": dt.string(), "_2": dt.string()})),
        (Image, dt.image()),
        (Image.Image, dt.image()),
        ((1, "a"), dt.struct({"_0": dt.int64(), "_1": dt.string()})),
        (
            [(1, "a", True, {"foo": [1.0]})],
            dt.list(
                dt.struct(
                    {
                        "_0": dt.int64(),
                        "_1": dt.string(),
                        "_2": dt.bool(),
                        "_3": dt.struct({"foo": dt.list(dt.float64())}),
                    }
                )
            ),
        ),
        ((True, False, False), dt.struct({"_0": dt.bool(), "_1": dt.bool(), "_2": dt.bool()})),
        (
            {
                "string": "s",
                "int": 1,
                "float": 1.0,
                "list": [1.0, 2.0, 3.0],
                "struct": {"foo": "bar"},
                "tuple": (1, "a"),
                "nested_tuples": ((1, "a"), ("2", "b")),
                "nested_list": [[1.0, 2.0], [3.0, 4.0]],
            },
            dt.struct(
                {
                    "string": dt.string(),
                    "int": dt.int64(),
                    "float": dt.float64(),
                    "list": dt.list(dt.float64()),
                    "struct": dt.struct({"foo": dt.string()}),
                    "tuple": dt.struct({"_0": dt.int64(), "_1": dt.string()}),
                    "nested_tuples": dt.struct(
                        {
                            "_0": dt.struct({"_0": dt.int64(), "_1": dt.string()}),
                            "_1": dt.struct({"_0": dt.string(), "_1": dt.string()}),
                        }
                    ),
                    "nested_list": dt.list(dt.list(dt.float64())),
                }
            ),
        ),
    ],
)
def test_dtype_inference(user_provided_type, expected_datatype):
    actual = dt._infer_type(user_provided_type)
    assert actual == expected_datatype
