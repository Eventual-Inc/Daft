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
    ],
)
def test_dtype_inference(user_provided_type, expected_datatype):
    actual = dt._infer_type(user_provided_type)
    assert actual == expected_datatype
