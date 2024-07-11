from __future__ import annotations

import pytest
import tiktoken

from daft.datatype import DataType
from daft.expressions import col
from daft.series import Series
from tests.expressions.typing.conftest import assert_typing_resolve_vs_runtime_behavior

DEFAULT_ENCODINGS = [
    "r50k_base",
    "p50k_base",
    "p50k_edit",
    "cl100k_base",
    "o200k_base",
]


@pytest.mark.parametrize("encoding", DEFAULT_ENCODINGS)
def test_tokenize_encode(encoding):
    test_data = [
        "hello world",
        "Hello, world!",
        "",
        None,
        "A bit of a longer sentence",
        "kjsdgslgdjklsj",
        "      ",
        "üțf-8 ťèştìňġ",
        "123 234 345",
        None,
    ]
    s = Series.from_pylist(test_data).rename("col")

    assert_typing_resolve_vs_runtime_behavior(
        data=[s],
        expr=col("col").str.tokenize_encode(encoding),
        run_kernel=lambda: s.str.tokenize_encode(encoding),
        resolvable=True,
    )


@pytest.mark.parametrize("encoding", DEFAULT_ENCODINGS)
@pytest.mark.parametrize(
    "datatype",
    [DataType.int32(), DataType.int64(), DataType.uint32(), DataType.uint64()],
)
def test_tokenize_decode(encoding, datatype):
    model = tiktoken.get_encoding(encoding)
    test_data = model.encode_batch(
        [
            "hello world",
            "Hello, world!",
            "",
            "A bit of a longer sentence",
            "kjsdgslgdjklsj",
            "      ",
            "üțf-8 ťèştìňġ",
            "123 234 345",
        ]
    )
    s = Series.from_pylist(test_data).rename("col").cast(DataType.list(datatype))

    assert_typing_resolve_vs_runtime_behavior(
        data=[s],
        expr=col("col").str.tokenize_decode(encoding),
        run_kernel=lambda: s.str.tokenize_decode(encoding),
        resolvable=True,
    )
