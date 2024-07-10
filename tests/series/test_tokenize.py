import pytest
import tiktoken

import daft
import daft.errors
from daft import DataType
from daft.exceptions import DaftCoreException, DaftTypeError

DEFAULT_ENCODINGS = [
    "r50k_base",
    "p50k_base",
    "p50k_edit",
    "cl100k_base",
    "o200k_base",
]


@pytest.mark.parametrize("encoding", DEFAULT_ENCODINGS)
def test_tokenize_encoding(encoding: str) -> None:
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

    s = daft.Series.from_pylist(test_data)
    a = s.str.tokenize_encode(encoding).to_pylist()
    assert a[3] is None and a[-1] is None

    # openai encoder errors on Nones
    test_data = [t for t in test_data if t is not None]
    a = [t for t in a if t is not None]
    openai_enc = tiktoken.get_encoding(encoding).encode_batch(test_data)
    assert a == openai_enc


@pytest.mark.parametrize("encoding", DEFAULT_ENCODINGS)
@pytest.mark.parametrize(
    "num_type",
    [DataType.int32(), DataType.int64(), DataType.uint32(), DataType.uint64()],
)
def test_tokenize_decoding(encoding: str, num_type: DataType) -> None:
    test_data = [
        "hello world",
        "Hello, world!",
        "",
        "placeholder",
        "A bit of a longer sentence",
        "kjsdgslgdjklsj",
        "      ",
        "üțf-8 ťèştìňġ",
        "123 234 345",
        "placeholder",
    ]
    openai_enc = tiktoken.get_encoding(encoding)
    token_data = openai_enc.encode_batch(test_data)

    # openai encoder errors on Nones
    test_data[3] = test_data[-1] = None
    token_data[3] = token_data[-1] = None

    s = daft.Series.from_pylist(token_data).cast(DataType.list(num_type))
    a = s.str.tokenize_decode(encoding).to_pylist()
    assert a == test_data


@pytest.mark.parametrize("encoding", DEFAULT_ENCODINGS)
def test_tokenize_decode_invalid_dtype(encoding: str):
    test_data = [["these", "are"], ["not", "integers"]]

    s = daft.Series.from_pylist(test_data)
    assert s.datatype() == DataType.list(DataType.string())
    with pytest.raises(DaftTypeError, match="expected integer list inner type"):
        s.str.tokenize_decode(encoding)


@pytest.mark.parametrize("encoding", DEFAULT_ENCODINGS)
def test_tokenize_decode_invalid_tokens(encoding: str):
    test_data = [
        [606, 315, 252612, 374, 3958],
        [606, 315, -1521, 374, 3958],
    ]
    for t in test_data:
        s = daft.Series.from_pylist([t])
        with pytest.raises(DaftCoreException):
            s.str.tokenize_decode(encoding)
