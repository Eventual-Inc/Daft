import pytest
import tiktoken

import daft
import daft.errors
from daft import DataType, col
from daft.exceptions import DaftCoreException

DEFAULT_ENCODINGS = [
    "r50k_base",
    "p50k_base",
    "p50k_edit",
    "cl100k_base",
    "o200k_base",
]
P50K_REGEX = "'(?:[sdmt]|ll|ve|re)| ?\\p{L}+| ?\\p{N}+| ?[^\\s\\p{L}\\p{N}]+|\\s+(?!\\S)|\\s+"
END_TOKENS = [50256, 50256, 50256, 100257, 199999]


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

    s = daft.from_pydict({"a": test_data})
    a = s.select(col("a").str.tokenize_encode(encoding)).to_pydict()["a"]
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

    s = daft.from_pydict({"a": token_data}).select(col("a").cast(DataType.list(num_type)))
    a = s.select(col("a").str.tokenize_decode(encoding)).to_pydict()["a"]
    assert a == test_data


@pytest.mark.parametrize("encoding", DEFAULT_ENCODINGS)
def test_tokenize_decode_invalid_dtype(encoding: str):
    test_data = [["these", "are"], ["not", "integers"]]

    s = daft.from_pydict({"a": test_data})
    with pytest.raises(DaftCoreException):
        s.select(col("a").str.tokenize_encode(encoding)).collect()


@pytest.mark.parametrize("encoding", DEFAULT_ENCODINGS)
def test_tokenize_decode_invalid_tokens(encoding: str):
    test_data = [
        [606, 315, 252612, 374, 3958],
        [606, 315, -1521, 374, 3958],
    ]
    for t in test_data:
        s = daft.from_pydict({"a": [t]})
        with pytest.raises(DaftCoreException, match="Input has bad token"):
            s.select(col("a").str.tokenize_decode(encoding)).collect()


def test_tokenize_base64_fail():
    file_path = "tests/assets/tokens/bad_base64.tiktoken"
    test_data = ["this should fail"]
    s = daft.from_pydict({"a": test_data})
    with pytest.raises(DaftCoreException, match="Error decoding base 64 token IGFyZQ= with rank 389"):
        s.select(col("a").str.tokenize_encode(file_path, pattern=P50K_REGEX)).collect()


def test_tokenize_rank_parse_fail():
    file_path = "tests/assets/tokens/bad_rank.tiktoken"
    test_data = ["this should fail"]
    s = daft.from_pydict({"a": test_data})
    with pytest.raises(DaftCoreException, match="Error parsing rank number 4I5"):
        s.select(col("a").str.tokenize_encode(file_path, pattern=P50K_REGEX)).collect()


def test_tokenize_invalid_token_fail():
    file_path = "tests/assets/tokens/bad_token.tiktoken"
    test_data = ["this should fail"]
    s = daft.from_pydict({"a": test_data})
    with pytest.raises(DaftCoreException, match="Invalid line in token file"):
        s.select(col("a").str.tokenize_encode(file_path, pattern=P50K_REGEX)).collect()


def test_tokenize_empty_file_fail():
    file_path = "tests/assets/tokens/empty.tiktoken"
    test_data = ["this should fail"]
    s = daft.from_pydict({"a": test_data})
    with pytest.raises(DaftCoreException, match="Token file has no tokens"):
        s.select(col("a").str.tokenize_encode(file_path, pattern=P50K_REGEX)).collect()


def test_tokenize_missing_pattern_fail():
    file_path = "tests/assets/tokens/tokens_5k.tiktoken"
    test_data = ["this should fail"]
    s = daft.from_pydict({"a": test_data})
    with pytest.raises(DaftCoreException, match="Pattern must be provided for non-builtin token sets"):
        s.select(col("a").str.tokenize_encode(file_path)).collect()


def test_tokenize_llama3_special_tokens():
    file_path = "tests/assets/tokens/tokens_5k.tiktoken"
    test_data = [
        "<|begin_of_text|><|end_of_text|>",
        "<|reserved_special_token_0|><|reserved_special_token_1|><|reserved_special_token_2|>",
        "<|reserved_special_token_3|>",
        "<|start_header_id|><|end_header_id|><|reserved_special_token_4|><|eot_id|>",
        "<|reserved_special_token_255|><|reserved_special_token_256|>",
    ]
    s = daft.from_pydict({"a": test_data})
    a = s.select(col("a").str.tokenize_encode(file_path, pattern=P50K_REGEX, special_tokens="llama3")).to_pydict()["a"]
    expected = [
        [5000, 5001],
        [5002, 5003, 5004],
        [5005],
        [5006, 5007, 5008, 5009],
        [
            5260,
            27,
            91,
            411,
            712,
            276,
            62,
            4125,
            2413,
            62,
            83,
            4233,
            62,
            1495,
            21,
            91,
            29,
        ],
    ]
    assert a == expected


def test_tokenize_unsupported_special_tokens():
    file_path = "tests/assets/tokens/tokens_5k.tiktoken"
    test_data = ["this should fail"]
    s = daft.from_pydict({"a": test_data})
    with pytest.raises(DaftCoreException, match="Provided special token set is not supported"):
        s.select(
            col("a").str.tokenize_encode(file_path, pattern=P50K_REGEX, special_tokens="thisdoesntexist")
        ).collect()


def test_tokenize_special_tokens_disabled():
    file_path = "tests/assets/tokens/tokens_5k.tiktoken"
    test_data = ["<|begin_of_text|><|end_of_text|>"]
    s = daft.from_pydict({"a": test_data})
    a = s.select(
        col("a").str.tokenize_encode(
            file_path,
            pattern=P50K_REGEX,
            special_tokens="llama3",
            use_special_tokens=False,
        )
    ).to_pydict()["a"]
    assert 5000 not in a[0] and 5001 not in a[0]


@pytest.mark.parametrize(["encoding", "end_token"], zip(DEFAULT_ENCODINGS, END_TOKENS))
def test_tokenize_builtin_tokens(encoding, end_token):
    test_data = ["<|endoftext|>"]

    s = daft.from_pydict({"a": test_data})
    a = s.select(col("a").str.tokenize_encode(encoding, use_special_tokens=True)).to_pydict()["a"]
    assert len(a[0]) == 1 and a[0][0] == end_token


@pytest.mark.parametrize(["encoding", "end_token"], zip(DEFAULT_ENCODINGS, END_TOKENS))
def test_tokenize_builtin_tokens_disabled(encoding, end_token):
    test_data = ["<|endoftext|>"]

    s = daft.from_pydict({"a": test_data})
    a = s.select(col("a").str.tokenize_encode(encoding, use_special_tokens=False)).to_pydict()["a"]
    assert len(a[0]) >= 1 and a[0][0] != end_token
