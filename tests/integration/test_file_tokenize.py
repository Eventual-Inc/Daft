import base64

import pytest
import tiktoken

import daft
from daft import col

TOKEN_FILE = "tests/assets/tokens/tokens_5k.tiktoken"
HTTP_TOKEN_FILE = (
    "https://daft-public-data.s3.us-west-2.amazonaws.com/test_fixtures/tiktoken_testing/p50k_base.tiktoken"
)
AWS_TOKEN_FILE = "s3://daft-public-data/test_fixtures/tiktoken_testing/p50k_base.tiktoken"
P50K_REGEX = "'(?:[sdmt]|ll|ve|re)| ?\\p{L}+| ?\\p{N}+| ?[^\\s\\p{L}\\p{N}]+|\\s+(?!\\S)|\\s+"


def get_openai_enc():
    # from https://github.com/openai/tiktoken/blob/main/tiktoken/load.py
    with open(TOKEN_FILE, "rb") as f:
        file_bytes = f.read()
    enc_dict = {
        base64.b64decode(token): int(rank) for token, rank in (line.split() for line in file_bytes.splitlines() if line)
    }
    return tiktoken.Encoding("test_encoding", pat_str=P50K_REGEX, mergeable_ranks=enc_dict, special_tokens={})


openai_enc = get_openai_enc()


def test_file_token_encode():
    test_data = ["hello custom tokenizer!", "hopefully this works", "", "wow!"]
    df = daft.from_pydict({"a": test_data})
    res = df.select(col("a").str.tokenize_encode(TOKEN_FILE, pattern=P50K_REGEX)).to_pydict()["a"]

    openai_res = openai_enc.encode_batch(test_data)
    assert res == openai_res


def test_file_token_decode():
    test_data = ["hello custom tokenizer!", "hopefully this works", "", "wow!"]
    token_data = openai_enc.encode_batch(test_data)
    df = daft.from_pydict({"a": token_data})
    res = df.select(col("a").str.tokenize_decode(TOKEN_FILE, pattern=P50K_REGEX)).to_pydict()["a"]
    assert res == test_data


@pytest.mark.integration()
def test_file_http_download():
    test_data = ["hello custom tokenizer!", "hopefully this works", "", "wow!"]
    df = daft.from_pydict({"a": test_data})
    res = df.select(col("a").str.tokenize_encode(HTTP_TOKEN_FILE, pattern=P50K_REGEX)).to_pydict()["a"]

    http_openai_enc = tiktoken.get_encoding("p50k_base")
    openai_res = http_openai_enc.encode_batch(test_data)
    assert res == openai_res

    df = daft.from_pydict({"b": openai_res})
    res = df.select(col("b").str.tokenize_decode(HTTP_TOKEN_FILE, pattern=P50K_REGEX)).to_pydict()["b"]
    assert res == test_data


@pytest.mark.integration()
def test_file_aws_download():
    test_data = ["hello custom tokenizer!", "hopefully this works", "", "wow!"]
    df = daft.from_pydict({"a": test_data})
    res = df.select(col("a").str.tokenize_encode(AWS_TOKEN_FILE, pattern=P50K_REGEX)).to_pydict()["a"]

    aws_openai_enc = tiktoken.get_encoding("p50k_base")
    openai_res = aws_openai_enc.encode_batch(test_data)
    assert res == openai_res

    df = daft.from_pydict({"b": openai_res})
    res = df.select(col("b").str.tokenize_decode(AWS_TOKEN_FILE, pattern=P50K_REGEX)).to_pydict()["b"]
    assert res == test_data


@pytest.mark.integration()
def test_file_aws_download_ioconfig():
    io_config = daft.io.IOConfig(
        s3=daft.io.S3Config(
            region_name="us-west-2",
            anonymous=True,
        )
    )
    test_data = ["hello custom tokenizer!", "hopefully this works", "", "wow!"]
    df = daft.from_pydict({"a": test_data})
    res = df.select(col("a").str.tokenize_encode(AWS_TOKEN_FILE, io_config=io_config, pattern=P50K_REGEX)).to_pydict()[
        "a"
    ]

    aws_openai_enc = tiktoken.get_encoding("p50k_base")
    openai_res = aws_openai_enc.encode_batch(test_data)
    assert res == openai_res

    df = daft.from_pydict({"b": openai_res})
    res = df.select(col("b").str.tokenize_decode(AWS_TOKEN_FILE, io_config=io_config, pattern=P50K_REGEX)).to_pydict()[
        "b"
    ]
    assert res == test_data
