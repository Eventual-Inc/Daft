import base64

import tiktoken

import daft
from daft import col

TOKEN_FILE = "tests/assets/tokens_5k.tiktoken"


def get_openai_enc():
    # from https://github.com/openai/tiktoken/blob/main/tiktoken/load.py
    with open(TOKEN_FILE, "rb") as f:
        file_bytes = f.read()
    enc_dict = {
        base64.b64decode(token): int(rank) for token, rank in (line.split() for line in file_bytes.splitlines() if line)
    }
    pattern = r"""'(?:[sdmt]|ll|ve|re)| ?\p{L}+| ?\p{N}+| ?[^\s\p{L}\p{N}]+|\s+(?!\S)|\s+"""
    return tiktoken.Encoding("test_encoding", pat_str=pattern, mergeable_ranks=enc_dict, special_tokens={})


openai_enc = get_openai_enc()


def test_file_token_encode():
    test_data = ["hello custom tokenizer!", "hopefully this works", "", "wow!"]
    df = daft.from_pydict({"a": test_data})
    res = df.select(col("a").str.tokenize_encode(TOKEN_FILE)).to_pydict()["a"]

    openai_res = openai_enc.encode_batch(test_data)
    assert res == openai_res
