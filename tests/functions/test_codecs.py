from __future__ import annotations

import pytest

import daft
from daft import col

# see:
#  - https://docs.python.org/3/library/codecs.html#text-encodings
#  - https://docs.python.org/3/library/codecs.html#binary-transforms

TEXT = b"""
Every great magic trick consists of three parts or acts. The first part is
called "The Pledge". The magician shows you something ordinary: a deck of
cards, a bird or a man. He shows you this object. Perhaps he asks you to
inspect it to see if it is indeed real, unaltered, normal. But of course...
it probably isn't. The second act is called "The Turn". The magician takes
the ordinary something and makes it do something extraordinary. Now you're
looking for the secret... but you won't find it, because of course you're
not really looking. You don't really want to know. You want to be fooled.
But you wouldn't clap yet. Because making something disappear isn't enough;
you have to bring it back. That's why every magic trick has a third act,
the hardest part, the part we call "The Prestige". - Christopher Priest.
""" + bytes("🔥", encoding="utf-8")

###
# helpers
###


def _assert_eq(actual, expect):
    actual.to_pydict() == expect.to_pydict()


def _test_codec(codec: str, buff: bytes):
    _test_encode(codec, input=TEXT, output=buff)
    _test_decode(codec, input=buff, output=TEXT)
    _test_roundtrip(codec, input=TEXT)


def _test_encode(codec: str, input: bytes, output: bytes):
    df = daft.from_pydict({"v": [input]})
    actual = df.select(col("v").encode(codec))
    expect = daft.from_pydict({"v": [output]})
    _assert_eq(actual, expect)


def _test_decode(codec: str, input: bytes, output: bytes):
    df = daft.from_pydict({"v": [input]})
    actual = df.select(col("v").decode(codec))
    expect = daft.from_pydict({"v": [output]})
    _assert_eq(actual, expect)


def _test_roundtrip(codec: str, input: bytes):
    df1 = daft.from_pydict({"v": [input]})
    df2 = df1.select(col("v").encode(codec).decode(codec))
    _assert_eq(df1, df2)


def test_null_handling():
    import zlib

    #
    df1 = daft.from_pydict({"v": [None, zlib.compress(TEXT)]})
    df2 = daft.from_pydict({"v": [zlib.compress(TEXT), None]})
    result1 = df1.select(col("v").decode("zlib")).to_pydict()
    result2 = df2.select(col("v").decode("zlib")).to_pydict()
    #
    # assert validity is included
    assert result1["v"][0] is None
    assert result2["v"][1] is None


def test_with_strings():
    import zlib

    _test_encode("zlib", input=str(TEXT), output=zlib.compress(TEXT))


###
# codecs
###


def test_codec_deflate():
    import zlib

    # https://stackoverflow.com/questions/1089662/python-inflate-and-deflate-implementations
    # strip 2-byte zlib header and the 4-byte checksum
    _test_codec("deflate", buff=zlib.compress(TEXT)[2:-4])


def test_codec_gzip():
    import gzip

    _test_codec("gz", buff=gzip.compress(TEXT))
    _test_codec("gzip", buff=gzip.compress(TEXT))


def test_codec_zlib():
    import zlib

    _test_codec("zlib", buff=zlib.compress(TEXT))


def test_codec_base64():
    with pytest.raises(Exception, match="unsupported codec"):
        _test_codec("base64", None)


def test_codec_zstd():
    with pytest.raises(Exception, match="unsupported codec"):
        _test_codec("zstd", None)


def test_codec_bz2():
    with pytest.raises(Exception, match="unsupported codec"):
        _test_codec("bz2", None)


###
# utf-8 special handling
###


def test_decode_utf8():
    df = daft.from_pydict({"bytes": [None, TEXT]})
    actual = df.select(col("bytes").decode("utf-8"))
    expect = {"bytes": [None, TEXT.decode("utf-8")]}
    assert actual.to_pydict() == expect


def test_try_decode_utf8():
    # source
    # https://stackoverflow.com/questions/1301402/example-invalid-utf8-string
    # https://www.php.net/manual/en/reference.pcre.pattern.modifiers.php#54805
    valid_ascii = b"a"
    valid_2_octet = b"\xc3\xb1"
    invalid_2_octet = b"\xc3\x28"
    invalid_seq_id = b"\xa0\xa1"
    valid_3_octet = b"\xe2\x82\xa1"
    invalid_3_octet_2nd = b"\xe2\x28\xa1"
    invalid_3_octet_3rd = b"\xe2\x82\x28"
    valid_4_octet = b"\xf0\x90\x8c\xbc"
    invalid_4_octet_2nd = b"\xf0\x28\x8c\xbc"
    invalid_4_octet_3rd = b"\xf0\x90\x28\xbc"
    invalid_4_octet_4th = b"\xf0\x28\x8c\x28"
    valid_5_octet = b"\xf8\xa1\xa1\xa1\xa1"
    valid_6_octet = b"\xfc\xa1\xa1\xa1\xa1\xa1"

    df = daft.from_pydict(
        {
            "bytes": [
                None,
                TEXT,
                valid_ascii,
                valid_2_octet,
                invalid_2_octet,
                invalid_seq_id,
                valid_3_octet,
                invalid_3_octet_2nd,
                invalid_3_octet_3rd,
                valid_4_octet,
                invalid_4_octet_2nd,
                invalid_4_octet_3rd,
                invalid_4_octet_4th,
                valid_5_octet,
                valid_6_octet,
            ],
            "note": [
                "None",
                "Valid TEXT",
                "Valid ASCII",
                "Valid 2 Octet Sequence",
                "Invalid 2 Octet Sequence",
                "Invalid Sequence Identifier",
                "Valid 3 Octet Sequence",
                "Invalid 3 Octet Sequence (in 2nd Octet)",
                "Invalid 3 Octet Sequence (in 3rd Octet)",
                "Valid 4 Octet Sequence",
                "Invalid 4 Octet Sequence (in 2nd Octet)",
                "Invalid 4 Octet Sequence (in 3rd Octet)",
                "Invalid 4 Octet Sequence (in 4th Octet)",
                "Valid 5 Octet Sequence (but not Unicode!)",
                "Valid 6 Octet Sequence (but not Unicode!)",
            ],
        }
    )

    result = df.select(col("bytes").try_decode("utf-8").alias("decoded"), col("note"))

    expected = {
        "decoded": [
            None,
            TEXT.decode("utf-8"),
            str(valid_ascii, "utf-8"),
            str(valid_2_octet, "utf-8"),
            None,  # Invalid 2 Octet Sequence
            None,  # Invalid Sequence Identifier
            str(valid_3_octet, "utf-8"),
            None,  # Invalid 3 Octet Sequence (in 2nd Octet)
            None,  # Invalid 3 Octet Sequence (in 3rd Octet)
            str(valid_4_octet, "utf-8"),
            None,  # Invalid 4 Octet Sequence (in 2nd Octet)
            None,  # Invalid 4 Octet Sequence (in 3rd Octet)
            None,  # Invalid 4 Octet Sequence (in 4th Octet)
            None,  # Valid 5 Octet Sequence (but not Unicode!)
            None,  # Valid 6 Octet Sequence (but not Unicode!)
        ],
        "note": df.to_pydict()["note"],
    }

    assert result.to_pydict() == expected


@pytest.mark.skip("sanity perf checking")
def test_try_decode_utf8_perf():
    from daft import DataType as dt

    @daft.udf(return_dtype=dt.string())
    def try_decode_utf8_udf(binary_series):
        strings = []
        for binary in binary_series:
            try:
                string = binary.decode("utf-8")
                strings.append(string)
            except UnicodeDecodeError:
                strings.append(None)
        return strings

    # Create a large dataset with valid and invalid UTF-8 strings
    n = 1_000_000
    valid_data = [TEXT] * (n // 2)
    invalid_data = [b"\xc3\x28"] * (n // 2)  # Invalid UTF-8 sequence
    mixed_data = valid_data + invalid_data

    df = daft.from_pydict({"bytes": mixed_data})

    import statistics
    import time

    # run tests multiple times for statistical significance
    udf_times = []
    native_times = []
    iterations = 11  # first one is warm-up

    for i in range(iterations):
        # test udf
        start = time.time()
        _ = df.select(try_decode_utf8_udf(col("bytes")).alias("decoded")).collect()
        elapsed = time.time() - start
        if i > 0:  # Skip the first iteration (warm-up)
            udf_times.append(elapsed)
        # test try_decode('utf-8')
        start = time.time()
        _ = df.select(col("bytes").try_decode("utf-8").alias("decoded")).collect()
        elapsed = time.time() - start
        if i > 0:  # Skip the first iteration (warm-up)
            native_times.append(elapsed)

    udf_time = sum(udf_times) / len(udf_times)
    native_time = sum(native_times) / len(native_times)

    # Statistical summary
    udf_stats = {
        "mean": udf_time,
        "median": statistics.median(udf_times),
        "min": min(udf_times),
        "max": max(udf_times),
        "stdev": statistics.stdev(udf_times) if len(udf_times) > 1 else 0,
    }

    native_stats = {
        "mean": native_time,
        "median": statistics.median(native_times),
        "min": min(native_times),
        "max": max(native_times),
        "stdev": statistics.stdev(native_times) if len(native_times) > 1 else 0,
    }

    print(f"Native try_decode stats (seconds): {native_stats}")
    print(f"UDF try_decode stats (seconds): {udf_stats}")
    print(f"Average speedup: {udf_time / native_time:.2f}x")


###
# try_ tests
###


def test_try_encode():
    pass


def test_try_decode():
    import gzip
    import zlib

    df = daft.from_pydict(
        {
            "bytes": [
                None,
                zlib.compress(TEXT),
                gzip.compress(TEXT),
                TEXT,
            ]
        }
    )
    actual = df.select(
        col("bytes").try_decode("zlib").alias("try_zlib"),
        col("bytes").try_decode("gzip").alias("try_gzip"),
    )
    expect = {
        "try_zlib": [None, TEXT, None, None],
        "try_gzip": [None, None, TEXT, None],
    }
    assert actual.to_pydict() == expect
