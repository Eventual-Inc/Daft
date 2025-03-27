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
"""

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

###
# try_ tests
###

def test_try_encode():
    pass

def test_try_decode():
    import zlib
    import gzip

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
        "try_zlib": [ None, TEXT, None, None ],
        "try_gzip": [ None, None, TEXT, None ],
    }
    assert actual.to_pydict() == expect


