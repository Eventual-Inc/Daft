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
    _test_encode(codec, input=str(TEXT), output=buff)
    _test_decode(codec, input=buff, output=str(TEXT))
    _test_roundtrip(codec, input=str(TEXT))


def _test_encode(codec: str, input: str, output: bytes):
    df = daft.from_pydict({"v": [input]})
    actual = df.select(col("v").encode(codec))
    expect = daft.from_pydict({"v": [output]})
    _assert_eq(actual, expect)


def _test_decode(codec: str, input: bytes, output: str):
    df = daft.from_pydict({"v": [input]})
    actual = df.select(col("v").decode(codec))
    expect = daft.from_pydict({"v": [output]})
    _assert_eq(actual, expect)


def _test_roundtrip(codec: str, input: str):
    df1 = daft.from_pydict({"v": [input]})
    df2 = df1.select(col("v").encode(codec).decode(codec))
    _assert_eq(df1, df2)


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
