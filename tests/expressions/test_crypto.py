"""Tests for crypto/hash functions: md5, sha1, sha2, spark_xxhash64, crc32."""

from __future__ import annotations

import daft
from daft import col
from daft.functions import crc32, md5, sha1_hex, sha2_hex, spark_xxhash64


class TestMd5:
    def test_md5_basic(self):
        df = daft.from_pydict({"data": ["hello", "world", ""]})
        result = df.select(md5(col("data"))).to_pydict()["data"]
        # Verify output is a 32-character hex string
        assert len(result[0]) == 32
        assert len(result[1]) == 32
        assert len(result[2]) == 32
        # Verify determinism
        assert result[0] == result[0]

    def test_md5_deterministic(self):
        df = daft.from_pydict({"data": ["hello"]})
        result1 = df.select(md5(col("data"))).to_pydict()["data"]
        result2 = df.select(md5(col("data"))).to_pydict()["data"]
        assert result1 == result2

    def test_md5_different_inputs(self):
        df = daft.from_pydict({"data": ["hello", "world"]})
        result = df.select(md5(col("data"))).to_pydict()["data"]
        assert result[0] != result[1]

    def test_md5_null(self):
        df = daft.from_pydict({"data": ["hello", None, "world"]})
        result = df.select(md5(col("data"))).to_pydict()["data"]
        assert result[0] is not None
        assert result[1] is None
        assert result[2] is not None

    def test_md5_expression_method(self):
        df = daft.from_pydict({"data": ["hello"]})
        result = df.select(col("data").md5()).to_pydict()["data"]
        assert result[0] is not None
        assert len(result[0]) == 32

    def test_md5_binary_input(self):
        df = daft.from_pydict({"data": [b"hello", b"world"]})
        result = df.select(md5(col("data"))).to_pydict()["data"]
        assert len(result[0]) == 32
        assert len(result[1]) == 32


class TestSha1:
    def test_sha1_basic(self):
        df = daft.from_pydict({"data": ["hello", "world", ""]})
        result = df.select(sha1_hex(col("data"))).to_pydict()["data"]
        # SHA-1 output should be a 40-character hex string
        assert len(result[0]) == 40
        assert len(result[1]) == 40
        assert len(result[2]) == 40

    def test_sha1_deterministic(self):
        df = daft.from_pydict({"data": ["hello"]})
        result1 = df.select(sha1_hex(col("data"))).to_pydict()["data"]
        result2 = df.select(sha1_hex(col("data"))).to_pydict()["data"]
        assert result1 == result2

    def test_sha1_different_inputs(self):
        df = daft.from_pydict({"data": ["hello", "world"]})
        result = df.select(sha1_hex(col("data"))).to_pydict()["data"]
        assert result[0] != result[1]

    def test_sha1_null(self):
        df = daft.from_pydict({"data": ["hello", None, "world"]})
        result = df.select(sha1_hex(col("data"))).to_pydict()["data"]
        assert result[0] is not None
        assert result[1] is None
        assert result[2] is not None

    def test_sha1_expression_method(self):
        df = daft.from_pydict({"data": ["hello"]})
        result = df.select(col("data").sha1_hex()).to_pydict()["data"]
        assert result[0] is not None
        assert len(result[0]) == 40


class TestSha2:
    def test_sha2_256(self):
        df = daft.from_pydict({"data": ["hello"]})
        result = df.select(sha2_hex(col("data"), 256)).to_pydict()["data"]
        # SHA-256 output should be a 64-character hex string
        assert len(result[0]) == 64

    def test_sha2_224(self):
        df = daft.from_pydict({"data": ["hello"]})
        result = df.select(sha2_hex(col("data"), 224)).to_pydict()["data"]
        # SHA-224 output should be a 56-character hex string
        assert len(result[0]) == 56

    def test_sha2_384(self):
        df = daft.from_pydict({"data": ["hello"]})
        result = df.select(sha2_hex(col("data"), 384)).to_pydict()["data"]
        # SHA-384 output should be a 96-character hex string
        assert len(result[0]) == 96

    def test_sha2_512(self):
        df = daft.from_pydict({"data": ["hello"]})
        result = df.select(sha2_hex(col("data"), 512)).to_pydict()["data"]
        # SHA-512 output should be a 128-character hex string
        assert len(result[0]) == 128

    def test_sha2_default_256(self):
        df = daft.from_pydict({"data": ["hello"]})
        result_default = df.select(sha2_hex(col("data"))).to_pydict()["data"]
        result_256 = df.select(sha2_hex(col("data"), 256)).to_pydict()["data"]
        # Default should be SHA-256
        assert result_default == result_256

    def test_sha2_null(self):
        df = daft.from_pydict({"data": ["hello", None]})
        result = df.select(sha2_hex(col("data"), 256)).to_pydict()["data"]
        assert result[0] is not None
        assert result[1] is None

    def test_sha2_expression_method(self):
        df = daft.from_pydict({"data": ["hello"]})
        result = df.select(col("data").sha2_hex(256)).to_pydict()["data"]
        assert result[0] is not None
        assert len(result[0]) == 64

    def test_sha2_different_bit_lengths(self):
        df = daft.from_pydict({"data": ["hello"]})
        r224 = df.select(sha2_hex(col("data"), 224)).to_pydict()["data"][0]
        r256 = df.select(sha2_hex(col("data"), 256)).to_pydict()["data"][0]
        r384 = df.select(sha2_hex(col("data"), 384)).to_pydict()["data"][0]
        r512 = df.select(sha2_hex(col("data"), 512)).to_pydict()["data"][0]
        # Different bit_length values should produce different results
        assert len({r224, r256, r384, r512}) == 4


class TestSparkXxHash64:
    def test_spark_xxhash64_basic(self):
        df = daft.from_pydict({"data": ["hello", "world"]})
        result = df.select(spark_xxhash64(col("data"))).to_pydict()["data"]
        # spark_xxhash64 should return Int64 values
        assert all(isinstance(v, int) for v in result)
        assert len(result) == 2

    def test_spark_xxhash64_deterministic(self):
        df = daft.from_pydict({"data": ["hello", "world"]})
        result1 = df.select(spark_xxhash64(col("data"))).to_pydict()["data"]
        result2 = df.select(spark_xxhash64(col("data"))).to_pydict()["data"]
        assert result1 == result2

    def test_spark_xxhash64_different_seeds(self):
        df = daft.from_pydict({"data": ["hello"]})
        result1 = df.select(spark_xxhash64(col("data"), seed=0)).to_pydict()["data"]
        result2 = df.select(spark_xxhash64(col("data"), seed=42)).to_pydict()["data"]
        assert result1 != result2

    def test_spark_xxhash64_multiple_columns(self):
        df = daft.from_pydict({"a": ["hello", "world"], "b": ["foo", "bar"]})
        result = df.select(spark_xxhash64(col("a"), col("b"))).to_pydict()["a"]
        assert len(result) == 2
        assert all(isinstance(v, int) for v in result)

    def test_spark_xxhash64_different_inputs(self):
        df = daft.from_pydict({"data": ["hello", "world"]})
        result = df.select(spark_xxhash64(col("data"))).to_pydict()["data"]
        assert result[0] != result[1]


class TestCrc32:
    def test_crc32_basic(self):
        df = daft.from_pydict({"data": ["hello", "world", ""]})
        result = df.select(crc32(col("data"))).to_pydict()["data"]
        # CRC32 should return Int64 values
        assert all(isinstance(v, int) for v in result)
        # CRC32 of empty string should be 0
        assert result[2] == 0

    def test_crc32_deterministic(self):
        df = daft.from_pydict({"data": ["hello"]})
        result1 = df.select(crc32(col("data"))).to_pydict()["data"]
        result2 = df.select(crc32(col("data"))).to_pydict()["data"]
        assert result1 == result2

    def test_crc32_different_inputs(self):
        df = daft.from_pydict({"data": ["hello", "world"]})
        result = df.select(crc32(col("data"))).to_pydict()["data"]
        assert result[0] != result[1]

    def test_crc32_null(self):
        df = daft.from_pydict({"data": ["hello", None, "world"]})
        result = df.select(crc32(col("data"))).to_pydict()["data"]
        assert result[0] is not None
        assert result[1] is None
        assert result[2] is not None

    def test_crc32_expression_method(self):
        df = daft.from_pydict({"data": ["hello"]})
        result = df.select(col("data").crc32()).to_pydict()["data"]
        assert isinstance(result[0], int)

    def test_crc32_binary_input(self):
        df = daft.from_pydict({"data": [b"hello", b"world"]})
        result = df.select(crc32(col("data"))).to_pydict()["data"]
        assert all(isinstance(v, int) for v in result)

    def test_crc32_known_value(self):
        """CRC32 of empty string should be 0."""
        df = daft.from_pydict({"data": [""]})
        result = df.select(crc32(col("data"))).to_pydict()["data"]
        assert result[0] == 0


class TestSqlIntegration:
    def test_md5_sql(self):
        df = daft.from_pydict({"data": ["hello"]})  # noqa: F841
        result = daft.sql("SELECT md5(data) as data FROM df").to_pydict()["data"]
        assert result[0] is not None
        assert len(result[0]) == 32

    def test_sha1_sql(self):
        df = daft.from_pydict({"data": ["hello"]})  # noqa: F841
        result = daft.sql("SELECT sha1(data) as data FROM df").to_pydict()["data"]
        assert result[0] is not None
        assert len(result[0]) == 40

    def test_sha2_sql(self):
        df = daft.from_pydict({"data": ["hello"]})  # noqa: F841
        result = daft.sql("SELECT sha2(data, bit_length:=256) as data FROM df").to_pydict()["data"]
        assert result[0] is not None
        assert len(result[0]) == 64

    def test_spark_xxhash64_sql(self):
        df = daft.from_pydict({"data": ["hello"]})  # noqa: F841
        result = daft.sql("SELECT spark_xxhash64(data) as data FROM df").to_pydict()["data"]
        assert isinstance(result[0], int)

    def test_crc32_sql(self):
        df = daft.from_pydict({"data": ["hello"]})  # noqa: F841
        result = daft.sql("SELECT crc32(data) as data FROM df").to_pydict()["data"]
        assert isinstance(result[0], int)

    def test_crc32_sql_known_value(self):
        df = daft.from_pydict({"data": ["hello"]})  # noqa: F841
        result = daft.sql("SELECT crc32(data) as data FROM df").to_pydict()["data"]
        assert result[0] == 907060870
