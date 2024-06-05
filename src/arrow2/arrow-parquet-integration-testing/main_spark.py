"""
Verifies that spark can correctly read a delta-encoded utf8 column written by arrow2.
"""
import os
import pyspark.sql

from main import _prepare, _expected


def test(
    file: str,
    version: str,
    column: str,
    compression: str,
    encoding: str,
):
    """
    Tests that pyspark can read a parquet file written by arrow2.

    In arrow2: read IPC, write parquet
    In pyarrow: read (same) IPC to Python
    In pyspark: read (written) parquet to Python
    assert that they are equal
    """
    # read IPC to Python
    expected = _expected(file)
    column_index = next(i for i, c in enumerate(expected.column_names) if c == column)
    expected = expected[column].combine_chunks().tolist()

    # write parquet
    path = _prepare(file, version, compression, encoding, encoding, [column_index])

    # read parquet to Python
    spark = pyspark.sql.SparkSession.builder.config(
        # see https://stackoverflow.com/a/62024670/931303
        "spark.sql.parquet.enableVectorizedReader",
        "false",
    ).getOrCreate()

    result = spark.read.parquet(path).select(column).collect()
    result = [r[column] for r in result]
    os.remove(path)

    # assert equality
    assert expected == result


test("generated_null", "2", "f1", "uncompressed", "delta")

test("generated_primitive", "2", "utf8_nullable", "uncompressed", "delta")
test("generated_primitive", "2", "utf8_nullable", "snappy", "delta")
test("generated_primitive", "2", "int32_nullable", "uncompressed", "delta")
test("generated_primitive", "2", "int32_nullable", "snappy", "delta")
test("generated_primitive", "2", "int16_nullable", "uncompressed", "delta")
test("generated_primitive", "2", "int16_nullable", "snappy", "delta")

test("generated_dictionary", "1", "dict0", "uncompressed", "plain")
test("generated_dictionary", "1", "dict0", "snappy", "plain")
test("generated_dictionary", "2", "dict0", "uncompressed", "plain")
test("generated_dictionary", "2", "dict0", "snappy", "plain")

test("generated_dictionary", "1", "dict1", "uncompressed", "plain")
test("generated_dictionary", "1", "dict1", "snappy", "plain")
test("generated_dictionary", "2", "dict1", "uncompressed", "plain")
test("generated_dictionary", "2", "dict1", "snappy", "plain")

test("generated_dictionary", "1", "dict2", "uncompressed", "plain")
test("generated_dictionary", "1", "dict2", "snappy", "plain")
test("generated_dictionary", "2", "dict2", "uncompressed", "plain")
test("generated_dictionary", "2", "dict2", "snappy", "plain")
