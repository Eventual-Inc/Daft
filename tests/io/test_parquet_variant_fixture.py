"""Tests reading a Variant parquet fixture produced by PySpark 4.0.

The fixture at tests/assets/parquet-data/spark-variant.parquet was generated
by scripts/generate_variant_fixture.py using Spark 4.0.0 via Docker. It
contains 10 rows with various JSON structures encoded as Variant.
"""

from __future__ import annotations

import os

import pytest

import daft
from daft import DataType, col

FIXTURE_PATH = os.path.join(os.path.dirname(__file__), "..", "assets", "parquet-data", "spark-variant.parquet")


@pytest.fixture
def spark_variant_df():
    return daft.read_parquet(FIXTURE_PATH)


def test_spark_variant_schema(spark_variant_df):
    schema = spark_variant_df.schema()
    assert schema["variant_col"].dtype == DataType.variant()
    assert schema["variant_col"].dtype.is_variant()
    assert schema["id"].dtype == DataType.string()


def test_spark_variant_row_count(spark_variant_df):
    collected = spark_variant_df.collect()
    assert len(collected) == 10


def test_spark_variant_to_json(spark_variant_df):
    result = spark_variant_df.with_column("json", col("variant_col").variant_to_json()).select("id", "json").collect()
    rows = result.to_pydict()
    id_to_json = dict(zip(rows["id"], rows["json"]))

    assert '"Alice"' in id_to_json["object"]
    assert '"scores"' in id_to_json["object"]
    assert "[]" == id_to_json["empty_array"]
    assert "{}" == id_to_json["empty_object"]
    assert '"hello"' == id_to_json["string_val"]
    assert "42" == id_to_json["number_val"]
    assert "true" == id_to_json["bool_val"]
    assert "null" == id_to_json["null_val"]


def test_spark_variant_get_field(spark_variant_df):
    result = spark_variant_df.with_column("name", col("variant_col").variant_get("name")).select("id", "name").collect()
    rows = result.to_pydict()
    id_to_name = dict(zip(rows["id"], rows["name"]))

    assert id_to_name["object"] == '"Alice"'
    assert id_to_name["nested"] is None
    assert id_to_name["array"] is None


def test_spark_variant_get_nested_path(spark_variant_df):
    result = (
        spark_variant_df.with_column("city", col("variant_col").variant_get("user.address.city"))
        .select("id", "city")
        .collect()
    )
    rows = result.to_pydict()
    id_to_city = dict(zip(rows["id"], rows["city"]))

    assert id_to_city["nested"] == '"NYC"'
    assert id_to_city["object"] is None


def test_spark_variant_parquet_roundtrip(spark_variant_df, tmp_path):
    """Read Spark fixture, write from Daft, read back.

    Verifies Daft-to-Daft roundtrip with Spark-originated data.
    """
    from daft.context import execution_config_ctx

    collected = spark_variant_df.collect()
    with execution_config_ctx(native_parquet_writer=True):
        collected.write_parquet(str(tmp_path))
        read_back = daft.read_parquet(str(tmp_path)).collect()

    assert read_back.schema()["variant_col"].dtype.is_variant()
    assert len(read_back) == 10

    original_json = (
        collected.with_column("j", col("variant_col").variant_to_json()).select("j").collect().to_pydict()["j"]
    )
    roundtrip_json = (
        read_back.with_column("j", col("variant_col").variant_to_json()).select("j").collect().to_pydict()["j"]
    )
    assert original_json == roundtrip_json


def test_cross_validation_daft_write_spark_read(spark_variant_df, tmp_path):
    """Write a Variant parquet from Daft, then read it back from PySpark via Docker.

    Skipped if Docker is not available. This is the cross-engine validation test.
    """
    import shutil
    import subprocess

    docker = shutil.which("docker")
    if docker is None:
        pytest.skip("Docker not available")

    from daft.context import execution_config_ctx

    out_dir = str(tmp_path / "daft_output")
    with execution_config_ctx(native_parquet_writer=True):
        spark_variant_df.write_parquet(out_dir)

    parquet_files = [f for f in os.listdir(out_dir) if f.endswith(".parquet")]
    assert len(parquet_files) > 0
    parquet_file = os.path.join(out_dir, parquet_files[0])

    script = """
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
df = spark.read.parquet("/work/output.parquet")
df.printSchema()
df.show(truncate=False)
# Spark reads Daft-written variant as a struct with value/metadata binary fields.
# Validate the data is intact: struct has the expected fields and row count matches.
assert df.count() == 10, f"Expected 10 rows, got {df.count()}"
assert "value" in [f.name for f in df.schema["variant_col"].dataType.fields]
assert "metadata" in [f.name for f in df.schema["variant_col"].dataType.fields]
print("CROSS_VALIDATION_PASSED")
spark.stop()
"""
    script_path = str(tmp_path / "validate.py")
    with open(script_path, "w") as f:
        f.write(script)

    result = subprocess.run(
        [
            docker,
            "run",
            "--rm",
            "-v",
            f"{parquet_file}:/work/output.parquet",
            "-v",
            f"{script_path}:/work/validate.py",
            "apache/spark:4.0.0-python3",
            "/opt/spark/bin/spark-submit",
            "--master",
            "local[*]",
            "/work/validate.py",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )

    assert "CROSS_VALIDATION_PASSED" in result.stdout, (
        f"Cross-validation failed.\nstdout: {result.stdout[-500:]}\nstderr: {result.stderr[-500:]}"
    )
