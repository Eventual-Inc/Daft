"""Generate a Variant parquet fixture using PySpark 4.0.

Run via Docker:
    docker run --rm -v $(pwd):/work apache/spark:4.0.0-python3 \
        /opt/spark/bin/spark-submit --master 'local[*]' /work/scripts/generate_variant_fixture.py

The output is written to /work/tests/assets/parquet-data/spark-variant.parquet
"""

from __future__ import annotations

import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

OUTPUT_DIR = "/work/tests/assets/parquet-data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "spark-variant.parquet")

spark = SparkSession.builder.appName("VariantFixture").getOrCreate()

data = [
    ("object", '{"name": "Alice", "age": 30, "scores": [85, 90, 92]}'),
    ("nested", '{"user": {"address": {"city": "NYC", "zip": "10001"}}}'),
    ("array", '[1, 2, 3, null, "string"]'),
    ("primitives", '{"int": 42, "float": 3.14, "bool": true, "null_val": null}'),
    ("empty_object", "{}"),
    ("empty_array", "[]"),
    ("string_val", '"hello"'),
    ("number_val", "42"),
    ("bool_val", "true"),
    ("null_val", "null"),
]

df = spark.createDataFrame(data, ["id", "json_str"])
df = df.withColumn("variant_col", F.parse_json("json_str")).select("id", "variant_col")

# Coalesce to 1 file for a single-file fixture
if os.path.exists(OUTPUT_FILE):
    shutil.rmtree(OUTPUT_FILE)

df.coalesce(1).write.mode("overwrite").parquet(OUTPUT_FILE)

# Verify
read_back = spark.read.parquet(OUTPUT_FILE)
read_back.show(truncate=False)
read_back.printSchema()
print(f"Wrote {read_back.count()} rows to {OUTPUT_FILE}")

spark.stop()
