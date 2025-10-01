# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import FixedType, NestedField, StringType, UUIDType
from pyspark.sql import SparkSession
from pyspark.sql.functions import array, col, current_date, date_add, expr, struct

spark = SparkSession.builder.getOrCreate()

spark.sql(
    """
  CREATE DATABASE IF NOT EXISTS default;
"""
)

schema = Schema(
    NestedField(field_id=1, name="uuid_col", field_type=UUIDType(), required=False),
    NestedField(field_id=2, name="fixed_col", field_type=FixedType(25), required=False),
)

catalog = load_catalog(
    "local",
    **{
        "type": "rest",
        "uri": "http://rest:8181",
        "s3.endpoint": "http://minio:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
    },
)

catalog.create_table(identifier="default.test_uuid_and_fixed_unpartitioned", schema=schema)

spark.sql(
    """
    INSERT INTO default.test_uuid_and_fixed_unpartitioned VALUES
    ('102cb62f-e6f8-4eb0-9973-d9b012ff0967', CAST('1234567890123456789012345' AS BINARY)),
    ('ec33e4b2-a834-4cc3-8c4a-a1d3bfc2f226', CAST('1231231231231231231231231' AS BINARY)),
    ('639cccce-c9d2-494a-a78c-278ab234f024', CAST('12345678901234567ass12345' AS BINARY)),
    ('c1b0d8e0-0b0e-4b1e-9b0a-0e0b0d0c0a0b', CAST('asdasasdads12312312312111' AS BINARY)),
    ('923dae77-83d6-47cd-b4b0-d383e64ee57e', CAST('qweeqwwqq1231231231231111' AS BINARY));
    """
)

spark.sql(
    """
  CREATE OR REPLACE TABLE default.test_null_nan
  USING iceberg
  AS SELECT
    1            AS idx,
    float('NaN') AS col_numeric
UNION ALL SELECT
    2            AS idx,
    null         AS col_numeric
UNION ALL SELECT
    3            AS idx,
    1            AS col_numeric
"""
)

spark.sql(
    """
  CREATE OR REPLACE TABLE default.test_null_nan_rewritten
  USING iceberg
  AS SELECT * FROM default.test_null_nan
"""
)

spark.sql(
    """
CREATE OR REPLACE TABLE default.test_limit as
  SELECT * LATERAL VIEW explode(ARRAY(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)) AS idx;
"""
)

spark.sql(
    """
CREATE OR REPLACE TABLE default.test_positional_mor_deletes (
    dt     date,
    number integer,
    letter string
)
USING iceberg
TBLPROPERTIES (
    'write.delete.mode'='merge-on-read',
    'write.update.mode'='merge-on-read',
    'write.merge.mode'='merge-on-read',
    'format-version'='2'
);
"""
)

# Partitioning is not really needed, but there is a bug:
# https://github.com/apache/iceberg/pull/7685
spark.sql(
    """
    ALTER TABLE default.test_positional_mor_deletes ADD PARTITION FIELD years(dt) AS dt_years
"""
)

spark.sql(
    """
INSERT INTO default.test_positional_mor_deletes
VALUES
    (CAST('2023-03-01' AS date), 1, 'a'),
    (CAST('2023-03-02' AS date), 2, 'b'),
    (CAST('2023-03-03' AS date), 3, 'c'),
    (CAST('2023-03-04' AS date), 4, 'd'),
    (CAST('2023-03-05' AS date), 5, 'e'),
    (CAST('2023-03-06' AS date), 6, 'f'),
    (CAST('2023-03-07' AS date), 7, 'g'),
    (CAST('2023-03-08' AS date), 8, 'h'),
    (CAST('2023-03-09' AS date), 9, 'i'),
    (CAST('2023-03-10' AS date), 10, 'j'),
    (CAST('2023-03-11' AS date), 11, 'k'),
    (CAST('2023-03-12' AS date), 12, 'l');
"""
)

spark.sql(
    """
ALTER TABLE default.test_positional_mor_deletes CREATE TAG tag_12
    """
)

spark.sql(
    """
ALTER TABLE default.test_positional_mor_deletes CREATE BRANCH without_5
    """
)

spark.sql(
    """
DELETE FROM default.test_positional_mor_deletes.branch_without_5 WHERE number = 5
    """
)


spark.sql(
    """
DELETE FROM default.test_positional_mor_deletes WHERE number = 9
"""
)

spark.sql(
    """
  CREATE OR REPLACE TABLE default.test_positional_mor_double_deletes (
    dt     date,
    number integer,
    letter string
  )
  USING iceberg
  TBLPROPERTIES (
    'write.delete.mode'='merge-on-read',
    'write.update.mode'='merge-on-read',
    'write.merge.mode'='merge-on-read',
    'format-version'='2'
  );
"""
)

# Partitioning is not really needed, but there is a bug:
# https://github.com/apache/iceberg/pull/7685
spark.sql(
    """
    ALTER TABLE default.test_positional_mor_double_deletes ADD PARTITION FIELD years(dt) AS dt_years
"""
)

spark.sql(
    """
INSERT INTO default.test_positional_mor_double_deletes
VALUES
    (CAST('2023-03-01' AS date), 1, 'a'),
    (CAST('2023-03-02' AS date), 2, 'b'),
    (CAST('2023-03-03' AS date), 3, 'c'),
    (CAST('2023-03-04' AS date), 4, 'd'),
    (CAST('2023-03-05' AS date), 5, 'e'),
    (CAST('2023-03-06' AS date), 6, 'f'),
    (CAST('2023-03-07' AS date), 7, 'g'),
    (CAST('2023-03-08' AS date), 8, 'h'),
    (CAST('2023-03-09' AS date), 9, 'i'),
    (CAST('2023-03-10' AS date), 10, 'j'),
    (CAST('2023-03-11' AS date), 11, 'k'),
    (CAST('2023-03-12' AS date), 12, 'l');
"""
)

spark.sql(
    """
    DELETE FROM default.test_positional_mor_double_deletes WHERE number = 9
"""
)

spark.sql(
    """
    DELETE FROM default.test_positional_mor_double_deletes WHERE letter == 'f'
"""
)

all_types_dataframe = (
    spark.range(0, 5, 1, 5)
    .withColumnRenamed("id", "longCol")
    .withColumn("intCol", expr("CAST(longCol AS INT)"))
    .withColumn("floatCol", expr("CAST(longCol AS FLOAT)"))
    .withColumn("doubleCol", expr("CAST(longCol AS DOUBLE)"))
    .withColumn("dateCol", date_add(current_date(), 1))
    .withColumn("timestampCol", expr("TO_TIMESTAMP(dateCol)"))
    .withColumn("stringCol", expr("CAST(dateCol AS STRING)"))
    .withColumn("booleanCol", expr("longCol > 5"))
    .withColumn("binaryCol", expr("CAST(longCol AS BINARY)"))
    .withColumn("byteCol", expr("CAST(longCol AS BYTE)"))
    .withColumn("decimalCol", expr("CAST(longCol AS DECIMAL(10, 2))"))
    .withColumn("shortCol", expr("CAST(longCol AS SHORT)"))
    .withColumn("mapCol", expr("MAP(longCol, decimalCol)"))
    .withColumn("arrayCol", expr("ARRAY(longCol)"))
    .withColumn("structCol", expr("STRUCT(mapCol, arrayCol)"))
)

all_types_dataframe.writeTo("default.test_all_types").tableProperty("format-version", "2").partitionedBy(
    "intCol"
).createOrReplace()

for table_name, partition in [
    ("test_partitioned_by_identity", "ts"),
    ("test_partitioned_by_years", "years(dt)"),
    ("test_partitioned_by_months", "months(dt)"),
    ("test_partitioned_by_days", "days(ts)"),
    ("test_partitioned_by_hours", "hours(ts)"),
    ("test_partitioned_by_truncate", "truncate(1, letter)"),
    ("test_partitioned_by_bucket", "bucket(16, number)"),
]:
    spark.sql(
        f"""
      CREATE OR REPLACE TABLE default.{table_name} (
        dt     date,
        ts     timestamp,
        number integer,
        letter string
      )
      USING iceberg;
    """
    )

    spark.sql(f"ALTER TABLE default.{table_name} ADD PARTITION FIELD {partition}")

    spark.sql(
        f"""
    INSERT INTO default.{table_name}
    VALUES
        (CAST('2022-03-01' AS date), CAST('2022-03-01 01:22:00' AS timestamp), 1, 'a'),
        (CAST('2022-03-02' AS date), CAST('2022-03-02 02:22:00' AS timestamp), 2, 'b'),
        (CAST('2022-03-03' AS date), CAST('2022-03-03 03:22:00' AS timestamp), 3, 'c'),
        (CAST('2022-03-04' AS date), CAST('2022-03-04 04:22:00' AS timestamp), 4, 'd'),
        (CAST('2023-03-05' AS date), CAST('2023-03-05 05:22:00' AS timestamp), 5, 'e'),
        (CAST('2023-03-06' AS date), CAST('2023-03-06 06:22:00' AS timestamp), 6, 'f'),
        (CAST('2023-03-07' AS date), CAST('2023-03-07 07:22:00' AS timestamp), 7, 'g'),
        (CAST('2023-03-08' AS date), CAST('2023-03-08 08:22:00' AS timestamp), 8, 'h'),
        (CAST('2023-03-09' AS date), CAST('2023-03-09 09:22:00' AS timestamp), 9, 'i'),
        (CAST('2023-03-10' AS date), CAST('2023-03-10 10:22:00' AS timestamp), 10, 'j'),
        (CAST('2023-03-11' AS date), CAST('2023-03-11 11:22:00' AS timestamp), 11, 'k'),
        (CAST('2023-03-12' AS date), CAST('2023-03-12 12:22:00' AS timestamp), 12, 'l');
    """
    )

# There is an issue with CREATE OR REPLACE
# https://github.com/apache/iceberg/issues/8756
spark.sql(
    """
DROP TABLE IF EXISTS default.test_table_version
"""
)

spark.sql(
    """
CREATE TABLE default.test_table_version (
    dt     date,
    number integer,
    letter string
)
USING iceberg
TBLPROPERTIES (
    'format-version'='1'
);
"""
)

spark.sql(
    """
CREATE TABLE default.test_table_sanitized_character (
    `letter/abc` string
)
USING iceberg
TBLPROPERTIES (
    'format-version'='1'
);
"""
)

spark.sql(
    """
INSERT INTO default.test_table_sanitized_character
VALUES
    ('123')
"""
)

spark.sql(
    """
  CREATE OR REPLACE TABLE default.test_add_new_column
  USING iceberg
  AS SELECT
        1            AS idx
    UNION ALL SELECT
        2            AS idx
    UNION ALL SELECT
        3            AS idx
"""
)

spark.sql("ALTER TABLE default.test_add_new_column ADD COLUMN name STRING")
spark.sql("INSERT INTO default.test_add_new_column VALUES (3, 'abc'), (4, 'def')")

# In Iceberg the data and schema evolves independently. We can add a column
# that should show up when querying the data, but is not yet represented in a Parquet file

spark.sql(
    """
  CREATE OR REPLACE TABLE default.test_new_column_with_no_data
  USING iceberg
  AS SELECT
        1            AS idx
    UNION ALL SELECT
        2            AS idx
    UNION ALL SELECT
        3            AS idx
"""
)

spark.sql("ALTER TABLE default.test_new_column_with_no_data ADD COLUMN name STRING")


###
# Renaming columns test table
###

renaming_columns_dataframe = (
    spark.range(1, 2, 3)
    .withColumnRenamed("id", "idx")
    .withColumn("data", col("idx") * 10)
    .withColumn("structcol", struct("idx"))
    .withColumn("structcol_oldname", struct("idx"))
    .withColumn("nested_list_struct_col", array("structcol", "structcol", "structcol"))
    .withColumn("deleted_and_then_overwritten_col", col("idx"))
)
renaming_columns_dataframe.writeTo("default.test_table_rename").tableProperty("format-version", "2").createOrReplace()
spark.sql("ALTER TABLE default.test_table_rename RENAME COLUMN idx TO idx_renamed")
spark.sql("ALTER TABLE default.test_table_rename RENAME COLUMN structcol.idx TO idx_renamed")
spark.sql("ALTER TABLE default.test_table_rename RENAME COLUMN structcol_oldname TO structcol_2")

test_table_rename_tbl = catalog.load_table("default.test_table_rename")
with test_table_rename_tbl.update_schema() as txn:
    txn.rename_column("nested_list_struct_col.idx", "idx_renamed")


with test_table_rename_tbl.update_schema() as txn:
    txn.delete_column("deleted_and_then_overwritten_col")
    txn.add_column("deleted_and_then_overwritten_col", StringType())


spark.sql(
    """
  CREATE OR REPLACE TABLE default.test_evolve_partitioning (
      dt     date
  )
  USING iceberg
  PARTITIONED BY (months(dt))
"""
)

spark.sql("INSERT INTO default.test_evolve_partitioning VALUES (CAST('2021-01-01' AS date))")

spark.sql(
    """
    ALTER TABLE default.test_evolve_partitioning
    REPLACE PARTITION FIELD dt_month WITH days(dt)
"""
)

spark.sql("INSERT INTO default.test_evolve_partitioning VALUES (CAST('2021-02-01' AS date))")


###
# Multi-snapshot table
###

spark.sql("""
  CREATE OR REPLACE TABLE default.test_snapshotting
  USING iceberg
  AS SELECT
    1            AS idx,
    float('NaN') AS col_numeric
UNION ALL SELECT
    2            AS idx,
    null         AS col_numeric
UNION ALL SELECT
    3            AS idx,
    1            AS col_numeric
""")

spark.sql("INSERT INTO default.test_snapshotting VALUES (4, 1)")


###
# MOR (Merge-on-Read) Complex Scenario Test Table
# Used to test the accuracy of Count push down function in complex delete file scenarios
###

spark.sql(
    """
    CREATE OR REPLACE TABLE default.test_overlapping_deletes (
        id integer,
        name string,
        value double,
        category string
    )
    USING iceberg
    TBLPROPERTIES (
        'write.delete.mode'='merge-on-read',
        'write.update.mode'='merge-on-read',
        'write.merge.mode'='merge-on-read',
        'format-version'='2'
    );
"""
)

spark.sql(
    """
    INSERT INTO default.test_overlapping_deletes
    VALUES
        (1, 'Alice', 100.0, 'A'),
        (2, 'Bob', 200.0, 'B'),
        (3, 'Charlie', 300.0, 'A'),
        (4, 'David', 400.0, 'B'),
        (5, 'Eve', 500.0, 'A'),
        (6, 'Frank', 600.0, 'B'),
        (7, 'Grace', 700.0, 'A'),
        (8, 'Henry', 800.0, 'B'),
        (9, 'Ivy', 900.0, 'A'),
        (10, 'Jack', 1000.0, 'B'),
        (11, 'Kate', 1100.0, 'A'),
        (12, 'Leo', 1200.0, 'B'),
        (13, 'Mary', 1300.0, 'A'),
        (14, 'Nick', 1400.0, 'B'),
        (15, 'Olivia', 1500.0, 'A');
"""
)

spark.sql(
    """
    DELETE FROM default.test_overlapping_deletes WHERE id <= 5
"""
)

spark.sql(
    """
    DELETE FROM default.test_overlapping_deletes WHERE id <= 3
"""
)

spark.sql(
    """
    DELETE FROM default.test_overlapping_deletes WHERE id >= 4 AND id <= 8
"""
)

# Mixed Delete Type Test Table - Testing the Mixed Processing of Position Delete and Equality Delete

spark.sql(
    """
    CREATE OR REPLACE TABLE default.test_mixed_delete_types (
        id integer,
        name string,
        age integer,
        department string,
        salary double,
        active boolean
    )
    USING iceberg
    TBLPROPERTIES (
        'write.delete.mode'='merge-on-read',
        'write.update.mode'='merge-on-read',
        'write.merge.mode'='merge-on-read',
        'format-version'='2'
    );
"""
)

spark.sql(
    """
    INSERT INTO default.test_mixed_delete_types
    VALUES
        (1, 'Alice', 25, 'Engineering', 75000.0, true),
        (2, 'Bob', 30, 'Marketing', 65000.0, true),
        (3, 'Charlie', 35, 'Engineering', 85000.0, true),
        (4, 'David', 28, 'Sales', 60000.0, false),
        (5, 'Eve', 32, 'Engineering', 90000.0, true),
        (6, 'Frank', 45, 'Marketing', 70000.0, true),
        (7, 'Grace', 29, 'Engineering', 80000.0, true),
        (8, 'Henry', 38, 'Sales', 55000.0, false),
        (9, 'Ivy', 26, 'Engineering', 78000.0, true),
        (10, 'Jack', 33, 'Marketing', 68000.0, true),
        (11, 'Kate', 31, 'Engineering', 82000.0, true),
        (12, 'Leo', 27, 'Sales', 58000.0, true),
        (13, 'Mary', 34, 'Engineering', 88000.0, true),
        (14, 'Nick', 29, 'Marketing', 66000.0, false),
        (15, 'Olivia', 36, 'Engineering', 92000.0, true),
        (16, 'Paul', 40, 'Sales', 62000.0, true),
        (17, 'Quinn', 28, 'Engineering', 76000.0, true),
        (18, 'Rachel', 32, 'Marketing', 69000.0, true),
        (19, 'Steve', 37, 'Engineering', 87000.0, true),
        (20, 'Tina', 30, 'Sales', 61000.0, false);
"""
)

spark.sql(
    """
    DELETE FROM default.test_mixed_delete_types WHERE id IN (2, 5, 8, 11, 14)
"""
)

spark.sql(
    """
    DELETE FROM default.test_mixed_delete_types WHERE department = 'Sales' AND active = false
"""
)

spark.sql(
    """
    DELETE FROM default.test_mixed_delete_types WHERE age < 30 AND salary < 70000
"""
)

spark.sql(
    """
    INSERT INTO default.test_mixed_delete_types
    VALUES
        (2, 'Lily', 60, 'Sales', 2000.0, true),
        (21, 'Lucy', 28, 'Engineering', 76000.0, true);
"""
)
