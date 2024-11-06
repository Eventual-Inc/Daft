from __future__ import annotations

import pathlib
import time

import pyarrow as pa
import pyarrow.parquet as papq
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from daft.daft import connect_start


def test_read_parquet(tmpdir):
    # Convert tmpdir to Path object
    test_dir = pathlib.Path(tmpdir)
    input_parquet_path = test_dir / "input.parquet"

    # Create sample data with sequential IDs
    sample_data = pa.Table.from_pydict({"id": [0, 1, 2, 3, 4]})

    # Write sample data to input parquet file
    papq.write_table(sample_data, input_parquet_path)

    # Start Daft Connect server
    # TODO: Add env var to control server embedding
    connect_start("sc://localhost:50051")

    # Initialize Spark Connect session
    spark_session: SparkSession = (
        SparkSession.builder.appName("DaftParquetReadWriteTest").remote("sc://localhost:50051").getOrCreate()
    )

    # Read input parquet with Spark Connect
    spark_df: DataFrame = spark_session.read.parquet(str(input_parquet_path))

    # Write DataFrame to output parquet
    output_parquet_path = test_dir / "output.parquet"
    spark_df.write.parquet(str(output_parquet_path))

    # Verify output matches input
    output_data = papq.read_table(output_parquet_path)
    assert output_data.equals(sample_data)

    # Clean up Spark session
    spark_session.stop()
    time.sleep(2)  # Allow time for session cleanup
