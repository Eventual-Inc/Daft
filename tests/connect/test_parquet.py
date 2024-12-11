from __future__ import annotations

import os
import tempfile


def test_write_parquet(spark_session):
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create DataFrame from range(10)
        df = spark_session.range(10)

        # Write DataFrame to parquet directory
        parquet_dir = os.path.join(temp_dir, "test.parquet")
        df.write.parquet(parquet_dir)

        # List all files in the parquet directory
        parquet_files = [f for f in os.listdir(parquet_dir) if f.endswith(".parquet")]

        # Assert there is at least one parquet file
        assert len(parquet_files) > 0, "Expected at least one parquet file to be written"

        # Read back from the parquet directory (not specific file)
        df_read = spark_session.read.parquet(parquet_dir)

        # Verify the data is unchanged
        df_pandas = df.toPandas()
        df_read_pandas = df_read.toPandas()
        assert df_pandas["id"].equals(df_read_pandas["id"]), "Data should be unchanged after write/read"
