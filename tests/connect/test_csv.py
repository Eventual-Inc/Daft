from __future__ import annotations

import tempfile
import shutil
import os


def test_write_csv(spark_session):
    # Create a temporary directory
    temp_dir = tempfile.mkdtemp()
    try:
        # Create DataFrame from range(10)
        df = spark_session.range(10)

        # Write DataFrame to CSV directory
        csv_dir = os.path.join(temp_dir, "test.csv")
        df.write.csv(csv_dir)

        # List all files in the CSV directory
        csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv')]
        print(f"CSV files in directory: {csv_files}")

        # Assert there is at least one CSV file
        assert len(csv_files) > 0, "Expected at least one CSV file to be written"

        # Read back from the CSV directory (not specific file)
        df_read = spark_session.read.csv(csv_dir)

        # Verify the data is unchanged
        df_pandas = df.toPandas()
        df_read_pandas = df_read.toPandas()
        assert df_pandas["id"].equals(df_read_pandas["id"]), "Data should be unchanged after write/read"

    finally:
        # Clean up temp directory
        shutil.rmtree(temp_dir)
