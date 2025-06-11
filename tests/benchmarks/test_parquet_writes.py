from __future__ import annotations

import random
import string
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import daft
from daft.context import set_execution_config
from daft.functions import monotonically_increasing_id


class ColumnType(Enum):
    INT = "int"
    FLOAT = "float"
    STRING = "string"
    LARGE_STRING = "large_string"
    BOOLEAN = "boolean"


@dataclass
class ColumnSpec:
    name: str
    type: ColumnType
    null_percentage: float = 0.0
    string_avg_length: int | None = None  # For STRING and LARGE_STRING types.

    def __post_init__(self):
        # Set default string lengths based on type.
        if self.string_avg_length is None:
            if self.type == ColumnType.STRING:
                self.string_avg_length = 50
            elif self.type == ColumnType.LARGE_STRING:
                self.string_avg_length = 2000


@dataclass
class FileConfig:
    """Flexible configuration for file generation supporting both count-based and schema-based approaches.

    Examples:
        # Schema-based approach with interleaved columns
        config = FileConfig(
            num_rows=1000,
            schema=[
                ColumnSpec("id", ColumnType.INT),
                ColumnSpec("name", ColumnType.STRING, string_avg_length=100),
                ColumnSpec("score", ColumnType.FLOAT),
                ColumnSpec("description", ColumnType.LARGE_STRING),
                ColumnSpec("active", ColumnType.BOOLEAN, null_percentage=0.1),
            ]
        )

        # Convenient pattern-based approach
        config = FileConfig.from_pattern(
            num_rows=1000,
            pattern="int,string,float,boolean",
            repeat=3
        )
    """

    num_rows: int
    schema: list[ColumnSpec] | None = None

    @classmethod
    def from_pattern(
        cls,
        num_rows: int,
        pattern: str,
        repeat: int = 1,
        null_percentage: float = 0.0,
        string_avg_length: int = 50,
        large_string_avg_length: int = 2000,
    ) -> FileConfig:
        """Create a FileConfig from a pattern string.

        Args:
            num_rows: Number of rows in the file
            pattern: Comma-separated column types (e.g., "int,string,float,boolean")
            repeat: How many times to repeat the pattern
            null_percentage: Default null percentage for all columns
            string_avg_length: Default length for string columns
            large_string_avg_length: Default length for large string columns

        Example:
            FileConfig.from_pattern(1000, "int,string,float", repeat=2)
            # Creates: int, string, float, int, string, float columns
        """
        type_mapping = {
            "int": ColumnType.INT,
            "float": ColumnType.FLOAT,
            "string": ColumnType.STRING,
            "large_string": ColumnType.LARGE_STRING,
            "boolean": ColumnType.BOOLEAN,
        }

        pattern_types = [type_mapping[t.strip().lower()] for t in pattern.split(",")]
        schema = []

        for i in range(repeat):
            for j, col_type in enumerate(pattern_types):
                col_name = f"{col_type.value}_{i}_{j}"
                col_spec = ColumnSpec(name=col_name, type=col_type, null_percentage=null_percentage)

                # Set string lengths
                if col_type == ColumnType.STRING:
                    col_spec.string_avg_length = string_avg_length
                elif col_type == ColumnType.LARGE_STRING:
                    col_spec.string_avg_length = large_string_avg_length

                schema.append(col_spec)

        return cls(num_rows=num_rows, schema=schema)

    def get_total_columns(self) -> int:
        if self.schema:
            return len(self.schema)
        else:
            return 0

    def get_column_specs(self) -> list[ColumnSpec]:
        if self.schema:
            return self.schema
        return []


def generate_column_data(spec: ColumnSpec, num_rows: int) -> pa.Array:
    """Generate data for a single column based on its specification."""
    random.seed(9001)  # For reproducibility

    # Pre-allocate for better performance with large datasets
    if spec.type == ColumnType.INT:
        values = [random.randint(-1000000, 1000000) for _ in range(num_rows)]
    elif spec.type == ColumnType.FLOAT:
        values = [random.uniform(-1000.0, 1000.0) for _ in range(num_rows)]
    elif spec.type == ColumnType.BOOLEAN:
        values = [random.choice([True, False]) for _ in range(num_rows)]
    elif spec.type in (ColumnType.STRING, ColumnType.LARGE_STRING):
        avg_length = spec.string_avg_length or 50
        # Use numpy-style generation for better performance with large datasets
        import numpy as np

        lengths = np.random.normal(avg_length, avg_length * 0.2, num_rows).astype(int)
        lengths = np.clip(lengths, 1, avg_length * 3)  # Ensure reasonable bounds

        chars = string.ascii_letters + string.digits + " "
        values = ["".join(random.choices(chars, k=length)) for length in lengths]
    else:
        raise ValueError(f"Unknown column type: {spec.type}")

    # Apply null percentage.
    if spec.null_percentage > 0:
        num_nulls = int(num_rows * spec.null_percentage)
        null_indices = random.sample(range(num_rows), num_nulls)
        for idx in null_indices:
            values[idx] = None

    # Convert to PyArrow array with appropriate type.
    if spec.type == ColumnType.INT:
        return pa.array(values, type=pa.int64())
    elif spec.type == ColumnType.FLOAT:
        return pa.array(values, type=pa.float64())
    elif spec.type == ColumnType.BOOLEAN:
        return pa.array(values, type=pa.bool_())
    elif spec.type in (ColumnType.STRING, ColumnType.LARGE_STRING):
        return pa.array(values, type=pa.string())


def generate_parquet_file(config: FileConfig, output_path: Path) -> None:
    start_time = time.time()

    print(f"Generating {output_path.name} with {config.num_rows} rows, {config.get_total_columns()} columns")

    # Generate data for all columns.
    arrays = []
    names = []

    for i, spec in enumerate(config.get_column_specs()):
        if i % 10 == 0 and i > 0:  # Progress indicator for large datasets
            print(f"  -> Processing column {i}/{config.get_total_columns()}")
        arrays.append(generate_column_data(spec, config.num_rows))
        names.append(spec.name)

    # Create PyArrow table.
    table = pa.table(arrays, names=names)

    # Write to parquet.
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        table,
        output_path,
        compression="snappy",  # Fast compression
    )

    elapsed = time.time() - start_time
    file_size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  -> Created {output_path.name}: {file_size_mb:.2f} MB in {elapsed:.1f}s")


# Test data configurations for different benchmark scenarios
TEST_CONFIGS = {
    "small_mixed": FileConfig.from_pattern(
        num_rows=1000000,
        pattern="int,string,float,boolean",
        repeat=4,
    ),
    "large_strings": FileConfig.from_pattern(
        num_rows=500000,
        pattern="int,large_string,large_string,string",
        repeat=10,
    ),
    "integer_heavy": FileConfig.from_pattern(num_rows=5000000, pattern="int", repeat=10, null_percentage=0.0),
    "boolean_heavy": FileConfig.from_pattern(num_rows=10000000, pattern="boolean", repeat=20, null_percentage=0.05),
    "wide_table": FileConfig.from_pattern(
        num_rows=1000000,
        pattern="int,string,float,boolean",
        repeat=25,  # 100 columns total
        null_percentage=0.1,
        string_avg_length=30,
    ),
    "high_nulls": FileConfig(
        num_rows=2000000,
        schema=[
            ColumnSpec("id", ColumnType.INT),
            ColumnSpec("optional_name", ColumnType.STRING, null_percentage=0.7),
            ColumnSpec("optional_score", ColumnType.FLOAT, null_percentage=0.8),
            ColumnSpec("maybe_active", ColumnType.BOOLEAN, null_percentage=0.6),
            ColumnSpec("sparse_data", ColumnType.LARGE_STRING, null_percentage=0.9),
        ],
    ),
}


@pytest.fixture(scope="session")
def test_data_dir():
    data_dir = Path(__file__).parent.parent.parent / "benchmarking" / "parquet" / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    print(f"\nGenerating benchmark data in {data_dir}")

    # Generate all test files (only if they don't already exist to save time).
    for i, (config_name, config) in enumerate(TEST_CONFIGS.items()):
        output_path = data_dir / f"{config_name}-{config.num_rows:_}.parquet"
        if not output_path.exists():
            print(f"\n[{i+1}/{len(TEST_CONFIGS)}] Generating {config_name}...")
            generate_parquet_file(config, output_path)
        else:
            file_size_mb = output_path.stat().st_size / (1024 * 1024)
            print(f"[{i+1}/{len(TEST_CONFIGS)}] {config_name}: Using existing file ({file_size_mb:.2f} MB)")

    print(f"\nBenchmark data ready in {data_dir}")
    return data_dir


@pytest.mark.benchmark(group="parquet_writes")
@pytest.mark.parametrize("writer", ["native", "pyarrow"])
@pytest.mark.parametrize("target", ["local", "s3"])
@pytest.mark.parametrize("config_name", list(TEST_CONFIGS.keys()))
def test_parquet_write_performance(tmp_path, benchmark_with_memray, test_data_dir, writer, target, config_name):
    # Configure writer.
    if writer == "native":
        set_execution_config(native_parquet_writer=True)
    else:
        set_execution_config(native_parquet_writer=False)

    # Read the test file.
    config = TEST_CONFIGS[config_name]
    input_file = test_data_dir / f"{config_name}-{config.num_rows:_}.parquet"
    df = daft.read_parquet(str(input_file))
    df = df.with_column("id", monotonically_increasing_id())
    df.collect()

    def benchmark_write():
        # Write to output.
        if target == "local":
            output_file = tmp_path / f"{config_name}_{writer}.parquet"
        else:
            output_file = f"s3://eventual-dev-benchmarking-fixtures/write-outputs/{config_name}_{writer}.parquet"
        df.write_parquet(str(output_file), write_mode="overwrite")

        return output_file

    benchmark_group = f"write_{config_name}-{target}"
    result_file = benchmark_with_memray(benchmark_write, benchmark_group)

    # Verify the file was written correctly.
    assert result_file.exists()
    results_df = daft.read_parquet(str(result_file))
    assert results_df.sort("id").to_arrow() == df.sort("id").to_arrow()
