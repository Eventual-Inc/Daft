# /// script
# dependencies = ['numpy']
# ///

import argparse
import random
import time
from functools import partial
from typing import Any, Dict, Optional

import numpy as np
import pyarrow as pa

import daft
import daft.context
from daft.io._generator import read_generator
from daft.table.table import Table

# Constants
GB = 1 << 30
MB = 1 << 20
KB = 1 << 10
ROW_SIZE = 100 * KB


def parse_size(size_str: str) -> int:
    """Convert human-readable size string to bytes."""
    units = {"B": 1, "KB": KB, "MB": MB, "GB": GB}
    size_str = size_str.upper()
    value, unit = size_str.split(" ")
    return int(float(value) * units[unit])


def get_skewed_distribution(num_partitions: int, skew_factor: float) -> np.ndarray:
    """Generate a skewed distribution using a power law.

    Higher skew_factor means more skewed distribution.
    """
    if skew_factor <= 0:
        return np.ones(num_partitions) / num_partitions

    # Generate power law distribution
    x = np.arange(1, num_partitions + 1)
    weights = 1.0 / (x**skew_factor)
    return weights / weights.sum()


def get_partition_size(base_size: int, size_variation: float, partition_idx: int) -> int:
    """Calculate size for a specific partition with variation.

    Args:
        base_size: The base partition size in bytes
        size_variation: Float between 0 and 1 indicating maximum variation (e.g., 0.2 = ±20%)
        partition_idx: Index of the partition (used for random seed)

    Returns:
        Adjusted partition size in bytes
    """
    if size_variation <= 0:
        return base_size

    # Use partition_idx as seed for consistent variation per partition
    random.seed(f"size_{partition_idx}")

    # Generate variation factor between (1-variation) and (1+variation)
    variation_factor = 1.0 + random.uniform(-size_variation, size_variation)

    # Ensure we don't go below 10% of base size
    min_size = base_size * 0.1
    return max(int(base_size * variation_factor), int(min_size))


def generate(
    num_partitions: int,
    base_partition_size: int,
    skew_factor: float,
    timing_variation: float,
    size_variation: float,
    partition_idx: int,
):
    """Generate data for a single partition with optional skew, timing and size variations."""
    # Calculate actual partition size with variation
    actual_partition_size = get_partition_size(base_partition_size, size_variation, partition_idx)
    num_rows = actual_partition_size // ROW_SIZE

    # Apply skewed distribution if specified
    if skew_factor > 0:
        weights = get_skewed_distribution(num_partitions, skew_factor)
        data = {
            "ints": pa.array(
                np.random.choice(num_partitions, size=num_rows, p=weights, replace=True).astype(np.uint64),
            ),
            "bytes": pa.array(
                [np.random.bytes(ROW_SIZE) for _ in range(num_rows)],
                type=pa.binary(ROW_SIZE),
            ),
        }
    else:
        data = {
            "ints": pa.array(np.random.randint(0, num_partitions, size=num_rows)),
            "bytes": pa.array(
                [np.random.bytes(ROW_SIZE) for _ in range(num_rows)],
                type=pa.binary(ROW_SIZE),
            ),
        }

    # Simulate varying processing times if specified
    if timing_variation > 0:
        random.seed(f"timing_{partition_idx}")
        delay = random.uniform(0, timing_variation)
        time.sleep(delay)

    yield Table.from_pydict(data)


def generator(
    num_partitions: int,
    partition_size: int,
    skew_factor: float,
    timing_variation: float,
    size_variation: float,
):
    """Generate data for all partitions."""
    for i in range(num_partitions):
        yield partial(
            generate,
            num_partitions,
            partition_size,
            skew_factor,
            timing_variation,
            size_variation,
            i,
        )


def setup_daft(shuffle_algorithm: Optional[str] = None):
    """Configure Daft execution settings."""
    daft.context.set_runner_ray()
    daft.context.set_execution_config(shuffle_algorithm=shuffle_algorithm, pre_shuffle_merge_threshold=8 * GB)


def create_schema():
    """Create the Daft schema for the dataset."""
    return daft.Schema._from_field_name_and_types([("ints", daft.DataType.uint64()), ("bytes", daft.DataType.binary())])


def run_benchmark(
    num_partitions: int,
    partition_size: int,
    skew_factor: float,
    timing_variation: float,
    size_variation: float,
    shuffle_algorithm: Optional[str] = None,
) -> Dict[str, Any]:
    """Run the memory benchmark and return statistics."""
    setup_daft(shuffle_algorithm)
    schema = create_schema()

    def benchmark_func():
        return (
            read_generator(
                generator(
                    num_partitions,
                    partition_size,
                    skew_factor,
                    timing_variation,
                    size_variation,
                ),
                schema,
            )
            .repartition(num_partitions, "ints")
            .collect()
        )

    start_time = time.time()

    benchmark_func()

    end_time = time.time()
    return end_time - start_time


def main():
    parser = argparse.ArgumentParser(description="Run memory benchmark for data processing")
    parser.add_argument("--partitions", type=int, default=1000, help="Number of partitions")
    parser.add_argument(
        "--partition-size",
        type=str,
        default="100 MB",
        help="Base size for each partition (e.g., 300 MB, 1 GB)",
    )
    parser.add_argument(
        "--skew-factor",
        type=float,
        default=0.0,
        help="Skew factor for partition distribution (0.0 for uniform, higher for more skew)",
    )
    parser.add_argument(
        "--timing-variation",
        type=float,
        default=0.0,
        help="Maximum random delay in seconds for partition processing",
    )
    parser.add_argument(
        "--size-variation",
        type=float,
        default=0.0,
        help="Maximum partition size variation as fraction (0.0-1.0, e.g., 0.2 for ±20%%)",
    )
    parser.add_argument("--shuffle-algorithm", type=str, default=None, help="Shuffle algorithm to use")

    args = parser.parse_args()

    if not 0 <= args.size_variation <= 1:
        parser.error("Size variation must be between 0 and 1")

    partition_size_bytes = parse_size(args.partition_size)

    print("Running benchmark with configuration:")
    print(f"Partitions: {args.partitions}")
    print(f"Base partition size: {args.partition_size} ({partition_size_bytes} bytes)")
    print(f"Size variation: ±{args.size_variation*100:.0f}%")
    print(f"Row size: {ROW_SIZE/KB:.0f}KB (fixed)")
    print(f"Skew factor: {args.skew_factor}")
    print(f"Timing variation: {args.timing_variation}s")
    print(f"Shuffle algorithm: {args.shuffle_algorithm or 'default'}")

    try:
        timing = run_benchmark(
            num_partitions=args.partitions,
            partition_size=partition_size_bytes,
            skew_factor=args.skew_factor,
            timing_variation=args.timing_variation,
            size_variation=args.size_variation,
            shuffle_algorithm=args.shuffle_algorithm,
        )

        print("\nRan benchmark with configuration:")
        print(f"Partitions: {args.partitions}")
        print(f"Base partition size: {args.partition_size} ({partition_size_bytes} bytes)")
        print(f"Size variation: ±{args.size_variation*100:.0f}%")
        print(f"Row size: {ROW_SIZE/KB:.0f}KB (fixed)")
        print(f"Skew factor: {args.skew_factor}")
        print(f"Timing variation: {args.timing_variation}s")
        print(f"Shuffle algorithm: {args.shuffle_algorithm or 'default'}")
        print("\nResults:")
        print(f"Total time: {timing:.2f}s")

    except Exception as e:
        print(f"Error running benchmark: {e!s}")
        raise


if __name__ == "__main__":
    main()
