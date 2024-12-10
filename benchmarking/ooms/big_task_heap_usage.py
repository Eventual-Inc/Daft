# /// script
# dependencies = ['numpy']
# ///

import argparse
import functools

import daft
from daft.io._generator import read_generator
from daft.table.table import Table

NUM_PARTITIONS = 8


@daft.udf(return_dtype=daft.DataType.binary())
def mock_inflate_data(data, inflation_factor):
    return [x * inflation_factor for x in data.to_pylist()]


@daft.udf(return_dtype=daft.DataType.binary())
def mock_deflate_data(data, deflation_factor):
    return [x[: int(len(x) / deflation_factor)] for x in data.to_pylist()]


def generate(num_rows_per_partition):
    yield Table.from_pydict({"foo": [b"x" for _ in range(num_rows_per_partition)]})


def generator(
    num_partitions: int,
    num_rows_per_partition: int,
):
    """Generate data for all partitions."""
    for i in range(num_partitions):
        yield functools.partial(generate, num_rows_per_partition)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        "Runs a workload which is a simple map workload, but it will run 2 custom UDFs which first inflates the data, and then deflates it. "
        "It starts with 1KB partitions, then runs inflation and subsequently deflation. We expect this to OOM if the heap memory usage exceeds "
        "`MEM / N_CPUS` on a given worker node."
    )
    parser.add_argument("--num-partitions", type=int, default=8)
    parser.add_argument("--num-rows-per-partition", type=int, default=1000)
    parser.add_argument("--inflation-factor", type=int, default=100)
    parser.add_argument("--deflation-factor", type=int, default=100)
    args = parser.parse_args()

    df = read_generator(
        generator(args.num_partitions, args.num_rows_per_partition),
        schema=daft.Schema._from_field_name_and_types([("foo", daft.DataType.binary())]),
    )

    df.collect()
    print(df)

    # Big memory explosion
    df = df.with_column("foo", mock_inflate_data(df["foo"], args.inflation_factor))

    # Big memory reduction
    df = df.with_column("foo", mock_deflate_data(df["foo"], args.deflation_factor))

    df.explain(True)

    df.collect()
    print(df)
