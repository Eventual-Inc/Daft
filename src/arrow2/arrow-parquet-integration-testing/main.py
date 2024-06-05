import subprocess
import os

import pyarrow.ipc
import pyarrow.parquet as pq


def get_file_path(file: str):
    return f"../testing/arrow-testing/data/arrow-ipc-stream/integration/1.0.0-littleendian/{file}.arrow_file"


def _prepare(
    file: str,
    version: str,
    compression: str,
    encoding_utf8: str,
    encoding_int: str,
    projection=None,
):
    write = f"{file}.parquet"

    args = [
        "cargo",
        "run",
        "--",
        "--json",
        file,
        "--output",
        write,
        "--version",
        version,
        "--encoding-utf8",
        encoding_utf8,
        "--encoding-int",
        encoding_int,
        "--compression",
        compression,
    ]

    if projection:
        projection = list(map(str, projection))
        args += ["--projection", ",".join(projection)]

    subprocess.call(args)
    return write


def _expected(file: str) -> pyarrow.Table:
    return pyarrow.ipc.RecordBatchFileReader(get_file_path(file)).read_all()


# types without a native parquet logical representation
# There is currently no specification on how to represent these in parquet,
# and thus we ignore them in comparisons
non_native_types = [
    pyarrow.date64(),
    pyarrow.time32("s"),
    pyarrow.timestamp("s"),
    # the issue here is the second, not the tz
    pyarrow.timestamp("s", tz="UTC"),
]


def variations():
    for version in ["1", "2"]:
        for file in [
            "generated_primitive",
            "generated_primitive_no_batches",
            "generated_primitive_zerolength",
            "generated_null",
            "generated_null_trivial",
            "generated_primitive_large_offsets",
            "generated_datetime",
            "generated_decimal",
            "generated_interval",
            # see https://issues.apache.org/jira/browse/ARROW-13486 and
            # https://issues.apache.org/jira/browse/ARROW-13487
            # "generated_dictionary",
            # requires writing Struct
            # "generated_duplicate_fieldnames",
            # requires writing un-nested List
            # "generated_custom_metadata",
        ]:
            # pyarrow does not support decoding "delta"-encoded values.
            for encoding_int in ["plain", "delta"]:
                if encoding_int == "delta" and file in {"generated_primitive", "generated_null"}:
                    # see https://issues.apache.org/jira/browse/ARROW-17465
                    continue

                for compression in ["uncompressed", "zstd", "snappy"]:
                    yield (version, file, compression, "plain", encoding_int)


if __name__ == "__main__":
    for (version, file, compression, encoding_utf8, encoding_int) in variations():
        expected = _expected(file)
        path = _prepare(file, version, compression, encoding_utf8, encoding_int)

        table = pq.read_table(path)
        os.remove(path)

        for c1, c2 in zip(expected, table):
            if c1.type in non_native_types:
                continue
            if str(c1.type) in ["month_interval", "day_time_interval"]:
                # pyarrow does not support interval types from parquet
                continue
            assert c1 == c2, (c1, c2)
