from __future__ import annotations

import datetime
import decimal

import pyarrow as pa
import pytest

deltalake = pytest.importorskip("deltalake")


@pytest.fixture(params=[1, 2, 10])
def num_partitions(request) -> int:
    yield request.param


@pytest.fixture(
    params=[
        pytest.param((lambda _: None, "a"), id="unpartitioned"),
        pytest.param((lambda i: i, "a"), id="int_partitioned"),
        pytest.param((lambda i: i * 1.5, "b"), id="float_partitioned"),
        pytest.param((lambda i: f"foo_{i}", "c"), id="string_partitioned"),
        pytest.param((lambda i: f"foo_{i}".encode(), "d"), id="string_partitioned"),
        pytest.param((lambda i: datetime.datetime(2024, 2, i + 1), "f"), id="timestamp_partitioned"),
        pytest.param((lambda i: datetime.date(2024, 2, i + 1), "g"), id="date_partitioned"),
        pytest.param((lambda i: decimal.Decimal(str(1000 + i) + ".567"), "h"), id="decimal_partitioned"),
    ]
)
def partition_generator(request) -> (callable, str):
    yield request.param


@pytest.fixture
def base_table() -> pa.Table:
    yield pa.table(
        {
            "a": [1, 2, 3],
            "b": [1.1, 2.2, 3.3],
            "c": ["foo", "bar", "baz"],
            "d": [b"foo", b"bar", b"baz"],
            "e": [True, False, True],
            "f": [datetime.datetime(2024, 2, 10), datetime.datetime(2024, 2, 11), datetime.datetime(2024, 2, 12)],
            "g": [datetime.date(2024, 2, 10), datetime.date(2024, 2, 11), datetime.date(2024, 2, 12)],
            "h": pa.array(
                [decimal.Decimal("1234.567"), decimal.Decimal("1233.456"), decimal.Decimal("1232.345")],
                type=pa.decimal128(7, 3),
            ),
            "i": [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
            "j": [{"x": 1, "y": False}, {"y": True, "z": "foo"}, {"x": 5, "z": "bar"}],
            # TODO(Clark): Uncomment test case when MapArray support is merged.
            # "k": pa.array(
            #     [[("x", 1), ("y", 0)], [("a", 2), ("b", 45)], [("c", 4), ("d", 18)]],
            #     type=pa.map_(pa.string(), pa.int64()),
            # ),
            # TODO(Clark): Wait for more temporal type support in Delta Lake.
            # "l": [
            #     datetime.time(hour=1, minute=2, second=4, microsecond=5),
            #     datetime.time(hour=3, minute=4, second=5, microsecond=6),
            #     datetime.time(hour=4, minute=5, second=6, microsecond=7),
            # ],
            # "m": [
            #     datetime.timedelta(days=1, seconds=2, minutes=5, hours=6, weeks=7),
            #     datetime.timedelta(days=2),
            #     datetime.timedelta(hours=4),
            # ],
        }
    )


@pytest.fixture
def local_deltalake_table(tmp_path, base_table, num_partitions, partition_generator) -> deltalake.DeltaTable:
    partition_generator, _ = partition_generator
    path = tmp_path / "some_table"
    parts = []
    for i in range(num_partitions):
        part_value = partition_generator(i)
        part = base_table.append_column("part_idx", pa.array([part_value if part_value is not None else i] * 3))
        deltalake.write_deltalake(
            path, part, mode="append", partition_by="part_idx" if part_value is not None else None
        )
        parts.append(part)
    # NOTE: Delta Lake returns files in reverse-chronological order (most recently written first) from the transaction
    # log.
    yield path, list(reversed(parts))
