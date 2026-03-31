from __future__ import annotations

import warnings

import pytest
from sqlalchemy import Column, Float, Integer, MetaData, String, Table, create_engine, insert

import daft
from daft.datatype import DataType


@pytest.fixture()
def sqlite_db(tmp_path):
    db_path = tmp_path / "test.db"
    url = f"sqlite:///{db_path}"
    engine = create_engine(url)
    metadata = MetaData()

    table = Table(
        "test_table",
        metadata,
        Column("id", Integer),
        Column("value", Float),
        Column("label", String(50)),
    )
    metadata.create_all(engine)

    with engine.begin() as conn:
        conn.execute(
            insert(table),
            [{"id": i, "value": float(i), "label": f"row_{i}"} for i in range(100)],
        )

    return url


@pytest.fixture()
def sqlite_null_partition_db(tmp_path):
    """Database where the partition column is entirely NULL."""
    db_path = tmp_path / "null_test.db"
    url = f"sqlite:///{db_path}"
    engine = create_engine(url)
    metadata = MetaData()

    table = Table(
        "null_table",
        metadata,
        Column("id", Integer),
        Column("value", Float),
        Column("label", String(50)),
    )
    metadata.create_all(engine)

    with engine.begin() as conn:
        conn.execute(
            insert(table),
            [{"id": None, "value": float(i), "label": f"row_{i}"} for i in range(50)],
        )

    return url


def test_sql_read_basic(sqlite_db):
    df = daft.read_sql("SELECT * FROM test_table", sqlite_db)
    result = df.sort("id").to_pydict()
    assert result["id"] == list(range(100))
    assert result["value"] == [float(i) for i in range(100)]
    assert result["label"] == [f"row_{i}" for i in range(100)]


@pytest.mark.parametrize("num_partitions", [2, 3, 4])
def test_sql_partitioned_read(sqlite_db, num_partitions):
    df = daft.read_sql(
        "SELECT * FROM test_table",
        sqlite_db,
        partition_col="id",
        num_partitions=num_partitions,
    )
    result = df.sort("id").to_pydict()
    assert result["id"] == list(range(100))
    assert result["value"] == [float(i) for i in range(100)]
    assert result["label"] == [f"row_{i}" for i in range(100)]


@pytest.mark.parametrize("num_partitions", [2, 3, 4])
@pytest.mark.parametrize("partition_bound_strategy", ["min-max", "percentile"])
def test_sql_partitioned_read_null_partition_col(sqlite_null_partition_db, num_partitions, partition_bound_strategy):
    """When the partition column is entirely NULL, should fall back to a single scan task."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        df = daft.read_sql(
            "SELECT * FROM null_table",
            sqlite_null_partition_db,
            partition_col="id",
            num_partitions=num_partitions,
            partition_bound_strategy=partition_bound_strategy,
            schema={"id": DataType.int64(), "value": DataType.float64(), "label": DataType.string()},
        )
        result = df.to_pydict()

    assert len(result["value"]) == 50
    assert any("Falling back to a single scan task" in str(warning.message) for warning in w)
