from __future__ import annotations

import warnings

import pytest
from sqlalchemy import Column, Float, Integer, MetaData, String, Table, create_engine, insert

import daft
from daft.datatype import DataType
from daft.sql.sql_connection import SQLConnection, _redact_url


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


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("trino://alice:hunter2@trino.example.com:443", "trino://alice:***@trino.example.com:443"),
        (
            "trino://alice:hunter2@trino.example.com:443/db?param=1",
            "trino://alice:***@trino.example.com:443/db?param=1",
        ),
        ("postgresql://user:p%40ss@host:5432/db", "postgresql://user:***@host:5432/db"),
        ("mysql+pymysql://:secret@host/db", "mysql+pymysql://***@host/db"),
        # No password — should be returned unchanged.
        ("sqlite:///my.db", "sqlite:///my.db"),
        ("mysql://user@host/db", "mysql://user@host/db"),
        ("trino://host:443", "trino://host:443"),
        ("not a url", "not a url"),
    ],
)
def test_redact_url(url, expected):
    assert _redact_url(url) == expected


def test_execute_sql_error_does_not_leak_password():
    """Connection failures must not include the password in the raised error."""
    password = "super-secret-pw"
    url = f"postgresql+psycopg2://alice:{password}@127.0.0.1:1/nope"
    conn = SQLConnection.from_url(url)

    with pytest.raises(RuntimeError) as exc_info:
        conn.execute_sql_query("SELECT 1")

    message = str(exc_info.value)
    assert password not in message
    assert "alice:***@127.0.0.1:1" in message


def test_repr_does_not_leak_password():
    password = "super-secret-pw"
    conn = SQLConnection.from_url(f"postgresql://alice:{password}@host:5432/db")
    assert password not in repr(conn)
