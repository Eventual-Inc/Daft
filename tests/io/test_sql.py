from __future__ import annotations

import warnings

import pytest
from sqlalchemy import Column, Float, Integer, MetaData, String, Table, create_engine, insert

import daft
from daft.datatype import DataType
from daft.sql.sql_connection import SQLConnection


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
    ("url", "secret"),
    [
        # userinfo password
        ("postgresql+psycopg2://alice:super-secret-pw@127.0.0.1:1/nope", "super-secret-pw"),
        # query-param token (Trino JWT shape, the customer-reported leak)
        (
            "trino://alice@127.0.0.1:1?auth=jwt&access_token=SUPER_SECRET_TOKEN&http_scheme=https",
            "SUPER_SECRET_TOKEN",
        ),
        # password with URL-special chars
        ("postgresql+psycopg2://alice:p#ss@127.0.0.1:1/nope", "p#ss"),
    ],
)
def test_execute_sql_error_does_not_leak_credentials(url, secret):
    """The raised RuntimeError must not echo any part of the connection URL.

    Secrets can appear anywhere in a URL (userinfo, query params, driver
    extras), so the URL is omitted from the error message entirely rather
    than relying on field-specific redaction.
    """
    pytest.importorskip("psycopg2")
    conn = SQLConnection.from_url(url)
    with pytest.raises(RuntimeError) as exc_info:
        conn.execute_sql_query("SELECT 1")
    assert secret not in str(exc_info.value)


def test_repr_does_not_leak_url():
    """SQLConnection.__repr__ must not echo the connection URL.

    Same reasoning as the error-message path. Show only dialect/driver.
    """
    secret = "super-secret-token-do-not-leak"
    conn = SQLConnection.from_url(f"trino://alice@trino.example.com:443?access_token={secret}")
    r = repr(conn)
    assert secret not in r
    assert "trino.example.com" not in r
    assert "alice" not in r
