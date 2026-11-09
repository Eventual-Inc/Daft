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


@pytest.fixture()
def adbc_sqlite():
    return pytest.importorskip("adbc_driver_sqlite")


def test_sql_read_adbc_basic(sqlite_db, adbc_sqlite):
    df = daft.read_sql("SELECT * FROM test_table", sqlite_db, backend="adbc")
    result = df.sort("id").to_pydict()
    assert result["id"] == list(range(100))
    assert result["value"] == [float(i) for i in range(100)]
    assert result["label"] == [f"row_{i}" for i in range(100)]


def test_sql_read_adbc_schema(sqlite_db, adbc_sqlite):
    df = daft.read_sql("SELECT * FROM test_table", sqlite_db, backend="adbc")
    assert df.schema()["id"].dtype == DataType.int64()
    assert df.schema()["value"].dtype == DataType.float64()
    assert df.schema()["label"].dtype == DataType.string()


def test_sql_read_adbc_pushdowns(sqlite_db, adbc_sqlite):
    df = daft.read_sql("SELECT * FROM test_table", sqlite_db, backend="adbc")
    result = df.where(daft.col("id") >= 90).select("id", "label").sort("id").limit(5).to_pydict()
    assert result == {"id": [90, 91, 92, 93, 94], "label": [f"row_{i}" for i in range(90, 95)]}


@pytest.mark.parametrize("num_partitions", [2, 3, 4])
@pytest.mark.parametrize("partition_bound_strategy", ["min-max", "percentile"])
def test_sql_read_adbc_partitioned(sqlite_db, adbc_sqlite, num_partitions, partition_bound_strategy):
    df = daft.read_sql(
        "SELECT * FROM test_table",
        sqlite_db,
        partition_col="id",
        num_partitions=num_partitions,
        partition_bound_strategy=partition_bound_strategy,
        backend="adbc",
    )
    result = df.sort("id").to_pydict()
    assert result["id"] == list(range(100))
    assert result["label"] == [f"row_{i}" for i in range(100)]


@pytest.mark.parametrize("num_partitions", [2, 3])
def test_sql_read_adbc_partitioned_null_partition_col(sqlite_null_partition_db, adbc_sqlite, num_partitions):
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        df = daft.read_sql(
            "SELECT * FROM null_table",
            sqlite_null_partition_db,
            partition_col="id",
            num_partitions=num_partitions,
            schema={"id": DataType.int64(), "value": DataType.float64(), "label": DataType.string()},
            backend="adbc",
        )
        result = df.to_pydict()

    assert len(result["value"]) == 50
    assert any("Falling back to a single scan task" in str(warning.message) for warning in w)


def test_sql_read_adbc_schema_hint(sqlite_db, adbc_sqlite):
    df = daft.read_sql(
        "SELECT * FROM test_table",
        sqlite_db,
        schema={"id": DataType.int32()},
        backend="adbc",
    )
    assert df.schema()["id"].dtype == DataType.int32()
    assert df.sort("id").to_pydict()["id"] == list(range(100))


def test_sql_read_adbc_matches_default_backend(sqlite_db, adbc_sqlite):
    query = "SELECT id, value * 2 AS doubled FROM test_table WHERE id < 10"
    expected = daft.read_sql(query, sqlite_db, backend="sqlalchemy").sort("id").to_pydict()
    actual = daft.read_sql(query, sqlite_db, backend="adbc").sort("id").to_pydict()
    assert actual == expected


def test_sql_connection_invalid_backend(sqlite_db):
    with pytest.raises(ValueError, match="Invalid SQL backend"):
        SQLConnection.from_url(sqlite_db, backend="odbc")


@pytest.mark.parametrize("backend", ["adbc", "connectorx"])
def test_sql_read_url_only_backend_rejects_connection_factory(sqlite_db, backend):
    def create_conn():
        return create_engine(sqlite_db).connect()

    with pytest.raises(ValueError, match="requires a database URL"):
        daft.read_sql("SELECT * FROM test_table", create_conn, backend=backend)


def test_sql_read_adbc_unsupported_dialect():
    with pytest.raises(ValueError, match="does not support the 'trino' dialect"):
        daft.read_sql("SELECT 1", "trino://user@localhost:8080/memory/default", backend="adbc")


def test_sql_read_connectorx_unsupported_dialect():
    with pytest.raises(ValueError, match="backend='connectorx' does not support"):
        daft.read_sql("SELECT 1", "trino://user@localhost:8080/memory/default", backend="connectorx")


def test_sql_read_adbc_missing_driver(sqlite_db, monkeypatch):
    import sys

    # A None entry in sys.modules makes the import raise ImportError, as if the driver were not installed.
    monkeypatch.setitem(sys.modules, "adbc_driver_sqlite.dbapi", None)
    with pytest.raises(ImportError, match="pip install adbc-driver-sqlite"):
        SQLConnection.from_url(sqlite_db, backend="adbc").execute_sql_query("SELECT 1")


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("sqlite:///relative.db", "relative.db"),
        ("sqlite:////abs/path.db", "/abs/path.db"),
        ("sqlite://", ":memory:"),
        ("postgresql://u:p@host:5432/db", "postgresql://u:p@host:5432/db"),
        ("postgres://u:p@host:5432/db", "postgresql://u:p@host:5432/db"),
        ("postgresql+psycopg2://u:p@host:5432/db?sslmode=require", "postgresql://u:p@host:5432/db?sslmode=require"),
    ],
)
def test_adbc_uri(url, expected):
    assert SQLConnection.from_url(url, backend="adbc")._adbc_uri() == expected


def test_sql_read_adbc_error_does_not_leak_credentials():
    pytest.importorskip("adbc_driver_postgresql")
    secret = "super-secret-pw"
    conn = SQLConnection.from_url(f"postgresql://alice:{secret}@127.0.0.1:1/nope", backend="adbc")
    with pytest.raises(RuntimeError) as exc_info:
        conn.execute_sql_query("SELECT 1")
    assert secret not in str(exc_info.value)
