from __future__ import annotations

import warnings

import pytest
from sqlalchemy import Column, Float, Integer, MetaData, String, Table, create_engine, insert

import daft
from daft.datatype import DataType
from daft.sql.sql_connection import SQLConnection, _redact_text, _redact_url


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
        # Empty username — SQLAlchemy preserves the ':' delimiter.
        ("mysql+pymysql://:secret@host/db", "mysql+pymysql://:***@host/db"),
        # No password — should be returned unchanged.
        ("sqlite:///my.db", "sqlite:///my.db"),
        ("mysql://user@host/db", "mysql://user@host/db"),
        ("trino://host:443", "trino://host:443"),
        ("not a url", "not a url"),
        # Non-numeric port — parsed.port raises ValueError; must not propagate
        # and must not leak the password.
        ("trino://alice:hunter2@host:badport/db", "<redacted>"),
        # Unescaped URL-special characters in the password: urllib.parse drops
        # them into the fragment / path / query and reports password=None, so
        # the previous implementation returned the URL unchanged — leaking the
        # password. SQLAlchemy's make_url parses these correctly.
        ("trino://alice:p#ss@host:443/db", "trino://alice:***@host:443/db"),
        ("trino://alice:p/ss@host:443/db", "trino://alice:***@host:443/db"),
        ("trino://alice:p?ss@host:443/db", "trino://alice:***@host:443/db"),
    ],
)
def test_redact_url(url, expected):
    assert _redact_url(url) == expected


@pytest.mark.parametrize(
    "password",
    [
        "simple",
        "p@ss",
        "p:ss",
        "p#ss",
        "p/ss",
        "p?ss",
        "p&ss",
        "!@#$%^&*()",
        "with space",
    ],
)
def test_redact_url_does_not_leak_for_any_password(password):
    """Regardless of URL-special chars in the password, redaction must not leak it.

    Covers `#`, `/`, `?`, etc. — characters that previously caused urllib.parse
    to silently mis-parse the URL and skip redaction.
    """
    url = f"trino://alice:{password}@host:443/db"
    redacted = _redact_url(url)
    assert password not in redacted, f"password leaked in redacted URL: {redacted!r}"


@pytest.mark.parametrize(
    "param_name",
    [
        "access_token",
        "auth_token",
        "token",
        "password",
        "pwd",
        "secret",
        "client_secret",
        "passphrase",
        "private_key_passphrase",
        "apikey",
        "api_key",
        "credentials",
        # Case-insensitive matching:
        "Access_Token",
        "PASSWORD",
    ],
)
def test_redact_url_redacts_sensitive_query_params(param_name):
    """Sensitive query-parameter values must be redacted, not just userinfo passwords.

    Drivers like Trino (JWT), Snowflake (key auth), Databricks (PAT), and
    generic API-key based connectors pass credentials as query parameters,
    so leaving these in error output leaks credentials.
    """
    secret = "SUPER_SECRET_VALUE_DO_NOT_LEAK"
    url = f"trino://alice@host:443?auth=jwt&{param_name}={secret}&http_scheme=https"
    redacted = _redact_url(url)
    assert secret not in redacted, f"{param_name} value leaked in: {redacted!r}"
    # Non-sensitive params should be preserved.
    assert "auth=jwt" in redacted
    assert "http_scheme=https" in redacted


def test_redact_url_fallback_when_sqlalchemy_unavailable(monkeypatch):
    """Force the urllib.parse fallback by simulating SQLAlchemy being absent.

    The fallback path is what runs in connectorx-only environments. Pin down
    that it behaves consistently with the primary path for the safe cases
    (no-password user-only URLs pass through unchanged) and still redacts
    the leak vectors (sensitive query params, special-char passwords).
    """
    from daft.sql import sql_connection as sc

    monkeypatch.setattr(sc, "_sa_make_url", None)

    # No-password user-only URL: must NOT over-redact (Greptile-flagged divergence).
    assert _redact_url("mysql://user@host/db") == "mysql://user@host/db"
    # Standard userinfo password — fallback redacts.
    assert _redact_url("postgresql://user:hunter2@host:5432/db") == "postgresql://user:***@host:5432/db"
    # Sensitive query param — fallback redacts.
    redacted = _redact_url("trino://alice@host:443?access_token=SECRET&http_scheme=https")
    assert "SECRET" not in redacted
    assert "access_token=***" in redacted
    # Special-char password — urlparse loses the password into the fragment;
    # over-redact since we can't safely reconstruct.
    assert _redact_url("trino://alice:p#ss@host:443/db") == "<redacted>"


def test_redact_url_customer_jwt_repro():
    """Customer-reported leak shape: JWT in `access_token=` query parameter, no userinfo password.

    The previous fix only handled `user:password@host` style URLs and left
    the access_token in plaintext in the raised RuntimeError.
    """
    secret = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.SECRET_PAYLOAD.SIGNATURE"
    url = f"trino://alice@trino.example.com:443?auth=jwt&access_token={secret}&http_scheme=https"
    redacted = _redact_url(url)
    assert secret not in redacted
    assert "access_token=***" in redacted


@pytest.mark.parametrize(
    ("text", "must_not_contain"),
    [
        # URL query param style
        ("?access_token=SUPER_SECRET&http_scheme=https", "SUPER_SECRET"),
        ("trino://alice@host:443?access_token=SUPER_SECRET&auth=jwt", "SUPER_SECRET"),
        # JDBC ;-separated property style
        ("jdbc:trino://host:443;user=alice;password=SUPER_SECRET;ssl=true", "SUPER_SECRET"),
        # name=value within driver error text
        ("could not connect: password=SUPER_SECRET host=foo", "SUPER_SECRET"),
        # Colon separator (common in dict-like reprs)
        ("kwargs: password: SUPER_SECRET, host: foo", "SUPER_SECRET"),
        # Case-insensitive name match
        ("?Access_Token=SUPER_SECRET", "SUPER_SECRET"),
        ("?PASSWORD=SUPER_SECRET", "SUPER_SECRET"),
        # Variant names
        ("?api_key=SUPER_SECRET", "SUPER_SECRET"),
        ("?apikey=SUPER_SECRET", "SUPER_SECRET"),
        ("?client_secret=SUPER_SECRET", "SUPER_SECRET"),
        ("?private_key_passphrase=SUPER_SECRET", "SUPER_SECRET"),
        ("?signature=SUPER_SECRET", "SUPER_SECRET"),
        ("?aws_access_key_id=SUPER_SECRET", "SUPER_SECRET"),
    ],
)
def test_redact_text_removes_secret(text, must_not_contain):
    """`_redact_text` redacts sensitive name=value pairs in arbitrary text.

    The defense-in-depth backstop must catch credentials wherever they
    appear in the wrapped error message — URL query params, JDBC-style
    `;`-separated property strings, dict-like reprs, etc. — not just in
    the URL field we explicitly pass through `_redact_url`.
    """
    redacted = _redact_text(text)
    assert must_not_contain not in redacted, f"secret leaked in: {redacted!r}"
    assert "***" in redacted


@pytest.mark.parametrize(
    "text",
    [
        "auth=jwt",  # auth is a scheme, not a secret
        "host=trino.example.com",
        "scheme=https",
        "port=443",
        "user=alice",
        "ssl=true",
        "[SQL: SELECT * FROM t WHERE k = 'v']",  # SQL embedded — must not get clobbered
    ],
)
def test_redact_text_preserves_non_sensitive(text):
    assert _redact_text(text) == text


def test_execute_sql_error_does_not_leak_credentials():
    """Connection failures must not include the connection URL in the raised error.

    Secrets can appear anywhere in a URL (userinfo, query params, driver
    extras), so the URL is now omitted from the error message entirely.
    """
    pytest.importorskip("psycopg2")
    password = "super-secret-pw"
    url = f"postgresql+psycopg2://alice:{password}@127.0.0.1:1/nope"
    conn = SQLConnection.from_url(url)

    with pytest.raises(RuntimeError) as exc_info:
        conn.execute_sql_query("SELECT 1")

    message = str(exc_info.value)
    assert password not in message
    # The URL itself is not echoed in the error message.
    assert "127.0.0.1" not in message or "from connection:" not in message
    assert "alice" not in message


def test_repr_does_not_leak_password():
    password = "super-secret-pw"
    conn = SQLConnection.from_url(f"postgresql://alice:{password}@host:5432/db")
    assert password not in repr(conn)
