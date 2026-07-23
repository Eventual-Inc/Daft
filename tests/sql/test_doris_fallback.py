"""Tests for Doris ConnectorX fallback (Issue #7199).

Covers three paths:
1. ConnectorX succeeds → SQLAlchemy not called
2. ConnectorX fails → fallback to SQLAlchemy succeeds
3. Both ConnectorX and SQLAlchemy fail → error preserves both causes
"""

from __future__ import annotations

import socket
from unittest.mock import MagicMock, patch

import pytest

from daft.sql.sql_connection import SQLConnection

# ── shared helpers ──────────────────────────────────────────────────────────


def _make_conn(url: str | None = None) -> SQLConnection:
    """Create a SQLConnection for a mysql:// URL (no driver → triggers ConnectorX)."""
    url = url or "mysql://user:pass@host:9030/db"
    return SQLConnection(url, "", "mysql", url)


# ── unit tests (mock) ───────────────────────────────────────────────────────


class TestConnectorXFallbackUnit:
    """Verify fallback logic without real database dependencies."""

    def test_connectorx_success_no_fallback(self):
        """When ConnectorX succeeds, SQLAlchemy should not be called."""
        conn = _make_conn()
        mock_cx = MagicMock()
        mock_cx.read_sql.return_value = MagicMock()

        with (
            patch.dict("sys.modules", {"connectorx": mock_cx}),
            patch.object(conn, "_execute_sql_query_with_sqlalchemy") as mock_sa,
        ):
            conn._execute_sql_query_with_connectorx("SELECT 1")

            mock_cx.read_sql.assert_called_once()
            mock_sa.assert_not_called()

    def test_connectorx_failure_fallback_to_sqlalchemy(self):
        """When ConnectorX fails, should fall back to SQLAlchemy."""
        conn = _make_conn()
        mock_cx = MagicMock()
        mock_cx.read_sql.side_effect = RuntimeError("COM_STMT_PREPARE not supported")

        with (
            patch.dict("sys.modules", {"connectorx": mock_cx}),
            patch.object(conn, "_execute_sql_query_with_sqlalchemy") as mock_sa,
        ):
            mock_sa.return_value = MagicMock()
            conn._execute_sql_query_with_connectorx("SELECT 1")

            mock_cx.read_sql.assert_called_once()
            mock_sa.assert_called_once_with("SELECT 1", _conn="mysql+pymysql://user:pass@host:9030/db")

    def test_both_fail_preserves_both_causes(self):
        """When both backends fail, the error must include both exception types and messages."""
        conn = _make_conn()
        mock_cx = MagicMock()
        mock_cx.read_sql.side_effect = RuntimeError("COM_STMT_PREPARE not supported")

        with (
            patch.dict("sys.modules", {"connectorx": mock_cx}),
            patch.object(conn, "_execute_sql_query_with_sqlalchemy") as mock_sa,
        ):
            mock_sa.side_effect = RuntimeError("SQLAlchemy connection failed")

            with pytest.raises(
                RuntimeError,
                match=r"ConnectorX.*COM_STMT_PREPARE.*SQLAlchemy.*SQLAlchemy connection failed",
            ):
                conn._execute_sql_query_with_connectorx("SELECT 1")

    def test_dual_failure_does_not_leak_credentials(self):
        """The raised RuntimeError must not echo the connection URL.

        Secrets can appear anywhere in a URL (userinfo, query params, driver
        extras), so the error message must strip the connection string.
        """
        secret = "super-secret-pw"
        url = f"mysql://alice:{secret}@doris.example.com:9030/db"
        conn = _make_conn(url)

        mock_cx = MagicMock()
        # Make the exception message include the full URL so that the
        # str.replace sanitization is actually exercised.
        mock_cx.read_sql.side_effect = RuntimeError(f"COM_STMT_PREPARE not supported for {url}")

        with (
            patch.dict("sys.modules", {"connectorx": mock_cx}),
            patch.object(conn, "_execute_sql_query_with_sqlalchemy") as mock_sa,
        ):
            mock_sa.side_effect = RuntimeError(f"SQLAlchemy connection failed to {url}")

            with pytest.raises(RuntimeError) as exc_info:
                conn._execute_sql_query_with_connectorx("SELECT 1")

            # The secret must not appear in the error message
            assert secret not in str(exc_info.value)
            # The host should also be redacted
            assert "doris.example.com" not in str(exc_info.value)

    def test_non_com_stmt_prepare_error_propagates(self):
        """Connection-level errors (auth, network) should NOT trigger fallback."""
        conn = _make_conn()
        mock_cx = MagicMock()
        mock_cx.read_sql.side_effect = RuntimeError("Access denied for user 'root'@'localhost'")

        with (
            patch.dict("sys.modules", {"connectorx": mock_cx}),
            patch.object(conn, "_execute_sql_query_with_sqlalchemy") as mock_sa,
        ):
            with pytest.raises(RuntimeError, match=r"Failed to execute sql: SELECT 1, error: RuntimeError"):
                conn._execute_sql_query_with_connectorx("SELECT 1")

            mock_sa.assert_not_called()

    def test_fallback_passes_rewritten_url(self):
        """Fallback passes mysql+pymysql:// as _conn to SQLAlchemy, self.conn unchanged."""
        url = "mysql://user:pass@host:9030/db"
        conn = _make_conn(url)
        mock_cx = MagicMock()
        mock_cx.read_sql.side_effect = RuntimeError("COM_STMT_PREPARE not supported")

        with (
            patch.dict("sys.modules", {"connectorx": mock_cx}),
            patch.object(conn, "_execute_sql_query_with_sqlalchemy") as mock_sa,
        ):
            mock_sa.return_value = MagicMock()
            conn._execute_sql_query_with_connectorx("SELECT 1")

            # self.conn must NOT be mutated.
            assert conn.conn == url
            # _conn override must be the rewritten mysql+pymysql:// URL.
            mock_sa.assert_called_once()
            assert mock_sa.call_args.kwargs["_conn"] == "mysql+pymysql://user:pass@host:9030/db"

    def test_read_schema_retries_after_null_types(self):
        """When LIMIT 0 produces null types after fallback, retries via SQLAlchemy directly."""
        import pyarrow as pa

        conn = _make_conn()
        mock_cx = MagicMock()
        mock_cx.read_sql.side_effect = RuntimeError("COM_STMT_PREPARE not supported")

        # First call (LIMIT 0 via fallback): returns null-typed schema.
        null_table = pa.table({"x": pa.array([], type=pa.null())})
        # Retry goes directly to _execute_sql_query_with_sqlalchemy → proper types.
        typed_table = pa.table({"x": pa.array([1, 2, 3], type=pa.int64())})

        with (
            patch.dict("sys.modules", {"connectorx": mock_cx}),
            patch.object(conn, "execute_sql_query") as mock_exec,
            patch.object(conn, "_execute_sql_query_with_sqlalchemy") as mock_sa,
        ):
            mock_exec.return_value = null_table
            mock_sa.return_value = typed_table

            schema = conn.read_schema("SELECT x FROM t", infer_schema_length=100)

        # First call: execute_sql_query (ConnectorX → fallback → LIMIT 0 → null).
        # Second call: _execute_sql_query_with_sqlalchemy directly (skips ConnectorX).
        assert mock_exec.call_count == 1
        mock_sa.assert_called_once()
        assert schema.to_pyarrow_schema().field("x").type == pa.int64()


# ── Docker integration test ─────────────────────────────────────────────────

_DORIS_HOST = "127.0.0.40"
_DORIS_PORT = 9032
_TEST_DB = "test_fallback"
_TEST_TABLE = "fallback_test"


def _doris_reachable() -> bool:
    try:
        s = socket.create_connection((_DORIS_HOST, _DORIS_PORT), timeout=2)
        s.close()
        return True
    except OSError:
        return False


@pytest.mark.integration
class TestDorisFallbackIntegration:
    """End-to-end test against a real Doris instance.

    Requires:
    - Doris Docker container reachable at 127.0.0.40:9032
    - PyMySQL installed (for SQLAlchemy fallback)
    """

    @pytest.fixture(scope="class")
    def doris_setup(self):
        """Create a test table in Doris and tear it down after the test."""
        if not _doris_reachable():
            pytest.skip("Doris Docker container not reachable at 127.0.0.40:9032")

        pytest.importorskip("pymysql")
        pytest.importorskip("sqlalchemy")

        import pymysql

        conn = pymysql.connect(host=_DORIS_HOST, port=_DORIS_PORT, user="root")
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {_TEST_DB}")
        cursor.execute(f"DROP TABLE IF EXISTS {_TEST_DB}.{_TEST_TABLE}")
        cursor.execute(
            f"CREATE TABLE {_TEST_DB}.{_TEST_TABLE} "
            "(id INT, value DOUBLE, label VARCHAR(50)) "
            "ENGINE=OLAP DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 "
            "PROPERTIES('replication_num' = '1')"
        )
        for i in range(20):
            cursor.execute(f"INSERT INTO {_TEST_DB}.{_TEST_TABLE} VALUES ({i}, {float(i)}, 'row_{i}')")
        conn.commit()
        conn.close()

        yield

        # Teardown
        conn = pymysql.connect(host=_DORIS_HOST, port=_DORIS_PORT, user="root")
        cursor = conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {_TEST_DB}.{_TEST_TABLE}")
        conn.commit()
        conn.close()

    def test_connectorx_failure_falls_back_to_sqlalchemy_doris(self, doris_setup):
        """ConnectorX fails on Doris (COM_STMT_PREPARE), SQLAlchemy fallback reads data.

        This is the critical end-to-end path: we mock ConnectorX to simulate
        the COM_STMT_PREPARE failure, then verify that data is correctly read
        through the SQLAlchemy fallback.
        """
        import daft

        url = f"mysql://root:@127.0.0.40:9032/{_TEST_DB}"
        mock_cx = MagicMock()
        mock_cx.read_sql.side_effect = RuntimeError(
            "MySqlError { ERROR 1064 (HY000): Unsupported command(COM_STMT_PREPARE) }"
        )

        with patch.dict("sys.modules", {"connectorx": mock_cx}):
            df = daft.read_sql(f"SELECT * FROM {_TEST_TABLE}", url)
            result = df.sort("id").to_pydict()

        assert result["id"] == list(range(20))
        assert result["value"] == [float(i) for i in range(20)]
        assert result["label"] == [f"row_{i}" for i in range(20)]
        mock_cx.read_sql.assert_called_once()
