"""Tests for SQLConnection engine caching and mysql:// rewrite."""

from __future__ import annotations

import builtins
import pickle
import threading
from unittest.mock import MagicMock, patch

from daft.sql.sql_connection import SQLConnection


class TestSQLConnectionEngineCaching:
    """Test that SQLAlchemy Engine is cached."""

    def test_engine_cached_on_first_call(self):
        """Engine should be created once and reused."""
        conn = SQLConnection("sqlite:///test.db", "", "sqlite", "sqlite:///test.db")
        assert conn._engine is None

        with patch("sqlalchemy.create_engine") as mock_create:
            mock_engine = MagicMock()
            mock_create.return_value = mock_engine

            engine1 = conn._get_or_create_engine()
            engine2 = conn._get_or_create_engine()

            assert engine1 is mock_engine
            assert engine2 is mock_engine
            mock_create.assert_called_once()

    def test_engine_not_created_for_callable(self):
        """Engine should not be created when conn is a callable."""
        conn_factory = MagicMock()
        conn = SQLConnection(conn_factory, "", "sqlite", "sqlite:///test.db")
        assert conn._get_or_create_engine() is None

    def test_engine_excluded_from_pickle(self):
        """Engine must be dropped during pickle to avoid serialization failures on distributed runners."""
        conn = SQLConnection("sqlite://", "", "sqlite", "sqlite://")
        conn._get_or_create_engine()  # populate live engine
        assert conn._engine is not None

        restored = pickle.loads(pickle.dumps(conn))
        assert restored._engine is None  # dropped, will be lazily recreated
        assert restored.conn == "sqlite://"
        assert restored.dialect == "sqlite"
        assert restored.driver == ""
        assert restored.url == "sqlite://"

    def test_engine_thread_safety(self):
        """Concurrent calls should not create multiple engines."""
        conn = SQLConnection("sqlite://", "", "sqlite", "sqlite://")
        engines = []

        def get_engine():
            engines.append(conn._get_or_create_engine())

        threads = [threading.Thread(target=get_engine) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert all(e is engines[0] for e in engines), "Thread safety failed: different engines returned"


class TestMySQLPyMySQLRewrite:
    """Test mysql:// to mysql+pymysql:// rewrite."""

    def test_no_rewrite_when_not_mysql(self):
        """Non-mysql URLs should not be rewritten."""
        conn = SQLConnection("postgresql://user:pass@host/db", "", "postgres", "postgresql://user:pass@host/db")

        with patch("sqlalchemy.create_engine") as mock_create:
            mock_create.return_value = MagicMock()
            conn._get_or_create_engine()
            call_args = mock_create.call_args[0][0]
            assert call_args == "postgresql://user:pass@host/db"

    def test_no_rewrite_when_mysql_with_driver(self):
        """mysql+pymysql:// should not be rewritten again."""
        conn = SQLConnection("mysql+pymysql://user:pass@host/db", "", "mysql", "mysql+pymysql://user:pass@host/db")

        with patch("sqlalchemy.create_engine") as mock_create:
            mock_create.return_value = MagicMock()
            conn._get_or_create_engine()
            call_args = mock_create.call_args[0][0]
            assert call_args == "mysql+pymysql://user:pass@host/db"

    def test_mysql_rewritten_when_mysqldb_absent(self):
        """mysql:// should be rewritten to mysql+pymysql:// when MySQLdb is not available."""
        conn = SQLConnection("mysql://user:pass@host/db", "", "mysql", "mysql://user:pass@host/db")

        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "MySQLdb":
                raise ImportError("No module named 'MySQLdb'")
            return real_import(name, *args, **kwargs)

        with (
            patch("sqlalchemy.create_engine") as mock_create,
            patch.object(builtins, "__import__", side_effect=mock_import),
        ):
            mock_create.return_value = MagicMock()
            conn._get_or_create_engine()
            call_args = mock_create.call_args[0][0]
            assert call_args == "mysql+pymysql://user:pass@host/db"
