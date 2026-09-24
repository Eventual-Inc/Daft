"""Tests for Gravitino client error handling (Issue #7200)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from daft.catalog.__gravitino._catalog import GravitinoCatalog
from daft.catalog.__gravitino._client import GravitinoTableNotFoundError, _io_config_from_storage_location


class TestS3CredentialKeyFormats:
    """Test that both hyphen and dot notation S3 keys are recognized."""

    def test_hyphen_notation(self):
        """Fileset Catalog uses hyphen notation."""
        properties = {
            "s3-access-key-id": "AKIAIOSFODNN7EXAMPLE",
            "s3-secret-access-key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "s3-endpoint": "https://s3.amazonaws.com",
            "s3-region": "us-east-1",
        }
        config = _io_config_from_storage_location("s3://bucket/path", properties)
        assert config is not None
        assert config.s3 is not None
        assert config.s3.key_id == "AKIAIOSFODNN7EXAMPLE"
        assert config.s3.access_key == "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        assert config.s3.region_name == "us-east-1"

    def test_dot_notation(self):
        """Iceberg Catalog uses dot notation."""
        properties = {
            "s3.access-key-id": "AKIAIOSFODNN7EXAMPLE",
            "s3.secret-access-key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "s3.endpoint": "https://s3.amazonaws.com",
            "s3.region": "eu-west-1",
        }
        config = _io_config_from_storage_location("s3://bucket/path", properties)
        assert config is not None
        assert config.s3 is not None
        assert config.s3.key_id == "AKIAIOSFODNN7EXAMPLE"
        assert config.s3.access_key == "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        assert config.s3.region_name == "eu-west-1"

    def test_hyphen_takes_precedence(self):
        """When both formats exist, hyphen notation takes precedence."""
        properties = {
            "s3-access-key-id": "HYPHEN_KEY",
            "s3.secret-access-key": "DOT_SECRET",
        }
        config = _io_config_from_storage_location("s3://bucket/path", properties)
        assert config is not None
        assert config.s3 is not None
        assert config.s3.key_id == "HYPHEN_KEY"

    def test_no_credentials_returns_none(self):
        """When no S3 credentials are found, should return None."""
        properties = {"some-other-key": "value"}
        config = _io_config_from_storage_location("s3://bucket/path", properties)
        assert config is None

    def test_s3a_scheme(self):
        """s3a scheme should also work."""
        properties = {
            "s3-access-key-id": "KEY",
            "s3-secret-access-key": "SECRET",
        }
        config = _io_config_from_storage_location("s3a://bucket/path", properties)
        assert config is not None
        assert config.s3 is not None


class TestHasTableErrorHandling:
    """Test that _has_table only catches GravitinoTableNotFoundError.

    These tests mock ``_inner.load_table`` directly rather than going through
    ``GravitinoClient.load_table``. This keeps the test focused on
    ``_has_table``'s own branching logic: it should return ``False`` for
    ``GravitinoTableNotFoundError`` and re-raise everything else.
    """

    def test_table_not_found_returns_false(self):
        """GravitinoTableNotFoundError should return False."""
        mock_inner = MagicMock()
        mock_inner.load_table.side_effect = GravitinoTableNotFoundError("Table cat.schema.nonexistent not found")

        catalog = GravitinoCatalog.__new__(GravitinoCatalog)
        catalog._inner = mock_inner

        ident = MagicMock()
        ident.__str__ = lambda self: "catalog.schema.table"

        result = catalog._has_table(ident)
        assert result is False

    def test_non_notfound_error_raises(self):
        """Any exception not GravitinoTableNotFoundError should propagate.

        In production, GravitinoClient.load_table wraps all non-404 errors
        (network timeouts, auth failures, server errors) into
        ``Exception("Failed to load table ...")``. _has_table should let these
        through because they represent real problems, not missing tables.
        """
        mock_inner = MagicMock()
        mock_inner.load_table.side_effect = Exception("Failed to load table cat.schema.tbl: ConnectionError")

        catalog = GravitinoCatalog.__new__(GravitinoCatalog)
        catalog._inner = mock_inner

        ident = MagicMock()
        ident.__str__ = lambda self: "catalog.schema.table"

        with pytest.raises(Exception, match="Failed to load table"):
            catalog._has_table(ident)
