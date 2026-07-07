"""Tests for Gravitino client error handling (Issue #7200)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from daft.catalog.__gravitino._catalog import GravitinoCatalog
from daft.catalog.__gravitino._client import _io_config_from_storage_location
from daft.dependencies import requests


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
    """Test that _has_table only catches not-found errors.

    These tests mock ``_inner.load_table`` directly rather than going through
    ``GravitinoClient.load_table``. In production, ``GravitinoClient.load_table``
    wraps all errors (including ConnectionError and HTTPError) into a plain
    ``Exception``, so the actual exception reaching ``_has_table`` is always
    ``Exception("Failed to load table ...")`` — never a typed error. Mocking at
    the inner layer keeps the test focused on ``_has_table``'s own branching
    logic without depending on the client's error-wrapping behavior.
    """

    def test_table_not_found_returns_false(self):
        """404 errors should return False."""
        mock_inner = MagicMock()
        mock_inner.load_table.side_effect = Exception("Table not found")

        catalog = GravitinoCatalog.__new__(GravitinoCatalog)
        catalog._inner = mock_inner

        ident = MagicMock()
        ident.__str__ = lambda self: "catalog.schema.table"

        result = catalog._has_table(ident)
        assert result is False

    def test_network_error_raises(self):
        """Network errors should be re-raised, not swallowed."""
        mock_inner = MagicMock()
        mock_inner.load_table.side_effect = ConnectionError("Network timeout")

        catalog = GravitinoCatalog.__new__(GravitinoCatalog)
        catalog._inner = mock_inner

        ident = MagicMock()
        ident.__str__ = lambda self: "catalog.schema.table"

        with pytest.raises(ConnectionError, match="Network timeout"):
            catalog._has_table(ident)

    def test_auth_error_raises(self):
        """Authentication errors should be re-raised."""
        mock_inner = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_inner.load_table.side_effect = requests.exceptions.HTTPError("401 Unauthorized", response=mock_response)

        catalog = GravitinoCatalog.__new__(GravitinoCatalog)
        catalog._inner = mock_inner

        ident = MagicMock()
        ident.__str__ = lambda self: "catalog.schema.table"

        with pytest.raises(requests.exceptions.HTTPError):
            catalog._has_table(ident)
