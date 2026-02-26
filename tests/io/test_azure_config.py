from __future__ import annotations

from daft.daft import AzureConfig, IOConfig


class TestAzureConfig:
    """Tests for AzureConfig class."""

    def test_azure_config_default(self):
        """Test default AzureConfig values."""
        config = AzureConfig()
        assert config.storage_account is None
        assert config.access_key is None
        assert config.sas_token is None
        assert config.bearer_token is None
        assert config.tenant_id is None
        assert config.client_id is None
        assert config.client_secret is None
        assert config.use_fabric_endpoint is False
        assert config.anonymous is False
        assert config.endpoint_url is None
        assert config.use_ssl is True
        assert config.max_connections == 8

    def test_azure_config_with_max_connections(self):
        """Test AzureConfig with custom max_connections."""
        config = AzureConfig(max_connections=16)
        assert config.max_connections == 16

    def test_azure_config_with_max_connections_zero(self):
        """Test AzureConfig accepts max_connections=0 (clamped to 1 at runtime)."""
        config = AzureConfig(max_connections=0)
        assert config.max_connections == 0

    def test_azure_config_with_all_params(self):
        """Test AzureConfig with all parameters set."""
        config = AzureConfig(
            storage_account="teststorage",
            access_key="test-key",
            sas_token="test-sas",
            bearer_token="test-bearer",
            tenant_id="test-tenant",
            client_id="test-client",
            client_secret="test-secret",
            use_fabric_endpoint=True,
            anonymous=True,
            endpoint_url="https://custom.endpoint.com",
            use_ssl=False,
            max_connections=32,
        )
        assert config.storage_account == "teststorage"
        assert config.access_key == "test-key"
        assert config.sas_token == "test-sas"
        assert config.bearer_token == "test-bearer"
        assert config.tenant_id == "test-tenant"
        assert config.client_id == "test-client"
        assert config.client_secret == "test-secret"
        assert config.use_fabric_endpoint is True
        assert config.anonymous is True
        assert config.endpoint_url == "https://custom.endpoint.com"
        assert config.use_ssl is False
        assert config.max_connections == 32

    def test_azure_config_replace(self):
        """Test AzureConfig replace method."""
        config = AzureConfig(
            storage_account="old-account",
            max_connections=8,
        )

        new_config = config.replace(
            storage_account="new-account",
            max_connections=16,
        )

        # Check the new config has updated values
        assert new_config.storage_account == "new-account"
        assert new_config.max_connections == 16

        # Original config should be unchanged
        assert config.storage_account == "old-account"
        assert config.max_connections == 8

    def test_azure_config_replace_preserves_unset(self):
        """Test that replace preserves values not explicitly replaced."""
        config = AzureConfig(
            storage_account="my-account",
            max_connections=32,
        )

        new_config = config.replace(storage_account="other-account")

        assert new_config.storage_account == "other-account"
        assert new_config.max_connections == 32

    def test_azure_config_repr(self):
        """Test AzureConfig __repr__ method."""
        config = AzureConfig(
            storage_account="teststorage",
            max_connections=16,
        )

        repr_str = repr(config)
        assert "AzureConfig" in repr_str
        assert "teststorage" in repr_str


class TestIOConfigWithAzure:
    """Tests for IOConfig with AzureConfig."""

    def test_io_config_with_azure(self):
        """Test IOConfig with AzureConfig."""
        azure_config = AzureConfig(
            storage_account="teststorage",
            max_connections=16,
        )
        io_config = IOConfig(azure=azure_config)

        assert io_config.azure.storage_account == "teststorage"
        assert io_config.azure.max_connections == 16

    def test_io_config_default_azure(self):
        """Test that IOConfig has default AzureConfig."""
        io_config = IOConfig()
        assert io_config.azure is not None
        assert io_config.azure.storage_account is None
        assert io_config.azure.max_connections == 8

    def test_io_config_replace_azure(self):
        """Test IOConfig replace method with AzureConfig."""
        io_config = IOConfig()
        new_azure = AzureConfig(storage_account="new-account", max_connections=32)
        new_io_config = io_config.replace(azure=new_azure)

        assert new_io_config.azure.storage_account == "new-account"
        assert new_io_config.azure.max_connections == 32
        # Original should be unchanged
        assert io_config.azure.storage_account is None
        assert io_config.azure.max_connections == 8
