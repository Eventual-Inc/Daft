from __future__ import annotations

from daft.daft import CosConfig, IOConfig


class TestCosConfig:
    """Tests for CosConfig class."""

    def test_cos_config_default(self):
        """Test default CosConfig values."""
        config = CosConfig()
        assert config.region is None
        assert config.endpoint is None
        assert config.secret_id is None
        assert config.secret_key is None
        assert config.security_token is None
        assert config.anonymous is False
        assert config.max_retries == 3
        assert config.retry_timeout_ms == 30000
        assert config.connect_timeout_ms == 10000
        assert config.read_timeout_ms == 30000
        assert config.max_concurrent_requests == 50
        assert config.max_connections == 50

    def test_cos_config_with_all_params(self):
        """Test CosConfig with all parameters set."""
        config = CosConfig(
            region="ap-guangzhou",
            endpoint="https://cos.ap-guangzhou.myqcloud.com",
            secret_id="test-secret-id",
            secret_key="test-secret-key",
            security_token="test-token",
            anonymous=False,
            max_retries=5,
            retry_timeout_ms=60000,
            connect_timeout_ms=20000,
            read_timeout_ms=60000,
            max_concurrent_requests=100,
            max_connections=100,
        )
        assert config.region == "ap-guangzhou"
        assert config.endpoint == "https://cos.ap-guangzhou.myqcloud.com"
        assert config.secret_id == "test-secret-id"
        assert config.secret_key == "test-secret-key"
        assert config.security_token == "test-token"
        assert config.anonymous is False
        assert config.max_retries == 5
        assert config.retry_timeout_ms == 60000
        assert config.connect_timeout_ms == 20000
        assert config.read_timeout_ms == 60000
        assert config.max_concurrent_requests == 100
        assert config.max_connections == 100

    def test_cos_config_replace(self):
        """Test CosConfig replace method."""
        config = CosConfig(
            region="ap-guangzhou",
            secret_id="old-id",
            secret_key="old-key",
        )

        new_config = config.replace(
            region="ap-beijing",
            secret_id="new-id",
        )

        # Check the new config has updated values
        assert new_config.region == "ap-beijing"
        assert new_config.secret_id == "new-id"
        # Replaced values should carry over
        assert new_config.secret_key == "old-key"

        # Original config should be unchanged
        assert config.region == "ap-guangzhou"
        assert config.secret_id == "old-id"

    def test_cos_config_replace_all_params(self):
        """Test CosConfig replace method with all parameters."""
        config = CosConfig()

        new_config = config.replace(
            region="ap-shanghai",
            endpoint="https://cos.ap-shanghai.myqcloud.com",
            secret_id="new-id",
            secret_key="new-key",
            security_token="new-token",
            anonymous=True,
            max_retries=10,
            retry_timeout_ms=45000,
            connect_timeout_ms=15000,
            read_timeout_ms=45000,
            max_concurrent_requests=200,
            max_connections=200,
        )

        assert new_config.region == "ap-shanghai"
        assert new_config.endpoint == "https://cos.ap-shanghai.myqcloud.com"
        assert new_config.secret_id == "new-id"
        assert new_config.secret_key == "new-key"
        assert new_config.security_token == "new-token"
        assert new_config.anonymous is True
        assert new_config.max_retries == 10
        assert new_config.retry_timeout_ms == 45000
        assert new_config.connect_timeout_ms == 15000
        assert new_config.read_timeout_ms == 45000
        assert new_config.max_concurrent_requests == 200
        assert new_config.max_connections == 200

    def test_cos_config_from_env(self, monkeypatch):
        """Test CosConfig from_env method."""
        # Set environment variables
        monkeypatch.setenv("COS_ENDPOINT", "https://cos.ap-guangzhou.myqcloud.com")
        monkeypatch.setenv("COS_REGION", "ap-guangzhou")
        monkeypatch.setenv("COS_SECRET_ID", "env-secret-id")
        monkeypatch.setenv("COS_SECRET_KEY", "env-secret-key")
        monkeypatch.setenv("COS_SECURITY_TOKEN", "env-token")

        config = CosConfig.from_env()

        assert config.endpoint == "https://cos.ap-guangzhou.myqcloud.com"
        assert config.region == "ap-guangzhou"
        assert config.secret_id == "env-secret-id"
        assert config.secret_key == "env-secret-key"
        assert config.security_token == "env-token"
        assert config.anonymous is False  # Should be False when secret_id is set

    def test_cos_config_from_env_tencentcloud_vars(self, monkeypatch):
        """Test CosConfig from_env with TENCENTCLOUD_ prefixed environment variables."""
        # Use TENCENTCLOUD_ prefixed variables
        monkeypatch.setenv("TENCENTCLOUD_REGION", "ap-beijing")
        monkeypatch.setenv("TENCENTCLOUD_SECRET_ID", "tc-secret-id")
        monkeypatch.setenv("TENCENTCLOUD_SECRET_KEY", "tc-secret-key")
        monkeypatch.setenv("TENCENTCLOUD_SECURITY_TOKEN", "tc-token")

        # Remove COS_ prefixed variables if they exist
        for var in ["COS_ENDPOINT", "COS_REGION", "COS_SECRET_ID", "COS_SECRET_KEY", "COS_SECURITY_TOKEN"]:
            monkeypatch.delenv(var, raising=False)

        config = CosConfig.from_env()

        assert config.region == "ap-beijing"
        assert config.secret_id == "tc-secret-id"
        assert config.secret_key == "tc-secret-key"
        assert config.security_token == "tc-token"

    def test_cos_config_from_env_anonymous(self, monkeypatch):
        """Test CosConfig from_env with anonymous access (no credentials)."""
        # Clear all COS related environment variables
        for var in [
            "COS_ENDPOINT",
            "COS_REGION",
            "COS_SECRET_ID",
            "COS_SECRET_KEY",
            "COS_SECURITY_TOKEN",
            "TENCENTCLOUD_REGION",
            "TENCENTCLOUD_SECRET_ID",
            "TENCENTCLOUD_SECRET_KEY",
            "TENCENTCLOUD_SECURITY_TOKEN",
        ]:
            monkeypatch.delenv(var, raising=False)

        config = CosConfig.from_env()

        # When no secret_id is provided, anonymous should be True
        assert config.anonymous is True

    def test_cos_config_repr(self):
        """Test CosConfig __repr__ method."""
        config = CosConfig(
            region="ap-guangzhou",
            endpoint="https://cos.ap-guangzhou.myqcloud.com",
            secret_id="test-id",
            secret_key="test-key",
        )

        repr_str = repr(config)
        assert "CosConfig" in repr_str
        assert "ap-guangzhou" in repr_str
        # Secret key should be masked
        assert "***" in repr_str

    def test_cos_config_anonymous_mode(self):
        """Test CosConfig with anonymous mode enabled."""
        config = CosConfig(anonymous=True)
        assert config.anonymous is True

    def test_cos_config_region_only(self):
        """Test CosConfig with only region specified (endpoint should be derived)."""
        config = CosConfig(region="ap-beijing")
        assert config.region == "ap-beijing"
        assert config.endpoint is None  # Endpoint will be derived at runtime


class TestIOConfigWithCos:
    """Tests for IOConfig with CosConfig."""

    def test_io_config_with_cos(self):
        """Test IOConfig with CosConfig."""
        cos_config = CosConfig(
            region="ap-guangzhou",
            secret_id="test-id",
            secret_key="test-key",
        )
        io_config = IOConfig(cos=cos_config)

        assert io_config.cos.region == "ap-guangzhou"
        assert io_config.cos.secret_id == "test-id"
        assert io_config.cos.secret_key == "test-key"

    def test_io_config_replace_cos(self):
        """Test IOConfig replace method with CosConfig."""
        io_config = IOConfig()
        new_cos = CosConfig(region="ap-shanghai")
        new_io_config = io_config.replace(cos=new_cos)

        assert new_io_config.cos.region == "ap-shanghai"
        # Original should be unchanged
        assert io_config.cos.region is None

    def test_io_config_default_cos(self):
        """Test that IOConfig has default CosConfig."""
        io_config = IOConfig()
        assert io_config.cos is not None
        assert io_config.cos.region is None
        assert io_config.cos.anonymous is False

    def test_io_config_hash_with_cos(self):
        """Test that IOConfig with CosConfig can be hashed."""
        cos_config = CosConfig(region="ap-guangzhou")
        io_config = IOConfig(cos=cos_config)

        # Should not raise
        hash_value = hash(io_config)
        assert isinstance(hash_value, int)

    def test_io_config_equality_with_cos(self):
        """Test IOConfig equality with CosConfig."""
        cos_config1 = CosConfig(region="ap-guangzhou")
        cos_config2 = CosConfig(region="ap-guangzhou")
        cos_config3 = CosConfig(region="ap-beijing")

        io_config1 = IOConfig(cos=cos_config1)
        io_config2 = IOConfig(cos=cos_config2)
        io_config3 = IOConfig(cos=cos_config3)

        # Same config should have same hash
        assert hash(io_config1) == hash(io_config2)
        # Different config should have different hash (with high probability)
        assert hash(io_config1) != hash(io_config3)
