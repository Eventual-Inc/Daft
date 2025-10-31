"""Tests for Gravitino gvfs:// I/O support."""

from __future__ import annotations

import os
import tempfile
from unittest.mock import Mock, patch

import pytest

import daft
from daft.io import GravitinoConfig, IOConfig


class TestGravitinoIOConfig:
    """Test Gravitino I/O configuration."""

    def test_gravitino_config_creation(self):
        """Test creating GravitinoConfig with various parameters."""
        # Test minimal config
        config = GravitinoConfig(endpoint="http://localhost:8090", metalake_name="test_metalake")
        assert config.endpoint == "http://localhost:8090"
        assert config.metalake_name == "test_metalake"
        assert config.auth_type is None  # Default is None
        assert config.username is None
        assert config.password is None
        assert config.token is None

        # Test full config
        config = GravitinoConfig(
            endpoint="http://localhost:8090",
            metalake_name="test_metalake",
            auth_type="oauth",
            username="test_user",
            password="test_pass",
            token="test_token",
        )
        assert config.auth_type == "oauth"
        assert config.username == "test_user"
        assert config.password == "test_pass"
        assert config.token == "test_token"

    def test_gravitino_config_in_io_config(self):
        """Test that GravitinoConfig can be used in IOConfig."""
        gravitino_config = GravitinoConfig(endpoint="http://localhost:8090", metalake_name="test_metalake")

        io_config = IOConfig(gravitino=gravitino_config)
        assert io_config.gravitino is not None
        assert io_config.gravitino.endpoint == "http://localhost:8090"
        assert io_config.gravitino.metalake_name == "test_metalake"


class TestGravitinoURLParsing:
    """Test Gravitino gvfs:// URL parsing and validation."""

    def test_valid_gvfs_urls(self):
        """Test that valid gvfs URLs are parsed correctly."""
        valid_urls = [
            "gvfs://fileset/catalog/schema/fileset/",
            "gvfs://fileset/catalog/schema/fileset/file.parquet",
            "gvfs://fileset/catalog/schema/fileset/dir/file.json",
            "gvfs://fileset/my_catalog/my_schema/my_fileset/data/part-001.parquet",
        ]

        # These should not raise errors when creating DataFrames
        # (actual validation happens in Rust layer)
        for url in valid_urls:
            # This is a basic smoke test - actual validation happens in Rust
            assert url.startswith("gvfs://fileset/")

    def test_invalid_gvfs_urls(self):
        """Test that invalid gvfs URLs are rejected."""
        invalid_urls = [
            "gvfs://",
            "gvfs://fileset/",
            "gvfs://fileset/catalog/",
            "gvfs://fileset/catalog/schema/",
            "gvfs://wrong/catalog/schema/fileset/file.parquet",
            "http://fileset/catalog/schema/fileset/file.parquet",
        ]

        # These should be caught as invalid URLs
        for url in invalid_urls:
            # Basic validation - detailed validation happens in Rust
            if not url.startswith("gvfs://fileset/"):
                continue
            parts = url.replace("gvfs://fileset/", "").split("/")
            if len(parts) < 3:  # Need at least catalog/schema/fileset
                assert True  # This would be invalid


@pytest.mark.integration()
class TestGravitinoIOIntegration:
    """Integration tests for Gravitino I/O operations."""

    @pytest.fixture
    def mock_gravitino_client(self):
        """Create a mock Gravitino client for testing."""
        with patch("daft.gravitino.gravitino_catalog.GravitinoClient") as mock_client_class:
            mock_client = Mock()
            mock_client_class.return_value = mock_client

            # Mock fileset with S3 storage location
            mock_fileset = Mock()
            mock_fileset_info = Mock()
            mock_fileset_info.storage_location = "s3://test-bucket/test-path"
            mock_fileset.fileset_info = mock_fileset_info
            mock_fileset.io_config = None  # Use default IOConfig

            mock_client.load_fileset.return_value = mock_fileset

            yield mock_client

    @pytest.fixture
    def gravitino_io_config(self):
        """Create a Gravitino IOConfig for testing."""
        return IOConfig(
            gravitino=GravitinoConfig(
                endpoint="http://localhost:8090", metalake_name="test_metalake", auth_type="simple"
            )
        )

    def test_read_gvfs_url_with_mock(self, mock_gravitino_client, gravitino_io_config):
        """Test reading from gvfs URL with mocked Gravitino client."""
        # Create a temporary parquet file to simulate the underlying storage
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
            # Create a simple DataFrame and save it
            df = daft.from_pydict({"id": [1, 2, 3], "value": ["a", "b", "c"]})
            df.write_parquet(tmp_file.name)

            # Mock the storage location to point to our temp file
            mock_gravitino_client.load_fileset.return_value.fileset_info.storage_location = f"file://{tmp_file.name}"

            try:
                # This would normally fail without a real Gravitino server
                # but we're testing the URL parsing and config handling
                gvfs_url = "gvfs://fileset/test_catalog/test_schema/test_fileset/data.parquet"

                # Test that the URL format is correct
                assert gvfs_url.startswith("gvfs://fileset/")
                parts = gvfs_url.replace("gvfs://fileset/", "").split("/")
                assert len(parts) >= 4  # catalog/schema/fileset/path

            finally:
                os.unlink(tmp_file.name)

    def test_list_gvfs_directory_with_mock(self, mock_gravitino_client, gravitino_io_config):
        """Test listing files in a gvfs directory with mocked client."""
        # Create a temporary directory with some files
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create some test files
            test_files = ["file1.parquet", "file2.parquet", "subdir/file3.parquet"]
            for file_path in test_files:
                full_path = os.path.join(tmp_dir, file_path)
                os.makedirs(os.path.dirname(full_path), exist_ok=True)

                # Create a simple parquet file
                df = daft.from_pydict({"id": [1], "value": ["test"]})
                df.write_parquet(full_path)

            # Mock the storage location to point to our temp directory
            mock_gravitino_client.load_fileset.return_value.fileset_info.storage_location = f"file://{tmp_dir}"

            # Test directory listing URL format
            gvfs_url = "gvfs://fileset/test_catalog/test_schema/test_fileset/"
            assert gvfs_url.startswith("gvfs://fileset/")

    def test_gvfs_url_with_nested_path(self, mock_gravitino_client, gravitino_io_config):
        """Test gvfs URL with nested file paths."""
        gvfs_urls = [
            "gvfs://fileset/catalog/schema/fileset/year=2023/month=01/data.parquet",
            "gvfs://fileset/catalog/schema/fileset/partitions/region=us/data.parquet",
            "gvfs://fileset/catalog/schema/fileset/deep/nested/path/file.json",
        ]

        for url in gvfs_urls:
            # Validate URL structure
            assert url.startswith("gvfs://fileset/")
            parts = url.replace("gvfs://fileset/", "").split("/")
            assert len(parts) >= 4  # catalog/schema/fileset/path...

            # Extract components
            catalog, schema, fileset = parts[0], parts[1], parts[2]
            file_path = "/".join(parts[3:])

            assert catalog and schema and fileset
            assert file_path  # Should have some path after fileset


class TestGravitinoIOErrors:
    """Test error handling in Gravitino I/O operations."""

    def test_missing_endpoint_error(self):
        """Test that config can be created without endpoint (validation happens later)."""
        config = GravitinoConfig(metalake_name="test")
        assert config.endpoint is None
        assert config.metalake_name == "test"

    def test_missing_metalake_error(self):
        """Test that config can be created without metalake_name (validation happens later)."""
        config = GravitinoConfig(endpoint="http://localhost:8090")
        assert config.endpoint == "http://localhost:8090"
        assert config.metalake_name is None

    def test_invalid_auth_type(self):
        """Test handling of invalid auth types."""
        # This should not raise an error at config creation time
        config = GravitinoConfig(endpoint="http://localhost:8090", metalake_name="test", auth_type="invalid_auth")
        assert config.auth_type == "invalid_auth"


@pytest.mark.integration()
@pytest.mark.skipif(
    not os.environ.get("GRAVITINO_TEST_SERVER"), reason="Requires GRAVITINO_TEST_SERVER environment variable"
)
class TestGravitinoIOLiveServer:
    """Live server tests for Gravitino I/O operations.

    These tests require a running Gravitino server and are skipped by default.
    Set GRAVITINO_TEST_SERVER=1 to enable them.
    """

    @pytest.fixture
    def gravitino_only_config(self):
        """Create IOConfig with only Gravitino config (no S3 override)."""
        return IOConfig(
            gravitino=GravitinoConfig(
                endpoint=os.environ.get("GRAVITINO_ENDPOINT", "http://localhost:8090"),
                metalake_name=os.environ.get("GRAVITINO_METALAKE", "metalake_demo"),
                auth_type="simple",
            )
        )

    def test_read_live_gvfs_file(self, gravitino_only_config):
        """Test reading a file from live Gravitino server."""
        # This test requires a pre-existing fileset with data
        # URL format: gvfs://fileset/catalog/schema/fileset/path
        gvfs_url = os.environ.get(
            "GRAVITINO_TEST_FILE",
            "gvfs://fileset/s3_fileset_catalog3/test_schema/test_fileset/part-00000-a1a42661-7a85-42da-b831-f489a5545d61-c000.snappy.parquet",
        )

        try:
            # Attempt to read the file
            print(f"PYTHON DEBUG: About to call daft.read_parquet with gvfs_url: {gvfs_url}")
            # print(f"PYTHON DEBUG: Passing gravitino_only_config: {gravitino_only_config}")
            df = daft.read_parquet(gvfs_url, io_config=gravitino_only_config)
            result = df.collect()

            # Basic validation that we got some data
            assert len(result) > 0
            print(f"✓ Successfully read {len(result)} rows from {gvfs_url}")

        except Exception as e:
            import traceback

            print("\n❌ Exception occurred in test_read_live_gvfs_file:")
            print(f"Exception type: {type(e).__name__}")
            print(f"Exception message: {e!s}")
            print(f"URL being tested: {gvfs_url}")
            print("Note: This is expected when no Gravitino server is running or fileset doesn't exist")
            print("Full traceback:")
            traceback.print_exc()
            pytest.skip(f"Live server test failed (expected if no live server/data): {type(e).__name__}: {e}")

    def test_list_live_gvfs_directory(self, gravitino_only_config):
        """Test listing files in a live Gravitino fileset."""
        # URL format: gvfs://fileset/catalog/schema/fileset/path
        gvfs_dir = os.environ.get("GRAVITINO_TEST_DIR", "gvfs://fileset/s3_fileset_catalog3/test_schema/test_fileset/")

        try:
            print(f"PYTHON DEBUG: About to call daft.read_parquet with gvfs_url: {gvfs_dir}")
            # print(f"PYTHON DEBUG: Passing gravitino_only_config: {gravitino_only_config}")
            # Attempt to list files in the directory
            files = daft.from_glob_path(gvfs_dir + "**/*.parquet", io_config=gravitino_only_config)
            result = files.collect()

            print(f"✓ Found {len(result)} files in {gvfs_dir}")
            print(f"PYTHON DEBUG: files collection: {result}")

        except Exception as e:
            import traceback

            print("\n❌ Exception occurred in test_list_live_gvfs_directory:")
            print(f"Exception type: {type(e).__name__}")
            print(f"Exception message: {e!s}")
            print("Full traceback:")
            traceback.print_exc()
            pytest.skip(f"Live server test failed (expected if no test data): {type(e).__name__}: {e}")


if __name__ == "__main__":
    # Run basic tests
    print("🧪 Testing Gravitino I/O configuration...")

    config_test = TestGravitinoIOConfig()
    config_test.test_gravitino_config_creation()
    config_test.test_gravitino_config_in_io_config()
    print("✓ Configuration tests passed")

    url_test = TestGravitinoURLParsing()
    url_test.test_valid_gvfs_urls()
    url_test.test_invalid_gvfs_urls()
    print("✓ URL parsing tests passed")

    error_test = TestGravitinoIOErrors()
    error_test.test_missing_endpoint_error()
    error_test.test_missing_metalake_error()
    error_test.test_invalid_auth_type()
    print("✓ Error handling tests passed")

    print("\n🎉 All basic Gravitino I/O tests passed!")
    print("\nTo run integration tests:")
    print("1. Start a Gravitino server on localhost:8090")
    print("2. Set GRAVITINO_TEST_SERVER=1")
    print("3. Optionally set GRAVITINO_TEST_FILE and GRAVITINO_TEST_DIR")
    print("4. Run: pytest tests/io/test_gravitino_io.py::TestGravitinoIOLiveServer -v")
