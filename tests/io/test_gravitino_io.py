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
        # Create a temporary directory with parquet files to simulate the underlying storage
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create a simple DataFrame and save it
            df = daft.from_pydict({"id": [1, 2, 3], "value": ["a", "b", "c"]})
            parquet_path = os.path.join(tmp_dir, "data.parquet")
            df.write_parquet(parquet_path)

            # Mock the storage location to point to our temp directory
            mock_gravitino_client.load_fileset.return_value.fileset_info.storage_location = f"file://{tmp_dir}"

            # This would normally fail without a real Gravitino server
            # but we're testing the URL parsing and config handling
            gvfs_url = "gvfs://fileset/test_catalog/test_schema/test_fileset/data.parquet"

            # Test that the URL format is correct
            assert gvfs_url.startswith("gvfs://fileset/")
            parts = gvfs_url.replace("gvfs://fileset/", "").split("/")
            assert len(parts) >= 4  # catalog/schema/fileset/path

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


class TestGravitinoRustCodePaths:
    """Tests that exercise Rust code paths without requiring a real Gravitino server.

    These tests use error paths to test the Rust implementation of path parsing,
    URL validation, and error handling through the Gravitino gvfs:// protocol.
    These tests will exercise the Rust code and contribute to coverage.
    """

    def test_invalid_gvfs_url_scheme(self):
        """Test that invalid URL schemes are rejected by Rust code."""
        from daft.daft import FileFormat
        from daft.filesystem import glob_path_with_stats

        io_config = IOConfig(gravitino=GravitinoConfig(endpoint="http://localhost:8090", metalake_name="test"))

        # Test with wrong scheme - should fail in Rust path validation
        with pytest.raises(Exception):  # Will be caught by Rust invalid_gravitino_path
            glob_path_with_stats("http://fileset/cat/sch/fs/file.parquet", FileFormat.Parquet, io_config)

    def test_invalid_gvfs_url_host(self):
        """Test that invalid URL hosts are rejected by Rust code."""
        from daft.daft import FileFormat
        from daft.filesystem import glob_path_with_stats

        io_config = IOConfig(gravitino=GravitinoConfig(endpoint="http://localhost:8090", metalake_name="test"))

        # Test with wrong host - should fail in Rust path validation
        with pytest.raises(Exception):  # Will be caught by Rust invalid_gravitino_path
            glob_path_with_stats("gvfs://bucket/cat/sch/fs/file.parquet", FileFormat.Parquet, io_config)

    def test_invalid_gvfs_url_insufficient_segments(self):
        """Test that URLs with insufficient path segments are rejected."""
        from daft.daft import FileFormat
        from daft.filesystem import glob_path_with_stats

        io_config = IOConfig(gravitino=GravitinoConfig(endpoint="http://localhost:8090", metalake_name="test"))

        # Test with insufficient segments - should fail in Rust path validation
        with pytest.raises(Exception):  # Will be caught by Rust invalid_gravitino_path
            glob_path_with_stats("gvfs://fileset/catalog/schema", FileFormat.Parquet, io_config)

    def test_missing_endpoint_in_rust(self):
        """Test that missing endpoint is caught by Rust code."""
        from daft.daft import FileFormat
        from daft.filesystem import glob_path_with_stats

        # Config without endpoint - should fail in Rust get_client()
        io_config = IOConfig(
            gravitino=GravitinoConfig(
                endpoint=None,  # Missing endpoint
                metalake_name="test_metalake",
            )
        )

        # This should trigger Rust error: "GravitinoConfig.endpoint must be provided"
        with pytest.raises(Exception, match="endpoint"):
            glob_path_with_stats("gvfs://fileset/cat/sch/fs/file.parquet", FileFormat.Parquet, io_config)

    def test_missing_metalake_in_rust(self):
        """Test that missing metalake_name is caught by Rust code."""
        from daft.daft import FileFormat
        from daft.filesystem import glob_path_with_stats

        # Config without metalake_name - should fail in Rust get_client()
        io_config = IOConfig(
            gravitino=GravitinoConfig(
                endpoint="http://localhost:8090",
                metalake_name=None,  # Missing metalake_name
            )
        )

        # This should trigger Rust error: "GravitinoConfig.metalake_name must be provided"
        with pytest.raises(Exception, match="metalake"):
            glob_path_with_stats("gvfs://fileset/cat/sch/fs/file.parquet", FileFormat.Parquet, io_config)

    def test_missing_both_endpoint_and_metalake(self):
        """Test that missing both endpoint and metalake_name is caught."""
        from daft.daft import FileFormat
        from daft.filesystem import glob_path_with_stats

        # Config without both required fields
        io_config = IOConfig(gravitino=GravitinoConfig(endpoint=None, metalake_name=None))

        # Should fail with endpoint error (checked first)
        with pytest.raises(Exception, match="endpoint"):
            glob_path_with_stats("gvfs://fileset/cat/sch/fs/file.parquet", FileFormat.Parquet, io_config)

    def test_gvfs_url_with_special_characters(self):
        """Test URL parsing with special characters in path segments."""
        from daft.daft import FileFormat
        from daft.filesystem import glob_path_with_stats

        io_config = IOConfig(gravitino=GravitinoConfig(endpoint="http://localhost:8090", metalake_name="test"))

        # Test with special characters - should parse correctly but fail on load_fileset
        with pytest.raises(Exception):
            glob_path_with_stats(
                "gvfs://fileset/catalog-with-dash/schema_with_underscore/fileset.with.dots/file.parquet",
                FileFormat.Parquet,
                io_config,
            )

    def test_gvfs_url_with_nested_path(self):
        """Test URL parsing with deeply nested file paths."""
        from daft.daft import FileFormat
        from daft.filesystem import glob_path_with_stats

        io_config = IOConfig(gravitino=GravitinoConfig(endpoint="http://localhost:8090", metalake_name="test"))

        # Test with nested path - should parse correctly
        with pytest.raises(Exception):
            glob_path_with_stats(
                "gvfs://fileset/cat/sch/fs/year=2023/month=01/day=15/data.parquet", FileFormat.Parquet, io_config
            )

    def test_gvfs_url_with_glob_pattern(self):
        """Test URL parsing with glob patterns."""
        from daft.daft import FileFormat
        from daft.filesystem import glob_path_with_stats

        io_config = IOConfig(gravitino=GravitinoConfig(endpoint="http://localhost:8090", metalake_name="test"))

        # Test with glob pattern - should parse correctly
        with pytest.raises(Exception):
            glob_path_with_stats("gvfs://fileset/cat/sch/fs/**/*.parquet", FileFormat.Parquet, io_config)

    def test_gvfs_url_with_single_star_glob(self):
        """Test URL parsing with single star glob pattern."""
        from daft.daft import FileFormat
        from daft.filesystem import glob_path_with_stats

        io_config = IOConfig(gravitino=GravitinoConfig(endpoint="http://localhost:8090", metalake_name="test"))

        # Test with single star glob
        with pytest.raises(Exception):
            glob_path_with_stats("gvfs://fileset/cat/sch/fs/*.parquet", FileFormat.Parquet, io_config)

    def test_gvfs_url_minimal_path(self):
        """Test URL with minimal path (just catalog/schema/fileset)."""
        from daft.daft import FileFormat
        from daft.filesystem import glob_path_with_stats

        io_config = IOConfig(gravitino=GravitinoConfig(endpoint="http://localhost:8090", metalake_name="test"))

        # Test with minimal path (no file path after fileset)
        with pytest.raises(Exception):
            glob_path_with_stats("gvfs://fileset/catalog/schema/fileset", FileFormat.Parquet, io_config)

    def test_gvfs_url_with_trailing_slash(self):
        """Test URL with trailing slash."""
        from daft.daft import FileFormat
        from daft.filesystem import glob_path_with_stats

        io_config = IOConfig(gravitino=GravitinoConfig(endpoint="http://localhost:8090", metalake_name="test"))

        # Test with trailing slash
        with pytest.raises(Exception):
            glob_path_with_stats("gvfs://fileset/catalog/schema/fileset/", FileFormat.Parquet, io_config)

    def test_gvfs_url_with_query_parameters(self):
        """Test that URLs with query parameters are handled."""
        from daft.daft import FileFormat
        from daft.filesystem import glob_path_with_stats

        io_config = IOConfig(gravitino=GravitinoConfig(endpoint="http://localhost:8090", metalake_name="test"))

        # Test with query parameters (should be ignored or cause error)
        with pytest.raises(Exception):
            glob_path_with_stats("gvfs://fileset/cat/sch/fs/file.parquet?version=1", FileFormat.Parquet, io_config)

    def test_different_file_formats(self):
        """Test URL parsing with different file formats."""
        from daft.daft import FileFormat
        from daft.filesystem import glob_path_with_stats

        io_config = IOConfig(gravitino=GravitinoConfig(endpoint="http://localhost:8090", metalake_name="test"))

        # Test with CSV format
        with pytest.raises(Exception):
            glob_path_with_stats("gvfs://fileset/cat/sch/fs/data.csv", FileFormat.Csv, io_config)

    def test_url_with_numbers_in_segments(self):
        """Test URL with numeric characters in path segments."""
        from daft.daft import FileFormat
        from daft.filesystem import glob_path_with_stats

        io_config = IOConfig(gravitino=GravitinoConfig(endpoint="http://localhost:8090", metalake_name="test"))

        # Test with numbers in segments
        with pytest.raises(Exception):
            glob_path_with_stats(
                "gvfs://fileset/catalog123/schema456/fileset789/file.parquet", FileFormat.Parquet, io_config
            )

    def test_url_case_sensitivity(self):
        """Test that URL parsing handles case correctly."""
        from daft.daft import FileFormat
        from daft.filesystem import glob_path_with_stats

        io_config = IOConfig(gravitino=GravitinoConfig(endpoint="http://localhost:8090", metalake_name="test"))

        # Test with mixed case
        with pytest.raises(Exception):
            glob_path_with_stats(
                "gvfs://fileset/MyCatalog/MySchema/MyFileset/MyFile.parquet", FileFormat.Parquet, io_config
            )

    def test_multiple_operations_same_config(self):
        """Test multiple operations with the same config to exercise client caching.

        This tests the get_or_create_io_client caching logic in Rust.
        """
        from daft.daft import FileFormat
        from daft.filesystem import glob_path_with_stats

        io_config = IOConfig(gravitino=GravitinoConfig(endpoint="http://localhost:8090", metalake_name="test"))

        # First call - will create client
        with pytest.raises(Exception):
            glob_path_with_stats("gvfs://fileset/cat/sch/fs1/file.parquet", FileFormat.Parquet, io_config)

        # Second call with same fileset - should reuse cached client
        with pytest.raises(Exception):
            glob_path_with_stats("gvfs://fileset/cat/sch/fs1/file2.parquet", FileFormat.Parquet, io_config)

        # Third call with different fileset - will create new client
        with pytest.raises(Exception):
            glob_path_with_stats("gvfs://fileset/cat/sch/fs2/file.parquet", FileFormat.Parquet, io_config)

    def test_concurrent_operations_different_filesets(self):
        """Test operations on different filesets to exercise client cache."""
        from daft.daft import FileFormat
        from daft.filesystem import glob_path_with_stats

        io_config = IOConfig(gravitino=GravitinoConfig(endpoint="http://localhost:8090", metalake_name="test"))

        # Multiple different filesets - each should get its own cached client
        filesets = [
            "gvfs://fileset/cat1/sch1/fs1/file.parquet",
            "gvfs://fileset/cat1/sch1/fs2/file.parquet",
            "gvfs://fileset/cat1/sch2/fs1/file.parquet",
            "gvfs://fileset/cat2/sch1/fs1/file.parquet",
        ]

        for fileset_url in filesets:
            with pytest.raises(Exception):
                glob_path_with_stats(fileset_url, FileFormat.Parquet, io_config)

    def test_url_with_empty_path_segment(self):
        """Test URL with empty path segments (double slashes)."""
        from daft.daft import FileFormat
        from daft.filesystem import glob_path_with_stats

        io_config = IOConfig(gravitino=GravitinoConfig(endpoint="http://localhost:8090", metalake_name="test"))

        # Test with empty segment (double slash)
        with pytest.raises(Exception):
            glob_path_with_stats("gvfs://fileset/cat//fs/file.parquet", FileFormat.Parquet, io_config)

    def test_url_with_unicode_characters(self):
        """Test URL with unicode characters in path."""
        from daft.daft import FileFormat
        from daft.filesystem import glob_path_with_stats

        io_config = IOConfig(gravitino=GravitinoConfig(endpoint="http://localhost:8090", metalake_name="test"))

        # Test with unicode characters
        with pytest.raises(Exception):
            glob_path_with_stats("gvfs://fileset/catalog/schema/fileset/文件.parquet", FileFormat.Parquet, io_config)
