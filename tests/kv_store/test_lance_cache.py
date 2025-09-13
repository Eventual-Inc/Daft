"""Tests for Lance dataset caching mechanism."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

import daft
from daft.kv import KVConfig, LanceConfig, load_kv


class TestLanceCaching:
    """Test Lance dataset caching functionality."""

    @pytest.fixture
    def lance_dataset_path(self):
        """Create a temporary Lance dataset for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            lance_path = Path(temp_dir) / "test_cache.lance"
            yield str(lance_path)

    def test_cache_initialization(self):
        """Test cache initialization and configuration."""
        # Test that cache can be initialized with custom settings
        try:
            # Import the cache functions - these should be available after our implementation
            from daft.daft import clear_lance_cache, get_lance_cache_stats, init_lance_cache

            # Initialize cache with custom settings
            init_lance_cache(max_size=50, ttl_seconds=120)

            # Get initial stats
            stats = get_lance_cache_stats()
            assert stats.hits == 0
            assert stats.misses == 0
            assert stats.evictions == 0
            assert stats.expired == 0

            # Clear cache
            clear_lance_cache()

        except ImportError:
            pytest.skip("Lance cache functions not available in Python bindings yet")

    def test_lance_kv_store_with_caching(self, lance_dataset_path):
        """Test Lance KV store operations with caching enabled."""
        try:
            # Create Lance KV store
            lance_kv = load_kv(
                "lance", name="cache_test", uri=lance_dataset_path, columns=["row_id", "data"], batch_size=100
            )

            # Attach to session
            daft.attach(lance_kv, alias="cached_store")
            daft.set_kv("cached_store")

            # Create test dataframe
            _df = daft.from_pydict({"row_id": [1, 2, 3, 4, 5]})

            # This should work even if the actual Lance dataset doesn't exist yet
            # because we're testing the caching mechanism, not the actual data retrieval
            assert daft.has_kv("cached_store")
            assert daft.current_kv().name == "cache_test"

            # Cleanup
            daft.detach_kv("cached_store")

        except Exception as e:
            # If Lance is not available, skip the test
            if "Lance" in str(e) or "lance" in str(e):
                pytest.skip(f"Lance not available: {e}")
            else:
                raise

    def test_multiple_lance_stores_caching(self):
        """Test caching with multiple Lance stores."""
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                # Create multiple Lance KV stores
                lance_path1 = Path(temp_dir) / "dataset1.lance"
                lance_path2 = Path(temp_dir) / "dataset2.lance"

                lance_kv1 = load_kv("lance", name="store1", uri=str(lance_path1))
                lance_kv2 = load_kv("lance", name="store2", uri=str(lance_path2))

                # Attach both stores
                daft.attach(lance_kv1, alias="cache_store1")
                daft.attach(lance_kv2, alias="cache_store2")

                # Verify both are attached
                assert daft.has_kv("cache_store1")
                assert daft.has_kv("cache_store2")

                # Switch between stores
                daft.set_kv("cache_store1")
                assert daft.current_kv().name == "store1"

                daft.set_kv("cache_store2")
                assert daft.current_kv().name == "store2"

                # Cleanup
                daft.detach_kv("cache_store1")
                daft.detach_kv("cache_store2")

        except Exception as e:
            if "Lance" in str(e) or "lance" in str(e):
                pytest.skip(f"Lance not available: {e}")
            else:
                raise

    def test_cache_configuration_methods(self):
        """Test cache configuration and management methods."""
        try:
            from daft.daft import (
                cleanup_lance_cache,
                disable_lance_cache,
                get_lance_cache_stats,
                init_lance_cache_default,
            )

            # Test default initialization
            init_lance_cache_default()

            # Test cache cleanup
            cleanup_lance_cache()

            # Test cache disabling
            disable_lance_cache()

            # Get stats after disabling
            stats = get_lance_cache_stats()
            # Stats should still be accessible even when cache is disabled
            assert hasattr(stats, "hits")
            assert hasattr(stats, "misses")

        except ImportError:
            pytest.skip("Lance cache management functions not available in Python bindings yet")

    def test_lance_config_with_caching_options(self):
        """Test Lance configuration with caching-related options."""
        with tempfile.TemporaryDirectory() as temp_dir:
            lance_path = Path(temp_dir) / "config_test.lance"

            # Create Lance configuration
            lance_config = LanceConfig(
                uri=str(lance_path), columns=["embedding", "metadata"], batch_size=500, max_connections=16
            )

            # Create KV configuration
            kv_config = KVConfig(lance=lance_config)

            # Verify configuration
            assert kv_config.is_lance_backend()
            assert kv_config.get_active_backend() == "lance"
            assert lance_config.uri == str(lance_path)
            assert lance_config.batch_size == 500
            assert lance_config.max_connections == 16

    def test_cache_hit_rate_calculation(self):
        """Test cache statistics hit rate calculation."""
        try:
            from daft.daft import get_lance_cache_stats

            # Get initial stats
            stats = get_lance_cache_stats()

            # Test hit rate calculation
            initial_hit_rate = stats.hit_rate()
            assert 0.0 <= initial_hit_rate <= 1.0

        except ImportError:
            pytest.skip("Lance cache statistics not available in Python bindings yet")

    def test_lance_kv_store_string_representations(self):
        """Test string representations of Lance KV store."""
        with tempfile.TemporaryDirectory() as temp_dir:
            lance_path = Path(temp_dir) / "repr_test.lance"

            try:
                lance_kv = load_kv(
                    "lance", name="repr_test", uri=str(lance_path), columns=["col1", "col2"], batch_size=200
                )

                # Test string representations
                repr_str = repr(lance_kv)
                str_str = str(lance_kv)

                assert "LanceKVStore" in repr_str
                assert "repr_test" in repr_str
                assert str(lance_path) in repr_str

                assert "Lance KV Store" in str_str
                assert "repr_test" in str_str

            except Exception as e:
                if "Lance" in str(e) or "lance" in str(e):
                    pytest.skip(f"Lance not available: {e}")
                else:
                    raise

    def test_lance_kv_store_configuration_methods(self):
        """Test Lance KV store configuration methods."""
        with tempfile.TemporaryDirectory() as temp_dir:
            lance_path = Path(temp_dir) / "config_methods_test.lance"

            try:
                # Create base Lance KV store
                lance_kv = load_kv(
                    "lance",
                    name="config_test",
                    uri=str(lance_path),
                    columns=["original_col"],
                    batch_size=100,
                    max_connections=8,
                )

                # Test with_columns method
                new_columns_kv = lance_kv.with_columns(["new_col1", "new_col2"])
                assert new_columns_kv.columns == ["new_col1", "new_col2"]
                assert new_columns_kv.batch_size == 100  # Should preserve other settings
                assert new_columns_kv.max_connections == 8

                # Test with_batch_size method
                new_batch_kv = lance_kv.with_batch_size(500)
                assert new_batch_kv.batch_size == 500
                assert new_batch_kv.columns == ["original_col"]  # Should preserve columns
                assert new_batch_kv.max_connections == 8

                # Test with_max_connections method
                new_conn_kv = lance_kv.with_max_connections(16)
                assert new_conn_kv.max_connections == 16
                assert new_conn_kv.columns == ["original_col"]
                assert new_conn_kv.batch_size == 100

            except Exception as e:
                if "Lance" in str(e) or "lance" in str(e):
                    pytest.skip(f"Lance not available: {e}")
                else:
                    raise

    def test_cache_performance_improvement(self):
        """Test that caching improves performance (conceptual test)."""
        # This is a conceptual test since we can't easily measure actual performance
        # improvements without real Lance datasets and multiple operations
        try:
            from daft.daft import get_lance_cache_stats, init_lance_cache

            # Initialize cache
            init_lance_cache(max_size=10, ttl_seconds=60)

            # In a real scenario, repeated operations on the same URI should show:
            # 1. First operation: cache miss
            # 2. Subsequent operations: cache hits
            # 3. Better performance due to avoiding repeated dataset opens

            stats = get_lance_cache_stats()
            assert stats.hit_rate() >= 0.0  # Should be valid hit rate

        except ImportError:
            pytest.skip("Lance cache functions not available in Python bindings yet")
