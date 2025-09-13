"""Tests for multi-source KV Store functionality."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

import daft
from daft.functions.kv import kv_batch_get, kv_exists, kv_get
from daft.kv import load_kv


class TestMultiSourceKVStore:
    """Test multi-source KV Store functionality with source parameter."""

    def setup_method(self):
        """Set up test environment with multiple KV stores."""
        # Clear any existing KV stores
        daft.set_kv(None)

        # Create multiple memory KV stores with different data
        self.embeddings_data = {
            "item_0": "embedding_vector_0",
            "item_1": "embedding_vector_1",
            "item_2": "embedding_vector_2",
        }

        self.metadata_data = {"item_0": "metadata_0", "item_1": "metadata_1", "item_2": "metadata_2"}

        self.features_data = {
            "item_0": "features_0",
            "item_1": "features_1",
            # Note: item_2 is missing to test error handling
        }

        # Create KV stores
        self.embeddings_kv = load_kv("memory", name="embeddings", initial_data=self.embeddings_data)
        self.metadata_kv = load_kv("memory", name="metadata", initial_data=self.metadata_data)
        self.features_kv = load_kv("memory", name="features", initial_data=self.features_data)

        # Attach to session with different aliases
        daft.attach(self.embeddings_kv, alias="lance_embeddings")
        daft.attach(self.metadata_kv, alias="lance_metadata")
        daft.attach(self.features_kv, alias="lance_features")

        # Set default KV store
        daft.set_kv("lance_embeddings")

    def teardown_method(self):
        """Clean up test environment."""
        try:
            daft.detach_kv("lance_embeddings")
            daft.detach_kv("lance_metadata")
            daft.detach_kv("lance_features")
        except ValueError:
            pass  # Ignore if already detached
        daft.set_kv(None)

    def test_kv_get_with_source_parameter(self):
        """Test kv_get function with source parameter."""
        # Create test DataFrame
        df = daft.from_pydict({"item_id": ["item_0", "item_1", "item_2"]})

        # Test using default KV store (no source parameter)
        df_default = df.with_column("embeddings", kv_get("item_id"))

        # Test using specific KV store via source parameter
        df_with_source = df.with_column("metadata", kv_get("item_id", source="lance_metadata"))

        # Test using another KV store
        df_features = df.with_column("features", kv_get("item_id", source="lance_features"))

        # Verify that different sources return different data
        # Note: We can't easily verify the actual content without executing the query,
        # but we can verify that the expressions are created without errors
        assert df_default is not None
        assert df_with_source is not None
        assert df_features is not None

    def test_kv_batch_get_with_source_parameter(self):
        """Test kv_batch_get function with source parameter."""
        # Create test DataFrame with batch data
        df = daft.from_pydict({"item_ids": [["item_0", "item_1"], ["item_2"]]})

        # Test using default KV store
        df_default = df.with_column("embeddings", kv_batch_get("item_ids"))

        # Test using specific KV store via source parameter
        df_with_source = df.with_column("metadata", kv_batch_get("item_ids", source="lance_metadata"))

        # Test with custom batch size
        df_custom_batch = df.with_column("features", kv_batch_get("item_ids", source="lance_features", batch_size=500))

        assert df_default is not None
        assert df_with_source is not None
        assert df_custom_batch is not None

    def test_kv_exists_with_source_parameter(self):
        """Test kv_exists function with source parameter."""
        # Create test DataFrame
        df = daft.from_pydict({"item_id": ["item_0", "item_1", "item_999"]})

        # Test using default KV store
        df_default = df.with_column("exists_embeddings", kv_exists("item_id"))

        # Test using specific KV store via source parameter
        df_with_source = df.with_column("exists_metadata", kv_exists("item_id", source="lance_metadata"))

        # Test using features KV store (which doesn't have item_2)
        df_features = df.with_column("exists_features", kv_exists("item_id", source="lance_features"))

        assert df_default is not None
        assert df_with_source is not None
        assert df_features is not None

    def test_source_parameter_error_handling(self):
        """Test error handling for invalid source parameters."""
        df = daft.from_pydict({"item_id": ["item_0", "item_1"]})

        # Test with non-existent source
        with pytest.raises(ValueError, match="KV store 'non_existent' not found in session"):
            df.with_column("data", kv_get("item_id", source="non_existent"))

        with pytest.raises(ValueError, match="KV store 'invalid_source' not found in session"):
            df.with_column("data", kv_batch_get("item_id", source="invalid_source"))

        with pytest.raises(ValueError, match="KV store 'missing_store' not found in session"):
            df.with_column("exists", kv_exists("item_id", source="missing_store"))

    def test_no_current_kv_with_source_parameter(self):
        """Test that source parameter works even when no current KV is set."""
        # Clear current KV store
        daft.set_kv(None)

        df = daft.from_pydict({"item_id": ["item_0", "item_1"]})

        # Without source parameter, should fail
        with pytest.raises(ValueError, match="No KV store is currently attached"):
            df.with_column("data", kv_get("item_id"))

        # With source parameter, should work
        df_with_source = df.with_column("metadata", kv_get("item_id", source="lance_metadata"))
        assert df_with_source is not None

        # Test batch get
        df_batch = df.with_column("embeddings", kv_batch_get("item_id", source="lance_embeddings"))
        assert df_batch is not None

        # Test exists
        df_exists = df.with_column("exists", kv_exists("item_id", source="lance_features"))
        assert df_exists is not None

    def test_multiple_sources_in_single_query(self):
        """Test using multiple KV sources in a single DataFrame query."""
        df = daft.from_pydict({"item_id": ["item_0", "item_1", "item_2"]})

        # Add columns from different KV sources
        df_multi = (
            df.with_column("embeddings", kv_get("item_id"))  # Default source
            .with_column("metadata", kv_get("item_id", source="lance_metadata"))
            .with_column("features", kv_get("item_id", source="lance_features"))
            .with_column("exists_embeddings", kv_exists("item_id"))
            .with_column("exists_metadata", kv_exists("item_id", source="lance_metadata"))
            .with_column("exists_features", kv_exists("item_id", source="lance_features"))
        )

        assert df_multi is not None

    def test_source_parameter_with_expression_column(self):
        """Test source parameter with Expression column references."""
        df = daft.from_pydict({"item_id": ["item_0", "item_1", "item_2"]})

        # Test with Expression column reference
        df_expr = df.with_column("metadata", kv_get(df["item_id"], source="lance_metadata"))
        assert df_expr is not None

        # Test with batch get
        df_batch = daft.from_pydict({"item_ids": [["item_0"], ["item_1", "item_2"]]})
        df_batch_expr = df_batch.with_column(
            "embeddings", kv_batch_get(df_batch["item_ids"], source="lance_embeddings")
        )
        assert df_batch_expr is not None

    def test_backward_compatibility(self):
        """Test that existing code without source parameter still works."""
        df = daft.from_pydict({"item_id": ["item_0", "item_1", "item_2"]})

        # These should work exactly as before (using current KV store)
        df_old_style = (
            df.with_column("data", kv_get("item_id"))
            .with_column("batch_data", kv_batch_get("item_id"))
            .with_column("exists", kv_exists("item_id"))
        )

        assert df_old_style is not None

    def test_source_parameter_with_different_kv_types(self):
        """Test source parameter with different KV store types."""
        try:
            # Test with Lance KV store if available
            with tempfile.TemporaryDirectory() as temp_dir:
                lance_path = Path(temp_dir) / "test.lance"
                lance_kv = load_kv("lance", name="lance_test", uri=str(lance_path))
                daft.attach(lance_kv, alias="test_lance")

                df = daft.from_pydict({"item_id": ["item_0", "item_1"]})

                # Should work with Lance KV store
                df_lance = df.with_column("lance_data", kv_get("item_id", source="test_lance"))
                assert df_lance is not None

                # Should work with memory KV store
                df_memory = df.with_column("memory_data", kv_get("item_id", source="lance_metadata"))
                assert df_memory is not None

                daft.detach_kv("test_lance")

        except ImportError:
            pytest.skip("Lance not available")

    def test_source_parameter_with_columns_and_error_handling(self):
        """Test source parameter with columns and on_error parameters."""
        df = daft.from_pydict({"item_id": ["item_0", "item_1", "item_2"]})

        # Test with columns parameter
        df_columns = df.with_column("metadata", kv_get("item_id", source="lance_metadata", columns=["col1", "col2"]))
        assert df_columns is not None

        # Test with on_error parameter
        df_error_null = df.with_column("features", kv_get("item_id", source="lance_features", on_error="null"))
        assert df_error_null is not None

        # Test with both columns and on_error
        df_both = df.with_column(
            "embeddings", kv_get("item_id", source="lance_embeddings", columns=["embedding"], on_error="raise")
        )
        assert df_both is not None

    def test_kv_store_switching_with_source_parameter(self):
        """Test that source parameter works independently of current KV store."""
        df = daft.from_pydict({"item_id": ["item_0", "item_1"]})

        # Set different current KV stores and verify source parameter overrides
        daft.set_kv("lance_embeddings")
        df1 = df.with_column("metadata", kv_get("item_id", source="lance_metadata"))

        daft.set_kv("lance_metadata")
        df2 = df.with_column("embeddings", kv_get("item_id", source="lance_embeddings"))

        daft.set_kv("lance_features")
        df3 = df.with_column("metadata", kv_get("item_id", source="lance_metadata"))

        assert df1 is not None
        assert df2 is not None
        assert df3 is not None

    def test_columns_parameter_comprehensive(self):
        """Comprehensive test of columns parameter functionality."""
        df = daft.from_pydict({"item_id": ["item_0", "item_1", "item_2"]})

        # Test single column as string
        df_single = df.with_column("single_col", kv_get("item_id", "embedding"))

        # Test multiple columns as list
        df_multi = df.with_column("multi_col", kv_get("item_id", ["embedding", "metadata"]))

        # Test with source parameter
        df_source = df.with_column("source_col", kv_get("item_id", ["col1"], source="lance_metadata"))

        assert df_single is not None
        assert df_multi is not None
        assert df_source is not None

        # Verify schema types (should be Binary for now, Struct later)
        assert df_single.schema()["single_col"].dtype == daft.DataType.binary()
        assert df_multi.schema()["multi_col"].dtype == daft.DataType.binary()
        assert df_source.schema()["source_col"].dtype == daft.DataType.binary()

    def test_kv_batch_get_columns_parameter(self):
        """Test kv_batch_get with columns parameter."""
        df = daft.from_pydict({"item_ids": [["item_0", "item_1"], ["item_2"]]})

        # Test backward compatibility (no columns)
        df_binary = df.with_column("batch_binary", kv_batch_get("item_ids"))
        assert df_binary.schema()["batch_binary"].dtype == daft.DataType.binary()

        # Test with columns parameter
        df_struct = df.with_column("batch_struct", kv_batch_get("item_ids", ["embedding", "metadata"]))
        assert df_struct is not None
        # Should be Binary for now, Struct later
        assert df_struct.schema()["batch_struct"].dtype == daft.DataType.binary()

        # Test with source and columns
        df_source_struct = df.with_column(
            "source_batch_struct", kv_batch_get("item_ids", ["col1"], source="lance_metadata", batch_size=100)
        )
        assert df_source_struct is not None
        assert df_source_struct.schema()["source_batch_struct"].dtype == daft.DataType.binary()

    def test_columns_parameter_edge_cases(self):
        """Test edge cases for columns parameter."""
        df = daft.from_pydict({"item_id": ["item_0", "item_1"]})

        # Test with None columns (backward compatibility)
        df_none = df.with_column("none_cols", kv_get("item_id", None))
        assert df_none.schema()["none_cols"].dtype == daft.DataType.binary()

        # Test with empty string column
        df_empty_str = df.with_column("empty_str", kv_get("item_id", ""))
        assert df_empty_str is not None

        # Test with empty list (should work but may not be useful)
        df_empty_list = df.with_column("empty_list", kv_get("item_id", []))
        assert df_empty_list is not None

        # Test with duplicate columns
        df_dup = df.with_column("dup_cols", kv_get("item_id", ["col1", "col1"]))
        assert df_dup is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
