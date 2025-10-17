from __future__ import annotations

import tempfile
from pathlib import Path

import lance
import pandas as pd
import pytest
from packaging import version as packaging_version

import daft
from daft.io.lance import create_scalar_index


def check_lance_version_compatibility():
    """Check if lance version supports distributed indexing."""
    try:
        lance_version = packaging_version.parse(lance.__version__)
        min_required_version = packaging_version.parse("0.36.0")
        return lance_version >= min_required_version
    except (AttributeError, Exception):
        return False


pytestmark = pytest.mark.skipif(
    not check_lance_version_compatibility(),
    reason="Distributed indexing requires pylance >= 0.36.0. Current version: {}".format(
        getattr(lance, "__version__", "unknown")
    ),
)


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def multi_fragment_lance_dataset(temp_dir):
    """Create a Lance dataset with multiple fragments for testing."""
    path = Path(temp_dir) / "multi_fragment_text.lance"
    # Create dataset with multiple fragments (2 rows per fragment)
    text_data = {
        "id": [1, 2, 3, 4, 5, 6, 7, 8],
        "text": [
            "The quick brown fox jumps over the lazy dog",
            "Python is a powerful programming language",
            "Machine learning algorithms are fascinating",
            "Data science requires statistical knowledge",
            "Natural language processing uses text analysis",
            "Distributed computing scales horizontally",
            "Daft framework enables parallel processing",
            "Lance format provides efficient storage",
        ],
        "category": [
            "animals",
            "tech",
            "ml",
            "data",
            "nlp",
            "distributed",
            "daft",
            "storage",
        ],
    }
    text_dataset = daft.from_pydict(text_data)
    text_dataset.write_lance(str(path), max_rows_per_file=2)
    return str(path)


def generate_multi_fragment_dataset(tmp_path, num_fragments=4, rows_per_fragment=250):
    """Generate a test dataset with multiple fragments."""
    all_data = []
    for frag_idx in range(num_fragments):
        for row_idx in range(rows_per_fragment):
            row_id = frag_idx * rows_per_fragment + row_idx
            all_data.append(
                {
                    "id": row_id,
                    "text": f"This is test document {row_id} with some sample text content for fragment {frag_idx}",
                    "fragment_id": frag_idx,
                }
            )

    df = pd.DataFrame(all_data)
    dataset = daft.from_pandas(df)

    path = Path(tmp_path) / "large_multi_fragment.lance"
    dataset.write_lance(str(path), max_rows_per_file=rows_per_fragment)
    return str(path)


class TestDistributedIndexing:
    """Test cases for distributed indexing functionality."""

    def test_build_distributed_index_basic(self, multi_fragment_lance_dataset):
        """Test basic distributed index building."""
        dataset_uri = multi_fragment_lance_dataset

        # Build distributed index
        create_scalar_index(
            url=dataset_uri,
            column="text",
            index_type="INVERTED",
            concurrency=2,
        )
        updated_dataset = lance.dataset(dataset_uri)

        # Verify the index was created
        indices = updated_dataset.list_indices()
        assert len(indices) > 0, "No indices found after building"

        # Find our index
        text_index = None
        for idx in indices:
            if "text" in idx["name"]:
                text_index = idx
                break

        assert text_index is not None, "Text index not found"
        assert text_index["type"] == "Inverted", f"Expected Inverted index, got {text_index['type']}"

    def test_build_distributed_index_with_name(self, multi_fragment_lance_dataset):
        """Test building distributed index with custom name."""
        dataset_uri = multi_fragment_lance_dataset
        custom_name = "custom_text_index"

        # Build distributed index with custom name
        create_scalar_index(
            url=dataset_uri,
            column="text",
            index_type="INVERTED",
            name=custom_name,
            concurrency=2,
        )
        updated_dataset = lance.dataset(dataset_uri)

        # Verify the index was created with correct name
        indices = updated_dataset.list_indices()
        index_names = [idx["name"] for idx in indices]
        assert custom_name in index_names, f"Custom index name '{custom_name}' not found in {index_names}"

    def test_build_distributed_index_search_functionality(self, multi_fragment_lance_dataset):
        """Test that the built index actually works for searching."""
        dataset_uri = multi_fragment_lance_dataset

        # Build distributed index
        create_scalar_index(
            url=dataset_uri,
            column="text",
            index_type="INVERTED",
            concurrency=2,
        )
        updated_dataset = lance.dataset(dataset_uri)

        # Test full-text search functionality
        search_term = "Python"
        results = updated_dataset.scanner(
            full_text_query=search_term,
            columns=["id", "text"],
        ).to_table()

        # Should find at least one result containing "Python"
        assert results.num_rows > 0, f"No results found for search term '{search_term}'"

        # Verify results contain the search term
        text_results = results.column("text").to_pylist()
        assert any(search_term in text for text in text_results), "Search results don't contain the search term"

    def test_build_distributed_index_large_dataset(self, temp_dir):
        """Test distributed indexing on a larger dataset with multiple fragments."""
        # Generate larger dataset
        dataset_uri = generate_multi_fragment_dataset(temp_dir, num_fragments=4, rows_per_fragment=50)

        # Build distributed index
        create_scalar_index(
            url=dataset_uri,
            column="text",
            index_type="INVERTED",
            concurrency=4,
        )
        updated_dataset = lance.dataset(dataset_uri)

        # Verify the index was created
        indices = updated_dataset.list_indices()
        assert len(indices) > 0, "No indices found after building"

        # Test search functionality
        search_term = "test"
        results = updated_dataset.scanner(
            full_text_query=search_term,
            columns=["id", "text"],
        ).to_table()

        assert results.num_rows > 0, f"No results found for search term '{search_term}'"

    def test_build_distributed_index_invalid_column(self, multi_fragment_lance_dataset):
        """Test error handling for invalid column."""
        dataset_uri = multi_fragment_lance_dataset

        with pytest.raises(ValueError, match="Column 'nonexistent' not found"):
            create_scalar_index(
                url=dataset_uri,
                column="nonexistent",
                index_type="INVERTED",
                concurrency=2,
            )

    def test_build_distributed_index_invalid_index_type(self, multi_fragment_lance_dataset):
        """Test error handling for invalid index type."""
        dataset_uri = multi_fragment_lance_dataset

        with pytest.raises(
            ValueError,
            match=r"Distributed indexing currently only supports 'INVERTED' and 'FTS' index types, not 'INVALID'",
        ):
            create_scalar_index(
                url=dataset_uri,
                column="text",
                index_type="INVALID",
                concurrency=2,
            )

    def test_build_distributed_index_invalid_num_workers(self, multi_fragment_lance_dataset):
        """Test error handling for invalid concurrency."""
        dataset_uri = multi_fragment_lance_dataset

        with pytest.raises(ValueError, match="concurrency must be positive"):
            create_scalar_index(
                url=dataset_uri,
                column="text",
                index_type="INVERTED",
                concurrency=0,
            )

    def test_build_distributed_index_empty_column(self, multi_fragment_lance_dataset):
        """Test error handling for empty column name."""
        dataset_uri = multi_fragment_lance_dataset

        with pytest.raises(ValueError, match="Column name cannot be empty"):
            create_scalar_index(
                url=dataset_uri,
                column="",
                index_type="INVERTED",
                concurrency=2,
            )

    def test_build_distributed_index_non_string_column(self, temp_dir):
        """Test error handling for non-string column."""
        # Create dataset with non-string column
        data = pd.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "numeric_col": [10, 20, 30, 40],
                "text": ["text1", "text2", "text3", "text4"],
            }
        )
        dataset = daft.from_pandas(data)
        path = Path(temp_dir) / "non_string_test.lance"
        dataset.write_lance(str(path), max_rows_per_file=2)

        with pytest.raises(TypeError, match="must be string type"):
            create_scalar_index(
                url=str(path),
                column="numeric_col",
                index_type="INVERTED",
                concurrency=2,
            )

    def test_build_distributed_index_with_daft_remote_args(self, multi_fragment_lance_dataset):
        """Test building distributed index with Daft options."""
        dataset_uri = multi_fragment_lance_dataset

        # Build distributed index with Daft options
        create_scalar_index(
            url=dataset_uri,
            column="text",
            index_type="INVERTED",
            concurrency=2,
            daft_remote_args={"num_cpus": 1},
        )

        updated_dataset = lance.dataset(dataset_uri)
        indices = updated_dataset.list_indices()
        assert len(indices) > 0, "No indices found after building"

    def test_build_distributed_index_with_storage_options(self, multi_fragment_lance_dataset):
        """Test building distributed index with storage options."""
        dataset_uri = multi_fragment_lance_dataset

        # Build distributed index with storage options
        create_scalar_index(
            url=dataset_uri,
            column="text",
            index_type="INVERTED",
            concurrency=2,
            storage_options={},  # Empty storage options should work
        )

        updated_dataset = lance.dataset(dataset_uri)
        indices = updated_dataset.list_indices()
        assert len(indices) > 0, "No indices found after building"

    def test_build_distributed_index_with_kwargs(self, multi_fragment_lance_dataset):
        """Test building distributed index with additional kwargs."""
        dataset_uri = multi_fragment_lance_dataset

        # Build distributed index with additional kwargs
        create_scalar_index(
            url=dataset_uri,
            column="text",
            index_type="INVERTED",
            concurrency=2,
            remove_stop_words=False,  # Additional kwarg for create_scalar_index
        )

        updated_dataset = lance.dataset(dataset_uri)
        indices = updated_dataset.list_indices()
        assert len(indices) > 0, "No indices found after building"

    def test_build_distributed_index_replace_false_existing_index(self, multi_fragment_lance_dataset):
        """Test that replace=False raises error when trying to create index with existing name."""
        dataset_uri = multi_fragment_lance_dataset
        index_name = "test_replace_false_index"

        # First, create an index
        create_scalar_index(
            url=dataset_uri,
            column="text",
            index_type="INVERTED",
            name=index_name,
            concurrency=2,
        )

        updated_dataset = lance.dataset(dataset_uri)
        indices = updated_dataset.list_indices()
        assert len(indices) > 0, "Initial index creation failed"

        # Now try to create another index with the same name but replace=False
        # The error might be raised as RuntimeError during distributed processing
        with pytest.raises((ValueError, RuntimeError)) as exc_info:
            create_scalar_index(
                url=dataset_uri,
                column="text",
                index_type="INVERTED",
                name=index_name,
                replace=False,
                concurrency=2,
            )

        # Verify the error message contains information about existing index
        error_msg = str(exc_info.value)
        assert "already exists" in error_msg and index_name in error_msg

    def test_build_distributed_index_replace_true_overwrite_existing(self, multi_fragment_lance_dataset):
        """Test that replace=True successfully overwrites existing index."""
        dataset_uri = multi_fragment_lance_dataset
        index_name = "test_replace_true_index"

        # First, create an index
        create_scalar_index(
            url=dataset_uri,
            column="text",
            index_type="INVERTED",
            name=index_name,
            concurrency=2,
        )

        updated_dataset = lance.dataset(dataset_uri)
        initial_indices = updated_dataset.list_indices()
        assert len(initial_indices) > 0, "Initial index creation failed"

        # Find our initial index
        initial_index = None
        for idx in initial_indices:
            if idx["name"] == index_name:
                initial_index = idx
                break
        assert initial_index is not None, "Initial index not found"

        # Now create another index with the same name but replace=True
        create_scalar_index(
            url=dataset_uri,
            column="text",
            index_type="INVERTED",
            name=index_name,
            replace=True,
            concurrency=2,
        )

        updated_dataset = lance.dataset(dataset_uri)
        final_indices = updated_dataset.list_indices()
        final_index = None
        for idx in final_indices:
            if idx["name"] == index_name:
                final_index = idx
                break

        assert final_index is not None, "Index should still exist after replacement"
        assert final_index["type"] == "Inverted", "Index type should remain Inverted"

        # Test that the replaced index still works for searching
        search_term = "Python"
        results = updated_dataset.scanner(
            full_text_query=search_term,
            columns=["id", "text"],
        ).to_table()

        assert results.num_rows > 0, f"No results found for search term '{search_term}' after index replacement"

    def test_build_distributed_index_auto_adjust_workers(self, temp_dir):
        """Test that concurrency is automatically adjusted if it exceeds fragment count."""
        # Create dataset with only 2 fragments
        data = {
            "id": [1, 2, 3, 4],
            "text": ["text1", "text2", "text3", "text4"],
        }
        dataset = daft.from_pydict(data)
        path = Path(temp_dir) / "small_dataset.lance"
        dataset.write_lance(str(path), max_rows_per_file=2)

        # Request more workers than fragments
        create_scalar_index(
            url=str(path),
            column="text",
            index_type="INVERTED",
            concurrency=10,  # More than the 2 fragments
        )

        # Should still work and create the index
        updated_dataset = lance.dataset(str(path))
        indices = updated_dataset.list_indices()
        assert len(indices) > 0, "No indices found after building"
