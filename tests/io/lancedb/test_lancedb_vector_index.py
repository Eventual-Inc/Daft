from __future__ import annotations

import random
from pathlib import Path

import pyarrow as pa
import pytest

lance = pytest.importorskip("lance")
from packaging import version as packaging_version

from daft.io.lance import create_index


def check_pylance_vector_compatibility() -> bool:
    """Check if lance version supports distributed vector indexing.

    Distributed IVF vector indices rely on pylance 2.0.1 (or newer) features
    exposed via the ``lance`` Python package.
    """
    try:
        lance_version = packaging_version.parse(lance.__version__)
        min_required_version = packaging_version.parse("2.0.1")
        return lance_version >= min_required_version
    except (AttributeError, Exception):  # pragma: no cover - defensive
        return False


# Skip all distributed vector indexing tests if lance / pylance is incompatible.
pytestmark = pytest.mark.skipif(
    not check_pylance_vector_compatibility(),
    reason=(
        "Distributed vector indexing requires pylance >= 2.0.1. Current version: {}".format(
            getattr(lance, "__version__", "unknown")
        )
    ),
)


def generate_multi_fragment_vector_dataset(
    tmp_path: Path,
    num_fragments: int = 4,
    rows_per_fragment: int = 64,
    dim: int = 128,
    seed: int = 0,
) -> str:
    """Generate a Lance dataset with a vector column and multiple fragments.

    The dataset is written directly via ``lance.write_dataset`` so that each
    fragment has the same number of rows, matching the expectations of the
    distributed vector index builder.
    """
    num_rows = num_fragments * rows_per_fragment
    rng = random.Random(seed)
    # Generate random vectors.
    data = [rng.gauss(0.0, 1.0) for _ in range(num_rows * dim)]
    # Construct a FixedSizeList "vector" column compatible with Lance's
    # vector index requirements.
    values = pa.array(data, type=pa.float32())
    vector_array = pa.FixedSizeListArray.from_arrays(values, dim)
    table = pa.Table.from_arrays([vector_array], names=["vector"])
    table = table.append_column("id", pa.array(range(num_rows), type=pa.int64()))
    path = tmp_path / "multi_fragment_vector.lance"
    lance.write_dataset(
        table,
        str(path),
        max_rows_per_file=rows_per_fragment,
    )
    return str(path)


class TestDistributedVectorIndexing:
    """Basic tests for distributed IVF vector index building via Daft."""

    def test_build_distributed_vector_index_basic(self, tmp_path: Path) -> None:
        """Build a distributed IVF_PQ index and verify nearest search works.

        This test exercises the full pipeline:
        1. Training IVF / PQ models (driver side).
        2. Distributed fragment-level index building via Daft UDFs.
        3. Metadata merge and final index commit.
        4. A simple nearest-neighbor query on the resulting index.
        """
        dim = 64
        dataset_uri = generate_multi_fragment_vector_dataset(
            tmp_path,
            num_fragments=4,
            rows_per_fragment=256,
            dim=dim,
        )
        concurrency = 2
        num_partitions = 4
        num_sub_vectors = 8
        # Log the key parameters so it is clear that the distributed path is
        # exercised at least once during test runs.
        print(
            f"[Daft IVF] Building distributed IVF_PQ index with "
            f"concurrency={concurrency}, num_partitions={num_partitions}, "
            f"num_sub_vectors={num_sub_vectors}",
        )
        # Build distributed vector index using the high-level Daft entrypoint.
        create_index(
            uri=dataset_uri,
            column="vector",
            index_type="IVF_PQ",
            name="idx_IVF_PQ",
            metric="l2",
            concurrency=concurrency,
            ivf_num_partitions=num_partitions,
            num_sub_vectors=num_sub_vectors,
            sample_rate=4,
        )
        updated_dataset = lance.dataset(dataset_uri)
        # Verify that the index was created with the expected type.
        indices = updated_dataset.list_indices()
        assert indices, "No indices found after distributed vector index build"
        vec_index = next((idx for idx in indices if idx["name"] == "idx_IVF_PQ"), None)
        assert vec_index is not None, "Vector index 'idx_IVF_PQ' not found"
        assert vec_index["type"] == "IVF_PQ", f"Expected IVF_PQ vector index, got {vec_index['type']}"
        # Run a simple nearest-neighbor query to ensure the index is usable.
        rng = random.Random(0)
        q = [rng.gauss(0.0, 1.0) for _ in range(dim)]
        result = updated_dataset.to_table(
            nearest={"column": "vector", "q": q, "k": 5},
            columns=["id", "vector"],
        )
        print(
            "[Daft IVF] Nearest neighbor query completed with %d results for k=5",
            result.num_rows,
        )
        assert result.num_rows == 5

    def test_create_index_rejects_scalar_index_type(self) -> None:
        with pytest.raises(ValueError, match="create_scalar_index"):
            create_index(uri="dummy", column="vector", index_type="BTREE")

    def test_invalid_metric_raises(self, tmp_path: Path) -> None:
        dataset_uri = generate_multi_fragment_vector_dataset(
            tmp_path,
            num_fragments=2,
            rows_per_fragment=32,
            dim=16,
            seed=1,
        )
        with pytest.raises(ValueError, match="Metric"):
            create_index(
                uri=dataset_uri,
                column="vector",
                index_type="IVF_FLAT",
                name="idx_invalid_metric",
                metric="not-a-metric",
                concurrency=1,
                ivf_num_partitions=2,
            )

    def test_replace_false_raises_when_index_exists(self, tmp_path: Path) -> None:
        dataset_uri = generate_multi_fragment_vector_dataset(
            tmp_path,
            num_fragments=2,
            rows_per_fragment=32,
            dim=16,
            seed=2,
        )
        create_index(
            uri=dataset_uri,
            column="vector",
            index_type="IVF_FLAT",
            name="idx_IVF_FLAT",
            metric="l2",
            concurrency=2,
            ivf_num_partitions=2,
            sample_rate=4,
        )
        with pytest.raises(ValueError, match="already exists"):
            create_index(
                uri=dataset_uri,
                column="vector",
                index_type="IVF_FLAT",
                name="idx_IVF_FLAT",
                replace=False,
                metric="l2",
                concurrency=2,
                ivf_num_partitions=2,
            )

    def test_normalize_index_type_accepts_enum_like(self) -> None:
        from daft.io.lance.lance_vector_index import _normalize_index_type

        class DummyIndexType:
            value = "ivf_pq"

        assert _normalize_index_type(DummyIndexType()) == "IVF_PQ"

    def test_create_index_with_different_metrics(self, tmp_path: Path) -> None:
        """Test creating vector indices with different distance metrics."""
        dataset_uri = generate_multi_fragment_vector_dataset(
            tmp_path,
            num_fragments=2,
            rows_per_fragment=32,
            dim=16,
            seed=3,
        )

        metrics = ["l2", "cosine", "euclidean", "dot"]
        for metric in metrics:
            index_name = f"idx_{metric}"
            create_index(
                uri=dataset_uri,
                column="vector",
                index_type="IVF_FLAT",
                name=index_name,
                metric=metric,
                concurrency=1,
                ivf_num_partitions=2,
                sample_rate=4,
            )

            updated_dataset = lance.dataset(dataset_uri)
            indices = updated_dataset.list_indices()
            vec_index = next((idx for idx in indices if idx["name"] == index_name), None)
            assert vec_index is not None, f"Vector index '{index_name}' not found"

    def test_create_index_with_different_index_types(self, tmp_path: Path) -> None:
        """Test creating different types of IVF vector indices."""
        dataset_uri = generate_multi_fragment_vector_dataset(
            tmp_path,
            num_fragments=2,
            rows_per_fragment=64,
            dim=32,
            seed=4,
        )

        index_types = ["IVF_FLAT", "IVF_SQ"]
        for index_type in index_types:
            index_name = f"idx_{index_type}"
            create_index(
                uri=dataset_uri,
                column="vector",
                index_type=index_type,
                name=index_name,
                metric="l2",
                concurrency=1,
                ivf_num_partitions=2,
                sample_rate=4,
            )

            updated_dataset = lance.dataset(dataset_uri)
            indices = updated_dataset.list_indices()
            vec_index = next((idx for idx in indices if idx["name"] == index_name), None)
            assert vec_index is not None, f"Vector index '{index_name}' not found"
            assert vec_index["type"] == index_type

    def test_create_index_replace_existing(self, tmp_path: Path) -> None:
        """Test that replace=True allows overwriting an existing index."""
        dataset_uri = generate_multi_fragment_vector_dataset(
            tmp_path,
            num_fragments=2,
            rows_per_fragment=32,
            dim=16,
            seed=5,
        )

        create_index(
            uri=dataset_uri,
            column="vector",
            index_type="IVF_FLAT",
            name="idx_replace",
            metric="l2",
            concurrency=1,
            ivf_num_partitions=2,
            sample_rate=4,
        )

        create_index(
            uri=dataset_uri,
            column="vector",
            index_type="IVF_FLAT",
            name="idx_replace",
            replace=True,
            metric="l2",
            concurrency=1,
            ivf_num_partitions=2,
            sample_rate=4,
        )

        updated_dataset = lance.dataset(dataset_uri)
        indices = updated_dataset.list_indices()
        vec_index = next((idx for idx in indices if idx["name"] == "idx_replace"), None)
        assert vec_index is not None, "Vector index 'idx_replace' not found after replace"

    def test_create_index_with_custom_name(self, tmp_path: Path) -> None:
        """Test creating an index with a custom name."""
        dataset_uri = generate_multi_fragment_vector_dataset(
            tmp_path,
            num_fragments=2,
            rows_per_fragment=32,
            dim=16,
            seed=6,
        )

        custom_name = "my_custom_vector_index"
        create_index(
            uri=dataset_uri,
            column="vector",
            index_type="IVF_FLAT",
            name=custom_name,
            metric="l2",
            concurrency=1,
            ivf_num_partitions=2,
            sample_rate=4,
        )

        updated_dataset = lance.dataset(dataset_uri)
        indices = updated_dataset.list_indices()
        vec_index = next((idx for idx in indices if idx["name"] == custom_name), None)
        assert vec_index is not None, f"Vector index '{custom_name}' not found"

    def test_create_index_default_name(self, tmp_path: Path) -> None:
        """Test creating an index without specifying a name (uses default)."""
        dataset_uri = generate_multi_fragment_vector_dataset(
            tmp_path,
            num_fragments=2,
            rows_per_fragment=32,
            dim=16,
            seed=7,
        )

        create_index(
            uri=dataset_uri,
            column="vector",
            index_type="IVF_FLAT",
            metric="l2",
            concurrency=1,
            ivf_num_partitions=2,
            sample_rate=4,
        )

        updated_dataset = lance.dataset(dataset_uri)
        indices = updated_dataset.list_indices()
        assert len(indices) > 0, "No indices found"

    def test_create_index_with_different_concurrency(self, tmp_path: Path) -> None:
        """Test creating an index with different concurrency settings."""
        dataset_uri = generate_multi_fragment_vector_dataset(
            tmp_path,
            num_fragments=3,
            rows_per_fragment=32,
            dim=16,
            seed=8,
        )

        for concurrency in [1, 2]:
            index_name = f"idx_concurrency_{concurrency}"
            create_index(
                uri=dataset_uri,
                column="vector",
                index_type="IVF_FLAT",
                name=index_name,
                metric="l2",
                concurrency=concurrency,
                ivf_num_partitions=2,
                sample_rate=4,
            )

            updated_dataset = lance.dataset(dataset_uri)
            indices = updated_dataset.list_indices()
            vec_index = next((idx for idx in indices if idx["name"] == index_name), None)
            assert vec_index is not None, f"Vector index '{index_name}' not found"

    def test_create_index_with_different_num_partitions(self, tmp_path: Path) -> None:
        """Test creating an index with different num_partitions settings."""
        dataset_uri = generate_multi_fragment_vector_dataset(
            tmp_path,
            num_fragments=4,
            rows_per_fragment=32,
            dim=16,
            seed=9,
        )

        for num_partitions in [2, 4]:
            index_name = f"idx_partitions_{num_partitions}"
            create_index(
                uri=dataset_uri,
                column="vector",
                index_type="IVF_FLAT",
                name=index_name,
                metric="l2",
                concurrency=1,
                ivf_num_partitions=num_partitions,
                sample_rate=4,
            )

            updated_dataset = lance.dataset(dataset_uri)
            indices = updated_dataset.list_indices()
            vec_index = next((idx for idx in indices if idx["name"] == index_name), None)
            assert vec_index is not None, f"Vector index '{index_name}' not found"
