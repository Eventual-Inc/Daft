from __future__ import annotations

import pytest

import daft


def write_test_files(tmpdir, num_files: int) -> None:
    for i in range(num_files):
        file_data = {
            "id": list(range(i * 100, (i + 1) * 100)),
        }
        df = daft.from_pydict(file_data)
        df.write_parquet(f"{tmpdir}/file_{i}.parquet")


def test_sharding_should_error_for_non_file_scans() -> None:
    """Test that sharding should error for non-file scans."""
    df = daft.from_pydict({"id": list(range(100)), "value": [f"val_{i}" for i in range(100)]})
    with pytest.raises(ValueError, match="Sharding is not supported for in-memory sources"):
        df._shard(strategy="file", world_size=5, rank=0).collect()


def test_sharding_invalid_arguments() -> None:
    df = daft.from_pydict({"a": [1, 2, 3]})

    # Test invalid strategy.
    with pytest.raises(ValueError, match="Only file-based sharding is supported"):
        df._shard(strategy="invalid", world_size=5, rank=0)

    # Test invalid world_size.
    with pytest.raises(ValueError, match="World size for sharding must be greater than zero"):
        df._shard(strategy="file", world_size=0, rank=0)

    # Test invalid rank.
    with pytest.raises(ValueError, match="Rank must be less than the world size for sharding"):
        df._shard(strategy="file", world_size=5, rank=5)


def test_sharding_with_file_scan(tmpdir) -> None:
    num_files = 100
    write_test_files(tmpdir, num_files)

    world_size = 10
    all_files = set()

    for rank in range(world_size):
        sharded_df = daft.read_parquet(f"{tmpdir}/**/*.parquet", file_path_column="file_path")._shard(
            strategy="file", world_size=world_size, rank=rank
        )
        results = sharded_df.select("file_path").distinct().collect()
        assert len(results) > 0, "Shards should be somewhat fairly distributed and have at least one file"
        for result in results:
            file_path = result["file_path"]
            assert file_path not in all_files, f"File {file_path} should only be assigned to one shard"
            all_files.add(file_path)

    assert len(all_files) == num_files, "All files should be assigned to some shard"


def test_sharding_distribution_fairness(tmpdir) -> None:
    num_files = 100
    write_test_files(tmpdir, num_files)

    world_size = 10
    files_per_shard = []

    for rank in range(world_size):
        sharded_df = daft.read_parquet(f"{tmpdir}/**/*.parquet", file_path_column="file_path")._shard(
            strategy="file", world_size=world_size, rank=rank
        )
        files_per_shard.append(sharded_df.select("file_path").distinct().count_rows())

    avg_files = sum(files_per_shard) / len(files_per_shard)

    # The distribution should be reasonably uniform.
    variance = sum((x - avg_files) ** 2 for x in files_per_shard) / len(files_per_shard)
    std_dev = variance**0.5
    coefficient_of_variation = std_dev / avg_files if avg_files > 0 else float("inf")
    assert (
        coefficient_of_variation <= 0.5
    ), f"The distribution for sharding is too variable with a coefficient of variation of {coefficient_of_variation:.2f}"


def test_sharding_consistency(tmpdir) -> None:
    num_files = 50
    for i in range(num_files):
        file_data = {
            "row_id": list(range(i * 100, (i + 1) * 100)),
        }
        df = daft.from_pydict(file_data)
        df.write_parquet(f"{tmpdir}/file_{i}.parquet")

    world_size = 10

    for rank in range(world_size):
        canonical_result = (
            daft.read_parquet(f"{tmpdir}/**/*.parquet", file_path_column="file_path")
            ._shard(strategy="file", world_size=world_size, rank=rank)
            .to_arrow()
        )
        # Shard contents and order should be consistent across runs.
        for _ in range(3):
            new_result = (
                daft.read_parquet(f"{tmpdir}/**/*.parquet", file_path_column="file_path")
                ._shard(strategy="file", world_size=world_size, rank=rank)
                .to_arrow()
            )
            assert canonical_result == new_result, "Sharding result is inconsistent between runs"


def test_sharding_with_pushdowns(tmpdir) -> None:
    num_files = 100
    write_test_files(tmpdir, num_files)

    sharded_df = (
        daft.read_parquet(f"{tmpdir}/**/*.parquet", file_path_column="file_path")
        .limit(10)
        .sort("id")
        .distinct()
        ._shard(strategy="file", world_size=3, rank=0)
    )

    assert sharded_df.count_rows() == 10
