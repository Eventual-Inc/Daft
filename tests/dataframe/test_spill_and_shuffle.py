"""End-to-end tests for sort_merge join (all 6 types), sort, and repartition correctness.

Every repartition, sort and sort-merge join test is parametrized with two memory modes:
  - no-spill (memory_limit=None): runs in-memory, no spill to disk
  - spill (memory_limit=1MB): forces spill to disk via low memory limit

Both modes must produce identical correct results.  If a bug only manifests
under spill (or only in-memory), the dual parametrization catches it.

"""

from __future__ import annotations

from contextlib import nullcontext

import pytest

import daft
from daft import col

# Use a very small memory limit to force spill.  The map_reduce_shuffle_spill_memory_limit_bytes
# value is used directly as the spill threshold by each operator.
SPILL_MEMORY_LIMIT = 1024 * 1024  # 1 MB

# Parametrize helper: every test runs once with no memory limit (no spill)
# and once with SPILL_MEMORY_LIMIT (triggers spill).
memory_limit_params = pytest.mark.parametrize(
    "memory_limit",
    [
        pytest.param(None, id="no-spill"),
        pytest.param(SPILL_MEMORY_LIMIT, id="spill"),
    ],
)


def _exec_ctx(memory_limit):
    """Return an execution_config_ctx if memory_limit is set, else a no-op context."""
    if memory_limit is not None:
        return daft.execution_config_ctx(map_reduce_shuffle_spill_memory_limit_bytes=memory_limit)
    return nullcontext()


# ========== Sort-Merge Join: All 6 Types ==========


@memory_limit_params
class TestSortMergeJoinAllTypes:
    """Correctness for inner/left/right/outer/semi/anti sort-merge joins.

    Uses n=20_000 so that the spill variant actually triggers spilling.
    """

    def test_sort_merge_join_inner(self, memory_limit):
        n = 20_000
        left = daft.from_pydict({"key": list(range(n)), "left_val": list(range(n))})
        right = daft.from_pydict({"key": list(range(0, n, 2)), "right_val": list(range(n // 2))})
        with _exec_ctx(memory_limit):
            result = left.join(right, on="key", how="inner", strategy="sort_merge").sort("key").collect().to_pydict()
        expected_keys = list(range(0, n, 2))
        assert result["key"] == expected_keys
        assert len(result["left_val"]) == n // 2
        assert len(result["right_val"]) == n // 2

    def test_sort_merge_join_left(self, memory_limit):
        n = 20_000
        left = daft.from_pydict({"key": list(range(n)), "left_val": list(range(n))})
        right = daft.from_pydict({"key": list(range(0, n, 2)), "right_val": list(range(n // 2))})
        with _exec_ctx(memory_limit):
            result = left.join(right, on="key", how="left", strategy="sort_merge").sort("key").collect().to_pydict()
        assert result["key"] == list(range(n))
        assert len(result["left_val"]) == n
        non_null_right = [v for v in result["right_val"] if v is not None]
        assert len(non_null_right) == n // 2

    def test_sort_merge_join_right(self, memory_limit):
        n = 20_000
        left = daft.from_pydict({"key": list(range(0, n, 2)), "left_val": list(range(n // 2))})
        right = daft.from_pydict({"key": list(range(n)), "right_val": list(range(n))})
        with _exec_ctx(memory_limit):
            result = left.join(right, on="key", how="right", strategy="sort_merge").sort("key").collect().to_pydict()
        assert result["key"] == list(range(n))
        assert len(result["right_val"]) == n
        non_null_left = [v for v in result["left_val"] if v is not None]
        assert len(non_null_left) == n // 2

    def test_sort_merge_join_outer(self, memory_limit):
        n = 20_000
        left = daft.from_pydict({"key": list(range(n)), "left_val": list(range(n))})
        right = daft.from_pydict(
            {
                "key": list(range(n - 1000, n + 1000)),
                "right_val": list(range(2000)),
            }
        )
        with _exec_ctx(memory_limit):
            result = left.join(right, on="key", how="outer", strategy="sort_merge").sort("key").collect().to_pydict()
        assert len(result["key"]) == n + 1000
        assert result["key"] == list(range(n + 1000))

    def test_sort_merge_join_semi(self, memory_limit):
        n = 20_000
        left = daft.from_pydict({"key": list(range(n)), "left_val": list(range(n))})
        right = daft.from_pydict({"key": list(range(0, n, 2)), "right_val": list(range(n // 2))})
        with _exec_ctx(memory_limit):
            result = left.join(right, on="key", how="semi", strategy="sort_merge").sort("key").collect().to_pydict()
        expected_keys = list(range(0, n, 2))
        assert result["key"] == expected_keys
        assert len(result["key"]) == len(set(result["key"])), "Semi join produced duplicate rows"
        assert "right_val" not in result

    def test_sort_merge_join_anti(self, memory_limit):
        n = 20_000
        left = daft.from_pydict({"key": list(range(n)), "left_val": list(range(n))})
        right = daft.from_pydict({"key": list(range(0, n, 2)), "right_val": list(range(n // 2))})
        with _exec_ctx(memory_limit):
            result = left.join(right, on="key", how="anti", strategy="sort_merge").sort("key").collect().to_pydict()
        expected_keys = list(range(1, n, 2))
        assert result["key"] == expected_keys
        assert "right_val" not in result


# ========== Sort-Merge Join: NULL Handling ==========


@memory_limit_params
class TestSortMergeJoinNulls:
    """Verify NULL key handling in sort_merge joins."""

    def _left_df(self):
        return daft.from_pydict({"key": [1, 2, None, 4], "left_val": ["a", "b", "c", "d"]})

    def _right_df(self):
        return daft.from_pydict({"key": [2, None, 5], "right_val": ["x", "y", "z"]})

    def test_sort_merge_join_inner_nulls_no_match(self, memory_limit):
        """NULLs should NOT match each other in inner join."""
        with _exec_ctx(memory_limit):
            result = (
                self._left_df()
                .join(self._right_df(), on="key", how="inner", strategy="sort_merge")
                .sort("key")
                .collect()
                .to_pydict()
            )
        assert result["key"] == [2]
        assert result["left_val"] == ["b"]
        assert result["right_val"] == ["x"]

    def test_sort_merge_join_semi_nulls(self, memory_limit):
        """NULLs should NOT match in semi join."""
        with _exec_ctx(memory_limit):
            result = (
                self._left_df()
                .join(self._right_df(), on="key", how="semi", strategy="sort_merge")
                .sort("key")
                .collect()
                .to_pydict()
            )
        assert result["key"] == [2]

    def test_sort_merge_join_anti_nulls(self, memory_limit):
        """NULL keys on left should appear in anti join (no match)."""
        with _exec_ctx(memory_limit):
            result = (
                self._left_df()
                .join(self._right_df(), on="key", how="anti", strategy="sort_merge")
                .sort("key")
                .collect()
                .to_pydict()
            )
        # Keys 1, None, 4 don't match right side (NULLs don't match)
        assert sorted([k for k in result["key"] if k is not None]) == [1, 4]
        assert None in result["key"]
        assert len(result["key"]) == 3


# ========== Sort-Merge Join: Duplicates ==========


@memory_limit_params
class TestSortMergeJoinDuplicates:
    """Verify cross-product behavior when keys have duplicates."""

    def test_sort_merge_join_inner_with_duplicates(self, memory_limit):
        left = daft.from_pydict({"key": [1, 1, 2], "left_val": ["a", "b", "c"]})
        right = daft.from_pydict({"key": [1, 1, 3], "right_val": ["x", "y", "z"]})
        with _exec_ctx(memory_limit):
            result = (
                left.join(right, on="key", how="inner", strategy="sort_merge")
                .sort(["key", "left_val", "right_val"])
                .collect()
                .to_pydict()
            )
        # 2x2 cross product for key=1
        assert result["key"] == [1, 1, 1, 1]
        assert len(result["left_val"]) == 4
        assert len(result["right_val"]) == 4

    def test_sort_merge_join_left_with_duplicates(self, memory_limit):
        left = daft.from_pydict({"key": [1, 1, 2], "left_val": ["a", "b", "c"]})
        right = daft.from_pydict({"key": [1, 3], "right_val": ["x", "z"]})
        with _exec_ctx(memory_limit):
            result = (
                left.join(right, on="key", how="left", strategy="sort_merge")
                .sort(["key", "left_val"])
                .collect()
                .to_pydict()
            )
        assert len(result["key"]) == 3
        assert result["key"] == [1, 1, 2]
        assert result["right_val"] == ["x", "x", None]

    def test_sort_merge_join_outer_with_duplicates(self, memory_limit):
        left = daft.from_pydict({"key": [1, 1], "left_val": ["a", "b"]})
        right = daft.from_pydict({"key": [1, 2], "right_val": ["x", "y"]})
        with _exec_ctx(memory_limit):
            result = left.join(right, on="key", how="outer", strategy="sort_merge").sort("key").collect().to_pydict()
        assert len(result["key"]) == 3
        assert sorted(result["key"]) == [1, 1, 2]


# ========== Sort-Merge Join: Semi/Anti with Duplicate Keys ==========


@memory_limit_params
class TestSortMergeJoinSemiAntiLarge:
    """Semi/Anti join with larger datasets and duplicate keys."""

    def test_semi_join_large_no_duplicates(self, memory_limit):
        """Semi join — no duplicate output rows."""
        n = 20_000
        left = daft.from_pydict({"key": list(range(n)), "left_val": list(range(n))})
        right = daft.from_pydict({"key": list(range(0, n, 2)), "right_val": list(range(n // 2))})
        with _exec_ctx(memory_limit):
            result = left.join(right, on="key", how="semi", strategy="sort_merge").sort("key").collect().to_pydict()
        expected_keys = list(range(0, n, 2))
        assert result["key"] == expected_keys
        assert len(result["key"]) == len(set(result["key"])), "Semi join produced duplicate rows"
        assert "right_val" not in result

    def test_anti_join_large_completeness(self, memory_limit):
        """Anti join — all non-matching rows preserved."""
        n = 20_000
        left = daft.from_pydict({"key": list(range(n)), "left_val": list(range(n))})
        right = daft.from_pydict({"key": list(range(0, n, 2)), "right_val": list(range(n // 2))})
        with _exec_ctx(memory_limit):
            result = left.join(right, on="key", how="anti", strategy="sort_merge").sort("key").collect().to_pydict()
        expected_keys = list(range(1, n, 2))
        assert result["key"] == expected_keys
        assert "right_val" not in result

    def test_semi_join_with_duplicate_keys(self, memory_limit):
        """Semi join where left has duplicate keys — each left row emitted at most once."""
        n = 2_000
        left_keys = sorted(list(range(n)) * 3)
        left = daft.from_pydict({"key": left_keys, "left_val": list(range(len(left_keys)))})
        right = daft.from_pydict({"key": list(range(0, n, 2)), "right_val": list(range(n // 2))})
        with _exec_ctx(memory_limit):
            result = (
                left.join(right, on="key", how="semi", strategy="sort_merge")
                .sort(["key", "left_val"])
                .collect()
                .to_pydict()
            )
        assert len(result["key"]) == (n // 2) * 3


# ========== Sort-Merge Join: Empty Tables ==========


@memory_limit_params
class TestSortMergeJoinEmpty:
    """Verify sort_merge join handles empty tables correctly for all 6 join types.

    Note: We use daft.from_pydict with at least one typed row, then filter to empty,
    because from_pydict({"key": []}) infers Null type which can't be sorted/joined.
    """

    def _empty_typed_df(self, schema):
        """Create an empty DataFrame with proper types by creating 1 row and filtering it out."""
        data = {}
        for col_name, sample_val in schema.items():
            data[col_name] = [sample_val]
        first_key = next(iter(schema.keys()))
        return daft.from_pydict(data).where(col(first_key) != col(first_key))

    @pytest.mark.parametrize("join_type", ["inner", "left", "right", "outer", "semi", "anti"])
    def test_empty_left(self, join_type, memory_limit):
        """Join with empty left table."""
        left = self._empty_typed_df({"key": 0, "left_val": "x"})
        right = daft.from_pydict({"key": [1, 2, 3], "right_val": ["a", "b", "c"]})
        with _exec_ctx(memory_limit):
            result = left.join(right, on="key", how=join_type, strategy="sort_merge").sort("key").collect().to_pydict()
        if join_type == "right":
            assert result["key"] == [1, 2, 3]
        elif join_type == "outer":
            assert result["key"] == [1, 2, 3]
        else:
            # inner, left, semi, anti with empty left → empty result
            assert len(result["key"]) == 0

    @pytest.mark.parametrize("join_type", ["inner", "left", "right", "outer", "semi", "anti"])
    def test_empty_right(self, join_type, memory_limit):
        """Join with empty right table."""
        left = daft.from_pydict({"key": [1, 2, 3], "left_val": ["a", "b", "c"]})
        right = self._empty_typed_df({"key": 0, "right_val": "x"})
        with _exec_ctx(memory_limit):
            result = left.join(right, on="key", how=join_type, strategy="sort_merge").sort("key").collect().to_pydict()
        if join_type in ("left", "outer"):
            assert result["key"] == [1, 2, 3]
        elif join_type == "anti":
            assert result["key"] == [1, 2, 3]
        elif join_type == "right":
            assert len(result["key"]) == 0
        else:
            # inner, semi with empty right → empty result
            assert len(result["key"]) == 0

    @pytest.mark.parametrize("join_type", ["inner", "left", "right", "outer", "semi", "anti"])
    def test_both_empty(self, join_type, memory_limit):
        """Join with both tables empty."""
        left = self._empty_typed_df({"key": 0, "left_val": "x"})
        right = self._empty_typed_df({"key": 0, "right_val": "x"})
        with _exec_ctx(memory_limit):
            result = left.join(right, on="key", how=join_type, strategy="sort_merge").collect().to_pydict()
        assert len(result["key"]) == 0


# ========== Repartition Correctness ==========


@memory_limit_params
class TestRepartitionCorrectness:
    """Verify repartition preserves all rows."""

    def test_repartition_hash_preserves_all_rows(self, memory_limit):
        data = {"key": list(range(20000)), "val": list(range(20000, 40000))}
        df = daft.from_pydict(data)
        with _exec_ctx(memory_limit):
            result = df.repartition(4, col("key")).sort("key").collect().to_pydict()
        assert result["key"] == list(range(20000))
        assert result["val"] == list(range(20000, 40000))

    def test_repartition_random_preserves_all_rows(self, memory_limit):
        data = {"key": list(range(20000)), "val": list(range(20000))}
        df = daft.from_pydict(data)
        with _exec_ctx(memory_limit):
            result = df.repartition(3).sort("key").collect().to_pydict()
        assert result["key"] == list(range(20000))
        assert result["val"] == list(range(20000))

    def test_repartition_single_partition(self, memory_limit):
        data = {"a": list(range(20000))}
        df = daft.from_pydict(data)
        with _exec_ctx(memory_limit):
            result = df.repartition(1).sort("a").collect().to_pydict()
        assert result["a"] == list(range(20000))

    def test_repartition_more_partitions_than_rows(self, memory_limit):
        data = {"a": [1, 2, 3]}
        df = daft.from_pydict(data)
        with _exec_ctx(memory_limit):
            result = df.repartition(10, col("a")).sort("a").collect().to_pydict()
        assert result["a"] == [1, 2, 3]


# ========== Distributed Shuffle Tests (Ray-meaningful) ==========


@memory_limit_params
class TestDistributedShuffleCorrectness:
    """Verify distributed shuffle correctness with larger datasets.

    On ray, these exercise the actual shuffle map/reduce path including
    target_block_size chunking in RepartitionSink.finalize().
    On native, repartition is a no-op so these are just sort verifications.
    """

    def test_shuffle_large_hash_repartition(self, memory_limit):
        """Hash repartition with 50K rows across 16 partitions."""
        n = 50_000
        data = {"key": list(range(n)), "val": [f"v{i}" for i in range(n)]}
        df = daft.from_pydict(data)
        with _exec_ctx(memory_limit):
            result = df.repartition(16, col("key")).sort("key").collect().to_pydict()
        assert result["key"] == list(range(n))
        assert result["val"] == [f"v{i}" for i in range(n)]

    def test_shuffle_large_random_repartition(self, memory_limit):
        """Random repartition with 50K rows across 8 partitions."""
        n = 50_000
        data = {"key": list(range(n)), "val": list(range(n))}
        df = daft.from_pydict(data)
        with _exec_ctx(memory_limit):
            result = df.repartition(8).sort("key").collect().to_pydict()
        assert result["key"] == list(range(n))
        assert result["val"] == list(range(n))

    def test_shuffle_chain_repartition(self, memory_limit):
        """Chaining two repartitions preserves data."""
        n = 10_000
        data = {"key": list(range(n)), "val": list(range(n))}
        df = daft.from_pydict(data)
        with _exec_ctx(memory_limit):
            result = df.repartition(8, col("key")).repartition(4, col("key")).sort("key").collect().to_pydict()
        assert result["key"] == list(range(n))
        assert result["val"] == list(range(n))

    def test_shuffle_repartition_then_sort_merge_join(self, memory_limit):
        """Repartition followed by sort_merge join preserves correctness."""
        n = 10_000
        left = daft.from_pydict({"key": list(range(n)), "left_val": list(range(n))})
        right = daft.from_pydict({"key": list(range(0, n, 2)), "right_val": list(range(n // 2))})
        with _exec_ctx(memory_limit):
            result = (
                left.repartition(8, col("key"))
                .join(right.repartition(8, col("key")), on="key", how="inner", strategy="sort_merge")
                .sort("key")
                .collect()
                .to_pydict()
            )
        expected_keys = list(range(0, n, 2))
        assert result["key"] == expected_keys
        assert len(result["left_val"]) == n // 2
        assert len(result["right_val"]) == n // 2

    def test_shuffle_with_nulls(self, memory_limit):
        """Repartition with NULL keys should preserve all rows including NULLs."""
        keys = list(range(1000)) + [None] * 100
        vals = list(range(1100))
        df = daft.from_pydict({"key": keys, "val": vals})
        with _exec_ctx(memory_limit):
            result = df.repartition(8, col("key")).sort("val").collect().to_pydict()
        assert result["val"] == list(range(1100))
        null_count = sum(1 for k in result["key"] if k is None)
        assert null_count == 100

    def test_shuffle_with_wide_schema(self, memory_limit):
        """Repartition with many columns to exercise larger row sizes."""
        n = 10_000
        data = {"key": list(range(n))}
        for i in range(10):
            data[f"col_{i}"] = list(range(i * n, (i + 1) * n))
        df = daft.from_pydict(data)
        with _exec_ctx(memory_limit):
            result = df.repartition(8, col("key")).sort("key").collect().to_pydict()
        assert result["key"] == list(range(n))
        for i in range(10):
            assert result[f"col_{i}"] == list(range(i * n, (i + 1) * n))

    def test_shuffle_with_custom_target_block_size(self, memory_limit):
        """Repartition with a custom map_reduce_shuffle_target_block_size config."""
        n = 10_000
        data = {"key": list(range(n)), "val": list(range(n))}
        with _exec_ctx(memory_limit):
            with daft.execution_config_ctx(map_reduce_shuffle_target_block_size=1024 * 64):  # 64KB
                result = daft.from_pydict(data).repartition(4, col("key")).sort("key").collect().to_pydict()
        assert result["key"] == list(range(n))
        assert result["val"] == list(range(n))


# ========== Sort Correctness ==========


@memory_limit_params
class TestSortCorrectness:
    """Verify sort produces correct ordering in both spill and non-spill modes."""

    def test_sort_basic_ascending(self, memory_limit):
        n = 10_000
        data = {"a": list(range(n, 0, -1)), "b": list(range(n))}
        with _exec_ctx(memory_limit):
            result = daft.from_pydict(data).sort("a").collect().to_pydict()
        assert result["a"] == list(range(1, n + 1))
        assert result["b"] == list(range(n - 1, -1, -1))

    def test_sort_basic_descending(self, memory_limit):
        n = 10_000
        data = {"a": list(range(n)), "b": list(range(n))}
        with _exec_ctx(memory_limit):
            result = daft.from_pydict(data).sort("a", desc=True).collect().to_pydict()
        assert result["a"] == list(range(n - 1, -1, -1))

    def test_sort_with_nulls(self, memory_limit):
        n = 10_000
        # Mix nulls into a large dataset
        values = list(range(n - 100)) + [None] * 100
        with _exec_ctx(memory_limit):
            result = daft.from_pydict({"a": values}).sort("a").collect().to_pydict()
        non_nulls = [v for v in result["a"] if v is not None]
        nulls = [v for v in result["a"] if v is None]
        assert non_nulls == list(range(n - 100))
        assert len(nulls) == 100

    def test_sort_with_duplicates(self, memory_limit):
        n = 10_000
        values = list(range(100)) * (n // 100)
        with _exec_ctx(memory_limit):
            result = daft.from_pydict({"a": values}).sort("a").collect().to_pydict()
        assert result["a"] == sorted(values)

    def test_sort_already_sorted(self, memory_limit):
        n = 10_000
        with _exec_ctx(memory_limit):
            result = daft.from_pydict({"a": list(range(n))}).sort("a").collect().to_pydict()
        assert result["a"] == list(range(n))

    def test_sort_multi_column(self, memory_limit):
        n = 10_000
        data = {
            "a": [i // 100 for i in range(n)],
            "b": [i % 100 for i in range(n, 0, -1)],
            "c": list(range(n)),
        }
        with _exec_ctx(memory_limit):
            result = daft.from_pydict(data).sort(["a", "b"]).collect().to_pydict()
        pairs = list(zip(result["a"], result["b"]))
        assert pairs == sorted(pairs)

    def test_sort_stable_with_ties(self, memory_limit):
        """Sort should be stable — equal elements maintain original order."""
        n = 10_000
        df = daft.from_pydict({"key": [0] * n, "val": list(range(n))})
        with _exec_ctx(memory_limit):
            result = df.sort("key").collect().to_pydict()
        assert result["key"] == [0] * n
        assert result["val"] == list(range(n))


# ========== Multi-Column Key Sort-Merge Join ==========


@memory_limit_params
class TestSortMergeJoinMultiColumnKey:
    """SMJ with multi-column join keys in both spill and non-spill modes."""

    def test_sort_merge_join_multi_column_key(self, memory_limit):
        n = 20_000
        left = daft.from_pydict(
            {
                "k1": [i // 100 for i in range(n)],
                "k2": [i % 100 for i in range(n)],
                "left_val": list(range(n)),
            }
        )
        right = daft.from_pydict(
            {
                "k1": [i // 100 for i in range(0, n, 2)],
                "k2": [i % 100 for i in range(0, n, 2)],
                "right_val": list(range(n // 2)),
            }
        )
        with _exec_ctx(memory_limit):
            result = (
                left.join(right, on=["k1", "k2"], how="inner", strategy="sort_merge")
                .sort(["k1", "k2"])
                .collect()
                .to_pydict()
            )
        assert len(result["k1"]) == n // 2
