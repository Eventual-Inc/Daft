"""Unit tests for daft.io.lance.utils.

These tests intentionally avoid importing the optional `lance` package so they
run in environments where Daft is installed without the `lance` extra. We only
exercise pure-Python helpers in `daft.io.lance.utils`.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from daft.io.lance.utils import distribute_fragments_balanced


@dataclass
class _FakeFragment:
    fragment_id: int
    rows: int

    def count_rows(self) -> int:
        return self.rows


def test_distribute_fragments_balanced_empty_input_returns_empty_list() -> None:
    assert distribute_fragments_balanced([], 3) == []


def test_distribute_fragments_balanced_rejects_non_positive_group_size() -> None:
    fragments = [_FakeFragment(fragment_id=0, rows=1)]
    with pytest.raises(ValueError):
        distribute_fragments_balanced(fragments, 0)
    with pytest.raises(ValueError):
        distribute_fragments_balanced(fragments, -1)


def test_distribute_fragments_balanced_uses_ceiling_division_for_group_count() -> None:
    # 10 fragments with group size 3 should produce ceil(10/3) == 4 groups,
    # not floor(10/3) == 3. This guards against a regression where the group
    # count was computed with floor division.
    fragments = [_FakeFragment(fragment_id=i, rows=1) for i in range(10)]
    result = distribute_fragments_balanced(fragments, fragment_group_size=3)

    assert len(result) == 4
    # Every original fragment id must appear exactly once across the groups.
    flattened = [fid for group in result for fid in group["fragment_ids"]]
    assert sorted(flattened) == list(range(10))
    # No group should exceed the requested fragment_group_size.
    for group in result:
        assert len(group["fragment_ids"]) <= 3


def test_distribute_fragments_balanced_exact_multiple_group_size() -> None:
    # When fragments divide evenly into the group size, ceiling and floor
    # division agree, so we should get exactly that many groups.
    fragments = [_FakeFragment(fragment_id=i, rows=1) for i in range(6)]
    result = distribute_fragments_balanced(fragments, fragment_group_size=3)

    assert len(result) == 2
    flattened = [fid for group in result for fid in group["fragment_ids"]]
    assert sorted(flattened) == list(range(6))


def test_distribute_fragments_balanced_single_group_when_size_exceeds_total() -> None:
    fragments = [_FakeFragment(fragment_id=i, rows=1) for i in range(3)]
    result = distribute_fragments_balanced(fragments, fragment_group_size=10)

    assert len(result) == 1
    assert sorted(result[0]["fragment_ids"]) == [0, 1, 2]


def test_distribute_fragments_balanced_handles_fragments_without_count_rows() -> None:
    class _BrokenFragment:
        def __init__(self, fragment_id: int) -> None:
            self.fragment_id = fragment_id

        def count_rows(self) -> int:  # pragma: no cover - exercised via except
            raise RuntimeError("size unavailable")

    fragments = [_BrokenFragment(i) for i in range(4)]
    result = distribute_fragments_balanced(fragments, fragment_group_size=2)

    assert len(result) == 2
    flattened = [fid for group in result for fid in group["fragment_ids"]]
    assert sorted(flattened) == [0, 1, 2, 3]
