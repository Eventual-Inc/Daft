"""Addressing Lance tables through a Lance Namespace instead of a URI.

Uses the "dir" namespace implementation so these run without a REST service.
"""

from __future__ import annotations

import pytest

import daft


@pytest.fixture
def ns_kwargs(tmp_path) -> dict:
    return {
        "namespace_impl": "dir",
        "namespace_properties": {"root": str(tmp_path / "root")},
        "table_id": ["tbl"],
    }


def test_write_read_roundtrip(ns_kwargs):
    daft.from_pydict({"id": [1, 2, 3], "v": ["a", "b", "c"]}).write_lance(**ns_kwargs).collect()

    df = daft.read_lance(**ns_kwargs).sort("id")
    assert df.to_pydict() == {"id": [1, 2, 3], "v": ["a", "b", "c"]}


def test_append(ns_kwargs):
    daft.from_pydict({"id": [1, 2]}).write_lance(**ns_kwargs).collect()
    daft.from_pydict({"id": [3]}).write_lance(mode="append", **ns_kwargs).collect()

    assert daft.read_lance(**ns_kwargs).count_rows() == 3


def test_merge_adds_columns(ns_kwargs):
    daft.from_pydict({"id": [1, 2, 3]}).write_lance(**ns_kwargs).collect()

    src = daft.read_lance(
        **ns_kwargs,
        default_scan_options={"with_row_address": True},
        include_fragment_id=True,
    )
    src = src.with_column("doubled", src["id"] * 2).select("fragment_id", "_rowaddr", "doubled")
    src.write_lance(mode="merge", **ns_kwargs).collect()

    assert daft.read_lance(**ns_kwargs).sort("id").to_pydict() == {
        "id": [1, 2, 3],
        "doubled": [2, 4, 6],
    }


def test_merge_creates_missing_table(ns_kwargs):
    """A merge against a table the namespace does not know about degrades to a create."""
    daft.from_pydict({"id": [1, 2]}).write_lance(mode="merge", **ns_kwargs).collect()

    assert daft.read_lance(**ns_kwargs).count_rows() == 2


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({}, "Must provide either"),
        ({"uri": "/tmp/some.lance", "namespace_impl": "dir", "table_id": ["t"]}, "Cannot provide both"),
        ({"namespace_impl": "dir"}, "'table_id' must be provided"),
        ({"table_id": ["t"]}, "'namespace_impl' must be provided"),
    ],
)
def test_rejects_ambiguous_addressing(kwargs, message):
    df = daft.from_pydict({"id": [1]})
    with pytest.raises(ValueError, match=message):
        df.write_lance(**kwargs)
