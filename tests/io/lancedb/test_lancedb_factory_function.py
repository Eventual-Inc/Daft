from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from daft.io.lance.lance_scan import _lancedb_table_factory_function
from daft.recordbatch import RecordBatch

PYARROW_LOWER_BOUND_SKIP = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) < (9, 0, 0)
pytestmark = pytest.mark.skipif(PYARROW_LOWER_BOUND_SKIP, reason="lance not supported on old versions of pyarrow")


def _build_large_lance_dataset(tmp_path_factory):
    lance = pytest.importorskip("lance")

    tmp_dir = tmp_path_factory.mktemp("lance_factory")
    # Create 4 fragments of 5 rows each
    total = 0
    for frag_idx in range(4):
        vecs = [[float(i), float(i + 0.5)] for i in range(5)]
        ints = [total + i for i in range(5)]
        tbl = pa.Table.from_pydict({"vector": vecs, "big_int": ints})
        mode = "append" if frag_idx > 0 else None
        lance.write_dataset(tbl, tmp_dir, mode=mode)
        total += 5
    return str(tmp_dir)


def test_importerror_when_lance_missing():
    # Ensure lance is not already imported and make importing it fail within the factory
    import builtins
    import sys

    saved = sys.modules.pop("lance", None)

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "lance":
            raise ImportError("No lance")
        return real_import(name, *args, **kwargs)

    try:
        with patch("builtins.__import__", side_effect=fake_import):
            with pytest.raises(ImportError) as ei:
                list(
                    _lancedb_table_factory_function(
                        ds_uri="/does/not/matter",
                        open_kwargs=None,
                        fragment_ids=[0],
                        required_columns=None,
                        filter=None,
                        limit=None,
                    )
                )
        assert "Unable to import the `lance` package" in str(ei.value)
        assert "pip install daft[lance]" in str(ei.value)
    finally:
        if saved is not None:
            sys.modules["lance"] = saved


@pytest.mark.parametrize("num_frags", [1, 2, 4])
@pytest.mark.parametrize("use_filter", [False, True])
@pytest.mark.parametrize("use_required_cols", [False, True])
@pytest.mark.parametrize("push_limit", [None, 3])
def test_reconstructs_dataset_and_reads_fragments(
    tmp_path_factory, num_frags, use_filter, use_required_cols, push_limit
):
    lance = pytest.importorskip("lance")

    ds_path = _build_large_lance_dataset(tmp_path_factory)

    # Build reference via direct lance scanning
    ds_direct = lance.dataset(ds_path)
    all_frags = ds_direct.get_fragments()
    assert len(all_frags) == 4

    frag_ids = [f.fragment_id for f in all_frags[:num_frags]]

    required_columns: list[str] | None = None
    if use_required_cols:
        required_columns = ["vector", "big_int"]

    arrow_filter: pa.compute.Expression | None = None
    if use_filter:
        arrow_filter = pc.greater_equal(pc.field("big_int"), pc.scalar(all_frags[0].count_rows()))

    # Use open_kwargs to ensure reconstruction path is exercised
    open_kwargs: dict[str, Any] = {"version": None}

    # Collect records from factory
    out_batches = list(
        _lancedb_table_factory_function(
            ds_uri=ds_direct.uri,
            open_kwargs=open_kwargs,
            fragment_ids=frag_ids,
            required_columns=required_columns,
            filter=arrow_filter,
            limit=push_limit,
        )
    )

    # Build equivalent direct scan and compute expected batch count
    direct_scanner = ds_direct.scanner(
        fragments=[ds_direct.get_fragment(fid) for fid in frag_ids],
        columns=required_columns,
        filter=arrow_filter,
        limit=push_limit,
    )
    direct_batches = list(direct_scanner.to_batches())
    assert len(out_batches) == len(direct_batches)

    # If no batches expected due to filters/limits, ensure factory also yields none and exit early
    if len(direct_batches) == 0:
        assert out_batches == []
        return

    # Per-batch row count equality
    def rb_len(rb: pa.RecordBatch) -> int:
        return rb.num_rows

    out_rows_by_batch: list[int] = []
    for py_rb in out_batches:
        rb = RecordBatch._from_pyrecordbatch(py_rb)
        pdict = rb.to_pydict()
        # use any column length as row count
        first_key = next(iter(pdict))
        out_rows_by_batch.append(len(pdict[first_key]))

    direct_rows_by_batch = [rb_len(rb) for rb in direct_batches]
    assert out_rows_by_batch == direct_rows_by_batch

    # Convert to pydict to compare against direct scanner
    agg: dict[str, list] | None = None
    for py_rb in out_batches:
        rb = RecordBatch._from_pyrecordbatch(py_rb)
        pdict = rb.to_pydict()
        if agg is None:
            agg = {k: list(v) for k, v in pdict.items()}
        else:
            for k in agg.keys():
                agg[k].extend(pdict[k])

    assert agg is not None

    expect_tbl = pa.Table.from_batches(direct_batches)
    expect_pydict = expect_tbl.to_pydict()

    # Order is deterministic across both since fragments and scanner settings match
    assert agg == expect_pydict


def test_raises_when_no_fragments(tmp_path_factory):
    lance = pytest.importorskip("lance")

    ds_path = _build_large_lance_dataset(tmp_path_factory)
    ds = lance.dataset(ds_path)

    with pytest.raises(RuntimeError) as ei:
        list(
            _lancedb_table_factory_function(
                ds_uri=ds.uri,
                open_kwargs=None,
                fragment_ids=[],
                required_columns=None,
                filter=None,
                limit=None,
            )
        )
    assert "Unable to find lance fragments" in str(ei.value)


def test_invalid_fragment_id_raises(tmp_path_factory):
    lance = pytest.importorskip("lance")

    ds_path = _build_large_lance_dataset(tmp_path_factory)
    ds = lance.dataset(ds_path)
    all_frags = ds.get_fragments()
    assert len(all_frags) == 4
    invalid_id = max(f.fragment_id for f in all_frags) + 1000

    with pytest.raises(Exception) as ei:
        list(
            _lancedb_table_factory_function(
                ds_uri=ds.uri,
                open_kwargs=None,
                fragment_ids=[invalid_id],
                required_columns=None,
                filter=None,
                limit=None,
            )
        )
    # Lance typically raises ValueError for missing fragments; assert message mentions fragment
    assert "fragment" in str(ei.value).lower()


def test_open_kwargs_version_selects_correct_version(tmp_path_factory):
    lance = pytest.importorskip("lance")

    tmp_dir = tmp_path_factory.mktemp("lance_versioned")

    # v1: write initial 2 rows
    tbl_v1 = pa.Table.from_pydict({"vector": [[0.0, 0.5], [1.0, 1.5]], "big_int": [0, 1]})
    lance.write_dataset(tbl_v1, tmp_dir)

    # v2: append 2 more rows
    tbl_v2 = pa.Table.from_pydict({"vector": [[2.0, 2.5], [3.0, 3.5]], "big_int": [2, 3]})
    lance.write_dataset(tbl_v2, tmp_dir, mode="append")

    ds_v2 = lance.dataset(str(tmp_dir))
    ds_v1 = lance.dataset(str(tmp_dir), version=1)

    # Use v1 fragments but reconstruct via open_kwargs version=1
    frag_ids_v1 = [f.fragment_id for f in ds_v1.get_fragments()]

    out_batches = list(
        _lancedb_table_factory_function(
            ds_uri=ds_v2.uri,
            open_kwargs={"version": 1},
            fragment_ids=frag_ids_v1,
            required_columns=["vector", "big_int"],
            filter=None,
            limit=None,
        )
    )

    direct_batches_v1 = list(ds_v1.scanner(fragments=ds_v1.get_fragments(), columns=["vector", "big_int"]).to_batches())

    assert len(out_batches) == len(direct_batches_v1)

    # Compare content equality
    agg: dict[str, list] | None = None
    for py_rb in out_batches:
        rb = RecordBatch._from_pyrecordbatch(py_rb)
        pdict = rb.to_pydict()
        if agg is None:
            agg = {k: list(v) for k, v in pdict.items()}
        else:
            for k in agg.keys():
                agg[k].extend(pdict[k])

    assert agg is not None

    expect_tbl = pa.Table.from_batches(direct_batches_v1)
    expect_pydict = expect_tbl.to_pydict()

    assert agg == expect_pydict
