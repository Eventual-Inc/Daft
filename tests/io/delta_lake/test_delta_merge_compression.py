from __future__ import annotations

import glob

import pytest

deltalake = pytest.importorskip("deltalake")

import pyarrow as pa
import pyarrow.parquet as pq

import daft
from daft.io.delta_lake._deltalake import distributed_merge_deltalake


def _codecs(path) -> set[str]:
    return {
        pq.ParquetFile(f).metadata.row_group(0).column(0).compression
        for f in glob.glob(f"{path}/**/*.parquet", recursive=True)
    }


# Sentinel object to distinguish "not provided" from None
_OMIT = object()


def _seed(path, partitioned: bool):
    df = daft.from_arrow(
        pa.table(
            {
                "k": pa.array([1, 2], pa.int64()),
                "p": pa.array(["a", "b"]),
                "v": pa.array([10, 20], pa.int64()),
            }
        )
    )
    kwargs = {"partition_cols": ["p"]} if partitioned else {}
    df.write_deltalake(str(path), compression="none", **kwargs)


def _merge(path, compression=_OMIT):
    src = daft.from_arrow(
        pa.table(
            {
                "k": pa.array([2, 3], pa.int64()),
                "p": pa.array(["b", "c"]),
                "v": pa.array([99, 30], pa.int64()),
            }
        )
    )
    kwargs = {} if compression is _OMIT else {"compression": compression}
    builder = distributed_merge_deltalake(
        str(path), src, predicate="source.k = target.k", on="k", **kwargs
    )
    builder.when_matched_update_all().when_not_matched_insert_all().execute()


def test_distributed_merge_defaults_to_snappy(tmp_path):
    _seed(tmp_path, partitioned=False)
    _merge(tmp_path)
    # Seed file is UNCOMPRESSED; every file the merge wrote must be SNAPPY.
    assert "SNAPPY" in _codecs(tmp_path)


@pytest.mark.parametrize("partitioned", [False, True], ids=["full_rewrite", "partition_scoped"])
def test_distributed_merge_honors_compression_on_both_branches(tmp_path, partitioned):
    """The partition-scoped branch calls the builder directly and would otherwise
    silently fall back to the builder's 'none' default."""
    _seed(tmp_path, partitioned=partitioned)
    _merge(tmp_path, compression="zstd")

    codecs = _codecs(tmp_path)
    assert "ZSTD" in codecs, codecs

    table = deltalake.DeltaTable(str(tmp_path)).to_pyarrow_table().sort_by("k")
    assert table.column("k").to_pylist() == [1, 2, 3]
    assert table.column("v").to_pylist() == [10, 99, 30]


def test_distributed_merge_rejects_unencodable_codec(tmp_path):
    _seed(tmp_path, partitioned=False)
    src = daft.from_arrow(pa.table({"k": pa.array([3], pa.int64()), "p": pa.array(["c"]), "v": pa.array([30], pa.int64())}))
    with pytest.raises(ValueError, match="lzo"):
        distributed_merge_deltalake(
            str(tmp_path), src, predicate="source.k = target.k", on="k", compression="lzo"
        )


from daft.io.delta_lake._deltalake import merge_deltalake


def _seed_simple(path):
    daft.from_arrow(
        pa.table({"k": pa.array([1, 2], pa.int64()), "v": pa.array([10, 20], pa.int64())})
    ).write_deltalake(str(path), compression="none")


def _source():
    return pa.table({"k": pa.array([2, 3], pa.int64()), "v": pa.array([99, 30], pa.int64())})


def test_merge_deltalake_defaults_to_snappy(tmp_path):
    _seed_simple(tmp_path)
    (
        merge_deltalake(str(tmp_path), _source(), predicate="s.k = t.k", source_alias="s", target_alias="t")
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )
    assert "SNAPPY" in _codecs(tmp_path)


def test_merge_deltalake_honors_explicit_compression(tmp_path):
    _seed_simple(tmp_path)
    (
        merge_deltalake(
            str(tmp_path), _source(), predicate="s.k = t.k",
            source_alias="s", target_alias="t", compression="zstd",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )
    assert "ZSTD" in _codecs(tmp_path)


def test_merge_deltalake_rejects_both_compression_and_writer_properties(tmp_path):
    _seed_simple(tmp_path)
    wp = deltalake.WriterProperties(compression="ZSTD")
    with pytest.raises(ValueError, match="writer_properties"):
        merge_deltalake(
            str(tmp_path), _source(), predicate="s.k = t.k",
            source_alias="s", target_alias="t",
            compression="snappy", writer_properties=wp,
        )


def test_merge_deltalake_passes_writer_properties_through_untouched(tmp_path):
    """A `dataclasses.replace`-based implementation would silently reset these fields.

    `deltalake.WriterProperties` is decorated @dataclass but declares no fields, so
    `replace()` copies nothing and returns a default object.
    """
    _seed_simple(tmp_path)
    wp = deltalake.WriterProperties(
        compression="ZSTD", max_row_group_size=4096, write_batch_size=512
    )
    (
        merge_deltalake(
            str(tmp_path), _source(), predicate="s.k = t.k",
            source_alias="s", target_alias="t", writer_properties=wp,
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )
    # The object we handed in must be unmodified, and its other options intact.
    assert vars(wp)["max_row_group_size"] == 4096
    assert vars(wp)["write_batch_size"] == 512
    assert "ZSTD" in _codecs(tmp_path)


def test_writer_properties_replace_is_lossy_upstream_bug():
    """Pins WHY we never call dataclasses.replace on WriterProperties."""
    import dataclasses

    wp = deltalake.WriterProperties(compression="SNAPPY", max_row_group_size=4096)
    assert dataclasses.fields(wp) == ()  # decorated @dataclass, declares no fields
    replaced = dataclasses.replace(wp, compression="ZSTD")
    assert vars(replaced)["max_row_group_size"] is None  # silently destroyed
