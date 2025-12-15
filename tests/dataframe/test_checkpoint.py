"""Consolidated pytest suite for Daft checkpoint.

Notes:
- Test functions consistently use the `test_` prefix with clear scenarios and expectations.
- Helper/build functions consistently use `helper_` / `build_` prefixes.
- Covers checkpoint behavior for single-source and multi-source, plus edge cases like invalid config.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import daft
from daft import col
from daft.daft import FileFormat
from tests.conftest import get_tests_daft_runner_name


# ========== Helpers: Runtime Utilities ==========
def helper_write_dataframe(
    df: daft.DataFrame,
    fmt: FileFormat,
    root_dir: Path,
    checkpoint_config=None,
    write_mode: str = "append",
) -> daft.DataFrame:
    """Write a DataFrame to a local directory (csv/parquet/json); supports checkpoint_config."""
    root_dir.mkdir(parents=True, exist_ok=True)
    if fmt == FileFormat.Csv:
        return df.write_csv(str(root_dir), write_mode=write_mode, checkpoint_config=checkpoint_config)
    elif fmt == FileFormat.Parquet:
        return df.write_parquet(str(root_dir), write_mode=write_mode, checkpoint_config=checkpoint_config)
    elif fmt == FileFormat.Json:
        return df.write_json(str(root_dir), write_mode=write_mode, checkpoint_config=checkpoint_config)
    else:
        raise ValueError(f"Unsupported format: {fmt}")


def helper_read_dataframe(fmt: FileFormat, root_dir: Path) -> daft.DataFrame:
    """Read a DataFrame from a local directory (csv/parquet/json)."""
    if fmt == FileFormat.Csv:
        return daft.read_csv(str(root_dir))
    elif fmt == FileFormat.Parquet:
        return daft.read_parquet(str(root_dir))
    elif fmt == FileFormat.Json:
        return daft.read_json(str(root_dir))
    else:
        raise ValueError(f"Unsupported format: {fmt}")


def helper_dataframe_metrics(df: daft.DataFrame) -> dict[str, object]:
    """Unified DataFrame metrics collection."""
    pdf = df.to_pandas().reset_index(drop=True)
    rows = [dict(zip(pdf.columns, row)) for row in pdf.to_numpy()]
    id_counts = {}
    if "id" in pdf.columns:
        vc = pdf["id"].value_counts(dropna=False)
        id_counts = {k: int(v) for k, v in vc.items()}
    return {"rows": rows, "id_counts": id_counts}


def helper_assert_data_equal(df_in: daft.DataFrame, df_out: daft.DataFrame, key_cols=("id",)) -> None:
    """Assert that two DataFrames have identical content (compared via normalized representation)."""

    def helper_normalize_pydict(df: daft.DataFrame, key_cols=("id",)) -> dict[str, list[str]]:
        """Normalize DataFrame to string values and sort by key_cols for cross-format comparison."""
        sorted_df = df.sort(list(key_cols))
        pdict = sorted_df.to_pydict()
        return {col_name: [str(v) for v in vals] for col_name, vals in pdict.items()}

    in_norm = helper_normalize_pydict(df_in, key_cols)
    out_norm = helper_normalize_pydict(df_out, key_cols)
    assert set(in_norm.keys()) == set(out_norm.keys()), f"columns mismatch: {in_norm.keys()} vs {out_norm.keys()}"
    for col_name in in_norm.keys():
        assert in_norm[col_name] == out_norm[col_name], f"column {col_name} mismatch"


# ========== Helpers: Data Builders (build_ prefix) ==========
def build_df_ids_sequential(n: int = 10) -> daft.DataFrame:
    """Build sequential id data: id=0..n-1, val='v{i}'."""
    return daft.from_pydict({"id": list(range(n)), "val": [f"v{i}" for i in range(n)]})


# ========== Tests: Checkpoint Normal (single_source) ==========


@pytest.mark.parametrize("fmt", [FileFormat.Csv, FileFormat.Parquet, FileFormat.Json])
@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_checkpoint_resume_e2e(tmp_path: Path, fmt):
    """Goal: end-to-end verification of checkpoint resume behavior."""
    ck = {"key_column": "id"}
    df_all = build_df_ids_sequential(100)
    df_first_50 = df_all.where(col("id") <= 50)

    root_dir = tmp_path / f"ckpt_{fmt.ext()}"
    helper_write_dataframe(df_first_50, fmt, root_dir, checkpoint_config=ck)
    df_after_first = helper_read_dataframe(fmt, root_dir)
    old_ids = set(df_after_first.select("id").to_pydict()["id"])

    helper_write_dataframe(df_all, fmt, root_dir, checkpoint_config=ck)
    df_final = helper_read_dataframe(fmt, root_dir)
    final_ids = set(df_final.select("id").to_pydict()["id"])
    new_ids = sorted(list(final_ids - old_ids))
    assert new_ids == list(range(51, 100)), f"Resume ids mismatch: {new_ids}"
    assert final_ids == set(range(100)), f"Final ids mismatch: {sorted(list(final_ids))}"


@pytest.mark.parametrize("fmt", [FileFormat.Csv, FileFormat.Parquet, FileFormat.Json])
@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_checkpoint_resume_e2e_with_custom_num_cpus(tmp_path: Path, fmt):
    """Goal: end-to-end verification of checkpoint resume behavior."""
    ck = {"key_column": "id", "num_buckets": 4, "num_cpus": 2.0}
    df_all = build_df_ids_sequential(100)
    df_first_50 = df_all.where(col("id") <= 50)

    root_dir = tmp_path / f"ckpt_{fmt.ext()}"
    helper_write_dataframe(df_first_50, fmt, root_dir, checkpoint_config=ck)
    df_after_first = helper_read_dataframe(fmt, root_dir)
    old_ids = set(df_after_first.select("id").to_pydict()["id"])

    helper_write_dataframe(df_all, fmt, root_dir, checkpoint_config=ck)
    df_final = helper_read_dataframe(fmt, root_dir)
    final_ids = set(df_final.select("id").to_pydict()["id"])
    new_ids = sorted(list(final_ids - old_ids))

    assert new_ids == list(range(51, 100)), f"Resume ids mismatch: {new_ids}"
    assert final_ids == set(range(100)), f"Final ids mismatch: {sorted(list(final_ids))}"


@pytest.mark.parametrize("input_fmt", [FileFormat.Csv, FileFormat.Parquet, FileFormat.Json])
@pytest.mark.parametrize("output_fmt", [FileFormat.Csv, FileFormat.Parquet, FileFormat.Json])
@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_checkpoint_multi_format(tmp_path: Path, input_fmt, output_fmt):
    """Goal: end-to-end cross-format verification (read from fmts x write to fmts)."""
    ck = {"key_column": "id"}
    df_src = build_df_ids_sequential(200)

    input_dir = tmp_path / f"input_{input_fmt.ext()}"
    helper_write_dataframe(df_src, input_fmt, input_dir, checkpoint_config=ck)
    df_in = helper_read_dataframe(input_fmt, input_dir)

    output_dir = tmp_path / f"output_{input_fmt.ext()}_to_{output_fmt.ext()}"
    helper_write_dataframe(df_in, output_fmt, output_dir, checkpoint_config=ck)
    df_out = helper_read_dataframe(output_fmt, output_dir)

    helper_assert_data_equal(df_in, df_out, key_cols=("id",))


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_checkpoint_from_glob_path(tmp_path: Path):
    """Goal: verify from_glob_path + checkpoint only appends missing keys in single-source.

    Scenario:
    - Create 10 text files in a temporary directory.
    - Read listing via from_glob_path (path/size/num_rows), sort by path.
    - First write 5 files as seed, then write all 10 with checkpoint.
    Expected: destination contains 10 rows total, each path appears exactly once.
    """
    # Create source directory and files
    src_dir = tmp_path / "glob_src"
    src_dir.mkdir(parents=True, exist_ok=True)
    for i in range(10):
        (src_dir / f"file_{i}.txt").write_text("x" * i)

    # Read listing and sort; build seed and full sets
    df_all = daft.from_glob_path(str(src_dir / "*.txt"))
    df_all = df_all.sort("path")
    df_all.show(100)
    df_seed = df_all.limit(5)

    # Write to destination with checkpoint (use 'path' as key)
    dest = tmp_path / "ckpt_from_glob"
    ck = {"key_column": "path"}
    helper_write_dataframe(df_seed, FileFormat.Parquet, dest, checkpoint_config=ck)
    helper_write_dataframe(df_all, FileFormat.Parquet, dest, checkpoint_config=ck)

    # Read back and verify 10 unique paths (no duplicates)
    out_df = helper_read_dataframe(FileFormat.Parquet, dest)
    metrics = helper_dataframe_metrics(out_df)
    assert len(metrics["rows"]) == 10, f"rows mismatch: {len(metrics['rows'])}"
    assert all(v == 1 for v in metrics["id_counts"].values()), f"id_counts mismatch: {metrics['id_counts']}"


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_checkpoint_single_source_with_filter_op(tmp_path: Path):
    dest = tmp_path / "ckpt_single_with_filter"
    ck = {"key_column": "id"}

    helper_write_dataframe(build_df_ids_sequential(10), FileFormat.Parquet, dest, checkpoint_config=ck)

    df_all = build_df_ids_sequential(20)
    df_filtered = df_all.where(col("id") < 15)
    helper_write_dataframe(df_filtered, FileFormat.Parquet, dest, checkpoint_config=ck)

    out_df = helper_read_dataframe(FileFormat.Parquet, dest)
    metrics = helper_dataframe_metrics(out_df)
    assert len(metrics["rows"]) == 15, f"rows mismatch: {len(metrics['rows'])}"


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_checkpoint_single_source_with_show_and_collect_op(tmp_path: Path):
    dest = tmp_path / "ckpt_single_with_show"
    ck = {"key_column": "id"}

    helper_write_dataframe(build_df_ids_sequential(10), FileFormat.Parquet, dest, checkpoint_config=ck)

    df_all = build_df_ids_sequential(20)
    df_all.show()
    df_all.collect()
    df_all = df_all.collect()

    helper_write_dataframe(df_all, FileFormat.Parquet, dest, checkpoint_config=ck)

    out_df = helper_read_dataframe(FileFormat.Parquet, dest)
    metrics = helper_dataframe_metrics(out_df)
    assert len(metrics["rows"]) == 20, f"rows mismatch: {len(metrics['rows'])}"
    assert all(v == 1 for v in metrics["id_counts"].values()), f"id_counts mismatch: {metrics['id_counts']}"


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_checkpoint_single_source_with_write_op(tmp_path: Path):
    dest_1 = tmp_path / "ckpt_single_with_write_1"
    dest_2 = tmp_path / "ckpt_single_with_write_2"
    ck = {"key_column": "id"}

    helper_write_dataframe(build_df_ids_sequential(10), FileFormat.Parquet, dest_1, checkpoint_config=ck)

    df_1 = build_df_ids_sequential(20)
    df_2 = helper_write_dataframe(df_1, FileFormat.Parquet, dest_1, checkpoint_config=ck)
    helper_write_dataframe(df_2, FileFormat.Parquet, dest_2, checkpoint_config=ck)

    out_df_1 = helper_read_dataframe(FileFormat.Parquet, dest_1)
    metrics = helper_dataframe_metrics(out_df_1)
    assert len(metrics["rows"]) == 20, f"rows mismatch: {len(metrics['rows'])}"
    assert all(v == 1 for v in metrics["id_counts"].values()), f"id_counts mismatch: {metrics['id_counts']}"

    out_df_2 = helper_read_dataframe(FileFormat.Parquet, dest_2)
    metrics = helper_dataframe_metrics(out_df_2)
    assert len(metrics["rows"]) == 1, f"rows mismatch: {len(metrics['rows'])}"


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_checkpoint_single_source_with_repartition_and_into_batches_op(tmp_path: Path):
    dest_1 = tmp_path / "ckpt_single_with_repartition_1"
    dest_2 = tmp_path / "ckpt_single_with_repartition_2"
    ck = {"key_column": "id"}
    helper_write_dataframe(build_df_ids_sequential(10), FileFormat.Parquet, dest_1, checkpoint_config=ck)
    df_all = build_df_ids_sequential(20)
    df1 = df_all.repartition(4)
    df2 = df_all.into_batches(2)
    helper_write_dataframe(df1, FileFormat.Parquet, dest_1, checkpoint_config=ck)
    helper_write_dataframe(df2, FileFormat.Parquet, dest_2, checkpoint_config=ck)

    out_df_1 = helper_read_dataframe(FileFormat.Parquet, dest_1)
    metrics = helper_dataframe_metrics(out_df_1)
    assert len(metrics["rows"]) == 20, f"rows mismatch: {len(metrics['rows'])}"
    assert all(v == 1 for v in metrics["id_counts"].values()), f"id_counts mismatch: {metrics['id_counts']}"

    out_df_2 = helper_read_dataframe(FileFormat.Parquet, dest_2)
    metrics = helper_dataframe_metrics(out_df_2)
    assert len(metrics["rows"]) == 20, f"rows mismatch: {len(metrics['rows'])}"
    assert all(v == 1 for v in metrics["id_counts"].values()), f"id_counts mismatch: {metrics['id_counts']}"


# ========== Tests: Checkpoint Boundaries (multi-source and invalid config) ==========


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_checkpoint_multi_source_join(tmp_path: Path):
    """Goal: verify checkpoint will raise an error for multi-source join scenarios."""
    dest = tmp_path / "ckpt_join"
    ck = {"key_column": "id"}

    helper_write_dataframe(build_df_ids_sequential(5), FileFormat.Parquet, dest)

    left = daft.from_pydict({"id": list(range(10)), "val": [f"v{i}" for i in range(10)]})
    right = daft.from_pydict({"id": list(range(10)), "rv": [f"v{i}" for i in range(50, 60)]})
    df = left.join(right, left_on=left["id"], right_on=right["id"], how="inner")
    df = df.exclude("rv")
    with pytest.raises(daft.exceptions.DaftCoreException, match="Checkpoint requires single-source logical plan"):
        helper_write_dataframe(df, FileFormat.Parquet, dest, checkpoint_config=ck)

    df.collect()
    helper_write_dataframe(df, FileFormat.Parquet, dest, checkpoint_config=ck)
    out_df = helper_read_dataframe(FileFormat.Parquet, dest)
    metrics = helper_dataframe_metrics(out_df)
    assert len(metrics["rows"]) == 10, f"rows mismatch: {len(metrics['rows'])}"

    df_result = build_df_ids_sequential(10)
    helper_assert_data_equal(out_df, df_result, key_cols=("id",))


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_checkpoint_multi_source_concat(tmp_path: Path):
    """Goal: verify checkpoint will raise an error for multi-source concat scenarios."""
    dest = tmp_path / "ckpt_concat"
    ck = {"key_column": "id"}

    helper_write_dataframe(build_df_ids_sequential(5), FileFormat.Parquet, dest, checkpoint_config=ck)

    df1 = daft.from_pydict({"id": list(range(5)), "val": [f"a{i}" for i in range(5)]})
    df2 = daft.from_pydict({"id": list(range(5)), "val": [f"b{i}" for i in range(5)]})
    with pytest.raises(daft.exceptions.DaftCoreException, match="Checkpoint requires single-source logical plan"):
        helper_write_dataframe(df1.concat(df2), FileFormat.Parquet, dest, checkpoint_config=ck)


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_checkpoint_multisource_union(tmp_path: Path):
    """Goal: verify checkpoint will raise an error for union (set union) scenarios."""
    dest = tmp_path / "ckpt_union"
    ck = {"key_column": "id"}

    helper_write_dataframe(build_df_ids_sequential(7), FileFormat.Parquet, dest, checkpoint_config=ck)

    df1 = daft.from_pydict({"id": [0, 1, 2, 3, 4], "val": [f"a{i}" for i in range(5)]})
    df2 = daft.from_pydict({"id": [3, 4, 5, 6], "val": [f"b{i}" for i in range(4)]})
    df_union = df1.union(df2)

    with pytest.raises(daft.exceptions.DaftCoreException, match="Checkpoint requires single-source logical plan"):
        helper_write_dataframe(df_union, FileFormat.Parquet, dest, checkpoint_config=ck)


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_checkpoint_multisource_intersect(tmp_path: Path):
    """Goal: verify checkpoint will raise an error for intersect (set intersection) scenarios."""
    dest = tmp_path / "ckpt_intersect"
    ck = {"key_column": "id"}

    helper_write_dataframe(build_df_ids_sequential(5), FileFormat.Parquet, dest, checkpoint_config=ck)

    df1 = daft.from_pydict({"id": [1, 2, 3], "b": [4, 5, 6]})
    df2 = daft.from_pydict({"id": [1, 2, 3], "b": [4, 8, 6]})
    df_inter = df1.intersect(df2)

    with pytest.raises(daft.exceptions.DaftCoreException, match="Checkpoint requires single-source logical plan"):
        helper_write_dataframe(df_inter, FileFormat.Parquet, dest, checkpoint_config=ck)


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_checkpoint_multisource_except_all(tmp_path: Path):
    """Goal: verify checkpoint will raise an error for except_all (multiset difference) scenarios."""
    dest = tmp_path / "ckpt_except_all"
    ck = {"key_column": "id"}

    helper_write_dataframe(build_df_ids_sequential(5), FileFormat.Parquet, dest, checkpoint_config=ck)

    df1 = daft.from_pydict({"id": [1, 1, 2, 2], "b": [4, 4, 6, 6]})
    df2 = daft.from_pydict({"id": [1, 2, 2], "b": [4, 6, 6]})
    df_except = df1.except_all(df2)

    with pytest.raises(daft.exceptions.DaftCoreException, match="Checkpoint requires single-source logical plan"):
        helper_write_dataframe(df_except, FileFormat.Parquet, dest, checkpoint_config=ck)


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_checkpoint_empty_dataframe(tmp_path: Path):
    """Goal: when writing an empty DataFrame with checkpoint, no rows are produced."""
    dest = tmp_path / "ckpt_empty_df"
    ck = {"key_column": "id"}

    df_empty = daft.from_pydict({"id": [], "val": []})
    helper_write_dataframe(df_empty, FileFormat.Parquet, dest, checkpoint_config=ck)
    out_df = helper_read_dataframe(FileFormat.Parquet, dest)
    metrics = helper_dataframe_metrics(out_df)
    rows = metrics["rows"]
    assert len(rows) == 0


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_checkpoint_empty_checkpoint(tmp_path: Path):
    """Goal: when writing a dataframe while checkpoint is empty, every rows will be written."""
    dest = tmp_path / "ckpt_empty_ck"
    ck = {"key_column": "id"}

    df_empty = daft.from_pydict({"id": [], "val": []})
    df_100 = build_df_ids_sequential(100)
    helper_write_dataframe(df_empty, FileFormat.Parquet, dest, checkpoint_config=ck)
    helper_write_dataframe(df_100, FileFormat.Parquet, dest, checkpoint_config=ck)
    out_df = helper_read_dataframe(FileFormat.Parquet, dest)
    metrics = helper_dataframe_metrics(out_df)
    rows = metrics["rows"]
    assert len(rows) == 100


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_checkpoint_unhashable_keys_raises_typeerror_when_init(tmp_path: Path):
    """Goal: unhashable keys must raise errors when initializing checkpoint actor."""
    # Destination and config
    dest = tmp_path / "ckpt_unhashable_keys"
    fmt = FileFormat.Parquet
    ck = {"key_column": "key", "num_buckets": 2}

    # Seed write: unhashable keys
    df_seed = daft.from_pydict({"key": [[1], [2], [3]], "val": ["x", "y", "z"]})
    helper_write_dataframe(df_seed, fmt, dest, checkpoint_config=ck)

    # Second write: lead to TypeError when init checkpoint actor
    df_bad = daft.from_pydict({"key": [[1], [2]], "val": ["p", "q"]})
    with pytest.raises(Exception):
        helper_write_dataframe(df_bad, fmt, dest, checkpoint_config=ck)


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_checkpoint_unhashable_keys_raises_typeerror_when_filter(tmp_path: Path):
    """Goal: unhashable keys must raise errors when filtering."""
    # Destination and config
    dest = tmp_path / "ckpt_unhashable_keys"
    fmt = FileFormat.Parquet
    ck = {"key_column": "key", "num_buckets": 2}

    # Seed write: hashable keys (strings)
    df_seed = daft.from_pydict({"key": ["a", "b", "c"], "val": ["x", "y", "z"]})
    helper_write_dataframe(df_seed, fmt, dest, checkpoint_config=ck)

    # Second write: unhashable keys (lists) — lead to TypeError when filter
    df_bad = daft.from_pydict({"key": [[1], [2]], "val": ["p", "q"]})
    with pytest.raises(Exception):
        helper_write_dataframe(df_bad, fmt, dest, checkpoint_config=ck)


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_checkpoint_invalid_config_raises(tmp_path: Path):
    """Goal: invalid checkpoint configuration must raise errors.

    Scenario: missing key_column/num_buckets.
    Expected: raise exception on write.
    """
    dest = tmp_path / "ckpt_invalid"
    df = build_df_ids_sequential(10)
    helper_write_dataframe(df, FileFormat.Parquet, dest, checkpoint_config={"key_column": "id"})

    # missing key_column
    with pytest.raises(ValueError, match="checkpoint_config_dict must contain 'key_column' key"):
        helper_write_dataframe(df, FileFormat.Parquet, dest, checkpoint_config={"num_buckets": 4})
    # missing key_column
    with pytest.raises(ValueError, match="checkpoint_config_dict must contain 'key_column' key"):
        helper_write_dataframe(df, FileFormat.Parquet, dest, checkpoint_config={"num_cpus": 1})
    # missing key_column with both num_buckets & num_cpus present
    with pytest.raises(ValueError, match="checkpoint_config_dict must contain 'key_column' key"):
        helper_write_dataframe(df, FileFormat.Parquet, dest, checkpoint_config={"num_buckets": 4, "num_cpus": 1})
    # TODO: add test for invalid key_column
    # invalid key_column
    # with pytest.raises(Exception):
    #     helper_write_dataframe(
    #         df, FileFormat.Parquet, dest, checkpoint_config={"key_column": "not_id"}
    #     )
    # invalid string num_cpus
    with pytest.raises(ValueError, match="'num_cpus' must be numeric"):
        helper_write_dataframe(
            df, FileFormat.Parquet, dest, checkpoint_config={"key_column": "id", "num_cpus": "hello"}
        )
    # invalid negative num_cpus
    with pytest.raises(ValueError, match="'num_cpus' must be > 0"):
        helper_write_dataframe(df, FileFormat.Parquet, dest, checkpoint_config={"key_column": "id", "num_cpus": -1})
    # invalid num_buckets float not integer
    with pytest.raises(ValueError, match="'num_buckets' must be a positive integer"):
        helper_write_dataframe(df, FileFormat.Parquet, dest, checkpoint_config={"key_column": "id", "num_buckets": 4.1})
    # invalid num_buckets negative
    with pytest.raises(ValueError, match="'num_buckets' must be a positive integer"):
        helper_write_dataframe(df, FileFormat.Parquet, dest, checkpoint_config={"key_column": "id", "num_buckets": -1})
    # invalid checkpoint_config(not dict)
    with pytest.raises(Exception):
        helper_write_dataframe(df, FileFormat.Parquet, dest, checkpoint_config=100)
    # placement group ready timeout
    with pytest.raises(RuntimeError, match="Checkpoint resource reservation timed out"):
        helper_write_dataframe(
            df, FileFormat.Parquet, dest, checkpoint_config={"key_column": "id", "num_buckets": 4, "num_cpus": 10000}
        )
