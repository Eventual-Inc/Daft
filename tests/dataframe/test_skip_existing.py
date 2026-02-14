"""Consolidated pytest suite for Daft skip_existing.

Notes:
- Test functions consistently use the `test_` prefix with clear scenarios and expectations.
- Helper/build functions consistently use `helper_` / `build_` prefixes.
- Covers skip_existing behavior for single-source and multi-source, plus edge cases like invalid config.
"""

from __future__ import annotations

import io
from pathlib import Path

import pytest

import daft
from daft import col
from daft.daft import FileFormat, WriteMode
from tests.conftest import get_tests_daft_runner_name


# ========== Helpers: Runtime Utilities ==========
def helper_write_dataframe(
    df: daft.DataFrame,
    fmt: FileFormat,
    root_dir: Path,
    skip_existing_config=None,
    write_mode: str = "append",
) -> daft.DataFrame:
    """Write a DataFrame to a local directory (csv/parquet/json)."""
    root_dir.mkdir(parents=True, exist_ok=True)

    has_existing_data = any(root_dir.rglob(f"*.{fmt.ext()}"))

    if skip_existing_config is not None and has_existing_data:
        if not isinstance(skip_existing_config, dict) or "key_column" not in skip_existing_config:
            raise ValueError("skip_existing_config must be a dict with key_column")
        num_key_filter_partitions = skip_existing_config.get("num_buckets")
        num_cpus = skip_existing_config.get("num_cpus")
        df = df.skip_existing(
            root_dir,
            on=skip_existing_config["key_column"],
            file_format=fmt,
            num_key_filter_partitions=4 if num_key_filter_partitions is None else num_key_filter_partitions,
            num_cpus=1.0 if num_cpus is None else num_cpus,
        )
    if fmt == FileFormat.Csv:
        return df.write_csv(str(root_dir), write_mode=write_mode)
    elif fmt == FileFormat.Parquet:
        return df.write_parquet(str(root_dir), write_mode=write_mode)
    elif fmt == FileFormat.Json:
        return df.write_json(str(root_dir), write_mode=write_mode)
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


# ========== Tests: skip_existing (single_source) ==========


@pytest.mark.parametrize("fmt", [FileFormat.Csv, FileFormat.Parquet, FileFormat.Json])
@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_skip_existing_e2e(tmp_path: Path, fmt):
    """Goal: end-to-end verification of skip_existing behavior."""
    cfg = {"key_column": "id"}
    df_all = build_df_ids_sequential(100)
    df_first_50 = df_all.where(col("id") <= 50)

    root_dir = tmp_path / f"existing_{fmt.ext()}"
    helper_write_dataframe(df_first_50, fmt, root_dir, skip_existing_config=cfg)
    df_after_first = helper_read_dataframe(fmt, root_dir)
    old_ids = set(df_after_first.select("id").to_pydict()["id"])

    helper_write_dataframe(df_all, fmt, root_dir, skip_existing_config=cfg)
    df_final = helper_read_dataframe(fmt, root_dir)
    final_ids = set(df_final.select("id").to_pydict()["id"])
    new_ids = sorted(list(final_ids - old_ids))
    assert new_ids == list(range(51, 100)), f"skip_existing ids mismatch: {new_ids}"
    assert final_ids == set(range(100)), f"Final ids mismatch: {sorted(list(final_ids))}"


@pytest.mark.parametrize("fmt", [FileFormat.Csv, FileFormat.Parquet, FileFormat.Json])
@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_skip_existing_e2e_with_custom_num_cpus(tmp_path: Path, fmt):
    """Goal: end-to-end verification of skip_existing behavior."""
    cfg = {"key_column": "id", "num_buckets": 4, "num_cpus": 2.0}
    df_all = build_df_ids_sequential(100)
    df_first_50 = df_all.where(col("id") <= 50)

    root_dir = tmp_path / f"existing_{fmt.ext()}"
    helper_write_dataframe(df_first_50, fmt, root_dir, skip_existing_config=cfg)
    df_after_first = helper_read_dataframe(fmt, root_dir)
    old_ids = set(df_after_first.select("id").to_pydict()["id"])

    helper_write_dataframe(df_all, fmt, root_dir, skip_existing_config=cfg)
    df_final = helper_read_dataframe(fmt, root_dir)
    final_ids = set(df_final.select("id").to_pydict()["id"])
    new_ids = sorted(list(final_ids - old_ids))

    assert new_ids == list(range(51, 100)), f"skip_existing ids mismatch: {new_ids}"
    assert final_ids == set(range(100)), f"Final ids mismatch: {sorted(list(final_ids))}"


@pytest.mark.parametrize("input_fmt", [FileFormat.Csv, FileFormat.Parquet, FileFormat.Json])
@pytest.mark.parametrize("output_fmt", [FileFormat.Csv, FileFormat.Parquet, FileFormat.Json])
@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_skip_existing_multi_format(tmp_path: Path, input_fmt, output_fmt):
    """Goal: end-to-end cross-format verification (read from fmts x write to fmts)."""
    df_src = build_df_ids_sequential(200)

    input_dir = tmp_path / f"input_{input_fmt.ext()}"
    helper_write_dataframe(df_src, input_fmt, input_dir)
    df_in = helper_read_dataframe(input_fmt, input_dir)

    output_dir = tmp_path / f"output_{input_fmt.ext()}_to_{output_fmt.ext()}"
    helper_write_dataframe(df_in.limit(50), output_fmt, output_dir)
    helper_write_dataframe(df_in, output_fmt, output_dir, skip_existing_config={"key_column": "id"})
    df_out = helper_read_dataframe(output_fmt, output_dir)

    metrics = helper_dataframe_metrics(df_out)
    assert len(metrics["rows"]) == 200
    assert all(v == 1 for v in metrics["id_counts"].values()), f"id_counts mismatch: {metrics['id_counts']}"
    helper_assert_data_equal(df_in, df_out, key_cols=("id",))


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_skip_existing_from_glob_path(tmp_path: Path):
    """Goal: verify from_glob_path + skip_existing only appends missing keys in single-source.

    Scenario:
    - Create 10 text files in a temporary directory.
    - Read listing via from_glob_path (path/size/num_rows), sort by path.
    - First write 5 files as seed, then write all 10 with skip_existing.
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

    # Write to destination with skip_existing (use 'path' as key)
    dest = tmp_path / "existing_from_glob"
    cfg = {"key_column": "path"}
    helper_write_dataframe(df_seed, FileFormat.Parquet, dest, skip_existing_config=cfg)
    helper_write_dataframe(df_all, FileFormat.Parquet, dest, skip_existing_config=cfg)

    # Read back and verify 10 unique paths (no duplicates)
    out_df = helper_read_dataframe(FileFormat.Parquet, dest)
    paths = out_df.select("path").to_pydict()["path"]
    assert len(paths) == 10
    assert len(set(paths)) == 10


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_skip_existing_single_source_with_filter_op(tmp_path: Path):
    dest = tmp_path / "existing_single_with_filter"
    cfg = {"key_column": "id"}

    helper_write_dataframe(build_df_ids_sequential(10), FileFormat.Parquet, dest, skip_existing_config=cfg)

    df_all = build_df_ids_sequential(20)
    df_filtered = df_all.where(col("id") < 15)
    helper_write_dataframe(df_filtered, FileFormat.Parquet, dest, skip_existing_config=cfg)

    out_df = helper_read_dataframe(FileFormat.Parquet, dest)
    metrics = helper_dataframe_metrics(out_df)
    assert len(metrics["rows"]) == 15, f"rows mismatch: {len(metrics['rows'])}"


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_skip_existing_single_source_with_show_and_collect_op(tmp_path: Path):
    dest = tmp_path / "existing_single_with_show"
    cfg = {"key_column": "id"}

    helper_write_dataframe(build_df_ids_sequential(10), FileFormat.Parquet, dest, skip_existing_config=cfg)

    df_all = build_df_ids_sequential(20)
    df_all.show()
    df_all.collect()
    df_all = df_all.collect()

    helper_write_dataframe(df_all, FileFormat.Parquet, dest, skip_existing_config=cfg)

    out_df = helper_read_dataframe(FileFormat.Parquet, dest)
    metrics = helper_dataframe_metrics(out_df)
    assert len(metrics["rows"]) == 20, f"rows mismatch: {len(metrics['rows'])}"
    assert all(v == 1 for v in metrics["id_counts"].values()), f"id_counts mismatch: {metrics['id_counts']}"


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_skip_existing_single_source_with_write_op(tmp_path: Path):
    dest_1 = tmp_path / "existing_single_with_write_1"
    dest_2 = tmp_path / "existing_single_with_write_2"
    cfg = {"key_column": "id"}

    helper_write_dataframe(build_df_ids_sequential(10), FileFormat.Parquet, dest_1, skip_existing_config=cfg)

    df_1 = build_df_ids_sequential(20)
    df_2 = helper_write_dataframe(df_1, FileFormat.Parquet, dest_1, skip_existing_config=cfg)
    helper_write_dataframe(df_2, FileFormat.Parquet, dest_2, skip_existing_config={"key_column": "path"})

    out_df_1 = helper_read_dataframe(FileFormat.Parquet, dest_1)
    metrics = helper_dataframe_metrics(out_df_1)
    assert len(metrics["rows"]) == 20, f"rows mismatch: {len(metrics['rows'])}"
    assert all(v == 1 for v in metrics["id_counts"].values()), f"id_counts mismatch: {metrics['id_counts']}"

    out_df_2 = helper_read_dataframe(FileFormat.Parquet, dest_2)
    metrics = helper_dataframe_metrics(out_df_2)
    assert len(metrics["rows"]) == 1, f"rows mismatch: {len(metrics['rows'])}"


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_skip_existing_single_source_with_repartition_and_into_batches_op(tmp_path: Path):
    dest_1 = tmp_path / "existing_single_with_repartition_1"
    dest_2 = tmp_path / "existing_single_with_repartition_2"
    cfg = {"key_column": "id"}
    helper_write_dataframe(build_df_ids_sequential(10), FileFormat.Parquet, dest_1, skip_existing_config=cfg)
    df_all = build_df_ids_sequential(20)
    df1 = df_all.repartition(4)
    df2 = df_all.into_batches(2)
    helper_write_dataframe(df1, FileFormat.Parquet, dest_1, skip_existing_config=cfg)
    helper_write_dataframe(df2, FileFormat.Parquet, dest_2, skip_existing_config=cfg)

    out_df_1 = helper_read_dataframe(FileFormat.Parquet, dest_1)
    metrics = helper_dataframe_metrics(out_df_1)
    assert len(metrics["rows"]) == 20, f"rows mismatch: {len(metrics['rows'])}"
    assert all(v == 1 for v in metrics["id_counts"].values()), f"id_counts mismatch: {metrics['id_counts']}"

    out_df_2 = helper_read_dataframe(FileFormat.Parquet, dest_2)
    metrics = helper_dataframe_metrics(out_df_2)
    assert len(metrics["rows"]) == 20, f"rows mismatch: {len(metrics['rows'])}"
    assert all(v == 1 for v in metrics["id_counts"].values()), f"id_counts mismatch: {metrics['id_counts']}"


# ========== Tests: skip_existing API ==========


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_skip_existing_multiple_calls_chain_semantics(tmp_path: Path):
    df = daft.from_pydict({"id": [1, 2, 3], "val": ["a", "b", "c"]})
    ckpt_a = tmp_path / "ckpt_a"
    ckpt_b = tmp_path / "ckpt_b"
    ckpt_a.mkdir(parents=True, exist_ok=True)
    ckpt_b.mkdir(parents=True, exist_ok=True)

    (ckpt_a / "part-0.csv").write_text("id,val\n1,a\n", encoding="utf-8")
    (ckpt_b / "part-0.csv").write_text("id,val\n2,b\n", encoding="utf-8")

    out = (
        df.skip_existing(ckpt_a, on="id", file_format="csv").skip_existing(ckpt_b, on="id", file_format="csv").collect()
    )
    assert out.select("id").to_pydict()["id"] == [3]


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_skip_existing_multiple_calls_distinct_key_columns_are_applied_in_order(tmp_path: Path):
    df = daft.from_pydict({"id": [1, 2, 3], "path": ["a", "b", "c"]})
    ckpt_id = tmp_path / "ckpt_id"
    ckpt_path = tmp_path / "ckpt_path"
    ckpt_id.mkdir(parents=True, exist_ok=True)
    ckpt_path.mkdir(parents=True, exist_ok=True)

    (ckpt_id / "part-0.csv").write_text("id\n1\n", encoding="utf-8")
    (ckpt_path / "part-0.csv").write_text("path\nb\n", encoding="utf-8")

    out = (
        df.skip_existing(ckpt_id, on="id", file_format="csv")
        .skip_existing(ckpt_path, on="path", file_format="csv")
        .collect()
    )
    assert out.select("id").to_pydict()["id"] == [3]


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_skip_existing_on_both_join_branches_maps_to_correct_inputs(tmp_path: Path):
    left = daft.from_pydict({"id": [1, 2, 3], "l": ["l1", "l2", "l3"]})
    right = daft.from_pydict({"rid": [1, 2, 3], "r": ["r1", "r2", "r3"]})

    ckpt_left = tmp_path / "ckpt_left"
    ckpt_right = tmp_path / "ckpt_right"
    ckpt_left.mkdir(parents=True, exist_ok=True)
    ckpt_right.mkdir(parents=True, exist_ok=True)

    (ckpt_left / "part-0.csv").write_text("id\n1\n", encoding="utf-8")
    (ckpt_right / "part-0.csv").write_text("rid\n2\n", encoding="utf-8")

    left = left.skip_existing(ckpt_left, on="id", file_format="csv")
    right = right.skip_existing(ckpt_right, on="rid", file_format="csv")
    out = left.join(right, left_on="id", right_on="rid", how="inner").collect()
    assert out.select("id").to_pydict()["id"] == [3]


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_skip_existing_multiple_calls_are_cumulative(tmp_path: Path):
    df = daft.from_pydict({"id": [1, 2, 3], "val": ["a", "b", "c"]})
    ckpt_a = tmp_path / "a"
    ckpt_b = tmp_path / "b"
    ckpt_a.mkdir(parents=True, exist_ok=True)
    ckpt_b.mkdir(parents=True, exist_ok=True)

    (ckpt_a / "part-0.csv").write_text("id,val\n1,a\n", encoding="utf-8")
    (ckpt_b / "part-0.csv").write_text("id,val\n2,b\n", encoding="utf-8")

    out = (
        df.skip_existing(ckpt_a, on="id", file_format="csv").skip_existing(ckpt_b, on="id", file_format="csv").collect()
    )
    assert out.select("id").to_pydict()["id"] == [3]


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_skip_existing_multiple_paths_single_call(tmp_path: Path):
    df = daft.from_pydict({"id": [1, 2, 3], "val": ["a", "b", "c"]})
    ckpt_a = tmp_path / "ckpt_a_multi"
    ckpt_b = tmp_path / "ckpt_b_multi"
    ckpt_a.mkdir(parents=True, exist_ok=True)
    ckpt_b.mkdir(parents=True, exist_ok=True)

    (ckpt_a / "part-0.csv").write_text("id\n1\n", encoding="utf-8")
    (ckpt_b / "part-0.csv").write_text("id\n2\n", encoding="utf-8")

    out = df.skip_existing([ckpt_a, ckpt_b], on="id", file_format="csv").collect()
    assert out.select("id").to_pydict()["id"] == [3]


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_skip_existing_csv_reader_args_applied(tmp_path: Path):
    ckpt_dir = tmp_path / "ckpt_csv_custom_delim"
    ckpt_dir.mkdir(parents=True, exist_ok=True)
    (ckpt_dir / "part-0.csv").write_text("id|val\n1|a\n2|b\n", encoding="utf-8")

    df = daft.from_pydict({"id": [1, 2, 3], "val": ["a", "b", "c"]})

    with pytest.raises(RuntimeError) as excinfo:
        df.skip_existing(ckpt_dir, on="id", file_format="csv").collect()
    msg = str(excinfo.value)
    assert "[skip_existing] Unable to read keys" in msg
    assert "id" in msg

    out = df.skip_existing(ckpt_dir, on="id", file_format="csv", delimiter="|").collect()
    assert out.select("id").to_pydict()["id"] == [3]


@pytest.mark.parametrize("fmt_alias", ["jsonl", "ndjson"])
@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_skip_existing_jsonl_and_ndjson_format_aliases(tmp_path: Path, fmt_alias: str):
    ckpt_dir = tmp_path / f"ckpt_{fmt_alias}_alias"
    seed_df = daft.from_pydict({"id": [1, 2], "val": ["a", "b"]})
    helper_write_dataframe(seed_df, FileFormat.Json, ckpt_dir)

    df = daft.from_pydict({"id": [1, 2, 3], "val": ["a", "b", "c"]})
    out = df.skip_existing(ckpt_dir, on="id", file_format=fmt_alias).collect()
    assert out.select("id").to_pydict()["id"] == [3]


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_skip_existing_batch_size_visible_in_explain(tmp_path: Path):
    root_dir = tmp_path / "out"
    seed_df = daft.from_pydict({"id": [1, 2], "val": ["a", "b"]})
    seed_df.write_parquet(str(root_dir), write_mode="overwrite")

    df = daft.from_pydict({"id": [1, 2, 3], "val": ["a", "b", "c"]}).skip_existing(
        root_dir,
        on="id",
        file_format="parquet",
        key_filter_batch_size=10,
        key_filter_loading_batch_size=1,
    )

    buf = io.StringIO()
    from daft.context import get_context

    io_config = get_context().daft_planning_config.default_io_config
    write_builder = df._builder.write_tabular(
        root_dir=root_dir,
        write_mode=WriteMode.from_str("append"),
        file_format=FileFormat.Parquet,
        io_config=io_config,
    )
    specs = write_builder._builder.get_skip_existing_specs()
    assert len(specs) == 1
    assert specs[0]["key_filter_batch_size"] == 10
    assert specs[0]["key_filter_loading_batch_size"] == 1

    pred = (col("id") > 0)._expr
    applied = write_builder._builder.apply_skip_existing_predicates([pred])
    from daft.logical.builder import LogicalPlanBuilder

    applied_builder = LogicalPlanBuilder(applied)
    write_df = daft.DataFrame(applied_builder)
    write_df.explain(show_all=True, file=buf)
    text = buf.getvalue()
    print(text)
    assert "Batch Size = 10" in text, text
    assert "== Optimized Logical Plan ==" in text
    assert "== Physical Plan ==" in text

    optimized_section = text.split("== Optimized Logical Plan ==")[1].split("== Physical Plan ==")[0]
    physical_section = text.split("== Physical Plan ==")[1]
    assert "SkipExisting:" not in optimized_section, text
    assert "SkipExisting:" not in physical_section, text
    assert "Batch Size = 10" in optimized_section, text
    assert "Batch Size = 10" in physical_section, text


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_skip_existing_missing_keys_raises(tmp_path: Path):
    ckpt_dir = tmp_path / "missing_skip_existing_keys"
    df = daft.from_pydict({"id": [1, 2, 3], "val": ["a", "b", "c"]})

    with pytest.raises(RuntimeError, match=r"\[skip_existing\] keys not found"):
        df.skip_existing(ckpt_dir, on="id", file_format="parquet").collect()


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_skip_existing_composite_key_filters_correctly(tmp_path: Path):
    fmt = FileFormat.Parquet
    root_dir = tmp_path / "existing_keys_composite"
    cfg = {"key_column": ["id", "grp"]}

    df_all = daft.from_pydict(
        {
            "id": [0, 0, 1, 1, 2, 2],
            "grp": [0, 1, 0, 1, 0, 1],
            "val": [0, 1, 2, 3, 4, 5],
        }
    )
    df_first = df_all.where(col("grp") == 0)

    helper_write_dataframe(df_first, fmt, root_dir, skip_existing_config=cfg)
    helper_write_dataframe(df_all, fmt, root_dir, skip_existing_config=cfg)

    df_final = helper_read_dataframe(fmt, root_dir)
    out = df_final.select("id", "grp").to_pydict()
    final_pairs = set(zip(out["id"], out["grp"]))
    assert final_pairs == {(0, 0), (0, 1), (1, 0), (1, 1), (2, 0), (2, 1)}
    assert len(out["id"]) == 6


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_skip_existing_empty_key_list_raises(tmp_path: Path):
    df = daft.from_pydict({"id": [1], "val": ["a"]})
    with pytest.raises(
        ValueError, match=r"\[skip_existing\] on must be a non-empty column name or list of column names"
    ):
        df.skip_existing(tmp_path / "a", on=[], file_format="parquet").collect()


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_skip_existing_invalid_format_string_raises(tmp_path: Path):
    df = daft.from_pydict({"id": [1], "val": ["a"]})
    with pytest.raises(ValueError, match=r"\[skip_existing\] Unsupported format"):
        df.skip_existing(tmp_path / "a", on="id", file_format="orc").collect()


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_skip_existing_invalid_num_key_filter_partitions_raises(tmp_path: Path):
    df = daft.from_pydict({"id": [1], "val": ["a"]})
    with pytest.raises(Exception, match="num_key_filter_partitions"):
        df.skip_existing(tmp_path / "a", on="id", file_format="parquet", num_key_filter_partitions=0).collect()


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_skip_existing_invalid_num_cpus_raises(tmp_path: Path):
    df = daft.from_pydict({"id": [1], "val": ["a"]})
    with pytest.raises(Exception, match="num_cpus"):
        df.skip_existing(tmp_path / "a", on="id", file_format="parquet", num_cpus=0).collect()
