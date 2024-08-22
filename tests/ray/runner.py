from __future__ import annotations

import pytest

import daft
from daft.context import get_context


@pytest.fixture(scope="function")
def disable_parquet_file_merging():
    try:
        old_execution_config = daft.context.get_context().daft_execution_config
        daft.set_execution_config(
            # Disables merging of ScanTasks of Parquet when reading small Parquet files
            scan_tasks_min_size_bytes=0,
            scan_tasks_max_size_bytes=0,
        )
        yield
    finally:
        daft.set_execution_config(config=old_execution_config)


@pytest.mark.skipif(get_context().runner_config.name != "ray", reason="Needs to run on Ray runner")
def test_active_plan_clean_up_df_show(disable_parquet_file_merging):
    path = "tests/assets/parquet-data/mvp.parquet"
    df = daft.read_parquet([path, path])
    df.show()
    runner = get_context().runner()
    assert len(runner.active_plans()) == 0


@pytest.mark.skipif(get_context().runner_config.name != "ray", reason="Needs to run on Ray runner")
def test_active_plan_single_iter_partitions(disable_parquet_file_merging):
    path = "tests/assets/parquet-data/mvp.parquet"
    df = daft.read_parquet([path, path])
    iter = df.iter_partitions()
    next(iter)
    runner = get_context().runner()
    assert len(runner.active_plans()) == 1
    del iter
    assert len(runner.active_plans()) == 0


@pytest.mark.skipif(get_context().runner_config.name != "ray", reason="Needs to run on Ray runner")
def test_active_plan_multiple_iter_partitions(disable_parquet_file_merging):
    path = "tests/assets/parquet-data/mvp.parquet"
    df = daft.read_parquet([path, path])
    iter = df.iter_partitions()
    next(iter)
    runner = get_context().runner()
    assert len(runner.active_plans()) == 1

    df2 = daft.read_parquet([path, path])
    iter2 = df2.iter_partitions()
    next(iter2)
    assert len(runner.active_plans()) == 2

    del iter
    assert len(runner.active_plans()) == 1

    del iter2
    assert len(runner.active_plans()) == 0


@pytest.mark.skipif(get_context().runner_config.name != "ray", reason="Needs to run on Ray runner")
def test_active_plan_with_show_and_write_parquet(tmpdir, disable_parquet_file_merging):
    df = daft.read_parquet("tests/assets/parquet-data/mvp.parquet")
    df = df.into_partitions(8)
    df = df.join(df, on="a")
    df.show()
    runner = get_context().runner()
    assert len(runner.active_plans()) == 0
    df.write_parquet(tmpdir.dirname)
    assert len(runner.active_plans()) == 0
