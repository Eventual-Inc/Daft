from __future__ import annotations

import pytest

import daft
from daft.context import get_context


@pytest.mark.skipif(get_context().runner_config.name != "ray", reason="Needs to run on Ray runner")
def test_active_plan_clean_up_df_show():
    path = "tests/assets/parquet-data/mvp.parquet"
    df = daft.read_parquet([path, path])
    df.show()
    runner = get_context().runner()
    assert len(runner.active_plans()) == 0


@pytest.mark.skipif(get_context().runner_config.name != "ray", reason="Needs to run on Ray runner")
def test_active_plan_single_iter_partitions():
    path = "tests/assets/parquet-data/mvp.parquet"
    df = daft.read_parquet([path, path])
    iter = df.iter_partitions()
    next(iter)
    runner = get_context().runner()
    assert len(runner.active_plans()) == 1
    del iter
    assert len(runner.active_plans()) == 0


@pytest.mark.skipif(get_context().runner_config.name != "ray", reason="Needs to run on Ray runner")
def test_active_plan_multiple_iter_partitions():
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
def test_active_plan_with_show_and_write_parquet(tmpdir):
    df = daft.read_parquet("tests/assets/parquet-data/mvp.parquet")
    df = df.into_partitions(8)
    df = df.join(df, on="a")
    df.show()
    runner = get_context().runner()
    assert len(runner.active_plans()) == 0
    df.write_parquet(tmpdir.dirname)
    assert len(runner.active_plans()) == 0
