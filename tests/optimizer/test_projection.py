from __future__ import annotations

import pytest

from daft.dataframe import DataFrame
from daft.internal.rule_runner import Once, RuleBatch, RuleRunner
from daft.logical.logical_plan import LogicalPlan
from daft.logical.optimizer import FoldProjections, PruneColumns


@pytest.fixture(scope="function")
def optimizer() -> RuleRunner[LogicalPlan]:
    return RuleRunner(
        [
            RuleBatch(
                "fold_projections",
                Once,
                [PruneColumns(), FoldProjections()],
            )
        ]
    )


@pytest.mark.skip()
def test_fold_projections(valid_data: list[dict[str, float]], optimizer) -> None:
    df = DataFrame.from_pylist(valid_data)
    df_unoptimized = df.select("sepal_length", "sepal_width").select("sepal_width")
    df_optimized = df.select("sepal_width")

    assert df_unoptimized.column_names == ["sepal_width"]
    assert optimizer(df_unoptimized.plan()).is_eq(df_optimized.plan())
