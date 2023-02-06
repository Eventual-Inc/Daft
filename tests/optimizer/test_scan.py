from __future__ import annotations

import pytest

from daft.internal.rule_runner import Once, RuleBatch, RuleRunner
from daft.logical.logical_plan import LogicalPlan
from daft.logical.optimizer import (
    FoldProjections,
    PushDownClausesIntoScan,
    PushDownPredicates,
)


@pytest.fixture(scope="function")
def optimizer() -> RuleRunner[LogicalPlan]:
    return RuleRunner(
        [RuleBatch("push_into_scan", Once, [PushDownPredicates(), FoldProjections(), PushDownClausesIntoScan()])]
    )
