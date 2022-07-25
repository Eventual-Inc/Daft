import copy
from typing import List

from daft.internal import rule
from daft.logical import logical_plan


def optimize_plan(
    plan: logical_plan.LogicalPlan, rules: List[rule.Rule[logical_plan.LogicalPlan]]
) -> logical_plan.LogicalPlan:
    optimized_plan = copy.deepcopy(plan)
    rule_runner = rule.RuleRunner([])  # RuleRunner doesn't actually do anything with the input rules at the moment??
    for r in rules:
        optimized_plan = rule_runner.run_single_rule(optimized_plan, r)
    return optimized_plan
