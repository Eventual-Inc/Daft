from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeVar

from loguru import logger

from daft.internal.rule import Rule
from daft.internal.treenode import TreeNode

TreeNodeType = TypeVar("TreeNodeType", bound="TreeNode")


@dataclass
class FixedPointPolicy:
    num_runs: int


Once = FixedPointPolicy(1)


@dataclass
class RuleBatch(Generic[TreeNodeType]):
    name: str
    mode: FixedPointPolicy
    rules: list[Rule[TreeNodeType]]


class RuleRunner(Generic[TreeNodeType]):
    def __init__(self, batches: list[RuleBatch[TreeNodeType]]) -> None:
        self._batches = batches

    def optimize(self, root: TreeNodeType) -> TreeNodeType:
        from copy import deepcopy

        root = deepcopy(root)
        for batch in self._batches:
            root = self._run_single_batch(root, batch)
        return root

    def __call__(self, root: TreeNodeType) -> TreeNodeType:
        return self.optimize(root)

    def _run_single_batch(self, root: TreeNodeType, batch: RuleBatch) -> TreeNodeType:
        logger.debug(f"Running optimizer batch: {batch.name}")
        max_runs = batch.mode.num_runs
        applied_least_one_rule = False
        for i in range(max_runs):
            if i > 0 and not applied_least_one_rule:
                logger.debug(f"Optimizer batch: {batch.name} terminating at iteration {i}. No rules applied")
                break
            logger.debug(f"Running optimizer batch: {batch.name}. Iteration {i} out of maximum {max_runs}")
            applied_least_one_rule = False
            for rule in batch.rules:
                result = root.apply_and_trickle_down(rule)
                if result is not None:
                    root = result
                    applied_least_one_rule = True

        else:
            if applied_least_one_rule:
                logger.debug(
                    f"Optimizer Batch {batch.name} reached max iteration {max_runs} and had changes in the last iteration"
                )

        return root
