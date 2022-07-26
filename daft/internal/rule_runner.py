from copy import deepcopy
from dataclasses import dataclass
from typing import Generic, List, TypeVar

from loguru import logger

from daft.internal.rule import Rule
from daft.internal.treenode import TreeNode

TreeNodeType = TypeVar("TreeNodeType", bound="TreeNode")


@dataclass
class FixedPointMode:
    num_runs: int


Once = FixedPointMode(1)


@dataclass
class RuleBatch(Generic[TreeNodeType]):
    name: str
    mode: FixedPointMode
    rules: List[Rule[TreeNodeType]]


class RuleRunner(Generic[TreeNodeType]):
    def __init__(self, batches: List[RuleBatch[TreeNodeType]]) -> None:
        self._batches = batches

    def optimize(self, root: TreeNodeType) -> TreeNodeType:
        root = deepcopy(root)
        for batch in self._batches:
            root = self._run_single_batch(root, batch)
        return root

    def __call__(self, root: TreeNodeType) -> TreeNodeType:
        return self.optimize(root)

    def _run_single_batch(self, root: TreeNodeType, batch: RuleBatch) -> TreeNodeType:
        logger.debug(f"Running optimizer batch: {batch.name}")
        max_runs = batch.mode.num_runs
        for i in range(max_runs):
            logger.debug(f"Running optimizer batch: {batch.name}. Iteration {i} out of maximum {max_runs}")
            for rule in batch.rules:
                result = root.apply_and_trickle_down(rule)
                if result is not None:
                    root = result
        else:
            if result is not None:
                logger.debug(f"Optimizer Batch {batch.name} reached max iteration {max_runs}")

        return root
