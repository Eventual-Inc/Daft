from daft.logical.logical_plan import LogicalPlan
from daft.runners.runner import Runner


class PyRunner(Runner):
    def __init__(self, plan: LogicalPlan) -> None:
        self._plan = plan
        self._run_order = self._plan.post_order()

    def run(self) -> None:
        for node in self._run_order:
            print(node)
