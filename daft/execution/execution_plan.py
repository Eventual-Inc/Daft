from __future__ import annotations

from dataclasses import dataclass
from io import StringIO
from typing import ClassVar

from daft.logical.logical_plan import LogicalPlan, OpLevel
from daft.resource_request import ResourceRequest


class ExecutionOp:
    logical_ops: list[LogicalPlan]
    num_partitions: int
    data_deps: list[int]
    is_global_op: ClassVar[bool] = False

    def __init__(self, logical_ops: list[LogicalPlan], num_partitions: int) -> None:
        self.logical_ops = logical_ops
        self.num_partitions = num_partitions
        all_deps = set()
        for node in logical_ops:
            for child in node._children():
                all_deps.add(child.id())
        self.data_deps = list(all_deps - {node.id() for node in logical_ops})

    def __repr__(self) -> str:
        builder = StringIO()
        builder.write(f"{self.__class__.__name__}(num_partitions={self.num_partitions})\n")
        for op in self.logical_ops:
            builder.write(f"\t{repr(op)}\n\n")
        return builder.getvalue()

    def resource_request(self) -> ResourceRequest:
        return ResourceRequest.max_resources([lop.resource_request() for lop in self.logical_ops])


class ForEachPartition(ExecutionOp):
    ...


class GlobalOp(ExecutionOp):
    is_global_op: ClassVar[bool] = True
    ...


@dataclass
class ExecutionPlan:
    execution_ops: list[ExecutionOp]

    def __repr__(self) -> str:
        builder = StringIO()
        builder.write(f"{self.__class__.__name__}\n")
        for op in self.execution_ops:
            op_str = repr(op)
            for line in op_str.splitlines():
                builder.write(f"\t{line}\n")
        return builder.getvalue()

    @classmethod
    def plan_from_logical(cls, lplan: LogicalPlan) -> ExecutionPlan:
        post_order = lplan.post_order()
        for_each_so_far: list[LogicalPlan] = []
        exec_plan: list[ExecutionOp] = []
        for lop in post_order:
            if lop.op_level() == OpLevel.ROW or lop.op_level() == OpLevel.PARTITION:
                if len(for_each_so_far) > 0:
                    if (for_each_so_far[-1].num_partitions() != lop.num_partitions()) or (len(lop._children()) == 0):
                        exec_plan.append(
                            ForEachPartition(for_each_so_far, num_partitions=for_each_so_far[-1].num_partitions())
                        )
                        for_each_so_far = []
                        # assert for_each_so_far[-1].num_partitions() == lop.num_partitions()
                for_each_so_far.append(lop)
                # assert for_each_so_far[-1]
            elif lop.op_level() == OpLevel.GLOBAL:
                if len(for_each_so_far) > 0:
                    exec_plan.append(
                        ForEachPartition(for_each_so_far, num_partitions=for_each_so_far[-1].num_partitions())
                    )
                    for_each_so_far = []
                exec_plan.append(GlobalOp([lop], num_partitions=lop.num_partitions()))
            else:
                raise NotImplementedError()

        if len(for_each_so_far) > 0:
            exec_plan.append(ForEachPartition(for_each_so_far, num_partitions=for_each_so_far[-1].num_partitions()))
            for_each_so_far = []

        return cls(exec_plan)
