from __future__ import annotations

from dataclasses import dataclass
from io import StringIO
from typing import ClassVar, List

from daft.logical.logical_plan import LogicalPlan, OpLevel


@dataclass
class ExecutionOp:
    logical_ops: List[LogicalPlan]
    num_partitions: int
    is_global_op: ClassVar[bool] = False

    def __repr__(self) -> str:
        builder = StringIO()
        builder.write(f"{self.__class__.__name__}(num_partitions={self.num_partitions})\n")
        for op in self.logical_ops:
            builder.write(f"\t{repr(op)}\n\n")
        return builder.getvalue()


@dataclass(repr=False)
class ForEachPartition(ExecutionOp):
    ...


@dataclass(repr=False)
class GlobalOp(ExecutionOp):
    is_global_op: ClassVar[bool] = True
    ...


@dataclass
class ExecutionPlan:
    execution_ops: List[ExecutionOp]

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
        for_each_so_far: List[LogicalPlan] = []
        exec_plan: List[ExecutionOp] = []
        for lop in post_order:
            if lop.op_level() == OpLevel.ROW or lop.op_level() == OpLevel.PARTITION:
                if len(for_each_so_far) > 0:
                    assert for_each_so_far[-1].num_partitions() == lop.num_partitions()
                for_each_so_far.append(lop)
                assert for_each_so_far[-1]
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
