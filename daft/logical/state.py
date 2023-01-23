import daft.logical.logical_plan
import pathlib
import cloudpickle
import daft.types
import typing
import pyarrow as pa
import daft.internal.treenode
import daft.logical.schema
import daft.expressions
import daft.runners.partitioning
from copy import deepcopy
import daft
import daft.context
import daft.runners.pyrunner


class State(object):
    plan: daft.logical.logical_plan.LogicalPlan

    def __init__(self, state=None, exprs_ids=None):
        if hasattr(state, 'plan'):
            state = state.plan()
        self.plan = state
        self.exprs_ids = exprs_ids
        if state:
            self.exprs_ids = [expr._id for expr in self.plan.schema().exprs if expr._id is not None]

    def save(self, path):
        pathlib.Path(path).write_bytes(cloudpickle.dumps(self))
        return path

    @classmethod
    def from_dataframe(cls, df):
        return cls(df.plan())

    @classmethod
    def load(cls, path):
        return cloudpickle.loads(pathlib.Path(path).read_bytes())

    def __repr__(self):
        return self.plan.pretty_print()

    @staticmethod
    def get_block_data(data):
        block_data: dict[str, tuple[daft.types.ExpressionType, typing.Any]] = {}
        for header in data:
            arr = data[header]

            if isinstance(arr, pa.Array) or isinstance(arr, pa.ChunkedArray):
                expr_type = daft.types.ExpressionType.from_arrow_type(arr.type)
                block_data[header] = (expr_type, arr.to_pylist() if daft.types.ExpressionType.is_py(expr_type) else arr)
                continue

            try:
                arrow_type = pa.infer_type(arr)
            except pa.lib.ArrowInvalid:
                arrow_type = None

            if arrow_type is None or pa.types.is_nested(arrow_type):
                found_types = {type(o) for o in data[header]} - {type(None)}
                block_data[header] = (
                    (daft.logical.schema.ExpressionType.python_object(), list(arr))
                    if len(found_types) > 1
                    else (daft.types.PythonExpressionType(found_types.pop()), list(arr))
                )
                continue

            expr_type = daft.types.ExpressionType.from_arrow_type(arrow_type)
            block_data[header] = (expr_type, list(arr) if daft.types.ExpressionType.is_py(expr_type) else pa.array(arr))
        return block_data

    def to_scan(self, data):
        if isinstance(data, daft.DataFrame):
            return data
        block_data = self.get_block_data(data)
        data_schema = daft.logical.schema.ExpressionList(
            [daft.expressions.ColumnExpression(header, expr_type=expr_type) for header, (expr_type, _) in
             block_data.items()]
        ).resolve()
        for expr, _id in zip(data_schema.exprs, self.exprs_ids):
            expr._id = _id

        data_vpartition = daft.runners.partitioning.vPartition.from_pydict(
            data={header: arr for header, (_, arr) in block_data.items()}, schema=data_schema, partition_id=0
        )

        result_set = daft.runners.pyrunner.LocalPartitionSet({0: data_vpartition})
        cache_entry = daft.context.get_context().runner().put_partition_set_into_cache(result_set)
        return daft.logical.logical_plan.InMemoryScan(
            cache_entry=cache_entry,
            schema=data_schema,
        )

    @staticmethod
    def _new_projection(node):
        plan = daft.logical.logical_plan.LogicalPlan(node.schema(), partition_spec=node.partition_spec(),
                                                     op_level=node.op_level())
        return daft.logical.logical_plan.Projection(plan, node.schema())

    @classmethod
    def infer(cls, data):
        """
        TODO
        * records (list of dicts)
        * json
        * pandas
        * numpy
        * pyarrow Table or Array
        * polars DataFrame or Series
        * path to file ?

        """
        if isinstance(data, daft.DataFrame):
            return data
        if isinstance(data, dict):
            return daft.DataFrame.from_pydict(data)

        raise NotImplementedError(f"Data type {type(data)} is not impemented yet")

    def inference(self, data, filters: bool = False, remainder: bool = True, **kwargs):
        """
        param: data -
        This function is used to apply transformations and modeling on new data.
        We should infer dicts, records json etc, here, so we can deploy it in production.
        """

        new_data = State.infer(data).plan()  # get the InmemoryScan
        state = deepcopy(self.plan)

        # Naive implementation - we should use a better algorithm to include cases like joins, etc.
        def dfs(node: daft.internal.treenode.TreeNode) -> None:
            if not node._children():
                return new_data
            new_children = [dfs(child) for child in node._children()]
            if isinstance(node, daft.logical.logical_plan.Filter) and not filters:
                node = self._new_projection(node)
            node = daft.logical.logical_plan.Projection.copy_with_new_children(node, new_children)

            return node

        state = dfs(state)
        return daft.DataFrame(state)

    def transform(self, data, **kwargs):
        """
        A common use case where we want to transform the data, including filters.
        Main use-case is data cleaning before training a model.
        """
        return self.inference(data, filters=True, remainder=False, **kwargs)

    # TODO
    def fit(self, data, **kwargs):
        """
        Re-calculating the state with new data
        """
        self.state = data.plan()
        return self

    # TODO
    def predict(self, data, **kwargs):
        """
        A discussion, is this a column we want the user to define explicitly? Or should we just return the last column added?
        The column should return as numpy for now as it will integrate nicely with the current ecosystem.
        """
        return self.inference(data, filters=False, remainder=True, **kwargs)[self.plan.schema().exprs[-1].name()]
