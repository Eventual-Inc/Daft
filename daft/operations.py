from typing import List, ForwardRef

from daft.function import UDFContext

Column = ForwardRef('Column')

class Operation:
    inputs: List[Column]
    def __init__(self, name: str, inputs: List[Column]) -> None:
        self.name = name
        self.inputs = input


class UDFOperation(Operation):
    def __init__(self, udf_context: UDFContext) -> None:
        super().__init__("UDF",[arg for arg in udf_context.args] + [arg for arg in udf_context.kwargs.values()])
        self.udf_context = udf_context
