from abc import abstractmethod
from typing import ForwardRef, List

Column = ForwardRef("Column")

from daft.column import Column, ColumnExpression, ColumnType
from daft.function import UDFContext


class Operation:
    inputs: List[Column]

    def __init__(self, name: str, inputs: List[Column]) -> None:
        self.name = name
        self.inputs = inputs

    @property
    @abstractmethod
    def output_names(self) -> List[str]:
        raise NotImplementedError()

    @property
    @abstractmethod
    def outputs(self) -> List[Column]:
        raise NotImplementedError()


class Stage:
    ...


class OneToOneStage(Stage):
    ...


class AllToAllStage(Stage):
    ...


class LimitOperation(Operation, AllToAllStage):
    def __init__(self, num: int, col: Column) -> None:
        super().__init__("Limit", [col])
        self._output_names = [col.name]
        self._outputs = [Column(name=col.name, column_type=ColumnType.RESULT, operation=self)]

    @property
    def output_names(self) -> List[str]:
        return self._output_names

    def outputs(self) -> List[Column]:
        return self._outputs


class WhereOperation(Operation):
    def __init__(self, expr: ColumnExpression, cols: List[Column]) -> None:
        super().__init__("Where", cols.copy())
        self._col_expr = expr
        self._output_names = [col.name for col in cols]
        self._outputs = [Column(name=col.name, column_type=ColumnType.RESULT, operation=self) for col in cols]

    @property
    def output_names(self) -> List[str]:
        return self._output_names

    def outputs(self) -> List[Column]:
        return self._outputs


class AliasOperation(Operation):
    def __init__(self, new_name: str, col: Column) -> None:
        super().__init__("Alias", [col])
        self._output_names = [new_name]
        self._outputs = [Column(name=new_name, column_type=ColumnType.RESULT, operation=self)]

    @property
    def output_names(self) -> List[str]:
        return self._output_names

    def outputs(self) -> List[Column]:
        return self._outputs


UDFContext = ForwardRef("UDFContext")


class UDFOperation(Operation):
    def __init__(self, output_name: str, udf_context: UDFContext) -> None:
        super().__init__("UDF", [arg for arg in udf_context.args] + [arg for arg in udf_context.kwargs.values()])
        self.udf_context = udf_context
        self._output_names = [output_name]
        self._outputs = [Column(name=output_name, column_type=ColumnType.RESULT, operation=self)]

    @property
    def output_names(self) -> List[str]:
        return self._output_names

    def outputs(self) -> List[Column]:
        return self._outputs
