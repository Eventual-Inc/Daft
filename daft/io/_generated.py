# isort: dont-add-import: from __future__ import annotations

from typing import Any, Callable, Iterator, List

from daft.daft import Pushdowns, PyTable, ScanOperatorHandle, ScanTask
from daft.dataframe import DataFrame
from daft.io.scan import PartitionField, ScanOperator
from daft.logical.builder import LogicalPlanBuilder
from daft.logical.schema import Schema


def read_generated(
    generator: Callable[[int, Any], Iterator["PyTable"]],
    num_partitions: int,
    schema: Schema,
    *generator_args: Any,
) -> DataFrame:
    """Create a DataFrame from generated_data

    Args:
        generator (Callable[[int, Any], Iterator[PyTable]]): a callable that generates PyTables
        num_partitions (int): the number of partitions to generate
        schema (Schema): the schema of the generated data
        generator_args (Any): additional arguments to pass to the generator

    Returns:
        DataFrame: a DataFrame containing the generated data
    """

    generated_data_scan_operator = GeneratedDataScanOperator(
        generator=generator,
        num_partitions=num_partitions,
        schema=schema,
        generator_args=generator_args,
    )
    handle = ScanOperatorHandle.from_python_scan_operator(generated_data_scan_operator)
    builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
    return DataFrame(builder)


class GeneratedDataScanOperator(ScanOperator):
    def __init__(
        self,
        generator: Callable[[int, Any], Iterator["PyTable"]],
        num_partitions: int,
        schema: Schema,
        generator_args: Any,
    ):
        self._generator = generator
        self._num_partitions = num_partitions
        self._schema = schema
        self._generator_args = generator_args

    def display_name(self) -> str:
        return "GeneratedDataScanOperator"

    def schema(self) -> Schema:
        return self._schema

    def partitioning_keys(self) -> List[PartitionField]:
        return []

    def can_absorb_filter(self) -> bool:
        return False

    def can_absorb_limit(self) -> bool:
        return False

    def can_absorb_select(self) -> bool:
        return False

    def multiline_display(self) -> List[str]:
        return [
            self.display_name(),
            f"Schema = {self.schema()}",
        ]

    def to_scan_tasks(self, pushdowns: Pushdowns) -> Iterator[ScanTask]:
        for i in range(self._num_partitions):
            yield ScanTask.python_factory_func_scan_task(
                module=self._generator.__module__,
                func_name=self._generator.__name__,
                func_args=(i, *self._generator_args),
                schema=self.schema()._schema,
                num_rows=None,
                size_bytes=None,
                pushdowns=pushdowns,
                stats=None,
            )
