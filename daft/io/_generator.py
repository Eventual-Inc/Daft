# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Callable

from daft.daft import PyPartitionField, PyPushdowns, PyRecordBatch, ScanOperatorHandle, ScanTask
from daft.dataframe import DataFrame
from daft.io.scan import ScanOperator
from daft.logical.builder import LogicalPlanBuilder
from daft.logical.schema import Schema

if TYPE_CHECKING:
    from daft.recordbatch.recordbatch import RecordBatch


def _generator_factory_function(func: Callable[[], Iterator["RecordBatch"]]) -> Iterator[PyRecordBatch]:
    for table in func():
        yield table._recordbatch


def read_generator(
    generators: Iterator[Callable[[], Iterator["RecordBatch"]]],
    schema: Schema,
) -> DataFrame:
    """Create a DataFrame from a generator function.

    Args:
        generator (Callable[[int, Any], Iterator[RecordBatch]]): a generator function that generates data
        num_partitions (int): the number of partitions to generate
        schema (Schema): the schema of the generated data
        generator_args (Any): additional arguments to pass to the generator

    Returns:
        DataFrame: a DataFrame containing the generated data

    Examples:
        >>> import daft
        >>> from daft.io._generator import read_generator
        >>> from daft.recordbatch.recordbatch import RecordBatch
        >>> from functools import partial
        >>>
        >>> # Set runner to Ray for distributed processing
        >>> daft.set_runner_ray()
        >>>
        >>> # Helper function to generate data for each partition
        >>> def generate(num_rows: int):
        ...     data = {"ints": [i for i in range(num_rows)]}
        ...     yield RecordBatch.from_pydict(data)
        >>>
        >>> # Generator function that yields partial functions for each partition
        >>> def generator(num_partitions: int):
        ...     for i in range(num_partitions):
        ...         yield partial(generate, 100)
        >>>
        >>> # Create DataFrame using read_generator and repartition the data
        >>> df = (
        ...     read_generator(
        ...         generator(num_partitions=100),
        ...         daft.Schema._from_field_name_and_types([("ints", daft.DataType.uint64())]),
        ...     )
        ...     .repartition(100, "ints")
        ...     .collect()
        ... )
    """
    generator_scan_operator = GeneratorScanOperator(
        generators=generators,
        schema=schema,
    )
    handle = ScanOperatorHandle.from_python_scan_operator(generator_scan_operator)
    builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
    return DataFrame(builder)


class GeneratorScanOperator(ScanOperator):
    def __init__(
        self,
        generators: Iterator[Callable[[], Iterator["RecordBatch"]]],
        schema: Schema,
    ):
        self._generators = list(generators)
        self._schema = schema

    def name(self) -> str:
        return self.display_name()

    def display_name(self) -> str:
        return "GeneratorScanOperator"

    def schema(self) -> Schema:
        return self._schema

    def partitioning_keys(self) -> list[PyPartitionField]:
        return []

    def can_absorb_filter(self) -> bool:
        return False

    def can_absorb_limit(self) -> bool:
        return False

    def can_absorb_select(self) -> bool:
        return False

    def multiline_display(self) -> list[str]:
        return [
            self.display_name(),
            f"Schema = {self.schema()}",
        ]

    def to_scan_tasks(self, pushdowns: PyPushdowns) -> Iterator[ScanTask]:
        for generator in self._generators:
            yield ScanTask.python_factory_func_scan_task(
                module=_generator_factory_function.__module__,
                func_name=_generator_factory_function.__name__,
                func_args=(generator,),
                schema=self.schema()._schema,
                num_rows=None,
                size_bytes=None,
                pushdowns=pushdowns,
                stats=None,
                source_type=self.name(),
            )
