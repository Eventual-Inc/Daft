# isort: dont-add-import: from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Iterator, List

from daft.daft import Pushdowns, PyTable, ScanOperatorHandle, ScanTask
from daft.dataframe import DataFrame
from daft.io.scan import PartitionField, ScanOperator
from daft.logical.builder import LogicalPlanBuilder
from daft.logical.schema import Schema

if TYPE_CHECKING:
    from daft.table.table import Table


def _generator_factory_function(func: Callable[[], Iterator["Table"]]) -> Iterator["PyTable"]:
    for table in func():
        yield table._table


def read_generator(
    generators: Iterator[Callable[[], Iterator["Table"]]],
    schema: Schema,
) -> DataFrame:
    """Create a DataFrame from a generator function.

    Example:
        >>> import daft
        >>> from daft.io._generator import read_generator
        >>> from daft.table.table import Table
        >>> from functools import partial
        >>>
        >>> # Set runner to Ray for distributed processing
        >>> daft.context.set_runner_ray()
        >>>
        >>> # Helper function to generate data for each partition
        >>> def generate(num_rows: int):
        ...     data = {"ints": [i for i in range(num_rows)]}
        ...     yield Table.from_pydict(data)
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

    Args:
        generator (Callable[[int, Any], Iterator[Table]]): a generator function that generates data
        num_partitions (int): the number of partitions to generate
        schema (Schema): the schema of the generated data
        generator_args (Any): additional arguments to pass to the generator

    Returns:
        DataFrame: a DataFrame containing the generated data
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
        generators: Iterator[Callable[[], Iterator["Table"]]],
        schema: Schema,
    ):
        self._generators = generators
        self._schema = schema

    def name(self) -> str:
        return self.display_name()

    def display_name(self) -> str:
        return "GeneratorScanOperator"

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
            )
