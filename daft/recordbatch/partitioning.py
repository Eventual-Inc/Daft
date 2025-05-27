import urllib.parse
from typing import Dict, List, Optional, Union

from daft import Series
from daft.expressions import ExpressionsProjection
from daft.recordbatch.recordbatch import RecordBatch

from .micropartition import MicroPartition


def partition_strings_to_path(
    root_path: str,
    parts: Dict[str, str],
    partition_null_fallback: str = "__HIVE_DEFAULT_PARTITION__",
) -> str:
    # Hive-style partition keys and values should be URL encoded.
    keys = [urllib.parse.quote(key) for key in parts.keys()]
    values = [partition_null_fallback if value is None else urllib.parse.quote(value) for value in parts.values()]
    postfix = "/".join(f"{k}={v}" for k, v in zip(keys, values))
    return f"{root_path}/{postfix}"


def partition_values_to_str_mapping(
    partition_values: Union[MicroPartition, RecordBatch],
) -> Dict[str, Series]:
    null_part = Series.from_pylist(
        [None]
    )  # This is to ensure that the null values are replaced with the default_partition_fallback value

    partition_strings = {}

    for column in partition_values.columns():
        string_names = column._to_str_values()
        null_filled = column.is_null().if_else(null_part, string_names)
        partition_strings[column.name()] = null_filled

    return partition_strings


class PartitionedTable:
    def __init__(self, table: MicroPartition, partition_keys: Optional[ExpressionsProjection]):
        self.table: MicroPartition = table
        self.partition_keys: Optional[ExpressionsProjection] = partition_keys
        self._partitions: Optional[list[MicroPartition]] = None
        self._partition_values: Optional[MicroPartition] = None

    def _create_partitions(self) -> None:
        if self.partition_keys is None or len(self.partition_keys) == 0:
            self._partitions = [self.table]
            self._partition_values = None
        else:
            self._partitions, self._partition_values = self.table.partition_by_value(partition_keys=self.partition_keys)

    def partitions(self) -> List[MicroPartition]:
        """Returns a list of MicroPartitions representing the table partitioned by the partition keys.

        If the table is not partitioned, returns the original table as the single element in the list.
        """
        if self._partitions is None:
            self._create_partitions()
        return self._partitions  # type: ignore

    def partition_values(self) -> Optional[MicroPartition]:
        """Returns the partition values, with each row corresponding to the partition at the same index in PartitionedTable.partitions().

        If the table is not partitioned, returns None.

        """
        if self._partition_values is None:
            self._create_partitions()
        return self._partition_values

    def partition_values_str(self) -> Optional[MicroPartition]:
        """Returns the partition values converted to human-readable strings, keeping null values as null.

        If the table is not partitioned, returns None.
        """
        partition_values = self.partition_values()

        if partition_values is None:
            return None
        else:
            partition_strings = partition_values_to_str_mapping(partition_values)
            return MicroPartition.from_pydict(partition_strings)
