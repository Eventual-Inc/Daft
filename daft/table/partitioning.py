from typing import Dict, List, Optional

from daft import Series
from daft.expressions import ExpressionsProjection

from .micropartition import MicroPartition


def partition_strings_to_path(
    root_path: str, parts: Dict[str, str], partition_null_fallback: str = "__HIVE_DEFAULT_PARTITION__"
) -> str:
    keys = parts.keys()
    values = [partition_null_fallback if value is None else value for value in parts.values()]
    postfix = "/".join(f"{k}={v}" for k, v in zip(keys, values))
    return f"{root_path}/{postfix}"


class PartitionedTable:
    def __init__(self, table: MicroPartition, partition_keys: Optional[ExpressionsProjection]):
        self.table = table
        self.partition_keys = partition_keys
        self._partitions = None
        self._partition_values = None

    def _create_partitions(self):
        if self.partition_keys is None or len(self.partition_keys) == 0:
            self._partitions = [self.table]
            self._partition_values = None
        else:
            self._partitions, self._partition_values = self.table.partition_by_value(partition_keys=self.partition_keys)

    def partitions(self) -> List[MicroPartition]:
        """
        Returns a list of MicroPartitions representing the table partitioned by the partition keys.

        If the table is not partitioned, returns the original table as the single element in the list.
        """
        if self._partitions is None:
            self._create_partitions()
        return self._partitions  # type: ignore

    def partition_values(self) -> Optional[MicroPartition]:
        """
        Returns the partition values, with each row corresponding to the partition at the same index in PartitionedTable.partitions().

        If the table is not partitioned, returns None.

        """
        if self._partition_values is None:
            self._create_partitions()
        return self._partition_values

    def partition_values_str(self) -> Optional[MicroPartition]:
        """
        Returns the partition values converted to human-readable strings, keeping null values as null.

        If the table is not partitioned, returns None.
        """
        null_part = Series.from_pylist([None])
        partition_values = self.partition_values()

        if partition_values is None:
            return None
        else:
            pkey_names = partition_values.column_names()

            partition_strings = {}

            for c in pkey_names:
                column = partition_values.get_column(c)
                string_names = column._to_str_values()
                null_filled = column.is_null().if_else(null_part, string_names)
                partition_strings[c] = null_filled

            return MicroPartition.from_pydict(partition_strings)
