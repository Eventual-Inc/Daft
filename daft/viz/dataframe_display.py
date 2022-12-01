from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from daft.dataframe.schema import DataFrameSchema
from daft.runners.partitioning import PartitionSet, vPartition
from daft.viz.repr import vpartition_repr, vpartition_repr_html

HAS_PILLOW = False
try:
    pass

    HAS_PILLOW = True
except ImportError:
    pass
if HAS_PILLOW:
    pass


@dataclass(frozen=True)
class DataFrameDisplay:

    partition_set: PartitionSet | None
    schema: DataFrameSchema
    user_message: str | None = None
    column_char_width: int = 20
    max_col_rows: int = 3
    num_rows: int = 10

    def _get_user_message(self) -> str:
        if self.user_message is not None:
            return self.user_message

        if self.partition_set is None:
            return "Dataframe not materialized"
        dataframe_num_rows = len(self.partition_set)
        if dataframe_num_rows == 0:
            return "(Materialized dataframe has no rows)"
        return f"(Showing first {min(self.num_rows, dataframe_num_rows)} of {dataframe_num_rows} rows)"

    def _get_vpartition(self) -> vPartition | None:
        if self.partition_set is None:
            return None
        if len(self.partition_set) <= self.num_rows:
            return self.partition_set._get_merged_vpartition()

        partition_lengths = self.partition_set.len_of_partitions()
        partition_lengths_cumsum = np.cumsum(partition_lengths)
        cumsum_gt_num_rows = partition_lengths_cumsum >= self.num_rows
        last_partition_to_collect = np.argmax(cumsum_gt_num_rows)

        return self.partition_set._get_merged_vpartition(partition_indices=list(range(last_partition_to_collect + 1)))

    def _repr_html_(self) -> str:
        vpartition = self._get_vpartition()
        return vpartition_repr_html(
            vpartition,
            self.schema,
            self.num_rows,
            self._get_user_message(),
            max_col_width=self.column_char_width,
            max_lines=self.max_col_rows,
        )

    def __repr__(self) -> str:
        vpartition = self._get_vpartition()
        return vpartition_repr(
            vpartition,
            self.schema,
            self.num_rows,
            self._get_user_message(),
            max_col_width=self.column_char_width,
            max_lines=self.max_col_rows,
        )
