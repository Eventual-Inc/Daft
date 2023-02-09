from __future__ import annotations

import pyarrow as pa

from daft.daft import PyDataType


class DataType:
    _dtype: PyDataType

    def __init__(self) -> None:
        raise NotImplementedError(
            "We do not support creating a DataType via __init__ "
            "use a creator method like DataType.int32() or use DataType.from_arrow_type(pa_type)"
        )

    @staticmethod
    def from_arrow_type(arrow_type: pa.lib.DataType) -> DataType:
        dt = DataType.__new__(DataType)

        dt._dtype = PyDataType.int32()
        return dt

    def __repr__(self) -> str:
        return f"DataType({self._dtype})"
