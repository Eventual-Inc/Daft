from __future__ import annotations

import pyarrow as pa

from daft.daft import PySeries
from daft.datatype import DataType


class Series:
    _series: PySeries | None

    @staticmethod
    def _from_pyseries(pyseries: PySeries) -> Series:
        s = Series()
        s._series = pyseries
        return s

    @staticmethod
    def from_arrow(array: pa.Array | pa.ChunkedArray, name: str = "arrow_series") -> Series:
        if isinstance(array, pa.Array):
            pys = PySeries.from_arrow(name, array)
            return Series._from_pyseries(pys)
        elif isinstance(array, pa.ChunkedArray):
            combined_array = array.combine_chunks()
            pys = PySeries.from_arrow(name, combined_array)
            return Series._from_pyseries(pys)
        else:
            raise ValueError(f"expected either PyArrow Array or Chunked Array, got {type(array)}")

    @staticmethod
    def from_pylist(data: list, name: str = "list_series") -> Series:
        if not isinstance(data, list):
            raise ValueError(f"expected a python list, got {type(data)}")
        arrow_array = pa.array(data)
        return Series.from_arrow(arrow_array, name=name)

    def cast(self, dtype: DataType) -> Series:
        if self._series is None:
            raise ValueError("This Series isn't backed by a Rust PySeries, can not cast")
        return Series._from_pyseries(self._series.cast(dtype._dtype))

    def name(self) -> str:
        if self._series is None:
            raise ValueError("This Series isn't backed by a Rust PySeries, can not get name")
        return self._series.name()

    def datatype(self) -> DataType:
        if self._series is None:
            raise ValueError("This Series isn't backed by a Rust PySeries, can not get datatype")

        return DataType._from_pydatatype(self._series.data_type())

    def to_arrow(self) -> pa.Array:
        if self._series is None:
            raise ValueError("This Series isn't backed by a Rust PySeries, can not convert to arrow")
        return self._series.to_arrow()

    def to_pylist(self) -> list:
        if self._series is None:
            raise ValueError("This Series isn't backed by a Rust PySeries, can not convert to arrow")
        return self._series.to_arrow().to_pylist()

    def __repr__(self) -> str:
        return repr(self._series)

    def __bool__(self) -> bool:
        raise ValueError(
            "Series don't have a truth value." "If you reached this error using `and` / `or`, use `&` / `|` instead."
        )

    def __add__(self, other: object) -> Series:
        if not isinstance(other, Series):
            raise ValueError(f"expected another Series but got {type(other)}")
        assert self._series is not None and other._series is not None
        return Series._from_pyseries(self._series + other._series)

    def __sub__(self, other: object) -> Series:
        if not isinstance(other, Series):
            raise ValueError(f"expected another Series but got {type(other)}")
        assert self._series is not None and other._series is not None
        return Series._from_pyseries(self._series - other._series)

    def __mul__(self, other: object) -> Series:
        if not isinstance(other, Series):
            raise ValueError(f"expected another Series but got {type(other)}")
        assert self._series is not None and other._series is not None
        return Series._from_pyseries(self._series * other._series)

    def __truediv__(self, other: object) -> Series:
        if not isinstance(other, Series):
            raise ValueError(f"expected another Series but got {type(other)}")
        assert self._series is not None and other._series is not None
        return Series._from_pyseries(self._series / other._series)

    def __mod__(self, other: object) -> Series:
        if not isinstance(other, Series):
            raise ValueError(f"expected another Series but got {type(other)}")
        assert self._series is not None and other._series is not None
        return Series._from_pyseries(self._series % other._series)

    def __eq__(self, other: object) -> Series:  # type: ignore[override]
        if not isinstance(other, Series):
            raise ValueError(f"expected another Series but got {type(other)}")
        assert self._series is not None and other._series is not None
        return Series._from_pyseries(self._series == other._series)

    def __ne__(self, other: object) -> Series:  # type: ignore[override]
        if not isinstance(other, Series):
            raise ValueError(f"expected another Series but got {type(other)}")
        assert self._series is not None and other._series is not None
        return Series._from_pyseries(self._series != other._series)

    def __gt__(self, other: object) -> Series:
        if not isinstance(other, Series):
            raise ValueError(f"expected another Series but got {type(other)}")
        assert self._series is not None and other._series is not None
        return Series._from_pyseries(self._series > other._series)

    def __lt__(self, other: object) -> Series:
        if not isinstance(other, Series):
            raise ValueError(f"expected another Series but got {type(other)}")
        assert self._series is not None and other._series is not None
        return Series._from_pyseries(self._series < other._series)

    def __ge__(self, other: object) -> Series:
        if not isinstance(other, Series):
            raise ValueError(f"expected another Series but got {type(other)}")
        assert self._series is not None and other._series is not None
        return Series._from_pyseries(self._series >= other._series)

    def __le__(self, other: object) -> Series:
        if not isinstance(other, Series):
            raise ValueError(f"expected another Series but got {type(other)}")
        assert self._series is not None and other._series is not None
        return Series._from_pyseries(self._series <= other._series)
