from __future__ import annotations

import pyarrow as pa

from daft.daft import PySeries
from daft.datatype import DataType

_NUMPY_AVAILABLE = True
try:
    import numpy as np
except ImportError:
    _NUMPY_AVAILABLE = False


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
            raise TypeError(f"expected either PyArrow Array or Chunked Array, got {type(array)}")

    @staticmethod
    def from_pylist(data: list, name: str = "list_series") -> Series:
        if not isinstance(data, list):
            raise TypeError(f"expected a python list, got {type(data)}")
        arrow_array = pa.array(data)
        return Series.from_arrow(arrow_array, name=name)

    @staticmethod
    def from_numpy(data: np.ndarray, name: str = "numpy_series") -> Series:
        if not isinstance(data, np.ndarray):
            raise TypeError(f"expected a numpy ndarray, got {type(data)}")
        arrow_array = pa.array(data)
        return Series.from_arrow(arrow_array, name=name)

    def cast(self, dtype: DataType) -> Series:
        if self._series is None:
            raise ValueError("This Series isn't backed by a Rust PySeries, can not cast")
        return Series._from_pyseries(self._series.cast(dtype._dtype))

    @staticmethod
    def concat(series: list[Series]) -> Series:
        pyseries = []
        for s in series:
            if not isinstance(s, Series):
                raise TypeError(f"Expected a Series for concat, got {type(s)}")
            pyseries.append(s._series)
        return Series._from_pyseries(PySeries.concat(pyseries))

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

    def filter(self, mask: Series) -> Series:
        if self._series is None:
            raise ValueError("This Series isn't backed by a Rust PySeries, can not convert to arrow")
        if not isinstance(mask, Series):
            raise TypeError(f"expected another Series but got {type(mask)}")
        return Series._from_pyseries(self._series.filter(mask._series))

    def take(self, idx: Series) -> Series:
        if self._series is None:
            raise ValueError("This Series isn't backed by a Rust PySeries, can not convert to arrow")
        if not isinstance(idx, Series):
            raise TypeError(f"expected another Series but got {type(idx)}")
        return Series._from_pyseries(self._series.take(idx._series))

    def argsort(self, descending: bool = False) -> Series:
        if self._series is None:
            raise ValueError("This Series isn't backed by a Rust PySeries, can not convert to arrow")

        if not isinstance(descending, bool):
            raise TypeError(f"expected `descending` to be bool, got {type(descending)}")

        return Series._from_pyseries(self._series.argsort(descending))

    def sort(self, descending: bool = False) -> Series:
        if self._series is None:
            raise ValueError("This Series isn't backed by a Rust PySeries, can not convert to arrow")

        if not isinstance(descending, bool):
            raise TypeError(f"expected `descending` to be bool, got {type(descending)}")

        return Series._from_pyseries(self._series.sort(descending))

    def hash(self, seed: Series | None = None) -> Series:
        if self._series is None:
            raise ValueError("This Series isn't backed by a Rust PySeries, can not convert to arrow")

        if not isinstance(seed, Series) and seed is not None:
            raise TypeError(f"expected `seed` to be Series, got {type(seed)}")

        return Series._from_pyseries(self._series.hash(seed._series if seed is not None else None))

    def __repr__(self) -> str:
        return repr(self._series)

    def __bool__(self) -> bool:
        raise ValueError(
            "Series don't have a truth value." "If you reached this error using `and` / `or`, use `&` / `|` instead."
        )

    def __len__(self) -> int:
        if self._series is None:
            raise ValueError("This Series isn't backed by a Rust PySeries, can not convert to arrow")
        return len(self._series)

    def size_bytes(self) -> int:
        if self._series is None:
            raise ValueError("This Series isn't backed by a Rust PySeries, can not get size bytes")

        return self._series.size_bytes()

    def __abs__(self) -> Series:
        assert self._series is not None
        return Series._from_pyseries(abs(self._series))

    def __add__(self, other: object) -> Series:
        if not isinstance(other, Series):
            raise TypeError(f"expected another Series but got {type(other)}")
        assert self._series is not None and other._series is not None
        return Series._from_pyseries(self._series + other._series)

    def __sub__(self, other: object) -> Series:
        if not isinstance(other, Series):
            raise TypeError(f"expected another Series but got {type(other)}")
        assert self._series is not None and other._series is not None
        return Series._from_pyseries(self._series - other._series)

    def __mul__(self, other: object) -> Series:
        if not isinstance(other, Series):
            raise TypeError(f"expected another Series but got {type(other)}")
        assert self._series is not None and other._series is not None
        return Series._from_pyseries(self._series * other._series)

    def __truediv__(self, other: object) -> Series:
        if not isinstance(other, Series):
            raise TypeError(f"expected another Series but got {type(other)}")
        assert self._series is not None and other._series is not None
        return Series._from_pyseries(self._series / other._series)

    def __mod__(self, other: object) -> Series:
        if not isinstance(other, Series):
            raise TypeError(f"expected another Series but got {type(other)}")
        assert self._series is not None and other._series is not None
        return Series._from_pyseries(self._series % other._series)

    def __eq__(self, other: object) -> Series:  # type: ignore[override]
        if not isinstance(other, Series):
            raise TypeError(f"expected another Series but got {type(other)}")
        assert self._series is not None and other._series is not None
        return Series._from_pyseries(self._series == other._series)

    def __ne__(self, other: object) -> Series:  # type: ignore[override]
        if not isinstance(other, Series):
            raise TypeError(f"expected another Series but got {type(other)}")
        assert self._series is not None and other._series is not None
        return Series._from_pyseries(self._series != other._series)

    def __gt__(self, other: object) -> Series:
        if not isinstance(other, Series):
            raise TypeError(f"expected another Series but got {type(other)}")
        assert self._series is not None and other._series is not None
        return Series._from_pyseries(self._series > other._series)

    def __lt__(self, other: object) -> Series:
        if not isinstance(other, Series):
            raise TypeError(f"expected another Series but got {type(other)}")
        assert self._series is not None and other._series is not None
        return Series._from_pyseries(self._series < other._series)

    def __ge__(self, other: object) -> Series:
        if not isinstance(other, Series):
            raise TypeError(f"expected another Series but got {type(other)}")
        assert self._series is not None and other._series is not None
        return Series._from_pyseries(self._series >= other._series)

    def __le__(self, other: object) -> Series:
        if not isinstance(other, Series):
            raise TypeError(f"expected another Series but got {type(other)}")
        assert self._series is not None and other._series is not None
        return Series._from_pyseries(self._series <= other._series)

    def __and__(self, other: object) -> Series:
        if not isinstance(other, Series):
            raise TypeError(f"expected another Series but got {type(other)}")
        assert self._series is not None and other._series is not None
        return Series._from_pyseries(self._series & other._series)

    def __or__(self, other: object) -> Series:
        if not isinstance(other, Series):
            raise TypeError(f"expected another Series but got {type(other)}")
        assert self._series is not None and other._series is not None
        return Series._from_pyseries(self._series | other._series)

    def __xor__(self, other: object) -> Series:
        if not isinstance(other, Series):
            raise TypeError(f"expected another Series but got {type(other)}")
        assert self._series is not None and other._series is not None
        return Series._from_pyseries(self._series ^ other._series)

    def dt_year(self) -> Series:
        assert self._series is not None
        return Series._from_pyseries(self._series.dt_year())

    @property
    def str(self) -> SeriesStringNamespace:
        series = SeriesStringNamespace.__new__(SeriesStringNamespace)
        series._series = self._series
        return series


class SeriesNamespace:
    _series: PySeries

    def __init__(self) -> None:
        raise NotImplementedError("We do not support creating a SeriesNamespace via __init__ ")


class SeriesStringNamespace(SeriesNamespace):
    def endswith(self, suffix: Series) -> Series:
        if not isinstance(suffix, Series):
            raise ValueError(f"expected another Series but got {type(suffix)}")
        assert self._series is not None and suffix._series is not None
        return Series._from_pyseries(self._series.utf8_endswith(suffix._series))
    
