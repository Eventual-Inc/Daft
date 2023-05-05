from __future__ import annotations

from typing import TypeVar

import pyarrow as pa

from daft.arrow_utils import ensure_array, ensure_chunked_array
from daft.daft import PySeries
from daft.datatype import DataType

_NUMPY_AVAILABLE = True
try:
    import numpy as np
except ImportError:
    _NUMPY_AVAILABLE = False

_PANDAS_AVAILABLE = True
try:
    import pandas as pd
except ImportError:
    _PANDAS_AVAILABLE = False


class Series:
    _series: PySeries

    def __init__(self) -> None:
        raise NotImplementedError("We do not support creating a Series via __init__ ")

    @staticmethod
    def _from_pyseries(pyseries: PySeries) -> Series:
        s = Series.__new__(Series)
        s._series = pyseries
        return s

    @staticmethod
    def from_arrow(array: pa.Array | pa.ChunkedArray, name: str = "arrow_series") -> Series:
        if DataType.from_arrow_type(array.type) == DataType.python():
            # If the Arrow type is not natively supported, go through the Python list path.
            return Series.from_pylist(array.to_pylist(), name=name, pyobj="force")
        elif isinstance(array, pa.Array):
            array = ensure_array(array)
            pys = PySeries.from_arrow(name, array)
            return Series._from_pyseries(pys)
        elif isinstance(array, pa.ChunkedArray):
            array = ensure_chunked_array(array)
            combined_array = array.combine_chunks()
            pys = PySeries.from_arrow(name, combined_array)
            return Series._from_pyseries(pys)
        else:
            raise TypeError(f"expected either PyArrow Array or Chunked Array, got {type(array)}")

    @staticmethod
    def from_pylist(data: list, name: str = "list_series", pyobj: str = "allow") -> Series:
        """Make a series from the given data.

        The resulting type depends on the setting of pyobjects:
            - "allow": Arrow-backed types if possible, else PyObject;
            - "disallow": Arrow-backed types only, raising error if not convertible;
            - "force": Store as PyObject types.
        """

        if not isinstance(data, list):
            raise TypeError(f"expected a python list, got {type(data)}")

        if pyobj not in {"allow", "disallow", "force"}:
            raise ValueError(f"pyobj: expected either 'allow', 'disallow', or 'force', but got {pyobj})")

        if pyobj == "force":
            pys = PySeries.from_pylist(name, data)
            return Series._from_pyseries(pys)

        try:
            arrow_array = pa.array(data)
            return Series.from_arrow(arrow_array, name=name)
        except pa.lib.ArrowInvalid:
            if pyobj == "disallow":
                raise
            pys = PySeries.from_pylist(name, data)
            return Series._from_pyseries(pys)

    @classmethod
    def from_numpy(cls, data: np.ndarray, name: str = "numpy_series") -> Series:
        if not isinstance(data, np.ndarray):
            raise TypeError(f"Expected a NumPy ndarray, got {type(data)}")
        if data.ndim <= 1:
            try:
                arrow_array = pa.array(data)
            except pa.ArrowInvalid:
                pass
            else:
                return cls.from_arrow(arrow_array, name=name)
        # TODO(Clark): Represent the tensor series with an Arrow extension type in order
        # to keep the series data contiguous.
        list_ndarray = [np.asarray(item) for item in data]
        return cls.from_pylist(list_ndarray, name=name, pyobj="force")

    @classmethod
    def from_pandas(cls, data: pd.Series, name: str = "pd_series") -> Series:
        if not isinstance(data, pd.Series):
            raise TypeError(f"expected a pandas Series, got {type(data)}")
        # First, try Arrow path.
        try:
            arrow_arr = pa.Array.from_pandas(data)
        except pa.ArrowInvalid:
            pass
        else:
            return cls.from_arrow(arrow_arr, name=name)
        # Second, fall back to NumPy path. Note that .from_numpy() does _not_ fall back to
        # the pylist representation for 1D arrays and instead raises an error that we can catch.
        # We do the pylist representation fallback ourselves since the pd.Series.to_list()
        # preserves more type information for types that are not natively representable in Python.
        try:
            ndarray = data.to_numpy()
            return cls.from_numpy(ndarray, name=name)
        except Exception:
            pass
        # Finally, fall back to pylist path.
        # NOTE: For element types that don't have a native Python representation,
        # a Pandas scalar object will be returned.
        return cls.from_pylist(data.to_list(), name=name, pyobj="force")

    def cast(self, dtype: DataType) -> Series:
        return Series._from_pyseries(self._series.cast(dtype._dtype))

    def _cast_to_python(self) -> Series:
        """Convert this Series into a Series of Python objects.

        Call Series.to_pylist() and create a new Series from the raw Pylist directly.

        This logic is needed by the Rust implementation of cast(),
        but is written here (i.e. not in Rust) for conciseness.

        Do not call this method directly in Python; call cast() instead.
        """
        pylist = self.to_pylist()
        return Series.from_pylist(pylist, self.name(), pyobj="force")

    def _pycast_to_pynative(self, typefn: type) -> Series:
        """Apply Python-level casting to this Series.

        Call Series.to_pylist(), apply the Python cast (e.g. str(x)),
        and create a new arrow-backed Series from the result.

        This logic is needed by the Rust implementation of cast(),
        but is written here (i.e. not in Rust) for conciseness.

        Do not call this method directly in Python; call cast() instead.
        """
        pylist = self.to_pylist()
        pylist = [typefn(_) if _ is not None else None for _ in pylist]
        return Series.from_pylist(pylist, self.name(), pyobj="disallow")

    @staticmethod
    def concat(series: list[Series]) -> Series:
        pyseries = []
        for s in series:
            if not isinstance(s, Series):
                raise TypeError(f"Expected a Series for concat, got {type(s)}")
            pyseries.append(s._series)
        return Series._from_pyseries(PySeries.concat(pyseries))

    def name(self) -> str:
        return self._series.name()

    def rename(self, name: str) -> Series:
        return Series._from_pyseries(self._series.rename(name))

    def datatype(self) -> DataType:
        return DataType._from_pydatatype(self._series.data_type())

    def to_arrow(self) -> pa.Array:
        return self._series.to_arrow()

    def to_pylist(self) -> list:
        if self.datatype()._is_python_type():
            return self._series.to_pylist()
        else:
            return self._series.to_arrow().to_pylist()

    def filter(self, mask: Series) -> Series:
        if not isinstance(mask, Series):
            raise TypeError(f"expected another Series but got {type(mask)}")
        return Series._from_pyseries(self._series.filter(mask._series))

    def take(self, idx: Series) -> Series:
        if not isinstance(idx, Series):
            raise TypeError(f"expected another Series but got {type(idx)}")
        return Series._from_pyseries(self._series.take(idx._series))

    def slice(self, start: int, end: int) -> Series:
        if not isinstance(start, int):
            raise TypeError(f"expected int for start but got {type(start)}")
        if not isinstance(end, int):
            raise TypeError(f"expected int for end but got {type(end)}")

        return Series._from_pyseries(self._series.slice(start, end))

    def argsort(self, descending: bool = False) -> Series:
        if not isinstance(descending, bool):
            raise TypeError(f"expected `descending` to be bool, got {type(descending)}")

        return Series._from_pyseries(self._series.argsort(descending))

    def sort(self, descending: bool = False) -> Series:
        if not isinstance(descending, bool):
            raise TypeError(f"expected `descending` to be bool, got {type(descending)}")

        return Series._from_pyseries(self._series.sort(descending))

    def hash(self, seed: Series | None = None) -> Series:
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
        return len(self._series)

    def size_bytes(self) -> int:
        """Returns the total sizes of all buffers used for representing this Series.

        In particular, this includes the:

        1. Buffer(s) used for data (applies any slicing if that occurs!)
        2. Buffer(s) used for offsets, if applicable (for variable-length arrow types)
        3. Buffer(s) used for validity, if applicable (arrow can choose to omit the validity bitmask)
        4. Recursively gets .size_bytes for any child arrays, if applicable (for nested types)
        """
        return self._series.size_bytes()

    def __abs__(self) -> Series:
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

    def __invert__(self) -> Series:
        assert self._series is not None
        return Series._from_pyseries(self._series.__invert__())

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

    def _count(self) -> Series:
        assert self._series is not None
        return Series._from_pyseries(self._series._count())

    def _min(self) -> Series:
        assert self._series is not None
        return Series._from_pyseries(self._series._min())

    def _max(self) -> Series:
        assert self._series is not None
        return Series._from_pyseries(self._series._max())

    def _mean(self) -> Series:
        assert self._series is not None
        return Series._from_pyseries(self._series._mean())

    def _sum(self) -> Series:
        assert self._series is not None
        return Series._from_pyseries(self._series._sum())

    def if_else(self, if_true: object, if_false: object) -> Series:
        if not isinstance(if_true, Series):
            raise ValueError(f"expected another Series but got {type(if_true)}")
        if not isinstance(if_false, Series):
            raise ValueError(f"expected another Series but got {type(if_false)}")
        assert self._series is not None and if_true._series is not None and if_false._series is not None
        # NOTE: Rust Series has a different ordering for if_else because of better static typing
        return Series._from_pyseries(if_true._series.if_else(if_false._series, self._series))

    def is_null(self) -> Series:
        assert self._series is not None
        return Series._from_pyseries(self._series.is_null())

    @property
    def float(self) -> SeriesFloatNamespace:
        return SeriesFloatNamespace.from_series(self)

    @property
    def str(self) -> SeriesStringNamespace:
        return SeriesStringNamespace.from_series(self)

    @property
    def dt(self) -> SeriesDateNamespace:
        return SeriesDateNamespace.from_series(self)

    def __reduce__(self) -> tuple:
        if self.datatype()._is_python_type():
            return (Series.from_pylist, (self.to_pylist(), self.name(), "force"))
        else:
            return (Series.from_arrow, (self.to_arrow(), self.name()))


SomeSeriesNamespace = TypeVar("SomeSeriesNamespace", bound="SeriesNamespace")


class SeriesNamespace:
    _series: PySeries

    def __init__(self) -> None:
        raise NotImplementedError("We do not support creating a SeriesNamespace via __init__ ")

    @classmethod
    def from_series(cls: type[SomeSeriesNamespace], series: Series) -> SomeSeriesNamespace:
        ns = cls.__new__(cls)
        ns._series = series._series
        return ns


class SeriesFloatNamespace(SeriesNamespace):
    def is_nan(self) -> Series:
        return Series._from_pyseries(self._series.is_nan())


class SeriesStringNamespace(SeriesNamespace):
    def endswith(self, suffix: Series) -> Series:
        if not isinstance(suffix, Series):
            raise ValueError(f"expected another Series but got {type(suffix)}")
        assert self._series is not None and suffix._series is not None
        return Series._from_pyseries(self._series.utf8_endswith(suffix._series))

    def startswith(self, prefix: Series) -> Series:
        if not isinstance(prefix, Series):
            raise ValueError(f"expected another Series but got {type(prefix)}")
        assert self._series is not None and prefix._series is not None
        return Series._from_pyseries(self._series.utf8_startswith(prefix._series))

    def contains(self, pattern: Series) -> Series:
        if not isinstance(pattern, Series):
            raise ValueError(f"expected another Series but got {type(pattern)}")
        assert self._series is not None and pattern._series is not None
        return Series._from_pyseries(self._series.utf8_contains(pattern._series))

    def concat(self, other: Series) -> Series:
        if not isinstance(other, Series):
            raise ValueError(f"expected another Series but got {type(other)}")
        assert self._series is not None and other._series is not None
        return Series._from_pyseries(self._series) + other

    def length(self) -> Series:
        assert self._series is not None
        return Series._from_pyseries(self._series.utf8_length())


class SeriesDateNamespace(SeriesNamespace):
    def day(self) -> Series:
        return Series._from_pyseries(self._series.dt_day())

    def month(self) -> Series:
        return Series._from_pyseries(self._series.dt_month())

    def year(self) -> Series:
        return Series._from_pyseries(self._series.dt_year())

    def day_of_week(self) -> Series:
        return Series._from_pyseries(self._series.dt_day_of_week())
