from __future__ import annotations

from typing import Any, Literal, TypeVar

import pyarrow as pa

from daft.arrow_utils import ensure_array, ensure_chunked_array
from daft.daft import CountMode, ImageFormat, ImageMode, PySeries
from daft.datatype import DataType
from daft.utils import pyarrow_supports_fixed_shape_tensor

_RAY_DATA_EXTENSIONS_AVAILABLE = True
try:
    from ray.data.extensions import (
        ArrowTensorArray,
        ArrowTensorType,
        ArrowVariableShapedTensorType,
    )
except ImportError:
    _RAY_DATA_EXTENSIONS_AVAILABLE = False

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

ARROW_VERSION = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric())


class Series:
    """
    A Daft Series is an array of data of a single type, and is usually a column in a DataFrame.
    """

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
        """
        Construct a Series from an pyarrow array or chunked array.

        Args:
            array: The pyarrow (chunked) array whose data we wish to put in the Series.
            name: The name associated with the Series; this is usually the column name.
        """
        if DataType.from_arrow_type(array.type) == DataType.python():
            # If the Arrow type is not natively supported, go through the Python list path.
            return Series.from_pylist(array.to_pylist(), name=name, pyobj="force")
        elif isinstance(array, pa.Array):
            array = ensure_array(array)
            if _RAY_DATA_EXTENSIONS_AVAILABLE and isinstance(array.type, ArrowTensorType):
                storage_series = Series.from_arrow(array.storage, name=name)
                series = storage_series.cast(
                    DataType.fixed_size_list(
                        DataType.from_arrow_type(array.type.scalar_type),
                        int(np.prod(array.type.shape)),
                    )
                )
                return series.cast(DataType.from_arrow_type(array.type))
            elif _RAY_DATA_EXTENSIONS_AVAILABLE and isinstance(array.type, ArrowVariableShapedTensorType):
                return Series.from_numpy(array.to_numpy(zero_copy_only=False), name=name)
            elif isinstance(array.type, getattr(pa, "FixedShapeTensorType", ())):
                series = Series.from_arrow(array.storage, name=name)
                return series.cast(DataType.from_arrow_type(array.type))
            else:
                pys = PySeries.from_arrow(name, array)
                return Series._from_pyseries(pys)
        elif isinstance(array, pa.ChunkedArray):
            array = ensure_chunked_array(array)
            arr_type = array.type
            if isinstance(arr_type, pa.BaseExtensionType):
                combined_storage_array = array.cast(arr_type.storage_type).combine_chunks()
                combined_array = arr_type.wrap_array(combined_storage_array)
            else:
                combined_array = array.combine_chunks()
            return Series.from_arrow(combined_array)
        else:
            raise TypeError(f"expected either PyArrow Array or Chunked Array, got {type(array)}")

    @staticmethod
    def from_pylist(data: list, name: str = "list_series", pyobj: str = "allow") -> Series:
        """Construct a Series from a Python list.

        The resulting type depends on the setting of pyobjects:
            - ``"allow"``: Arrow-backed types if possible, else PyObject;
            - ``"disallow"``: Arrow-backed types only, raising error if not convertible;
            - ``"force"``: Store as PyObject types.

        Args:
            data: The Python list whose data we wish to put in the Series.
            name: The name associated with the Series; this is usually the column name.
            pyobj: Whether we want to ``"allow"`` coercion to Arrow types, ``"disallow"``
                falling back to Python type representation, or ``"force"`` the data to only
                have a Python type representation. Default is ``"allow"``.
        """

        if not isinstance(data, list):
            raise TypeError(f"expected a python list, got {type(data)}")

        if pyobj not in {"allow", "disallow", "force"}:
            raise ValueError(f"pyobj: expected either 'allow', 'disallow', or 'force', but got {pyobj})")

        if pyobj == "force":
            pys = PySeries.from_pylist(name, data, pyobj=pyobj)
            return Series._from_pyseries(pys)

        try:
            arrow_array = pa.array(data)
            return Series.from_arrow(arrow_array, name=name)
        except pa.lib.ArrowInvalid:
            if pyobj == "disallow":
                raise
            pys = PySeries.from_pylist(name, data, pyobj=pyobj)
            return Series._from_pyseries(pys)

    @classmethod
    def from_numpy(cls, data: np.ndarray, name: str = "numpy_series") -> Series:
        """
        Construct a Series from a NumPy ndarray.

        If the provided NumPy ndarray is 1-dimensional, Daft will attempt to store the ndarray
        in a pyarrow Array. If the ndarray has more than 1 dimension OR storing the 1D array in Arrow failed,
        Daft will store the ndarray data as a Python list of NumPy ndarrays.

        Args:
            data: The NumPy ndarray whose data we wish to put in the Series.
            name: The name associated with the Series; this is usually the column name.
        """
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
        return cls.from_pylist(list_ndarray, name=name, pyobj="allow")

    @classmethod
    def from_pandas(cls, data: pd.Series, name: str = "pd_series") -> Series:
        """
        Construct a Series from a pandas Series.

        This will first try to convert the series into a pyarrow array, then will fall
        back to converting the series to a NumPy ndarray and going through that construction path,
        and will finally fall back to converting the series to a Python list and going through that
        path.

        Args:
            data: The pandas Series whose data we wish to put in the Daft Series.
            name: The name associated with the Series; this is usually the column name.
        """
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

    def to_arrow(self, cast_tensors_to_ray_tensor_dtype: bool = False) -> pa.Array:
        """
        Convert this Series to an pyarrow array.
        """
        dtype = self.datatype()
        if cast_tensors_to_ray_tensor_dtype and (dtype._is_tensor_type() or dtype._is_fixed_shape_tensor_type()):
            if not _RAY_DATA_EXTENSIONS_AVAILABLE:
                raise ValueError("Trying to convert tensors to Ray tensor dtypes, but Ray is not installed.")
            pyarrow_dtype = dtype.to_arrow_dtype(cast_tensor_to_ray_type=True)
            if isinstance(pyarrow_dtype, ArrowTensorType):
                assert dtype._is_fixed_shape_tensor_type()
                arrow_series = self._series.to_arrow()
                storage = arrow_series.storage
                list_size = storage.type.list_size
                storage = pa.ListArray.from_arrays(
                    pa.array(
                        list(range(0, (len(arrow_series) + 1) * list_size, list_size)),
                        pa.int32(),
                    ),
                    storage.values,
                )
                return pa.ExtensionArray.from_storage(pyarrow_dtype, storage)
            else:
                # Variable-shaped tensor columns can't be converted directly to Ray's variable-shaped tensor extension
                # type since it expects all tensor elements to have the same number of dimensions, which Daft does not enforce.
                # TODO(Clark): Convert directly to Ray's variable-shaped tensor extension type when all tensor
                # elements have the same number of dimensions, without going through pylist roundtrip.
                return ArrowTensorArray.from_numpy(self.to_pylist())
        elif dtype._is_fixed_shape_tensor_type() and pyarrow_supports_fixed_shape_tensor():
            pyarrow_dtype = dtype.to_arrow_dtype(cast_tensor_to_ray_type=False)
            arrow_series = self._series.to_arrow()
            return pa.ExtensionArray.from_storage(pyarrow_dtype, arrow_series.storage)
        else:
            return self._series.to_arrow()

    def to_pylist(self) -> list:
        """
        Convert this Series to a Python list.
        """
        if self.datatype()._is_python_type():
            return self._series.to_pylist()
        elif self.datatype()._should_cast_to_python():
            return self._series.cast(DataType.python()._dtype).to_pylist()
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

    def murmur3_32(self) -> Series:
        return Series._from_pyseries(self._series.murmur3_32())

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

    def ceil(self) -> Series:
        return Series._from_pyseries(self._series.ceil())

    def floor(self) -> Series:
        return Series._from_pyseries(self._series.floor())

    def sign(self) -> Series:
        return Series._from_pyseries(self._series.sign())

    def round(self, decimal: int) -> Series:
        return Series._from_pyseries(self._series.round(decimal))

    def sqrt(self) -> Series:
        return Series._from_pyseries(self._series.sqrt())

    def sin(self) -> Series:
        """The elementwise sine of a numeric series."""
        return Series._from_pyseries(self._series.sin())

    def cos(self) -> Series:
        """The elementwise cosine of a numeric series."""
        return Series._from_pyseries(self._series.cos())

    def tan(self) -> Series:
        """The elementwise tangent of a numeric series."""
        return Series._from_pyseries(self._series.tan())

    def cot(self) -> Series:
        """The elementwise cotangent of a numeric series"""
        return Series._from_pyseries(self._series.cot())

    def arcsin(self) -> Series:
        """The elementwise arc sine of a numeric series"""
        return Series._from_pyseries(self._series.arcsin())

    def arccos(self) -> Series:
        """The elementwise arc cosine of a numeric series"""
        return Series._from_pyseries(self._series.arccos())

    def arctan(self) -> Series:
        """The elementwise arc tangent of a numeric series"""
        return Series._from_pyseries(self._series.arctan())

    def arctan2(self, other: Series) -> Series:
        """Calculates the four quadrant arctangent of coordinates (y, x)"""
        if not isinstance(other, Series):
            raise TypeError(f"expected another Series but got {type(other)}")
        return Series._from_pyseries(self._series.arctan2(other._series))

    def arctanh(self) -> Series:
        """The elementwise inverse hyperbolic tangent of a numeric series"""
        return Series._from_pyseries(self._series.arctanh())

    def arccosh(self) -> Series:
        """The elementwise inverse hyperbolic cosine of a numeric series"""
        return Series._from_pyseries(self._series.arccosh())

    def arcsinh(self) -> Series:
        """The elementwise inverse hyperbolic sine of a numeric series"""
        return Series._from_pyseries(self._series.arcsinh())

    def radians(self) -> Series:
        """The elementwise radians of a numeric series"""
        return Series._from_pyseries(self._series.radians())

    def degrees(self) -> Series:
        """The elementwise degrees of a numeric series"""
        return Series._from_pyseries(self._series.degrees())

    def log2(self) -> Series:
        """The elementwise log2 of a numeric series"""
        return Series._from_pyseries(self._series.log2())

    def log10(self) -> Series:
        """The elementwise log10 of a numeric series"""
        return Series._from_pyseries(self._series.log10())

    def log(self, base: float) -> Series:
        """The elementwise log with given base, of a numeric series.
        Args:
            base: The base of the logarithm.
        """
        return Series._from_pyseries(self._series.log(base))

    def ln(self) -> Series:
        """The elementwise ln of a numeric series"""
        return Series._from_pyseries(self._series.ln())

    def exp(self) -> Series:
        """The e^self of a numeric series"""
        return Series._from_pyseries(self._series.exp())

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

    def __lshift__(self, other: object) -> Series:
        if not isinstance(other, Series):
            raise TypeError(f"expected another Series but got {type(other)}")
        assert self._series is not None and other._series is not None
        return Series._from_pyseries(self._series << other._series)

    def __rshift__(self, other: object) -> Series:
        if not isinstance(other, Series):
            raise TypeError(f"expected another Series but got {type(other)}")
        assert self._series is not None and other._series is not None
        return Series._from_pyseries(self._series >> other._series)

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

    def count(self, mode: CountMode = CountMode.Valid) -> Series:
        assert self._series is not None
        return Series._from_pyseries(self._series.count(mode))

    def min(self) -> Series:
        assert self._series is not None
        return Series._from_pyseries(self._series.min())

    def max(self) -> Series:
        assert self._series is not None
        return Series._from_pyseries(self._series.max())

    def mean(self) -> Series:
        assert self._series is not None
        return Series._from_pyseries(self._series.mean())

    def sum(self) -> Series:
        assert self._series is not None
        return Series._from_pyseries(self._series.sum())

    def shift_right(self, bits: Series) -> Series:
        if not isinstance(bits, Series):
            raise TypeError(f"expected another Series but got {type(bits)}")
        assert self._series is not None and bits._series is not None
        return Series._from_pyseries(self._series >> bits._series)

    def shift_left(self, bits: Series) -> Series:
        if not isinstance(bits, Series):
            raise TypeError(f"expected another Series but got {type(bits)}")
        assert self._series is not None and bits._series is not None
        return Series._from_pyseries(self._series << bits._series)

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

    def not_null(self) -> Series:
        assert self._series is not None
        return Series._from_pyseries(self._series.not_null())

    def fill_null(self, fill_value: object) -> Series:
        if not isinstance(fill_value, Series):
            raise ValueError(f"expected another Series but got {type(fill_value)}")
        assert self._series is not None and fill_value._series is not None
        return Series._from_pyseries(self._series.fill_null(fill_value._series))

    def minhash(
        self,
        num_hashes: int,
        ngram_size: int,
        seed: int = 1,
    ) -> Series:
        """
        Runs the MinHash algorithm on the series.

        For a string, calculates the minimum hash over all its ngrams,
        repeating with `num_hashes` permutations. Returns as a list of 32-bit unsigned integers.

        Tokens for the ngrams are delimited by spaces.
        MurmurHash is used for the initial hash.

        Args:
            num_hashes: The number of hash permutations to compute.
            ngram_size: The number of tokens in each shingle/ngram.
            seed (optional): Seed used for generating permutations and the initial string hashes. Defaults to 1.
        """
        if not isinstance(num_hashes, int):
            raise ValueError(f"expected an integer for num_hashes but got {type(num_hashes)}")
        if not isinstance(ngram_size, int):
            raise ValueError(f"expected an integer for ngram_size but got {type(ngram_size)}")
        if seed is not None and not isinstance(seed, int):
            raise ValueError(f"expected an integer or None for seed but got {type(seed)}")

        return Series._from_pyseries(self._series.minhash(num_hashes, ngram_size, seed))

    def _to_str_values(self) -> Series:
        return Series._from_pyseries(self._series.to_str_values())

    @property
    def float(self) -> SeriesFloatNamespace:
        return SeriesFloatNamespace.from_series(self)

    @property
    def str(self) -> SeriesStringNamespace:
        return SeriesStringNamespace.from_series(self)

    @property
    def dt(self) -> SeriesDateNamespace:
        return SeriesDateNamespace.from_series(self)

    @property
    def list(self) -> SeriesListNamespace:
        return SeriesListNamespace.from_series(self)

    @property
    def map(self) -> SeriesMapNamespace:
        return SeriesMapNamespace.from_series(self)

    @property
    def image(self) -> SeriesImageNamespace:
        return SeriesImageNamespace.from_series(self)

    @property
    def partitioning(self) -> SeriesPartitioningNamespace:
        return SeriesPartitioningNamespace.from_series(self)

    def __reduce__(self) -> tuple:
        if self.datatype()._is_python_type():
            return (Series.from_pylist, (self.to_pylist(), self.name(), "force"))
        else:
            return (Series.from_arrow, (self.to_arrow(), self.name()))

    def _debug_bincode_serialize(self) -> bytes:
        return self._series._debug_bincode_serialize()

    @classmethod
    def _debug_bincode_deserialize(cls, b: bytes) -> Series:
        return Series._from_pyseries(PySeries._debug_bincode_deserialize(b))


def item_to_series(name: str, item: Any) -> Series:
    if isinstance(item, list):
        series = Series.from_pylist(item, name)
    elif _NUMPY_AVAILABLE and isinstance(item, np.ndarray):
        series = Series.from_numpy(item, name)
    elif isinstance(item, Series):
        series = item
    elif isinstance(item, (pa.Array, pa.ChunkedArray)):
        series = Series.from_arrow(item, name)
    elif _PANDAS_AVAILABLE and isinstance(item, pd.Series):
        series = Series.from_pandas(item, name)
    else:
        raise ValueError(f"Creating a Series from data of type {type(item)} not implemented")
    return series


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

    def is_inf(self) -> Series:
        return Series._from_pyseries(self._series.is_inf())

    def not_nan(self) -> Series:
        return Series._from_pyseries(self._series.not_nan())

    def fill_nan(self, fill_value: Series) -> Series:
        if not isinstance(fill_value, Series):
            raise ValueError(f"expected another Series but got {type(fill_value)}")
        assert self._series is not None and fill_value._series is not None
        return Series._from_pyseries(self._series.fill_nan(fill_value._series))


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

    def match(self, pattern: Series) -> Series:
        if not isinstance(pattern, Series):
            raise ValueError(f"expected another Series but got {type(pattern)}")
        assert self._series is not None and pattern._series is not None
        return Series._from_pyseries(self._series.utf8_match(pattern._series))

    def split(self, pattern: Series, regex: bool = False) -> Series:
        if not isinstance(pattern, Series):
            raise ValueError(f"expected another Series but got {type(pattern)}")
        assert self._series is not None and pattern._series is not None
        return Series._from_pyseries(self._series.utf8_split(pattern._series, regex))

    def concat(self, other: Series) -> Series:
        if not isinstance(other, Series):
            raise ValueError(f"expected another Series but got {type(other)}")
        assert self._series is not None and other._series is not None
        return Series._from_pyseries(self._series) + other

    def extract(self, pattern: Series, index: int = 0) -> Series:
        if not isinstance(pattern, Series):
            raise ValueError(f"expected another Series but got {type(pattern)}")
        assert self._series is not None and pattern._series is not None
        return Series._from_pyseries(self._series.utf8_extract(pattern._series, index))

    def extract_all(self, pattern: Series, index: int = 0) -> Series:
        if not isinstance(pattern, Series):
            raise ValueError(f"expected another Series but got {type(pattern)}")
        assert self._series is not None and pattern._series is not None
        return Series._from_pyseries(self._series.utf8_extract_all(pattern._series, index))

    def replace(self, pattern: Series, replacement: Series, regex: bool = False) -> Series:
        if not isinstance(pattern, Series):
            raise ValueError(f"expected another Series but got {type(pattern)}")
        if not isinstance(replacement, Series):
            raise ValueError(f"expected another Series but got {type(replacement)}")
        assert self._series is not None and pattern._series is not None
        return Series._from_pyseries(self._series.utf8_replace(pattern._series, replacement._series, regex))

    def length(self) -> Series:
        assert self._series is not None
        return Series._from_pyseries(self._series.utf8_length())

    def lower(self) -> Series:
        assert self._series is not None
        return Series._from_pyseries(self._series.utf8_lower())

    def upper(self) -> Series:
        assert self._series is not None
        return Series._from_pyseries(self._series.utf8_upper())

    def lstrip(self) -> Series:
        assert self._series is not None
        return Series._from_pyseries(self._series.utf8_lstrip())

    def rstrip(self) -> Series:
        assert self._series is not None
        return Series._from_pyseries(self._series.utf8_rstrip())

    def reverse(self) -> Series:
        assert self._series is not None
        return Series._from_pyseries(self._series.utf8_reverse())

    def capitalize(self) -> Series:
        assert self._series is not None
        return Series._from_pyseries(self._series.utf8_capitalize())

    def left(self, nchars: Series) -> Series:
        if not isinstance(nchars, Series):
            raise ValueError(f"expected another Series but got {type(nchars)}")
        assert self._series is not None and nchars._series is not None
        return Series._from_pyseries(self._series.utf8_left(nchars._series))

    def right(self, nchars: Series) -> Series:
        if not isinstance(nchars, Series):
            raise ValueError(f"expected another Series but got {type(nchars)}")
        assert self._series is not None and nchars._series is not None
        return Series._from_pyseries(self._series.utf8_right(nchars._series))

    def find(self, substr: Series) -> Series:
        if not isinstance(substr, Series):
            raise ValueError(f"expected another Series but got {type(substr)}")
        assert self._series is not None and substr._series is not None
        return Series._from_pyseries(self._series.utf8_find(substr._series))

    def rpad(self, length: Series, pad: Series) -> Series:
        if not isinstance(length, Series):
            raise ValueError(f"expected another Series but got {type(length)}")
        if not isinstance(pad, Series):
            raise ValueError(f"expected another Series but got {type(pad)}")
        assert self._series is not None and length._series is not None and pad._series is not None
        return Series._from_pyseries(self._series.utf8_rpad(length._series, pad._series))

    def lpad(self, length: Series, pad: Series) -> Series:
        if not isinstance(length, Series):
            raise ValueError(f"expected another Series but got {type(length)}")
        if not isinstance(pad, Series):
            raise ValueError(f"expected another Series but got {type(pad)}")
        assert self._series is not None and length._series is not None and pad._series is not None
        return Series._from_pyseries(self._series.utf8_lpad(length._series, pad._series))

    def repeat(self, n: Series) -> Series:
        if not isinstance(n, Series):
            raise ValueError(f"expected another Series but got {type(n)}")
        assert self._series is not None and n._series is not None
        return Series._from_pyseries(self._series.utf8_repeat(n._series))

    def like(self, pattern: Series) -> Series:
        if not isinstance(pattern, Series):
            raise ValueError(f"expected another Series but got {type(pattern)}")
        assert self._series is not None and pattern._series is not None
        return Series._from_pyseries(self._series.utf8_like(pattern._series))

    def ilike(self, pattern: Series) -> Series:
        if not isinstance(pattern, Series):
            raise ValueError(f"expected another Series but got {type(pattern)}")
        assert self._series is not None and pattern._series is not None
        return Series._from_pyseries(self._series.utf8_ilike(pattern._series))

    def to_date(self, format: str) -> Series:
        if not isinstance(format, str):
            raise ValueError(f"expected str for format but got {type(format)}")
        assert self._series is not None
        return Series._from_pyseries(self._series.utf8_to_date(format))

    def to_datetime(self, format: str, timezone: str | None = None) -> Series:
        if not isinstance(format, str):
            raise ValueError(f"expected str for format but got {type(format)}")
        if timezone is not None and not isinstance(timezone, str):
            raise ValueError(f"expected str for timezone but got {type(timezone)}")
        assert self._series is not None
        return Series._from_pyseries(self._series.utf8_to_datetime(format, timezone))

    def substr(self, start: Series, length: Series | None = None) -> Series:
        if not isinstance(start, Series):
            raise ValueError(f"expected another Series but got {type(start)}")
        if length is not None and not isinstance(length, Series):
            raise ValueError(f"expected another Series but got {type(length)}")
        if length is None:
            length = Series.from_arrow(pa.array([None]))

        assert self._series is not None and start._series is not None
        return Series._from_pyseries(self._series.utf8_substr(start._series, length._series))

    def normalize(
        self,
        *,
        remove_punct: bool = True,
        lowercase: bool = True,
        nfd_unicode: bool = True,
        white_space: bool = True,
    ) -> Series:
        if not isinstance(remove_punct, bool):
            raise ValueError(f"expected bool for remove_punct but got {type(remove_punct)}")
        if not isinstance(lowercase, bool):
            raise ValueError(f"expected bool for lowercase but got {type(lowercase)}")
        if not isinstance(nfd_unicode, bool):
            raise ValueError(f"expected bool for nfd_unicode but got {type(nfd_unicode)}")
        if not isinstance(white_space, bool):
            raise ValueError(f"expected bool for white_space but got {type(white_space)}")
        assert self._series is not None
        return Series._from_pyseries(self._series.utf8_normalize(remove_punct, lowercase, nfd_unicode, white_space))

    def count_matches(self, patterns: Series, whole_words: bool = False, case_sensitive: bool = True) -> Series:
        if not isinstance(patterns, Series):
            raise ValueError(f"expected another Series but got {type(patterns)}")
        if not isinstance(whole_words, bool):
            raise ValueError(f"expected bool for whole_word but got {type(whole_words)}")
        if not isinstance(case_sensitive, bool):
            raise ValueError(f"expected bool for case_sensitive but got {type(case_sensitive)}")
        assert self._series is not None and patterns._series is not None
        return Series._from_pyseries(self._series.utf8_count_matches(patterns._series, whole_words, case_sensitive))


class SeriesDateNamespace(SeriesNamespace):
    def date(self) -> Series:
        return Series._from_pyseries(self._series.dt_date())

    def day(self) -> Series:
        return Series._from_pyseries(self._series.dt_day())

    def hour(self) -> Series:
        return Series._from_pyseries(self._series.dt_hour())

    def minute(self) -> Series:
        return Series._from_pyseries(self._series.dt_minute())

    def second(self) -> Series:
        return Series._from_pyseries(self._series.dt_second())

    def time(self) -> Series:
        return Series._from_pyseries(self._series.dt_time())

    def month(self) -> Series:
        return Series._from_pyseries(self._series.dt_month())

    def year(self) -> Series:
        return Series._from_pyseries(self._series.dt_year())

    def day_of_week(self) -> Series:
        return Series._from_pyseries(self._series.dt_day_of_week())

    def truncate(self, interval: str, relative_to: Series | None = None) -> Series:
        if relative_to is not None and not isinstance(relative_to, Series):
            raise ValueError(f"expected another Series but got {type(relative_to)}")
        if relative_to is None:
            relative_to = Series.from_arrow(pa.array([None]))
        return Series._from_pyseries(self._series.dt_truncate(interval, relative_to._series))


class SeriesPartitioningNamespace(SeriesNamespace):
    def days(self) -> Series:
        return Series._from_pyseries(self._series.partitioning_days())

    def hours(self) -> Series:
        return Series._from_pyseries(self._series.partitioning_hours())

    def months(self) -> Series:
        return Series._from_pyseries(self._series.partitioning_months())

    def years(self) -> Series:
        return Series._from_pyseries(self._series.partitioning_years())

    def iceberg_bucket(self, n: int) -> Series:
        return Series._from_pyseries(self._series.partitioning_iceberg_bucket(n))

    def iceberg_truncate(self, w: int) -> Series:
        return Series._from_pyseries(self._series.partitioning_iceberg_truncate(w))


class SeriesListNamespace(SeriesNamespace):
    def lengths(self) -> Series:
        return Series._from_pyseries(self._series.list_count(CountMode.All))

    def get(self, idx: Series, default: Series) -> Series:
        return Series._from_pyseries(self._series.list_get(idx._series, default._series))


class SeriesMapNamespace(SeriesNamespace):
    def get(self, key: Series) -> Series:
        return Series._from_pyseries(self._series.map_get(key._series))


class SeriesImageNamespace(SeriesNamespace):
    def decode(
        self,
        on_error: Literal["raise"] | Literal["null"] = "raise",
        mode: str | ImageMode | None = None,
    ) -> Series:
        raise_on_error = False
        if on_error == "raise":
            raise_on_error = True
        elif on_error == "null":
            raise_on_error = False
        else:
            raise NotImplementedError(f"Unimplemented on_error option: {on_error}.")
        if mode is not None:
            if isinstance(mode, str):
                mode = ImageMode.from_mode_string(mode.upper())
            if not isinstance(mode, ImageMode):
                raise ValueError(f"mode must be a string or ImageMode variant, but got: {mode}")
        return Series._from_pyseries(self._series.image_decode(raise_error_on_failure=raise_on_error, mode=mode))

    def encode(self, image_format: str | ImageFormat) -> Series:
        if isinstance(image_format, str):
            image_format = ImageFormat.from_format_string(image_format.upper())
        if not isinstance(image_format, ImageFormat):
            raise ValueError(f"image_format must be a string or ImageFormat variant, but got: {image_format}")
        return Series._from_pyseries(self._series.image_encode(image_format))

    def resize(self, w: int, h: int) -> Series:
        if not isinstance(w, int):
            raise TypeError(f"expected int for w but got {type(w)}")
        if not isinstance(h, int):
            raise TypeError(f"expected int for h but got {type(h)}")

        return Series._from_pyseries(self._series.image_resize(w, h))

    def to_mode(self, mode: str | ImageMode) -> Series:
        if isinstance(mode, str):
            mode = ImageMode.from_mode_string(mode.upper())
        if not isinstance(mode, ImageMode):
            raise ValueError(f"mode must be a string or ImageMode variant, but got: {mode}")
        return Series._from_pyseries(self._series.image_to_mode(mode))
