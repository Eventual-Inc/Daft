from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Literal, TypeVar

import daft.daft as native
from daft.arrow_utils import ensure_array, ensure_chunked_array
from daft.daft import CountMode, ImageFormat, ImageMode, PyRecordBatch, PySeries, PySeriesIterator
from daft.datatype import DataType, TimeUnit, _ensure_registered_super_ext_type
from daft.dependencies import np, pa, pd
from daft.schema import Field
from daft.utils import pyarrow_supports_fixed_shape_tensor

if TYPE_CHECKING:
    import builtins

    from daft.daft import PyDataType


class Series:
    """A Daft Series is an array of data of a single type, and is usually a column in a DataFrame."""

    _series: PySeries

    def __init__(self) -> None:
        raise NotImplementedError("We do not support creating a Series via __init__ ")

    def __iter__(self) -> PySeriesIterator:
        """Return an iterator over the elements of the Series."""
        return self._series.__iter__()

    @staticmethod
    def _from_pyseries(pyseries: PySeries) -> Series:
        s = Series.__new__(Series)
        s._series = pyseries
        return s

    @staticmethod
    def from_arrow(
        array: pa.Array | pa.ChunkedArray, name: str = "arrow_series", dtype: DataType | None = None
    ) -> Series:
        """Construct a Series from an pyarrow array or chunked array.

        Args:
            array: The pyarrow (chunked) array whose data we wish to put in the Series.
            name: The name associated with the Series; this is usually the column name.
            dtype: The DataType to use for the Series. If not provided, Daft will infer the
                DataType from the data.
        """
        _ensure_registered_super_ext_type()
        try:
            DataType.from_arrow_type(array.type, python_fallback=False)
        except TypeError:
            # If the Arrow type is not natively supported, go through the Python list path.
            return Series.from_pylist(array.to_pylist(), name=name, pyobj="force")
        if isinstance(array, pa.Array):
            array = ensure_array(array)
            if isinstance(array.type, getattr(pa, "FixedShapeTensorType", ())):
                series = Series.from_arrow(array.storage, name=name)
                return series.cast(dtype or DataType.from_arrow_type(array.type))
            else:
                pys = PySeries.from_arrow(name, array, dtype=dtype._dtype if dtype else None)
                return Series._from_pyseries(pys)
        elif isinstance(array, pa.ChunkedArray):
            array = ensure_chunked_array(array)
            arr_type = array.type
            if isinstance(arr_type, pa.BaseExtensionType):
                combined_storage_array = array.cast(arr_type.storage_type).combine_chunks()
                combined_array = arr_type.wrap_array(combined_storage_array)
            else:
                combined_array = array.combine_chunks()
            return Series.from_arrow(combined_array, name=name, dtype=dtype)
        else:
            raise TypeError(f"expected either PyArrow Array or Chunked Array, got {type(array)}")

    @staticmethod
    def from_pylist(
        data: list[Any],
        name: str = "list_series",
        dtype: DataType | None = None,
        pyobj: Literal["allow", "disallow", "force"] = "allow",
    ) -> Series:
        """Construct a Series from a Python list.

        If `dtype` is not defined, then the resulting type depends on the setting of `pyobj`:
            - ``"allow"``: Daft-native types if possible, else PyObject;
            - ``"disallow"``: Daft-native types only, raising error if not convertible;
            - ``"force"``: Always store as PyObject types. Equivalent to `dtype=daft.DataType.python()`.

        Args:
            data: The Python list whose data we wish to put in the Series.
            name: The name associated with the Series; this is usually the column name.
            dtype: The DataType to use for the Series. If not provided, Daft will infer the
                DataType from the data.
            pyobj: Whether we want to ``"allow"`` coercion to Arrow types, ``"disallow"``
                falling back to Python type representation. Default is ``"allow"``.
        """
        if not isinstance(data, list):
            raise TypeError(f"expected a python list, got {type(data)}")

        if pyobj not in {"allow", "disallow", "force"}:
            raise ValueError(f"pyobj: expected either 'allow', 'disallow', or 'force', but got {pyobj})")

        if pyobj == "force":
            dtype = DataType.python()

        pys = PySeries.from_pylist(data, name, None if dtype is None else dtype._dtype)
        series = Series._from_pyseries(pys)

        if pyobj == "disallow" and series.datatype().is_python():
            raise TypeError("Could not convert Python list to a Daft-native type, and pyobj='disallow' was set.")

        return series

    @classmethod
    def from_numpy(
        cls, data: np.ndarray[Any, Any], name: str = "numpy_series", dtype: DataType | None = None
    ) -> Series:
        """Construct a Series from a NumPy ndarray.

        If the provided NumPy ndarray is 1-dimensional, Daft will attempt to store the ndarray
        in a pyarrow Array. If the ndarray has more than 1 dimension OR storing the 1D array in Arrow failed,
        Daft will store the ndarray data as a Python list of NumPy ndarrays.

        Args:
            data: The NumPy ndarray whose data we wish to put in the Series.
            name: The name associated with the Series; this is usually the column name.
            dtype: The DataType to use for the Series. If not provided, Daft will infer the
                DataType from the data.
        """
        if not isinstance(data, np.ndarray):
            raise TypeError(f"Expected a NumPy ndarray, got {type(data)}")
        if data.ndim <= 1:
            try:
                arrow_array = pa.array(data)
            except pa.ArrowInvalid:
                pass
            else:
                return cls.from_arrow(arrow_array, name=name, dtype=dtype)
        # TODO(Clark): Represent the tensor series with an Arrow extension type in order
        # to keep the series data contiguous.
        return cls.from_pylist(list(data), name=name, dtype=dtype)

    @classmethod
    def from_pandas(cls, data: pd.Series[Any], name: str = "pd_series", dtype: DataType | None = None) -> Series:
        """Construct a Series from a pandas Series.

        This will first try to convert the series into a pyarrow array, then will fall
        back to converting the series to a NumPy ndarray and going through that construction path,
        and will finally fall back to converting the series to a Python list and going through that
        path.

        Args:
            data: The pandas Series whose data we wish to put in the Daft Series.
            name: The name associated with the Series; this is usually the column name.
            dtype: The DataType to use for the Series. If not provided, Daft will infer the
                DataType from the data.
        """
        if not isinstance(data, pd.Series):
            raise TypeError(f"expected a pandas Series, got {type(data)}")
        # First, try Arrow path.
        try:
            arrow_arr = pa.Array.from_pandas(data)
        except pa.ArrowInvalid:
            pass
        else:
            return cls.from_arrow(arrow_arr, name=name, dtype=dtype)
        # Second, fall back to NumPy path. Note that .from_numpy() does _not_ fall back to
        # the pylist representation for 1D arrays and instead raises an error that we can catch.
        # We do the pylist representation fallback ourselves since the pd.Series.to_list()
        # preserves more type information for types that are not natively representable in Python.
        try:
            ndarray = data.to_numpy()
            return cls.from_numpy(ndarray, name=name, dtype=dtype)
        except Exception:
            pass
        # Finally, fall back to pylist path.
        # NOTE: For element types that don't have a native Python representation,
        # a Pandas scalar object will be returned.
        return cls.from_pylist(data.to_list(), name=name, dtype=dtype if dtype else DataType.python())

    def cast(self, dtype: DataType) -> Series:
        return Series._from_pyseries(self._series.cast(dtype._dtype))

    def _pycast_to_pynative(self, typefn: type, dtype: PyDataType) -> Series:
        """Apply Python-level casting to this Series.

        Call Series.to_pylist(), apply the Python cast (e.g. str(x)),
        and create a new arrow-backed Series from the result.

        This logic is needed by the Rust implementation of cast(),
        but is written here (i.e. not in Rust) for conciseness.

        Do not call this method directly in Python; call cast() instead.
        """
        pylist = self.to_pylist()
        pylist = [typefn(val) if val is not None else None for val in pylist]
        return Series.from_pylist(pylist, self.name(), pyobj="disallow", dtype=DataType._from_pydatatype(dtype))

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
        """Convert this Series to an pyarrow array."""
        _ensure_registered_super_ext_type()

        dtype = self.datatype()

        # Special-case for PyArrow FixedShapeTensor if it is supported by the version of PyArrow
        # TODO: Push this down into self._series.to_arrow()?
        if dtype.is_fixed_shape_tensor() and pyarrow_supports_fixed_shape_tensor():
            pyarrow_dtype = dtype.to_arrow_dtype()
            arrow_series = self._series.to_arrow()
            return pa.ExtensionArray.from_storage(pyarrow_dtype, arrow_series.storage)
        else:
            return self._series.to_arrow()

    def to_pylist(self) -> list[Any]:
        """Convert this Series to a Python list."""
        return self._series.to_pylist()

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

    def argsort(self, descending: bool = False, nulls_first: bool | None = None) -> Series:
        if not isinstance(descending, bool):
            raise TypeError(f"expected `descending` to be bool, got {type(descending)}")
        if nulls_first is None:
            nulls_first = descending

        return Series._from_pyseries(self._series.argsort(descending, nulls_first))

    def sort(self, descending: bool = False, nulls_first: bool | None = None) -> Series:
        if not isinstance(descending, bool):
            raise TypeError(f"expected `descending` to be bool, got {type(descending)}")
        if nulls_first is None:
            nulls_first = descending
        return Series._from_pyseries(self._series.sort(descending, nulls_first))

    def hash(self, seed: Series | None = None) -> Series:
        return self._eval_expressions("hash", seed=seed)

    def murmur3_32(self) -> Series:
        return Series._from_pyseries(self._series.murmur3_32())

    def __repr__(self) -> str:
        return repr(self._series)

    def __getitem__(self, index: int) -> Any:
        return self._series[index]

    def __bool__(self) -> bool:
        raise ValueError(
            "Series don't have a truth value. If you reached this error using `and` / `or`, use `&` / `|` instead."
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
        return self._eval_expressions("ceil")

    def floor(self) -> Series:
        return self._eval_expressions("floor")

    def sign(self) -> Series:
        """The sign of a numeric series."""
        return self._eval_expressions("sign")

    def negate(self) -> Series:
        """The negative of a numeric series."""
        return self._eval_expressions("negate")

    def round(self, decimals: int = 0) -> Series:
        return self._eval_expressions("round", decimals=decimals)

    def clip(self, min: Series, max: Series) -> Series:
        return self._eval_expressions("clip", min, max)

    def sqrt(self) -> Series:
        return self._eval_expressions("sqrt")

    def cbrt(self) -> Series:
        return self._eval_expressions("cbrt")

    def sin(self) -> Series:
        """The elementwise sine of a numeric series."""
        return self._eval_expressions("sin")

    def cos(self) -> Series:
        """The elementwise cosine of a numeric series."""
        return self._eval_expressions("cos")

    def tan(self) -> Series:
        """The elementwise tangent of a numeric series."""
        return self._eval_expressions("tan")

    def csc(self) -> Series:
        """The elementwise cosecant of a numeric series."""
        return self._eval_expressions("csc")

    def sec(self) -> Series:
        """The elementwise secant of a numeric series."""
        return self._eval_expressions("sec")

    def cot(self) -> Series:
        """The elementwise cotangent of a numeric series."""
        return self._eval_expressions("cot")

    def sinh(self) -> Series:
        """The elementwise hyperbolic sine of a numeric series."""
        return self._eval_expressions("sinh")

    def cosh(self) -> Series:
        """The elementwise hyperbolic cosine of a numeric series."""
        return self._eval_expressions("cosh")

    def tanh(self) -> Series:
        """The elementwise hyperbolic tangent of a numeric series."""
        return self._eval_expressions("tanh")

    def arcsin(self) -> Series:
        """The elementwise arc sine of a numeric series."""
        return self._eval_expressions("arcsin")

    def arccos(self) -> Series:
        """The elementwise arc cosine of a numeric series."""
        return self._eval_expressions("arccos")

    def arctan(self) -> Series:
        """The elementwise arc tangent of a numeric series."""
        return self._eval_expressions("arctan")

    def arctan2(self, other: Series) -> Series:
        """Calculates the four quadrant arctangent of coordinates (y, x)."""
        if not isinstance(other, Series):
            raise TypeError(f"expected another Series but got {type(other)}")
        return self._eval_expressions("arctan2", other)

    def arctanh(self) -> Series:
        """The elementwise inverse hyperbolic tangent of a numeric series."""
        return self._eval_expressions("arctanh")

    def arccosh(self) -> Series:
        """The elementwise inverse hyperbolic cosine of a numeric series."""
        return self._eval_expressions("arccosh")

    def arcsinh(self) -> Series:
        """The elementwise inverse hyperbolic sine of a numeric series."""
        return self._eval_expressions("arcsinh")

    def radians(self) -> Series:
        """The elementwise radians of a numeric series."""
        return self._eval_expressions("radians")

    def degrees(self) -> Series:
        """The elementwise degrees of a numeric series."""
        return self._eval_expressions("degrees")

    def log2(self) -> Series:
        """The elementwise log2 of a numeric series."""
        return self._eval_expressions("log2")

    def log10(self) -> Series:
        """The elementwise log10 of a numeric series."""
        return self._eval_expressions("log10")

    def log(self, base: float) -> Series:
        """The elementwise log with given base, of a numeric series.

        Args:
            base: The base of the logarithm.
        """
        return self._eval_expressions("log", base=base)

    def pow(self, exp: float) -> Series:
        """The elementwise exponentiation of a series.

        Args:
            exp: The exponent to raise each element to.
        """
        return self._eval_expressions("pow", exp=exp)

    def power(self, exp: float) -> Series:
        """The elementwise exponentiation of a series.

        Args:
            exp: The exponent to raise each element to.
        """
        return self._eval_expressions("power", exp=exp)

    def ln(self) -> Series:
        """The elementwise ln of a numeric series."""
        return self._eval_expressions("ln")

    def log1p(self) -> Series:
        """The ln(self + 1) of a numeric series."""
        return self._eval_expressions("log1p")

    def exp(self) -> Series:
        """The e^self of a numeric series."""
        return self._eval_expressions("exp")

    def expm1(self) -> Series:
        """The e^self - 1 of a numeric series."""
        return self._eval_expressions("expm1")

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

    def __floordiv__(self, other: object) -> Series:
        if not isinstance(other, Series):
            raise TypeError(f"expected another Series but got {type(other)}")
        assert self._series is not None and other._series is not None
        return Series._from_pyseries(self._series // other._series)

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

    def stddev(self) -> Series:
        assert self._series is not None
        return Series._from_pyseries(self._series.stddev())

    def sum(self) -> Series:
        assert self._series is not None
        return Series._from_pyseries(self._series.sum())

    def product(self) -> Series:
        assert self._series is not None
        return Series._from_pyseries(self._series.product())

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

    def regexp_count(
        self,
        patterns: str | Series,
    ) -> Series:
        return self._eval_expressions(
            "regexp_count",
            patterns=patterns,
        )

    def minhash(
        self,
        num_hashes: int,
        ngram_size: int,
        seed: int = 1,
        hash_function: Literal["murmurhash3", "xxhash", "xxhash3_64", "xxhash64", "xxhash32", "sha1"] = "murmurhash3",
    ) -> Series:
        """Runs the MinHash algorithm on the series.

        For a string, calculates the minimum hash over all its ngrams,
        repeating with `num_hashes` permutations. Returns as a list of 32-bit unsigned integers.

        Tokens for the ngrams are delimited by spaces.
        MurmurHash is used for the initial hash.

        Args:
            num_hashes: The number of hash permutations to compute.
            ngram_size: The number of tokens in each shingle/ngram.
            seed (optional): Seed used for generating permutations and the initial string hashes. Defaults to 1.
            hash_function (optional): Hash function to use for initial string hashing. One of "murmur3", "xxhash3_64" (or alias "xxhash"), "xxhash64", "xxhash32", or "sha1". Defaults to "murmur3".
        """
        if not isinstance(num_hashes, int):
            raise ValueError(f"expected an integer for num_hashes but got {type(num_hashes)}")
        if not isinstance(ngram_size, int):
            raise ValueError(f"expected an integer for ngram_size but got {type(ngram_size)}")
        if seed is not None and not isinstance(seed, int):
            raise ValueError(f"expected an integer or None for seed but got {type(seed)}")
        if not isinstance(hash_function, str):
            raise ValueError(f"expected str for hash_function but got {type(hash_function)}")
        assert (
            hash_function
            in [
                "murmurhash3",
                "xxhash",
                "xxhash3_64",
                "xxhash64",
                "xxhash32",
                "sha1",
            ]
        ), f"hash_function must be one of 'murmurhash3', 'xxhash', 'xxhash3_64', 'xxhash64', 'xxhash32', 'sha1', got {hash_function}"

        return Series._from_pyseries(self._series.minhash(num_hashes, ngram_size, seed, hash_function))

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

    def __reduce__(
        self,
    ) -> (
        tuple[
            Callable[
                [
                    builtins.list[Any],
                    builtins.str,
                    DataType,
                ],
                Series,
            ],
            tuple[
                builtins.list[Any],
                builtins.str,
                DataType,
            ],
        ]
        | tuple[
            Callable[[pa.Array | pa.ChunkedArray, builtins.str, DataType], Series],
            tuple[pa.Array | pa.ChunkedArray, builtins.str, DataType],
        ]
    ):
        return (Series.from_arrow, (self.to_arrow(), self.name(), self.datatype()))

    def _debug_bincode_serialize(self) -> bytes:
        return self._series._debug_bincode_serialize()

    @classmethod
    def _debug_bincode_deserialize(cls, b: bytes) -> Series:
        return Series._from_pyseries(PySeries._debug_bincode_deserialize(b))

    def _eval_expressions(
        self,
        func_name: builtins.str,
        *args: Any,
        **kwargs: Any,
    ) -> Series:
        from daft.expressions.expressions import lit

        inputs: list[PySeries] = []
        arg_exprs = []
        kwarg_exprs = {}

        for arg in [self, *args]:
            if isinstance(arg, Series):
                index = len(inputs)
                field = Field.create(arg.name(), arg.datatype())
                inputs.append(arg._series)

                arg_exprs.append(native.bound_col(index, field._field))
            else:
                arg_exprs.append(lit(arg)._expr)

        for name, arg in kwargs.items():
            if arg is None:
                continue

            if isinstance(arg, Series):
                index = len(inputs)
                field = Field.create(arg.name(), arg.datatype())
                inputs.append(arg._series)

                kwarg_exprs[name] = native.bound_col(index, field._field)
            else:
                kwarg_exprs[name] = lit(arg)._expr

        rb = PyRecordBatch.from_pyseries_list(inputs)

        f = native.get_function_from_registry(func_name)
        expr = f(
            *arg_exprs,
            **kwarg_exprs,
        ).alias(self.name())

        rb = rb.eval_expression_list([expr])
        pyseries = rb.get_column(0)
        return Series._from_pyseries(pyseries)


def item_to_series(name: str, item: Any) -> Series:
    # Fast path
    if isinstance(item, Series):
        return item

    if isinstance(item, list):
        series = Series.from_pylist(item, name)
    elif np.module_available() and isinstance(item, np.ndarray):  # type: ignore[attr-defined]
        series = Series.from_numpy(item, name)
    elif isinstance(item, (pa.Array, pa.ChunkedArray)):
        series = Series.from_arrow(item, name)
    elif pd.module_available() and isinstance(item, pd.Series):  # type: ignore[attr-defined]
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

    def _eval_expressions(self, func_name: builtins.str, *args: Any, **kwargs: Any) -> Series:
        s = Series._from_pyseries(self._series)
        return s._eval_expressions(func_name, *args, **kwargs)


class SeriesFloatNamespace(SeriesNamespace):
    def is_nan(self) -> Series:
        return self._eval_expressions("is_nan")

    def is_inf(self) -> Series:
        return self._eval_expressions("is_inf")

    def not_nan(self) -> Series:
        return self._eval_expressions("not_nan")

    def fill_nan(self, fill_value: Series) -> Series:
        return self._eval_expressions("fill_nan", fill_value)


class SeriesStringNamespace(SeriesNamespace):
    def endswith(self, suffix: Series) -> Series:
        return self._eval_expressions("ends_with", suffix)

    def startswith(self, prefix: Series) -> Series:
        return self._eval_expressions("starts_with", prefix)

    def contains(self, pattern: Series) -> Series:
        return self._eval_expressions("utf8_contains", pattern)

    def match(self, pattern: Series) -> Series:
        return self._eval_expressions("regexp_match", pattern)

    def split(self, pattern: Series) -> Series:
        """Splits each string on the given literal pattern, into a list of strings.

        Args:
            pattern: The literal pattern on which each string should be split.

        Returns:
            Series: A List[String] series containing the string splits for each string.
        """
        return self._eval_expressions("split", pattern)

    def regexp_split(self, pattern: Series) -> Series:
        """Splits each string on the given regex pattern, into a list of strings.

        Args:
            pattern: The regex pattern on which each string should be split.

        Returns:
            Series: A List[String] series containing the string splits for each string.
        """
        return self._eval_expressions("regexp_split", pattern)

    def concat(self, other: Series) -> Series:
        if not isinstance(other, Series):
            raise ValueError(f"expected another Series but got {type(other)}")
        assert self._series is not None and other._series is not None
        return Series._from_pyseries(self._series) + other

    def extract(self, pattern: Series, index: int = 0) -> Series:
        return self._eval_expressions("regexp_extract", pattern, index=index)

    def extract_all(self, pattern: Series, index: int = 0) -> Series:
        return self._eval_expressions("regexp_extract_all", pattern, index=index)

    def replace(self, pattern: Series, replacement: Series, regex: bool = False) -> Series:
        f_name = "regexp_replace" if regex else "replace"
        return self._eval_expressions(f_name, pattern, replacement)

    def length(self) -> Series:
        return self._eval_expressions("length")

    def length_bytes(self) -> Series:
        return self._eval_expressions("length_bytes")

    def lower(self) -> Series:
        return self._eval_expressions("lower")

    def upper(self) -> Series:
        return self._eval_expressions("upper")

    def lstrip(self) -> Series:
        return self._eval_expressions("lstrip")

    def rstrip(self) -> Series:
        return self._eval_expressions("rstrip")

    def reverse(self) -> Series:
        return self._eval_expressions("reverse")

    def capitalize(self) -> Series:
        return self._eval_expressions("capitalize")

    def left(self, nchars: Series) -> Series:
        return self._eval_expressions("left", nchars)

    def right(self, nchars: Series) -> Series:
        return self._eval_expressions("right", nchars)

    def find(self, substr: Series) -> Series:
        return self._eval_expressions("find", substr)

    def rpad(self, length: Series, pad: Series) -> Series:
        return self._eval_expressions("rpad", length, pad)

    def lpad(self, length: Series, pad: Series) -> Series:
        return self._eval_expressions("lpad", length, pad)

    def repeat(self, n: Series) -> Series:
        return self._eval_expressions("repeat", n)

    def like(self, pattern: Series) -> Series:
        return self._eval_expressions("like", pattern)

    def ilike(self, pattern: Series) -> Series:
        return self._eval_expressions("ilike", pattern)

    def to_date(self, format: str) -> Series:
        return self._eval_expressions("to_date", format=format)

    def to_datetime(self, format: str, timezone: str | None = None) -> Series:
        return self._eval_expressions("to_datetime", format=format, timezone=timezone)

    def substr(self, start: Series, length: Series | None = None) -> Series:
        return self._eval_expressions("substr", start, length)

    def normalize(
        self,
        *,
        remove_punct: bool = False,
        lowercase: bool = False,
        nfd_unicode: bool = False,
        white_space: bool = False,
    ) -> Series:
        return self._eval_expressions(
            "normalize",
            remove_punct=remove_punct,
            lowercase=lowercase,
            nfd_unicode=nfd_unicode,
            white_space=white_space,
        )

    def count_matches(
        self,
        patterns: str | list[str],
        *,
        whole_words: bool = False,
        case_sensitive: bool = True,
    ) -> Series:
        return self._eval_expressions(
            "count_matches",
            patterns=patterns,
            whole_words=whole_words,
            case_sensitive=case_sensitive,
        )


class SeriesDateNamespace(SeriesNamespace):
    def date(self) -> Series:
        return self._eval_expressions("date")

    def day(self) -> Series:
        return self._eval_expressions("day")

    def hour(self) -> Series:
        return self._eval_expressions("hour")

    def minute(self) -> Series:
        return self._eval_expressions("minute")

    def second(self) -> Series:
        return self._eval_expressions("second")

    def millisecond(self) -> Series:
        return self._eval_expressions("millisecond")

    def microsecond(self) -> Series:
        return self._eval_expressions("microsecond")

    def nanosecond(self) -> Series:
        return self._eval_expressions("nanosecond")

    def unix_date(self) -> Series:
        return self._eval_expressions("unix_date")

    def time(self) -> Series:
        return self._eval_expressions("time")

    def month(self) -> Series:
        return self._eval_expressions("month")

    def quarter(self) -> Series:
        return self._eval_expressions("quarter")

    def year(self) -> Series:
        return self._eval_expressions("year")

    def day_of_week(self) -> Series:
        return self._eval_expressions("day_of_week")

    def day_of_month(self) -> Series:
        return self._eval_expressions("day_of_month")

    def day_of_year(self) -> Series:
        return self._eval_expressions("day_of_year")

    def week_of_year(self) -> Series:
        return self._eval_expressions("week_of_year")

    def truncate(self, interval: str, relative_to: Series | None = None) -> Series:
        return self._eval_expressions("truncate", relative_to, interval=interval)

    def to_unix_epoch(self, time_unit: str | TimeUnit | None = None) -> Series:
        return self._eval_expressions("to_unix_epoch", time_unit=time_unit)

    def strftime(self, fmt: str | None = None) -> Series:
        return self._eval_expressions("strftime", format=fmt)

    def total_seconds(self) -> Series:
        return self._eval_expressions("total_seconds")

    def total_milliseconds(self) -> Series:
        return self._eval_expressions("total_milliseconds")

    def total_microseconds(self) -> Series:
        return self._eval_expressions("total_microseconds")

    def total_nanoseconds(self) -> Series:
        return self._eval_expressions("total_nanoseconds")

    def total_minutes(self) -> Series:
        return self._eval_expressions("total_minutes")

    def total_hours(self) -> Series:
        return self._eval_expressions("total_hours")

    def total_days(self) -> Series:
        return self._eval_expressions("total_days")


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
    def length(self) -> Series:
        return self._eval_expressions("list_count", mode=CountMode.All)

    def get(self, idx: Series, default: Series) -> Series:
        return self._eval_expressions("list_get", idx=idx, default=default)

    def sort(self, desc: bool | Series = False, nulls_first: bool | Series | None = None) -> Series:
        return self._eval_expressions("list_sort", desc=desc, nulls_first=nulls_first)


class SeriesMapNamespace(SeriesNamespace):
    def get(self, key: Series) -> Series:
        return Series._from_pyseries(self._series.map_get(key._series))


class SeriesImageNamespace(SeriesNamespace):
    def decode(
        self,
        on_error: Literal["raise", "null"] = "raise",
        mode: str | ImageMode | None = None,
    ) -> Series:
        return self._eval_expressions("image_decode", on_error=on_error, mode=mode)

    def encode(self, image_format: str | ImageFormat) -> Series:
        return self._eval_expressions("image_encode", image_format=image_format)

    def resize(self, w: int, h: int) -> Series:
        return self._eval_expressions("image_resize", w=w, h=h)

    def to_mode(self, mode: str | ImageMode) -> Series:
        if isinstance(mode, str):
            mode = ImageMode.from_mode_string(mode.upper())
        if not isinstance(mode, ImageMode):
            raise ValueError(f"mode must be a string or ImageMode variant, but got: {mode}")
        return self._eval_expressions("to_mode", mode=mode)
