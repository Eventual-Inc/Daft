from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Union

from daft.context import get_context
from daft.daft import ImageMode, PyDataType, PyTimeUnit
from daft.dependencies import pa

if TYPE_CHECKING:
    import builtins

    import numpy as np


class TimeUnit:
    _timeunit: PyTimeUnit

    def __init__(self) -> None:
        raise NotImplementedError("Please use TimeUnit.from_str(), .s(), .ms(), .us(), or .ns() instead.")

    @staticmethod
    def _from_pytimeunit(o3: PyTimeUnit) -> TimeUnit:
        timeunit = TimeUnit.__new__(TimeUnit)
        timeunit._timeunit = o3
        return timeunit

    @classmethod
    def s(cls) -> TimeUnit:
        """Represents seconds."""
        return cls._from_pytimeunit(PyTimeUnit.seconds())

    @classmethod
    def ms(cls) -> TimeUnit:
        """Represents milliseconds."""
        return cls._from_pytimeunit(PyTimeUnit.milliseconds())

    @classmethod
    def us(cls) -> TimeUnit:
        """Represents microseconds."""
        return cls._from_pytimeunit(PyTimeUnit.microseconds())

    @classmethod
    def ns(cls) -> TimeUnit:
        """Represents nanoseconds."""
        return cls._from_pytimeunit(PyTimeUnit.nanoseconds())

    @classmethod
    def from_str(cls, unit: str) -> TimeUnit:
        unit = unit.lower()
        if unit == "s":
            return cls.s()
        elif unit == "ms":
            return cls.ms()
        elif unit == "us":
            return cls.us()
        elif unit == "ns":
            return cls.ns()
        else:
            raise ValueError("Unsupported unit: {unit}")

    def __str__(self) -> str:
        # These are the strings PyArrow uses.
        if self._timeunit == PyTimeUnit.seconds():
            return "s"
        elif self._timeunit == PyTimeUnit.milliseconds():
            return "ms"
        elif self._timeunit == PyTimeUnit.microseconds():
            return "us"
        elif self._timeunit == PyTimeUnit.nanoseconds():
            return "ns"
        else:
            assert False

    def __repr__(self) -> str:
        return f"TimeUnit.{self.__str__()}"


class DataType:
    """A Daft DataType defines the type of all the values in an Expression or DataFrame column."""

    _dtype: PyDataType

    @classmethod
    def _infer_type(cls, user_provided_type: DataTypeLike) -> DataType:
        from typing import get_args, get_origin

        if isinstance(user_provided_type, DataType):
            return user_provided_type
        elif isinstance(user_provided_type, dict):
            return DataType.struct({k: DataType._infer_type(user_provided_type[k]) for k in user_provided_type})
        elif get_origin(user_provided_type) is not None:
            origin_type = get_origin(user_provided_type)
            if origin_type is list:
                child_type = get_args(user_provided_type)[0]
                return DataType.list(DataType._infer_type(child_type))
            elif origin_type is dict:
                (key_type, val_type) = get_args(user_provided_type)
                return DataType.map(DataType._infer_type(key_type), DataType._infer_type(val_type))
            else:
                raise ValueError(f"Unrecognized Python origin type, cannot convert to Daft type: {origin_type}")
        elif isinstance(user_provided_type, type):
            if user_provided_type is str:
                return DataType.string()
            elif user_provided_type is int:
                return DataType.int64()
            elif user_provided_type is float:
                return DataType.float64()
            elif user_provided_type is bytes:
                return DataType.binary()
            elif user_provided_type is object:
                return DataType.python()
            else:
                raise ValueError(f"Unrecognized Python type, cannot convert to Daft type: {user_provided_type}")
        else:
            raise ValueError(f"Unable to infer Daft DataType for provided value: {user_provided_type}")

    @staticmethod
    def _from_pydatatype(pydt: PyDataType) -> DataType:
        dt = DataType.__new__(DataType)
        dt._dtype = pydt
        if dt == DataType.int8():
            return Int8Type()
        elif dt == DataType.int16():
            return Int16Type()
        elif dt == DataType.int32():
            return Int32Type()
        elif dt == DataType.int64():
            return Int64Type()
        elif dt == DataType.uint8():
            return UInt8Type()
        elif dt == DataType.uint16():
            return UInt16Type()
        elif dt == DataType.uint32():
            return UInt32Type()
        elif dt == DataType.uint64():
            return UInt64Type()
        elif dt == DataType.float32():
            return Float32Type()
        elif dt == DataType.float64():
            return Float64Type()
        elif dt == DataType.string():
            return StringType()
        elif dt == DataType.binary():
            return BinaryType()
        elif dt == DataType.null():
            return NullType()
        elif dt == DataType.bool():
            return BooleanType()
        return dt

    @classmethod
    def int8(cls) -> Int8Type:
        """Create an 8-bit integer DataType."""
        return Int8Type()

    @classmethod
    def int16(cls) -> Int16Type:
        """Create an 16-bit integer DataType."""
        return Int16Type()

    @classmethod
    def int32(cls) -> Int32Type:
        """Create an 32-bit integer DataType."""
        return Int32Type()

    @classmethod
    def int64(cls) -> Int64Type:
        """Create an 64-bit integer DataType."""
        return Int64Type()

    @classmethod
    def uint8(cls) -> UInt8Type:
        """Create an unsigned 8-bit integer DataType."""
        return UInt8Type()

    @classmethod
    def uint16(cls) -> UInt16Type:
        """Create an unsigned 16-bit integer DataType."""
        return UInt16Type()

    @classmethod
    def uint32(cls) -> UInt32Type:
        """Create an unsigned 32-bit integer DataType."""
        return UInt32Type()

    @classmethod
    def uint64(cls) -> UInt64Type:
        """Create an unsigned 64-bit integer DataType."""
        return UInt64Type()

    @classmethod
    def float32(cls) -> Float32Type:
        """Create a 32-bit float DataType."""
        return Float32Type()

    @classmethod
    def float64(cls) -> Float64Type:
        """Create a 64-bit float DataType."""
        return Float64Type()

    @classmethod
    def string(cls) -> StringType:
        """Create a String DataType: A string of UTF8 characters."""
        return StringType()

    @classmethod
    def bool(cls) -> BooleanType:
        """Create the Boolean DataType: Either ``True`` or ``False``."""
        return BooleanType()

    @classmethod
    def binary(cls) -> BinaryType:
        """Create a Binary DataType: A string of bytes."""
        return BinaryType()

    @classmethod
    def fixed_size_binary(cls, size: int) -> FixedSizeBinaryType:
        """Create a FixedSizeBinary DataType: A fixed-size string of bytes."""
        if not isinstance(size, int) or size <= 0:
            raise ValueError("The size for a fixed-size binary must be a positive integer, but got: ", size)
        return FixedSizeBinaryType(size)

    @classmethod
    def null(cls) -> NullType:
        """Creates the Null DataType: Always the ``Null`` value."""
        return NullType()

    @classmethod
    def decimal128(cls, precision: int, scale: int) -> DecimalType:
        """Fixed-precision decimal."""
        return DecimalType(precision, scale)

    @classmethod
    def date(cls) -> DateType:
        """Create a Date DataType: A date with a year, month and day."""
        return DateType()

    @classmethod
    def time(cls, timeunit: TimeUnit | str) -> TimeType:
        """Time DataType. Supported timeunits are "us", "ns"."""
        if isinstance(timeunit, str):
            timeunit = TimeUnit.from_str(timeunit)
        return TimeType(timeunit)

    @classmethod
    def timestamp(cls, timeunit: TimeUnit | str, timezone: str | None = None) -> TimestampType:
        """Timestamp DataType."""
        return TimestampType(timeunit, timezone)

    @classmethod
    def duration(cls, timeunit: TimeUnit | str) -> DurationType:
        """Duration DataType."""
        if isinstance(timeunit, str):
            timeunit = TimeUnit.from_str(timeunit)
        return DurationType(timeunit)

    @classmethod
    def interval(cls) -> IntervalType:
        """Interval DataType."""
        return IntervalType()

    @classmethod
    def list(cls, dtype: DataType) -> ListType:
        """Create a List DataType: Variable-length list, where each element in the list has type ``dtype``.

        Args:
            dtype: DataType of each element in the list
        """
        return ListType(dtype)

    @classmethod
    def fixed_size_list(cls, dtype: DataType, size: int) -> FixedSizeListType:
        """Create a FixedSizeList DataType: Fixed-size list, where each element in the list has type ``dtype`` and each list has length ``size``.

        Args:
            dtype: DataType of each element in the list
            size: length of each list
        """
        if not isinstance(size, int) or size <= 0:
            raise ValueError("The size for a fixed-size list must be a positive integer, but got: ", size)
        return FixedSizeListType(dtype, size)

    @classmethod
    def map(cls, key_type: DataType, value_type: DataType) -> MapType:
        """Create a Map DataType: A map is a nested type of key-value pairs that is implemented as a list of structs with two fields, key and value.

        Args:
            key_type: DataType of the keys in the map
            value_type: DataType of the values in the map
        """
        return MapType(key_type, value_type)

    @classmethod
    def struct(cls, fields: dict[str, DataType]) -> StructType:
        """Create a Struct DataType: a nested type which has names mapped to child types.

        Example:
            >>> DataType.struct({"name": DataType.string(), "age": DataType.int64()})

        Args:
            fields: Nested fields of the Struct
        """
        return StructType(fields)

    @classmethod
    def extension(cls, name: str, storage_dtype: DataType, metadata: str | None = None) -> ExtensionType:
        return ExtensionType(name, storage_dtype, metadata)

    @classmethod
    def embedding(cls, dtype: DataType, size: int) -> EmbeddingType:
        """Create an Embedding DataType: embeddings are fixed size arrays, where each element in the array has a **numeric** ``dtype`` and each array has a fixed length of ``size``.

        Args:
            dtype: DataType of each element in the list (must be numeric)
            size: length of each list
        """
        return EmbeddingType(dtype, size)

    @classmethod
    def image(
        cls, mode: str | ImageMode | None = None, height: int | None = None, width: int | None = None
    ) -> DataType:
        """Create an Image DataType: image arrays contain (height, width, channel) ndarrays of pixel values.

        Each image in the array has an :class:`~daft.ImageMode`, which describes the pixel dtype (e.g. uint8) and
        the number of image channels/bands and their logical interpretation (e.g. RGB).

        If the height, width, and mode are the same for all images in the array, specifying them when constructing
        this type is advised, since that will allow Daft to create a more optimized physical representation
        of the image array.

        If the height, width, or mode may vary across images in the array, leaving these fields unspecified when
        creating this type will cause Daft to represent this image array as a heterogeneous collection of images,
        where each image can have a different mode, height, and width. This is much more flexible, but will result
        in a less compact representation and may be make some operations less efficient.

        Args:
            mode: The mode of the image. By default, this is inferred from the underlying data.
                If height and width are specified, the mode must also be specified.
            height: The height of the image. By default, this is inferred from the underlying data.
                Must be specified if the width is specified.
            width: The width of the image. By default, this is inferred from the underlying data.
                Must be specified if the width is specified.
        """
        if isinstance(mode, str):
            mode = ImageMode.from_mode_string(mode.upper())
        if mode is not None and not isinstance(mode, ImageMode):
            raise ValueError(f"mode must be a string or ImageMode variant, but got: {mode}")
        if height is not None and width is not None:
            if not isinstance(height, int) or height <= 0:
                raise ValueError("Image height must be a positive integer, but got: ", height)
            if not isinstance(width, int) or width <= 0:
                raise ValueError("Image width must be a positive integer, but got: ", width)
        elif height is not None or width is not None:
            raise ValueError(
                f"Image height and width must either both be specified, or both not be specified, but got height={height}, width={width}"
            )
        if height is not None and width is not None and mode is not None:
            return FixedShapeImageType(mode, height, width)
        else:
            return ImageType(mode)

    @classmethod
    def tensor(
        cls,
        dtype: DataType,
        shape: tuple[int, ...] | None = None,
    ) -> DataType:
        """Create a tensor DataType: tensor arrays contain n-dimensional arrays of data of the provided ``dtype`` as elements, each of the provided ``shape``.

        If a ``shape`` is given, each ndarray in the column will have this shape.

        If ``shape`` is not given, the ndarrays in the column can have different shapes. This is much more flexible,
        but will result in a less compact representation and may be make some operations less efficient.

        Args:
            dtype: The type of the data contained within the tensor elements.
            shape: The shape of each tensor in the column. This is ``None`` by default, which allows the shapes of
                each tensor element to vary.
        """
        if shape is not None:
            return FixedShapeTensorType(dtype, shape)
        else:
            return TensorType(dtype)

    @classmethod
    def sparse_tensor(
        cls,
        dtype: DataType,
        shape: tuple[int, ...] | None = None,
        use_offset_indices: bool = False,
    ) -> DataType:
        """Create a SparseTensor DataType: SparseTensor arrays implemented as 'COO Sparse Tensor' representation of n-dimensional arrays of data of the provided ``dtype`` as elements, each of the provided ``shape``.

        If a ``shape`` is given, each ndarray in the column will have this shape.

        If ``shape`` is not given, the ndarrays in the column can have different shapes. This is much more flexible,
        but will result in a less compact representation and may be make some operations less efficient.

        The ``use_offset_indices`` parameter determines how the indices of the SparseTensor are stored:
        - ``False`` (default): Indices represent the actual positions of nonzero values.
        - ``True``: Indices represent the offsets between consecutive nonzero values.
        This can improve compression efficiency, especially when nonzero values are clustered together,
        as offsets between them are often zero, making them easier to compress.

        Args:
            dtype: The type of the data contained within the tensor elements.
            shape: The shape of each SparseTensor in the column. This is ``None`` by default, which allows the shapes of
                each tensor element to vary.
            use_offset_indices: Determines how indices are represented.
                Defaults to `False` (storing actual indices). If `True`, stores offsets between nonzero indices.
        """
        if shape is not None:
            return FixedShapeSparseTensorType(dtype, shape, use_offset_indices)
        else:
            return SparseTensorType(dtype, use_offset_indices)

    @classmethod
    def from_arrow_type(cls, arrow_type: pa.lib.DataType) -> DataType:
        """Maps a PyArrow DataType to a Daft DataType."""
        if pa.types.is_int8(arrow_type):
            return cls.int8()
        elif pa.types.is_int16(arrow_type):
            return cls.int16()
        elif pa.types.is_int32(arrow_type):
            return cls.int32()
        elif pa.types.is_int64(arrow_type):
            return cls.int64()
        elif pa.types.is_uint8(arrow_type):
            return cls.uint8()
        elif pa.types.is_uint16(arrow_type):
            return cls.uint16()
        elif pa.types.is_uint32(arrow_type):
            return cls.uint32()
        elif pa.types.is_uint64(arrow_type):
            return cls.uint64()
        elif pa.types.is_float32(arrow_type):
            return cls.float32()
        elif pa.types.is_float64(arrow_type):
            return cls.float64()
        elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            return cls.string()
        elif pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
            return cls.binary()
        elif pa.types.is_fixed_size_binary(arrow_type):
            return cls.fixed_size_binary(arrow_type.byte_width)
        elif pa.types.is_boolean(arrow_type):
            return cls.bool()
        elif pa.types.is_null(arrow_type):
            return cls.null()
        elif pa.types.is_decimal128(arrow_type):
            return cls.decimal128(arrow_type.precision, arrow_type.scale)
        elif pa.types.is_date32(arrow_type):
            return cls.date()
        elif pa.types.is_date64(arrow_type):
            return cls.timestamp(TimeUnit.ms())
        elif pa.types.is_time64(arrow_type):
            timeunit = TimeUnit.from_str(pa.type_for_alias(str(arrow_type)).unit)
            return cls.time(timeunit)
        elif pa.types.is_timestamp(arrow_type):
            timeunit = TimeUnit.from_str(arrow_type.unit)
            return cls.timestamp(timeunit=timeunit, timezone=arrow_type.tz)
        elif pa.types.is_duration(arrow_type):
            timeunit = TimeUnit.from_str(arrow_type.unit)
            return cls.duration(timeunit=timeunit)
        elif pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
            assert isinstance(arrow_type, (pa.ListType, pa.LargeListType))
            field = arrow_type.value_field
            return cls.list(cls.from_arrow_type(field.type))
        elif pa.types.is_fixed_size_list(arrow_type):
            assert isinstance(arrow_type, pa.FixedSizeListType)
            field = arrow_type.value_field
            return cls.fixed_size_list(cls.from_arrow_type(field.type), arrow_type.list_size)
        elif pa.types.is_struct(arrow_type):
            assert isinstance(arrow_type, pa.StructType)
            fields = [arrow_type[i] for i in range(arrow_type.num_fields)]
            return cls.struct({field.name: cls.from_arrow_type(field.type) for field in fields})
        elif pa.types.is_interval(arrow_type):
            return cls.interval()
        elif pa.types.is_map(arrow_type):
            assert isinstance(arrow_type, pa.MapType)
            return cls.map(
                key_type=cls.from_arrow_type(arrow_type.key_type),
                value_type=cls.from_arrow_type(arrow_type.item_type),
            )
        elif isinstance(arrow_type, getattr(pa, "FixedShapeTensorType", ())):
            scalar_dtype = cls.from_arrow_type(arrow_type.value_type)
            return cls.tensor(scalar_dtype, tuple(arrow_type.shape))
        elif isinstance(arrow_type, pa.PyExtensionType):
            # TODO(Clark): Add a native cross-lang extension type representation for PyExtensionTypes.
            raise ValueError(
                "pyarrow extension types that subclass pa.PyExtensionType can't be used in Daft, since they can't be "
                f"used in non-Python Arrow implementations and Daft uses the Rust Arrow2 implementation: {arrow_type}"
            )
        elif isinstance(arrow_type, pa.BaseExtensionType):
            name = arrow_type.extension_name

            if (get_context().get_or_create_runner().name == "ray") and (
                type(arrow_type).__reduce__ == pa.BaseExtensionType.__reduce__
            ):
                raise ValueError(
                    f"You are attempting to use a Extension Type: {arrow_type} with the default pyarrow `__reduce__` which breaks pickling for Extensions"
                    "To fix this, implement your own `__reduce__` on your extension type"
                    "For more details see this issue: "
                    "https://github.com/apache/arrow/issues/35599"
                )
            try:
                metadata = arrow_type.__arrow_ext_serialize__().decode()
            except AttributeError:
                metadata = None

            if name == "daft.super_extension":
                assert metadata is not None
                return cls._from_pydatatype(PyDataType.from_json(metadata))
            else:
                return cls.extension(
                    name,
                    cls.from_arrow_type(arrow_type.storage_type),
                    metadata,
                )
        else:
            # Fall back to a Python object type.
            # TODO(Clark): Add native support for remaining Arrow types.
            return cls.python()

    @classmethod
    def from_numpy_dtype(cls, np_type: np.dtype) -> DataType:
        """Maps a Numpy datatype to a Daft DataType."""
        arrow_type = pa.from_numpy_dtype(np_type)
        return cls.from_arrow_type(arrow_type)

    def to_arrow_dtype(self) -> pa.DataType:
        return self._dtype.to_arrow()

    @classmethod
    def python(cls) -> DataType:
        """Create a Python DataType: a type which refers to an arbitrary Python object."""
        return cls._from_pydatatype(PyDataType.python())

    def is_python(self) -> builtins.bool:
        """Returns whether the DataType is a Python object type."""
        # NOTE: This is currently used in a few places still. We can get rid of it once these are refactored away. To be discussed.
        # 1. Visualizations - we can get rid of it if we do all our repr and repr_html logic in a Series instead of in Python
        # 2. Hypothesis test data generation - we can get rid of it if we allow for creation of Series from a Python list and DataType

        return self == DataType.python()

    def _should_cast_to_python(self) -> builtins.bool:
        # NOTE: This is used to determine if we should cast a column to a Python object type when converting to PyList.
        # Map is a logical type, but we don't want to cast it to Python because the underlying physical type is a List,
        # which we can handle without casting to Python.
        return self.is_logical() and not self.is_map()

    def __repr__(self) -> str:
        return self._dtype.__repr__()

    def __eq__(self, other: object) -> builtins.bool:
        return isinstance(other, DataType) and self._dtype.is_equal(other._dtype)

    def __reduce__(self) -> tuple:
        return DataType._from_pydatatype, (self._dtype,)

    def __hash__(self) -> int:
        return self._dtype.__hash__()

    def is_numeric(self) -> builtins.bool:
        """Returns whether the DataType is a numeric type."""
        return self._dtype.is_numeric()

    def is_float(self) -> builtins.bool:
        """Returns whether the DataType is a floating point type."""
        return self._dtype.is_float()

    def is_integer(self) -> builtins.bool:
        """Returns whether the DataType is an integer type."""
        return self._dtype.is_integer()

    def is_image(self) -> builtins.bool:
        """Returns whether the DataType is an image type."""
        return self._dtype.is_image()

    def is_fixed_shape_image(self) -> builtins.bool:
        """Returns whether the DataType is a fixed shape image type."""
        return self._dtype.is_fixed_shape_image()

    def is_list(self) -> builtins.bool:
        """Returns whether the DataType is a list type."""
        return self._dtype.is_list()

    def is_tensor(self) -> builtins.bool:
        """Returns whether the DataType is a tensor type."""
        return self._dtype.is_tensor()

    def is_fixed_shape_tensor(self) -> builtins.bool:
        """Returns whether the DataType is a fixed shape tensor type."""
        return self._dtype.is_fixed_shape_tensor()

    def is_sparse_tensor(self) -> builtins.bool:
        """Returns whether the DataType is a sparse tensor type."""
        return self._dtype.is_sparse_tensor()

    def is_fixed_shape_sparse_tensor(self) -> builtins.bool:
        """Returns whether the DataType is a fixed shape sparse tensor type."""
        return self._dtype.is_fixed_shape_sparse_tensor()

    def is_map(self) -> builtins.bool:
        """Returns whether the DataType is a map type."""
        return self._dtype.is_map()

    def is_logical(self) -> builtins.bool:
        """Returns whether the DataType is a logical type."""
        return self._dtype.is_logical()

    def is_boolean(self) -> builtins.bool:
        """Returns whether the DataType is a boolean type."""
        return self._dtype.is_boolean()

    def is_string(self) -> builtins.bool:
        """Returns whether the DataType is a string type."""
        return self._dtype.is_string()

    def is_temporal(self) -> builtins.bool:
        """Returns whether the DataType is a temporal type."""
        return self._dtype.is_temporal()

    def is_timestamp(self) -> builtins.bool:
        """Returns whether the DataType is a timestamp type."""
        return self._dtype.is_timestamp()


class Int8Type(DataType):
    """8-bit integer type."""

    _dtype = PyDataType.int8()


class Int16Type(DataType):
    """16-bit integer type."""

    _dtype = PyDataType.int16()


class Int32Type(DataType):
    """32-bit integer type."""

    _dtype = PyDataType.int32()


class Int64Type(DataType):
    """64-bit integer type."""

    _dtype = PyDataType.int64()


class UInt8Type(DataType):
    """Unsigned 8-bit integer type."""

    _dtype = PyDataType.uint8()


class UInt16Type(DataType):
    """Unsigned 16-bit integer type."""

    _dtype = PyDataType.uint16()


class UInt32Type(DataType):
    """Unsigned 32-bit integer type."""

    _dtype = PyDataType.uint32()


class UInt64Type(DataType):
    """Unsigned 64-bit integer type."""

    _dtype = PyDataType.uint64()


class Float32Type(DataType):
    """32-bit floating point type."""

    _dtype = PyDataType.float32()


class Float64Type(DataType):
    """64-bit floating point type."""

    _dtype = PyDataType.float64()


class BooleanType(DataType):
    """Boolean type."""

    _dtype = PyDataType.bool()


class StringType(DataType):
    """UTF-8 encoded string type."""

    _dtype = PyDataType.string()


class BinaryType(DataType):
    """Binary type."""

    _dtype = PyDataType.binary()


class FixedSizeBinaryType(DataType):
    """Fixed-size binary type."""

    size: int

    def __init__(self, size: int) -> None:
        self._dtype = PyDataType.fixed_size_binary(size)
        self.size = size


class NullType(DataType):
    """Null type."""

    _dtype = PyDataType.null()


class DecimalType(DataType):
    """128-bit decimal type."""

    precision: int
    scale: int

    def __init__(self, precision: int, scale: int) -> None:
        self._dtype = PyDataType.decimal128(precision, scale)
        self.precision = precision
        self.scale = scale


class DateType(DataType):
    """Date type."""

    _dtype = PyDataType.date()


class TimeType(DataType):
    """Time type."""

    timeunit: TimeUnit

    def __init__(self, timeunit: TimeUnit | str) -> None:
        if isinstance(timeunit, str):
            timeunit = TimeUnit.from_str(timeunit)
        self._dtype = PyDataType.time(timeunit._timeunit)
        self.timeunit = timeunit


class TimestampType(DataType):
    """Timestamp type."""

    timeunit: TimeUnit
    timezone: str | None

    def __init__(self, timeunit: TimeUnit | str, timezone: str | None = None) -> None:
        if isinstance(timeunit, str):
            timeunit = TimeUnit.from_str(timeunit)

        self.timeunit = timeunit
        self.timezone = timezone
        self._dtype = PyDataType.timestamp(timeunit._timeunit, timezone)


class DurationType(DataType):
    """Duration type."""

    timeunit: TimeUnit

    def __init__(self, timeunit: TimeUnit | str) -> None:
        if isinstance(timeunit, str):
            timeunit = TimeUnit.from_str(timeunit)
        self._dtype = PyDataType.duration(timeunit._timeunit)
        self.timeunit = timeunit


class IntervalType(DataType):
    """Interval type."""

    _dtype = PyDataType.interval()


class ListType(DataType):
    """List type."""

    inner: DataType

    def __init__(self, dtype: DataType) -> None:
        self.inner = dtype
        self._dtype = PyDataType.list(dtype._dtype)


class FixedSizeListType(DataType):
    """Fixed-size list type."""

    inner: DataType
    size: int

    def __init__(self, dtype: DataType, size: int) -> None:
        self.inner = dtype
        self.size = size
        self._dtype = PyDataType.fixed_size_list(dtype._dtype, size)


class MapType(DataType):
    """Map type."""

    key: DataType
    value: DataType

    def __init__(self, key_type: DataType, value_type: DataType) -> None:
        self.key = key_type
        self.value = value_type
        self._dtype = PyDataType.map(key_type._dtype, value_type._dtype)


class StructType(DataType):
    """Struct type."""

    fields: dict[str, DataType]

    def __init__(self, fields: dict[str, DataType]) -> None:
        self.fields = fields
        self._dtype = PyDataType.struct({name: datatype._dtype for name, datatype in fields.items()})


class ExtensionType(DataType):
    """Extension type."""

    name: str
    storage_dtype: DataType
    metadata: str | None

    def __init__(self, name: str, storage_dtype: DataType, metadata: str | None = None) -> None:
        self._dtype = PyDataType.extension(name, storage_dtype._dtype, metadata)
        self.name = name
        self.storage_dtype = storage_dtype
        self.metadata = metadata


class EmbeddingType(DataType):
    """Embedding type."""

    dtype: DataType
    size: int

    def __init__(self, dtype: DataType, size: int) -> None:
        if not isinstance(size, int) or size <= 0:
            raise ValueError("The size for a embedding must be a positive integer, but got: ", size)
        self._dtype = PyDataType.embedding(dtype._dtype, size)
        self.dtype = dtype
        self.size = size


class ImageType(DataType):
    """Image type."""

    mode: ImageMode | None

    def __init__(self, mode: ImageMode | None = None) -> None:
        self._dtype = PyDataType.image(mode)
        self.mode = mode


class FixedShapeImageType(DataType):
    """fixed shape Image type."""

    mode: ImageMode
    height: int
    width: int

    def __init__(self, mode: ImageMode, height: int, width: int) -> None:
        self._dtype = PyDataType.image(mode, height, width)
        self.mode = mode
        self.height = height
        self.width = width


class TensorType(DataType):
    """Tensor type."""

    dtype: DataType

    def __init__(self, dtype: DataType) -> None:
        self._dtype = PyDataType.tensor(dtype._dtype)
        self.dtype = dtype


class FixedShapeTensorType(DataType):
    """Fixed Shape Tensor type."""

    dtype: DataType
    shape: tuple[int, ...]

    def __init__(self, dtype: DataType, shape: tuple[int, ...]) -> None:
        if not isinstance(shape, tuple) or not shape or any(not isinstance(n, int) for n in shape):
            raise ValueError("FixedShapeTensor shape must be a non-empty tuple of ints, but got: ", shape)
        self._dtype = PyDataType.tensor(dtype._dtype, shape)
        self.dtype = dtype
        self.shape = shape


class SparseTensorType(DataType):
    """SparseTensor type."""

    dtype: DataType
    use_offset_indices: bool

    def __init__(self, dtype: DataType, use_offset_indices: bool = False) -> None:
        self._dtype = PyDataType.sparse_tensor(dtype._dtype, shape=None, use_offset_indices=use_offset_indices)
        self.dtype = dtype
        self.use_offset_indices = use_offset_indices


class FixedShapeSparseTensorType(DataType):
    """Fixed ShapeSparseTensor type."""

    dtype: DataType
    shape: tuple[int, ...]
    use_offset_indices: bool

    def __init__(self, dtype: DataType, shape: tuple[int, ...], use_offset_indices: bool = False) -> None:
        if not isinstance(shape, tuple) or not shape or any(not isinstance(n, int) for n in shape):
            raise ValueError("FixedShapeTensor shape must be a non-empty tuple of ints, but got: ", shape)
        if shape is not None:
            if not isinstance(shape, tuple) or not shape or any(not isinstance(n, int) for n in shape):
                raise ValueError("SparseTensor shape must be a non-empty tuple of ints, but got: ", shape)
        self._dtype = PyDataType.sparse_tensor(dtype._dtype, shape, use_offset_indices)
        self.dtype = dtype
        self.shape = shape
        self.use_offset_indices = use_offset_indices


# Type alias for a union of types that can be inferred into a DataType
DataTypeLike = Union[DataType, type, str]


_EXT_TYPE_REGISTRATION_LOCK = threading.Lock()
_EXT_TYPE_REGISTERED = False
_STATIC_DAFT_EXTENSION = None


def _ensure_registered_super_ext_type():
    global _EXT_TYPE_REGISTERED
    global _STATIC_DAFT_EXTENSION

    # Double-checked locking: avoid grabbing the lock if we know that the ext type
    # has already been registered.
    if not _EXT_TYPE_REGISTERED:
        with _EXT_TYPE_REGISTRATION_LOCK:
            if not _EXT_TYPE_REGISTERED:

                class DaftExtension(pa.ExtensionType):
                    def __init__(self, dtype, metadata=b""):
                        # attributes need to be set first before calling
                        # super init (as that calls serialize)
                        self._metadata = metadata
                        super().__init__(dtype, "daft.super_extension")

                    def __reduce__(self):
                        return type(self).__arrow_ext_deserialize__, (self.storage_type, self.__arrow_ext_serialize__())

                    def __arrow_ext_serialize__(self):
                        return self._metadata

                    @classmethod
                    def __arrow_ext_deserialize__(cls, storage_type, serialized):
                        return cls(storage_type, serialized)

                _STATIC_DAFT_EXTENSION = DaftExtension
                pa.register_extension_type(DaftExtension(pa.null()))
                import atexit

                atexit.register(lambda: pa.unregister_extension_type("daft.super_extension"))
                _EXT_TYPE_REGISTERED = True


def get_super_ext_type():
    _ensure_registered_super_ext_type()
    return _STATIC_DAFT_EXTENSION
