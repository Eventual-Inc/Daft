from __future__ import annotations

import builtins
from typing import TYPE_CHECKING

import pyarrow as pa

from daft.context import get_context
from daft.daft import ImageMode, PyDataType, PyTimeUnit

if TYPE_CHECKING:
    import numpy as np

_RAY_DATA_EXTENSIONS_AVAILABLE = True
_TENSOR_EXTENSION_TYPES = []
try:
    import ray
except ImportError:
    _RAY_DATA_EXTENSIONS_AVAILABLE = False
else:
    _RAY_VERSION = tuple(int(s) for s in ray.__version__.split(".")[0:3])
    try:
        # Variable-shaped tensor column support was added in Ray 2.1.0.
        if _RAY_VERSION >= (2, 2, 0):
            from ray.data.extensions import (
                ArrowTensorType,
                ArrowVariableShapedTensorType,
            )

            _TENSOR_EXTENSION_TYPES = [ArrowTensorType, ArrowVariableShapedTensorType]
        else:
            from ray.data.extensions import ArrowTensorType

            _TENSOR_EXTENSION_TYPES = [ArrowTensorType]
    except ImportError:
        _RAY_DATA_EXTENSIONS_AVAILABLE = False


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


class DataType:
    """A Daft DataType defines the type of all the values in an Expression or DataFrame column"""

    _dtype: PyDataType

    def __init__(self) -> None:
        raise NotImplementedError(
            "We do not support creating a DataType via __init__ "
            "use a creator method like DataType.int32() or use DataType.from_arrow_type(pa_type)"
        )

    @staticmethod
    def _from_pydatatype(pydt: PyDataType) -> DataType:
        dt = DataType.__new__(DataType)
        dt._dtype = pydt
        return dt

    @classmethod
    def int8(cls) -> DataType:
        """Create an 8-bit integer DataType"""
        return cls._from_pydatatype(PyDataType.int8())

    @classmethod
    def int16(cls) -> DataType:
        """Create an 16-bit integer DataType"""
        return cls._from_pydatatype(PyDataType.int16())

    @classmethod
    def int32(cls) -> DataType:
        """Create an 32-bit integer DataType"""
        return cls._from_pydatatype(PyDataType.int32())

    @classmethod
    def int64(cls) -> DataType:
        """Create an 64-bit integer DataType"""
        return cls._from_pydatatype(PyDataType.int64())

    @classmethod
    def uint8(cls) -> DataType:
        """Create an unsigned 8-bit integer DataType"""
        return cls._from_pydatatype(PyDataType.uint8())

    @classmethod
    def uint16(cls) -> DataType:
        """Create an unsigned 16-bit integer DataType"""
        return cls._from_pydatatype(PyDataType.uint16())

    @classmethod
    def uint32(cls) -> DataType:
        """Create an unsigned 32-bit integer DataType"""
        return cls._from_pydatatype(PyDataType.uint32())

    @classmethod
    def uint64(cls) -> DataType:
        """Create an unsigned 64-bit integer DataType"""
        return cls._from_pydatatype(PyDataType.uint64())

    @classmethod
    def float32(cls) -> DataType:
        """Create a 32-bit float DataType"""
        return cls._from_pydatatype(PyDataType.float32())

    @classmethod
    def float64(cls) -> DataType:
        """Create a 64-bit float DataType"""
        return cls._from_pydatatype(PyDataType.float64())

    @classmethod
    def string(cls) -> DataType:
        """Create a String DataType: A string of UTF8 characters"""
        return cls._from_pydatatype(PyDataType.string())

    @classmethod
    def bool(cls) -> DataType:
        """Create the Boolean DataType: Either ``True`` or ``False``"""
        return cls._from_pydatatype(PyDataType.bool())

    @classmethod
    def binary(cls) -> DataType:
        """Create a Binary DataType: A string of bytes"""
        return cls._from_pydatatype(PyDataType.binary())

    @classmethod
    def fixed_size_binary(cls, size: int) -> DataType:
        """Create a FixedSizeBinary DataType: A fixed-size string of bytes"""
        if not isinstance(size, int) or size <= 0:
            raise ValueError("The size for a fixed-size binary must be a positive integer, but got: ", size)
        return cls._from_pydatatype(PyDataType.fixed_size_binary(size))

    @classmethod
    def null(cls) -> DataType:
        """Creates the Null DataType: Always the ``Null`` value"""
        return cls._from_pydatatype(PyDataType.null())

    @classmethod
    def decimal128(cls, precision: int, scale: int) -> DataType:
        """Fixed-precision decimal."""
        return cls._from_pydatatype(PyDataType.decimal128(precision, scale))

    @classmethod
    def date(cls) -> DataType:
        """Create a Date DataType: A date with a year, month and day"""
        return cls._from_pydatatype(PyDataType.date())

    @classmethod
    def time(cls, timeunit: TimeUnit | str) -> DataType:
        """Time DataType. Supported timeunits are "us", "ns"."""
        if isinstance(timeunit, str):
            timeunit = TimeUnit.from_str(timeunit)
        return cls._from_pydatatype(PyDataType.time(timeunit._timeunit))

    @classmethod
    def timestamp(cls, timeunit: TimeUnit | str, timezone: str | None = None) -> DataType:
        """Timestamp DataType."""
        if isinstance(timeunit, str):
            timeunit = TimeUnit.from_str(timeunit)
        return cls._from_pydatatype(PyDataType.timestamp(timeunit._timeunit, timezone))

    @classmethod
    def duration(cls, timeunit: TimeUnit | str) -> DataType:
        """Duration DataType."""
        if isinstance(timeunit, str):
            timeunit = TimeUnit.from_str(timeunit)
        return cls._from_pydatatype(PyDataType.duration(timeunit._timeunit))

    @classmethod
    def list(cls, dtype: DataType) -> DataType:
        """Create a List DataType: Variable-length list, where each element in the list has type ``dtype``

        Args:
            dtype: DataType of each element in the list
        """
        return cls._from_pydatatype(PyDataType.list(dtype._dtype))

    @classmethod
    def fixed_size_list(cls, dtype: DataType, size: int) -> DataType:
        """Create a FixedSizeList DataType: Fixed-size list, where each element in the list has type ``dtype``
        and each list has length ``size``.

        Args:
            dtype: DataType of each element in the list
            size: length of each list
        """
        if not isinstance(size, int) or size <= 0:
            raise ValueError("The size for a fixed-size list must be a positive integer, but got: ", size)
        return cls._from_pydatatype(PyDataType.fixed_size_list(dtype._dtype, size))

    @classmethod
    def map(cls, key_type: DataType, value_type: DataType) -> DataType:
        """Create a Map DataType: A map is a nested type of key-value pairs that is implemented as a list of structs with two fields, key and value.
        Args:
            key_type: DataType of the keys in the map
            value_type: DataType of the values in the map
        """
        return cls._from_pydatatype(PyDataType.map(key_type._dtype, value_type._dtype))

    @classmethod
    def struct(cls, fields: dict[str, DataType]) -> DataType:
        """Create a Struct DataType: a nested type which has names mapped to child types

        Example:
        >>> DataType.struct({"name": DataType.string(), "age": DataType.int64()})

        Args:
            fields: Nested fields of the Struct
        """
        return cls._from_pydatatype(PyDataType.struct({name: datatype._dtype for name, datatype in fields.items()}))

    @classmethod
    def extension(cls, name: str, storage_dtype: DataType, metadata: str | None = None) -> DataType:
        return cls._from_pydatatype(PyDataType.extension(name, storage_dtype._dtype, metadata))

    @classmethod
    def embedding(cls, dtype: DataType, size: int) -> DataType:
        """Create an Embedding DataType: embeddings are fixed size arrays, where each element
        in the array has a **numeric** ``dtype`` and each array has a fixed length of ``size``.

        Args:
            dtype: DataType of each element in the list (must be numeric)
            size: length of each list
        """
        if not isinstance(size, int) or size <= 0:
            raise ValueError("The size for a embedding must be a positive integer, but got: ", size)
        return cls._from_pydatatype(PyDataType.embedding(dtype._dtype, size))

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
        return cls._from_pydatatype(PyDataType.image(mode, height, width))

    @classmethod
    def tensor(
        cls,
        dtype: DataType,
        shape: tuple[int, ...] | None = None,
    ) -> DataType:
        """Create a tensor DataType: tensor arrays contain n-dimensional arrays of data of the provided ``dtype`` as elements, each of the provided
        ``shape``.

        If a ``shape`` is given, each ndarray in the column will have this shape.

        If ``shape`` is not given, the ndarrays in the column can have different shapes. This is much more flexible,
        but will result in a less compact representation and may be make some operations less efficient.

        Args:
            dtype: The type of the data contained within the tensor elements.
            shape: The shape of each tensor in the column. This is ``None`` by default, which allows the shapes of
                each tensor element to vary.
        """
        if shape is not None:
            if not isinstance(shape, tuple) or not shape or any(not isinstance(n, int) for n in shape):
                raise ValueError("Tensor shape must be a non-empty tuple of ints, but got: ", shape)
        return cls._from_pydatatype(PyDataType.tensor(dtype._dtype, shape))

    @classmethod
    def from_arrow_type(cls, arrow_type: pa.lib.DataType) -> DataType:
        """Maps a PyArrow DataType to a Daft DataType"""
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
        elif pa.types.is_map(arrow_type):
            assert isinstance(arrow_type, pa.MapType)
            return cls.map(
                key_type=cls.from_arrow_type(arrow_type.key_type),
                value_type=cls.from_arrow_type(arrow_type.item_type),
            )
        elif _RAY_DATA_EXTENSIONS_AVAILABLE and isinstance(arrow_type, tuple(_TENSOR_EXTENSION_TYPES)):
            scalar_dtype = cls.from_arrow_type(arrow_type.scalar_type)
            shape = arrow_type.shape if isinstance(arrow_type, ArrowTensorType) else None
            return cls.tensor(scalar_dtype, shape)
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

            if (get_context().runner_config.name == "ray") and (
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
        """Maps a Numpy datatype to a Daft DataType"""
        arrow_type = pa.from_numpy_dtype(np_type)
        return cls.from_arrow_type(arrow_type)

    def to_arrow_dtype(self, cast_tensor_to_ray_type: builtins.bool = False) -> pa.DataType:
        return self._dtype.to_arrow(cast_tensor_to_ray_type)

    @classmethod
    def python(cls) -> DataType:
        """Create a Python DataType: a type which refers to an arbitrary Python object"""
        return cls._from_pydatatype(PyDataType.python())

    def _is_python_type(self) -> builtins.bool:
        # NOTE: This is currently used in a few places still. We can get rid of it once these are refactored away. To be discussed.
        # 1. Visualizations - we can get rid of it if we do all our repr and repr_html logic in a Series instead of in Python
        # 2. Hypothesis test data generation - we can get rid of it if we allow for creation of Series from a Python list and DataType

        return self == DataType.python()

    def _is_tensor_type(self) -> builtins.bool:
        return self._dtype.is_tensor()

    def _is_fixed_shape_tensor_type(self) -> builtins.bool:
        return self._dtype.is_fixed_shape_tensor()

    def _is_image_type(self) -> builtins.bool:
        return self._dtype.is_image()

    def _is_fixed_shape_image_type(self) -> builtins.bool:
        return self._dtype.is_fixed_shape_image()

    def _is_numeric_type(self) -> builtins.bool:
        return self._dtype.is_numeric()

    def _is_map(self) -> builtins.bool:
        return self._dtype.is_map()

    def _is_logical_type(self) -> builtins.bool:
        return self._dtype.is_logical()

    def _is_temporal_type(self) -> builtins.bool:
        return self._dtype.is_temporal()

    def _should_cast_to_python(self) -> builtins.bool:
        # NOTE: This is used to determine if we should cast a column to a Python object type when converting to PyList.
        # Map is a logical type, but we don't want to cast it to Python because the underlying physical type is a List,
        # which we can handle without casting to Python.
        return self._is_logical_type() and not self._is_map()

    def __repr__(self) -> str:
        return self._dtype.__repr__()

    def __eq__(self, other: object) -> builtins.bool:
        return isinstance(other, DataType) and self._dtype.is_equal(other._dtype)

    def __reduce__(self) -> tuple:
        return DataType._from_pydatatype, (self._dtype,)

    def __hash__(self) -> int:
        return self._dtype.__hash__()


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


pa.register_extension_type(DaftExtension(pa.null()))
import atexit

atexit.register(lambda: pa.unregister_extension_type("daft.super_extension"))
