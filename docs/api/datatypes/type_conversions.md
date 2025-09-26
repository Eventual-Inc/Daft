# Type Conversions

## Daft to Python

<!-- Note: the conversions here should match the behavior of the Rust `impl IntoPyObject for Literal`: `src/daft-core/src-lit/python.rs` -->

This table shows the mapping from Daft DataTypes to Python types, as done in places such as [`Series.to_pylist`][daft.series.Series.to_pylist], [`Expression.cast`][daft.expressions.Expression.cast] to Python type, and arguments passed into functions decorated with `@daft.func`.

| Daft DataType                                                          | Python Type                                                                         |
|------------------------------------------------------------------------|-------------------------------------------------------------------------------------|
| Null                                                                   | `None`                                                                              |
| Boolean                                                                | `bool`                                                                              |
| Utf8                                                                   | `str`                                                                               |
| Binary<br>FixedSizeBinary                                              | `bytes`                                                                             |
| Int8<br>Uint8<br>Int16<br>UInt16<br>Int32<br>UInt32<br>Int64<br>UInt64 | `int`                                                                               |
| Timestamp                                                              | `datetime.datetime`                                                                 |
| Date                                                                   | `datetime.date`                                                                     |
| Time                                                                   | `datetime.time`                                                                     |
| Duration                                                               | `datetime.timedelta`                                                                |
| Interval                                                               | not supported                                                                       |
| Float32<br>Float64                                                     | `float`                                                                             |
| Decimal128                                                             | `decimal.Decimal`                                                                   |
| List[T]<br>FixedSizeList[T]                                            | `list[T]`                                                                           |
| Struct[k1: T1, k2: T2, ...]                                            | `{ "k1": <T1>, "k2": <T2>, ... }`                                                   |
| Map[K, V]                                                              | `dict[K, V]`                                                                        |
| Tensor[T]<br>FixedShapeTensor[T]                                       | `numpy.typing.NDArray[T]`                                                           |
| SparseTensor[T]<br>FixedShapeSparseTensor[T]                           | `{`<br>`"values": <T>,`<br>`"indices": [<int>],`<br>`"shape": [<int>]`<br>`}`       |
| Embedding[T]                                                           | `numpy.typing.NDArray[T]`                                                           |
| Image                                                                  | `numpy.typing.NDArray[numpy.uint8 | numpy.uint16 | numpy.float32]`                  |
| Python                                                                 | `Any`                                                                               |
| Extension[T]                                                           | `T`                                                                                 |

## Python to Daft

### From Python Type

<!-- Note: the conversions here should match the behavior of `DataType.infer_from_type` : `daft/datatype.py`  -->

This table shows the mapping from Python types to Daft types, such as when inferring the return type from the type hints of a function decorated with `@daft.func`.

To check the inferred DataType for a Python type, use [`DataType.infer_from_type`][daft.datatype.DataType.infer_from_type].

| **Python Type**                                         | **Daft DataType**                   |
|---------------------------------------------------------|-------------------------------------|
| `NoneType`                                              | Null                                |
| `bool`                                                  | Boolean                             |
| `str`                                                   | Utf8                                |
| `bytes`                                                 | Binary                              |
| `int`                                                   | Int64                               |
| `float`                                                 | Float64                             |
| `datetime.datetime`                                     | Timestamp[us]                       |
| `datetime.date`                                         | Date                                |
| `datetime.time`                                         | Time[us]                            |
| `datetime.timedelta`                                    | Duration[us]                        |
| `list[T]`                                               | List[T]                             |
| `dict[K, V]`                                            | Map[K, V]                           |
| `typing.TypedDict("...", { "k1": T1, "k2": T2, ... })`  | Struct[k1: T1, k2: T2, ...]         |
| `tuple[T0, T1, ..., TN]` (no ellipsis in actual type)   | Struct[_0: T0, _1: T1, ..., _N: TN] |
| `tuple[T, ...]`                                         | List[T]                             |
| `numpy.ndarray`                                         | Tensor[Python]                      |
| `numpy.typing.NDArray[T]`                               | Tensor[T]                           |
| `numpy.bool_`                                           | Boolean                             |
| `numpy.int8`                                            | Int8                                |
| `numpy.uint8`                                           | UInt8                               |
| `numpy.int16`                                           | Int16                               |
| `numpy.uint16`                                          | UInt16                              |
| `numpy.int32`                                           | Int32                               |
| `numpy.uint32`                                          | UInt32                              |
| `numpy.int64`                                           | Int64                               |
| `numpy.uint64`                                          | UInt64                              |
| `numpy.float32`                                         | Float32                             |
| `numpy.float64`                                         | Float64                             |
| `numpy.datetime64`                                      | Timestamp[us]                       |
| `pandas.Series`                                         | List[Python]                        |
| `PIL.Image.Image`                                       | Image[MIXED]                        |
| [`daft.Series`][daft.series.Series]                     | List[Python]                        |
| `daft.File`                                             | File                                |
| Everything else                                         | Python                              |

### From Python Object

<!-- Note: the conversions here should match the behavior of `DataType.infer_from_object` : `daft/datatype.py`  -->

In addition to the above table, this table shows the additional behavior when Daft converts Python objects to Daft types without an explicitly specified type, such as in [`daft.from_pydict`][daft.from_pydict] and [`Series.from_pylist`][daft.series.Series.from_pylist]. In these cases, Daft is able to derive information from the Python object that is not present in the object's type, allowing for better mapping to Daft types.

To check the inferred DataType for a Python object, use [`DataType.infer_from_object`][daft.datatype.DataType.infer_from_object].

| **Python Object**                                           | **Daft Type**                     |
|-------------------------------------------------------------|-----------------------------------|
| `int` value greater than 2^63-1 (max i64 value)             | UInt64                            |
| `dict` with fields: `{ "k1": <T1>, "k2": <T2>, ... }`       | Struct[k1: T1, k2: T2, ...]       |
| `decimal.Decimal` with `N` digits after the dot             | Decimal128[precision=38, scale=N] |
| `numpy.ndarray` with element type `T`                       | Tensor[T]                         |
| `pandas.Series` with element type `T`                       | List[T]                           |
| [`daft.Series`][daft.series.Series]  with element type  `T` | List[T]                           |
| `numpy.datetime64` with `U` = [datetime unit](https://numpy.org/doc/stable/reference/arrays.datetime.html#datetime-units) | - Date if `U` = "Y", "M", "W", or "D"<br>- Timestamp[s] if `U` = "h", "m", or "s"<br>- Timestamp[ms] if `U` = "ms"<br>- Timestamp[us] if `U` = "us"<br>- Timestamp[ns] if `U` = "ns", "ps", "fs", or "as" |
| `PIL.Image.Image` with `M` = [image mode](https://pillow.readthedocs.io/en/stable/handbook/concepts.html#concept-modes) | Image[M]<br>(supported modes: L, LA, RGB, RGBA) |
