# DataTypes

## Type Conversions

### Daft to Python

<!-- Note: the conversions here should match the behavior of the Rust `impl IntoPyObject for Literal`: `src/daft-core/src-lit/python.rs` -->

This table shows the mapping from Daft DataTypes to Python types, as done in places such as [`Series.to_pylist`][daft.series.Series.to_pylist], [`Expression.cast`][daft.expressions.Expression.cast] to Python type, and arguments passed into functions decorated with `@daft.func`.

| Daft DataType                                                        | Python Type                                                                         |
|----------------------------------------------------------------------|-------------------------------------------------------------------------------------|
| Null                                                                 | `None`                                                                              |
| Boolean                                                              | `bool`                                                                              |
| Utf8                                                                 | `str`                                                                               |
| Binary, FixedSizeBinary                                              | `bytes`                                                                             |
| Int8, Uint8, Int16, UInt16, Int32, UInt32, Int64, UInt64             | `int`                                                                               |
| Timestamp                                                            | `datetime.datetime`                                                                 |
| Date                                                                 | `datetime.date`                                                                     |
| Time                                                                 | `datetime.time`                                                                     |
| Duration                                                             | `datetime.timedelta`                                                                |
| Interval                                                             | not supported                                                                       |
| Float32, Float64                                                     | `float`                                                                             |
| Decimal                                                              | `decimal.Decimal`                                                                   |
| List[T], FixedSizeList[T]                                            | `list[T]`                                                                           |
| Struct \{<field_1\>: T1, <field_2\>: T2, ...\}                       | `dict[str, T1 | T2 | ...]`                                                          |
| Map[K, V]                                                            | `dict[K, V]`                                                                        |
| Tensor[T], FixedShapeTensor[T]                                       | `numpy.typing.NDArray[T]`                                                           |
| SparseTensor[T], FixedShapeSparseTensor[T]                           | `{`<br>`"values": T,`<br>`"indices": list[int],`<br>`"shape": list[int]`<br>`}`     |
| Embedding[T]                                                         | `numpy.typing.NDArray[T]`                                                           |
| Image                                                                | `numpy.typing.NDArray[numpy.uint8 | numpy.uint16 | numpy.float32]`                  |
| Python                                                               | `Any`                                                                               |
| Extension[T]                                                         | `T`                                                                                 |

### Python to Daft
TODO

## daft.DataType

Daft provides simple DataTypes that are ubiquituous in many DataFrames such as numbers, strings and dates - all the way up to more complex types like tensors and images. Learn more about [DataTypes](../core_concepts.md#datatypes) in Daft User Guide.

::: daft.datatype.DataType
    options:
        filters: ["!^_"]

<!-- add more pages to filters to include them, see dataframe for example -->

<!-- fix: do we need class datatype> -->
