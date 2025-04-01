# SQL Data Types

These tables define how Daft's [DataType](../api/datatypes.md) maps to the common SQL types and aliases.

!!! note "Note"

    In these tables, the **Type** column identifies the Daft [DataType](../api/datatypes.md), whereas the **Name** and **Aliases** columns represent supported SQL syntax.

## Boolean Type

| Type                                  | Name      | Aliases | Description       |
| ------------------------------------- | --------- | ------- | ----------------- |
| [`bool`][daft.datatype.DataType.bool] | `BOOLEAN` | `BOOL`  | `TRUE` or `FALSE` |


## Numeric Types

| Type                                              | Name                       | Aliases                         | Description             |
| ------------------------------------------------- | -------------------------- | ------------------------------- | ----------------------- |
| [`int8`][daft.datatype.DataType.int8]             | `TINYINT`                  | `INT1`                          | 8-bit signed integer    |
| [`int16`][daft.datatype.DataType.int16]           | `SMALLINT`                 | `INT2`, `INT16`                 | 16-bit signed integer   |
| [`int32`][daft.datatype.DataType.int32]           | `INT`, `INTEGER`           | `INT4`, `INT32`                 | 32-bit signed integer   |
| [`int64`][daft.datatype.DataType.int64]           | `BIGINT`                   | `INT8`, `INT64`                 | 64-bit signed integer   |
| [`uint8`][daft.datatype.DataType.uint8]           | `TINYINT UNSIGNED`         | `UINT1`                         | 8-bit unsigned integer  |
| [`uint16`][daft.datatype.DataType.uint16]         | `SMALLINT UNSIGNED`        | `UINT2`, `UINT16`               | 16-bit unsigned integer |
| [`uint32`][daft.datatype.DataType.uint32]         | `INT UNSIGNED`             | `UINT4`, `UINT32`               | 32-bit unsigned integer |
| [`uint64`][daft.datatype.DataType.uint64]         | `BIGINT UNSIGNED`          | `UINT8`, `UINT64`               | 64-bit unsigned integer |
| [`float32`][daft.datatype.DataType.float32]       | `REAL`                     | `FLOAT(P)`, `FLOAT32`           | 32-bit floating point   |
| [`float64`][daft.datatype.DataType.float64]       | `DOUBLE [PRECISION]`       | `FLOAT(P)`, `FLOAT64` , `FLOAT` | 64-bit floating point   |
| [`decimal128`][daft.datatype.DataType.decimal128] | `DEC(P,S)`, `DECIMAL(P,S)` | `NUMERIC(P,S)`                  | Fixed-point number      |


## Text Types

| Type                                      | Name      | Aliases                                 | Description                          |
| ----------------------------------------- | --------- | --------------------------------------- | ------------------------------------ |
| [`string`][daft.datatype.DataType.string] | `VARCHAR` | `STRING`, `TEXT`                        | Variable-length string (see warning) |

!!! warning "Warning"

    Daft uses UTF-8, and there is no fixed-length character string type. You may use the fixed-sized binary type to specify a fixed size in bytes.


## Binary Types

| Type                                                            | Name        | Aliases    | Description                 |
| --------------------------------------------------------------- | ----------- | ---------- | --------------------------- |
| [`binary`][daft.datatype.DataType.binary]                       | `BINARY`    | `BYTES`    | Variable-length byte string |
| [`fixed_size_binary`][daft.datatype.DataType.fixed_size_binary] | `BINARY(N)` | `BYTES(N)` | Fixed-length byte string    |

!!! warning "Warning"

    SQL defines `BINARY` and `BINARY VARYING` for fixed-length and variable-length binary strings respectively. Like PostgreSQL, Daft does not use the SQL "up-to length" semantic, and instead opts for variable length if none is given. These aliases are [subject to change](https://github.com/Eventual-Inc/Daft/issues/3955).


## Datetime Types

| Type                                            | Names       | Aliases | Description             |
| ----------------------------------------------- | ----------- | ------- | ----------------------- |
| [`date`][daft.datatype.DataType.date]           | `DATE`      |         | Date without Time       |
| [`time`][daft.datatype.DataType.time]           | `TIME`      |         | Time without Date       |
| [`timestamp`][daft.datatype.DataType.timestamp] | `TIMESTAMP` |         | Date with Time          |
| [`interval`][daft.datatype.DataType.interval]   | `INTERVAL`  |         | Time Interval (YD & DT) |
| [`duration`][daft.datatype.DataType.duration]   | -           |         | Time Duration           |

!!! warning "Warning"

    Daft does not currently support `TIME WITH TIMEZONE` and `TIMESTAMP WITH TIMEZONE`, please see [Github #3957](https://github.com/Eventual-Inc/Daft/issues/3957) and feel free to bump this issue if you need it prioritized.


## Array Types

| Type                                                        | Name         | Aliases | Description                      |
| ----------------------------------------------------------- | ------------ | ------- | -------------------------------- |
| [`list`][daft.datatype.DataType.list]                       | `T ARRAY[]`  | `T[]`   | Variable-length list of elements |
| [`fixed_size_list`][daft.datatype.DataType.fixed_size_list] | `T ARRAY[N]` | `T[N]`  | Fixed-length list of elements    |


## Map & Struct Types

| Type                                      | Syntax              | Example                 | Description                         |
| ----------------------------------------- | ------------------- | ----------------------- | ----------------------------------- |
| [`map`][daft.datatype.DataType.map]       | `MAP(K, V)`         | `MAP(INT, BOOL)`        | Key-value pairs with the same types |
| [`struct`][daft.datatype.DataType.struct] | `STRUCT(FIELDS...)` | `STRUCT(A INT, B BOOL)` | Named values of varying types       |


## Complex Types

| Type                                            | Syntax                      | Example                  | Description                |
| ----------------------------------------------- | --------------------------- | ------------------------ | -------------------------- |
| [`tensor`][daft.datatype.DataType.tensor]       | `TENSOR(T, [shape...])`     | `TENSOR(INT)`            | Multi-dimensional array    |
| [`image`][daft.datatype.DataType.image]         | `IMAGE(mode, [dimensions])` | `IMAGE('RGB', 256, 256)` | Image data array           |
| [`embedding`][daft.datatype.DataType.embedding] | `EMBEDDING(T, N)`           | `EMBEDDING(INT, 100)`    | Fixed-length numeric array |

### Tensor Type

The `TENSOR(T, [shape...])` type represents n-dimensional arrays of data of the provided type `T`, each of the provided shape. The shape is given as an optional list of integers.

**Examples**

```sql
TENSOR(INT)
TENSOR(INT, 10, 10, 10)
```

### Image Type

The `IMAGE(mode, [dimensions])` type represents an array of pixels with designated pixel type. The supported modes are covered in [ImageMode][daft.daft.ImageMode]. If the height, width, and mode are the same for all images in the array, specifying them when constructing this type is advised, since that will allow Daft to create a more optimized physical representation of the image array.

**Examples**

```sql
IMAGE('RGB')
IMAGE('RGB', 256, 256)
```

### Embedding Type

The `EMBEDDING(T, N)` type represents a fixed-length `N` array of **numeric** type `T`.

**Examples**

```sql
EMBEDDING(INT16, 256)
EMBEDDING(INT32, 100)
```
