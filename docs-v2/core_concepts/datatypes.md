# DataTypes

All columns in a Daft DataFrame have a DataType (also often abbreviated as `dtype`).

All elements of a column are of the same dtype, or they can be the special Null value (indicating a missing value).

Daft provides simple DataTypes that are ubiquituous in many DataFrames such as numbers, strings and dates - all the way up to more complex types like tensors and images.

!!! tip "Tip"

    For a full overview on all the DataTypes that Daft supports, see the [DataType API Reference](https://www.getdaft.io/projects/docs/en/stable/api_docs/datatype.html).


## Numeric DataTypes

Numeric DataTypes allows Daft to represent numbers. These numbers can differ in terms of the number of bits used to represent them (8, 16, 32 or 64 bits) and the semantic meaning of those bits
(float vs integer vs unsigned integers).

Examples:

1. [`DataType.int8()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/datatype.html#daft.DataType.int8): represents an 8-bit signed integer (-128 to 127)
2. [`DataType.float32()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/datatype.html#daft.DataType.float32): represents a 32-bit float (a float number with about 7 decimal digits of precision)

Columns/expressions with these datatypes can be operated on with many numeric expressions such as `+` and `*`.

See also: [Numeric Expressions](https://www.getdaft.io/projects/docs/en/stable/user_guide/expressions.html#userguide-numeric-expressions)

## Logical DataTypes

The [`Boolean`](https://www.getdaft.io/projects/docs/en/stable/api_docs/datatype.html#daft.DataType.bool) DataType represents values which are boolean values: `True`, `False` or `Null`.

Columns/expressions with this dtype can be operated on using logical expressions such as ``&`` and [`.if_else()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/expression_methods/daft.Expression.if_else.html#daft.Expression.if_else).

See also: [Logical Expressions](https://www.getdaft.io/projects/docs/en/stable/user_guide/expressions.html#userguide-logical-expressions)

## String Types

Daft has string types, which represent a variable-length string of characters.

As a convenience method, string types also support the `+` Expression, which has been overloaded to support concatenation of elements between two [`DataType.string()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/datatype.html#daft.DataType.string) columns.

1. [`DataType.string()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/datatype.html#daft.DataType.string): represents a string of UTF-8 characters
2. [`DataType.binary()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/datatype.html#daft.DataType.binary): represents a string of bytes

See also: [String Expressions](https://www.getdaft.io/projects/docs/en/stable/user_guide/expressions.html#userguide-string-expressions)

## Temporal

Temporal dtypes represent data that have to do with time.

Examples:

1. [`DataType.date()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/datatype.html#daft.DataType.date): represents a Date (year, month and day)
2. [`DataType.timestamp()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/datatype.html#daft.DataType.timestamp): represents a Timestamp (particular instance in time)

See also: [Temporal Expressions](https://www.getdaft.io/projects/docs/en/stable/api_docs/expressions.html#api-expressions-temporal)

## Nested

Nested DataTypes wrap other DataTypes, allowing you to compose types into complex data structures.

Examples:

1. [`DataType.list(child_dtype)`](https://www.getdaft.io/projects/docs/en/stable/api_docs/datatype.html#daft.DataType.list): represents a list where each element is of the child `dtype`
2. [`DataType.struct({"field_name": child_dtype})`](https://www.getdaft.io/projects/docs/en/stable/api_docs/datatype.html#daft.DataType.struct): represents a structure that has children `dtype`s, each mapped to a field name

## Python

The [`DataType.python()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/datatype.html#daft.DataType.python) dtype represent items that are Python objects.

!!! warning "Warning"

    Daft does not impose any invariants about what *Python types* these objects are. To Daft, these are just generic Python objects!

Python is AWESOME because it's so flexible, but it's also slow and memory inefficient! Thus we recommend:

1. **Cast early!**: Casting your Python data into native Daft DataTypes if possible - this results in much more efficient downstream data serialization and computation.
2. **Use Python UDFs**: If there is no suitable Daft representation for your Python objects, use Python UDFs to process your Python data and extract the relevant data to be returned as native Daft DataTypes!

!!! note "Note"

    If you work with Python classes for a generalizable use-case (e.g. documents, protobufs), it may be that these types are good candidates for "promotion" into a native Daft type! Please get in touch with the Daft team and we would love to work together on building your type into canonical Daft types.

## Complex Types

Daft supports many more interesting complex DataTypes, for example:

* [`DataType.tensor()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/datatype.html#daft.DataType.tensor): Multi-dimensional (potentially uniformly-shaped) tensors of data
* [`DataType.embedding()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/datatype.html#daft.DataType.embedding): Lower-dimensional vector representation of data (e.g. words)
* [`DataType.image()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/datatype.html#daft.DataType.image): NHWC images

Daft abstracts away the in-memory representation of your data and provides kernels for many common operations on top of these data types. For supported image operations see the [image expressions API reference](https://www.getdaft.io/projects/docs/en/stable/api_docs/expressions.html#api-expressions-images).

For more complex algorithms, you can also drop into a Python UDF to process this data using your custom Python libraries.

Please add suggestions for new DataTypes to our [Github Discussions page](https://github.com/Eventual-Inc/Daft/discussions)!
