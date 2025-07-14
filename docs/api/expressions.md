# Expressions

Daft Expressions allow you to express some computation that needs to happen in a DataFrame. This page provides an overview of all the functionality that is provided by Daft Expressions. Learn more about [Expressions](../core_concepts.md#expressions) in Daft User Guide.

<div class="grid cards api" markdown>

* [**Constructors**](#constructors)

    Create expressions from columns, literals, structs, and SQL strings.

* [**Generic**](#generic)

    Basic utilities for aliasing, casting, null handling, and serialization.

* [**Numeric**](#numeric)

    Mathematical operations including arithmetic and trigonometric functions.

* [**Logical**](#numeric)

    Boolean operations, comparisons, and membership testing.

* [**Bitwise Operations**](#bitwise-operations)

    Bitwise logical operations and bit shifting on integer values.

* [**Aggregation**](#aggregation)

    Summary statistics, counts, and reductions across groups.

* [**Transformation**](#transformation)

    Data encoding, hashing, JSON querying, and format conversions.

<!-- These will change after namespaces are flattened -->

* [**String**](#string)

    String operations via `.str` namespace for text manipulation.

* [**Binary**](#binary)

    Binary data operations via `.binary` namespace for byte processing.

* [**Float**](#float)

    Floating-point operations via `.float` namespace for numeric computations.

* [**Temporal**](#temporal)

    Date, time, and timestamp operations via `.dt` namespace.

* [**List**](#list)

    Array/list operations via `.list` namespace for sequence manipulation.

* [**Struct**](#struct)

    Structured data with named fields via `.struct` namespace.

* [**Map**](#map)

    Key-value operations via `.map` namespace for dictionary-like data.

* [**Image**](#image)

    Image processing operations via `.image` namespace for computer vision.

* [**Partitioning**](#partitioning)

    Data distribution control via `.partition` namespace for distributed processing.

* [**URL**](#url)

    URL parsing and component extraction via `.url` namespace.

* [**JSON**](#json)

    JSON parsing, querying, and manipulation via `.json` namespace.

* [**Embedding**](#embedding)

    Vector embeddings and similarity operations via `.embedding` namespace.

* [**Visitor**](#visitor)

    Traversal and inspection utilities for examining expression trees and structures.

</div>

## Constructors

<!-- BEGIN GENERATED TABLE -->
| Method | Description |
|--------|-------------|
| [`col`][daft.expressions.col] | Creates an Expression referring to the column with the provided name. |
| [`deserialize`][daft.expressions.expressions.Expression.deserialize] | Deserializes the expression (string) using the specified format and data type. |
| [`interval`][daft.expressions.interval] | Creates an Expression representing an interval. |
| [`list_`][daft.expressions.list_] | Constructs a list from the item expressions. |
| [`lit`][daft.expressions.lit] | Creates an Expression representing a column with every value set to the provided value. |
| [`sql_expr`][daft.sql.sql.sql_expr] | Parses a SQL string into a Daft Expression. |
| [`struct`][daft.expressions.struct] | Constructs a struct from the input field expressions. |
| [`to_struct`][daft.expressions.expressions.Expression.to_struct] | Constructs a struct from the input field expressions. |
| [`try_deserialize`][daft.expressions.expressions.Expression.try_deserialize] | Deserializes the expression (string) using the specified format and data type, inserting nulls on failures. |
| [`udf`][daft.expressions.expressions.Expression.udf] |  |
<!-- END GENERATED TABLE -->

::: daft.expressions.col
::: daft.expressions.expressions.Expression.deserialize
::: daft.expressions.interval
::: daft.expressions.list_
::: daft.expressions.lit
::: daft.sql.sql.sql_expr
::: daft.expressions.struct
::: daft.expressions.expressions.Expression.to_struct
::: daft.expressions.expressions.Expression.try_deserialize
::: daft.expressions.expressions.Expression.udf

## Generic

<!-- BEGIN GENERATED TABLE -->
| Method | Description |
|--------|-------------|
| [`__bool__`][daft.expressions.expressions.Expression.__bool__] |  |
| [`__getitem__`][daft.expressions.expressions.Expression.__getitem__] | Syntactic sugar for `Expression.list.get` and `Expression.struct.get`. |
| [`__hash__`][daft.expressions.expressions.Expression.__hash__] | Return hash(self). |
| [`__repr__`][daft.expressions.expressions.Expression.__repr__] | Return repr(self). |
| [`alias`][daft.expressions.expressions.Expression.alias] | Gives the expression a new name. |
| [`apply`][daft.expressions.expressions.Expression.apply] | Apply a function on each value in a given expression. |
| [`as_py`][daft.expressions.expressions.Expression.as_py] | Returns this literal expression as a python value, raises a ValueError if this is not a literal expression. |
| [`cast`][daft.expressions.expressions.Expression.cast] | Casts an expression to the given datatype if possible. |
| [`coalesce`][daft.expressions.coalesce] | Returns the first non-null value in a list of expressions. If all inputs are null, returns null. |
| [`explode`][daft.expressions.expressions.Expression.explode] | Explode a list expression. |
| [`fill_null`][daft.expressions.expressions.Expression.fill_null] | Fills null values in the Expression with the provided fill_value. |
| [`if_else`][daft.expressions.expressions.Expression.if_else] | Conditionally choose values between two expressions using the current boolean expression as a condition. |
| [`name`][daft.expressions.expressions.Expression.name] |  |
| [`serialize`][daft.expressions.expressions.Expression.serialize] | Serializes the expression as a string using the specified format. |
| [`to_arrow_expr`][daft.expressions.expressions.Expression.to_arrow_expr] | Returns this expression as a pyarrow.compute.Expression for integrations with other systems. |
| [`try_decode`][daft.expressions.expressions.Expression.try_decode] | Decodes or returns null, see `Expression.decode`. |
| [`try_encode`][daft.expressions.expressions.Expression.try_encode] | Encodes or returns null, see `Expression.encode`. |
<!-- END GENERATED TABLE -->

::: daft.expressions.expressions.Expression.__bool__
::: daft.expressions.expressions.Expression.__getitem__
::: daft.expressions.expressions.Expression.__hash__
::: daft.expressions.expressions.Expression.__repr__
::: daft.expressions.expressions.Expression.alias
::: daft.expressions.expressions.Expression.apply
::: daft.expressions.expressions.Expression.as_py
::: daft.expressions.expressions.Expression.cast
::: daft.expressions.coalesce
::: daft.expressions.expressions.Expression.explode
::: daft.expressions.expressions.Expression.fill_null
::: daft.expressions.expressions.Expression.if_else
::: daft.expressions.expressions.Expression.name
::: daft.expressions.expressions.Expression.serialize
::: daft.expressions.expressions.Expression.to_arrow_expr
::: daft.expressions.expressions.Expression.try_decode
::: daft.expressions.expressions.Expression.try_encode

## Numeric

<!-- BEGIN GENERATED TABLE -->
| Method | Description |
|--------|-------------|
| [`__abs__`][daft.expressions.expressions.Expression.__abs__] | Absolute of a numeric expression. |
| [`__add__`][daft.expressions.expressions.Expression.__add__] | Adds two numeric expressions or concatenates two string expressions (``e1 + e2``). |
| [`__floordiv__`][daft.expressions.expressions.Expression.__floordiv__] | Floor divides two numeric expressions (``e1 / e2``). |
| [`__mod__`][daft.expressions.expressions.Expression.__mod__] | Takes the mod of two numeric expressions (``e1 % e2``). |
| [`__mul__`][daft.expressions.expressions.Expression.__mul__] | Multiplies two numeric expressions (``e1 * e2``). |
| [`__radd__`][daft.expressions.expressions.Expression.__radd__] |  |
| [`__rfloordiv__`][daft.expressions.expressions.Expression.__rfloordiv__] | Reverse floor divides two numeric expressions (``e2 / e1``). |
| [`__rmod__`][daft.expressions.expressions.Expression.__rmod__] | Takes the mod of two numeric expressions (``e1 % e2``). |
| [`__rmul__`][daft.expressions.expressions.Expression.__rmul__] |  |
| [`__rsub__`][daft.expressions.expressions.Expression.__rsub__] |  |
| [`__rtruediv__`][daft.expressions.expressions.Expression.__rtruediv__] |  |
| [`__sub__`][daft.expressions.expressions.Expression.__sub__] | Subtracts two numeric expressions (``e1 - e2``). |
| [`__truediv__`][daft.expressions.expressions.Expression.__truediv__] | True divides two numeric expressions (``e1 / e2``). |
| [`abs`][daft.expressions.expressions.Expression.abs] | Absolute of a numeric expression. |
| [`arccos`][daft.expressions.expressions.Expression.arccos] | The elementwise arc cosine of a numeric expression. |
| [`arccosh`][daft.expressions.expressions.Expression.arccosh] | The elementwise inverse hyperbolic cosine of a numeric expression. |
| [`arcsin`][daft.expressions.expressions.Expression.arcsin] | The elementwise arc sine of a numeric expression. |
| [`arcsinh`][daft.expressions.expressions.Expression.arcsinh] | The elementwise inverse hyperbolic sine of a numeric expression. |
| [`arctan`][daft.expressions.expressions.Expression.arctan] | The elementwise arc tangent of a numeric expression. |
| [`arctan2`][daft.expressions.expressions.Expression.arctan2] | Calculates the four quadrant arctangent of coordinates (y, x), in radians. |
| [`arctanh`][daft.expressions.expressions.Expression.arctanh] | The elementwise inverse hyperbolic tangent of a numeric expression. |
| [`cbrt`][daft.expressions.expressions.Expression.cbrt] | The cube root of a numeric expression. |
| [`ceil`][daft.expressions.expressions.Expression.ceil] | The ceiling of a numeric expression. |
| [`clip`][daft.expressions.expressions.Expression.clip] | Clips an expression to the given minimum and maximum values. |
| [`cos`][daft.expressions.expressions.Expression.cos] | The elementwise cosine of a numeric expression. |
| [`cosh`][daft.expressions.expressions.Expression.cosh] | The elementwise hyperbolic cosine of a numeric expression. |
| [`cot`][daft.expressions.expressions.Expression.cot] | The elementwise cotangent of a numeric expression. |
| [`csc`][daft.expressions.expressions.Expression.csc] | The elementwise cosecant of a numeric expression. |
| [`degrees`][daft.expressions.expressions.Expression.degrees] | The elementwise degrees of a numeric expression. |
| [`exp`][daft.expressions.expressions.Expression.exp] | The e^self of a numeric expression. |
| [`expm1`][daft.expressions.expressions.Expression.expm1] | The e^self - 1 of a numeric expression. |
| [`floor`][daft.expressions.expressions.Expression.floor] | The floor of a numeric expression. |
| [`ln`][daft.expressions.expressions.Expression.ln] | The elementwise natural log of a numeric expression. |
| [`log`][daft.expressions.expressions.Expression.log] | The elementwise log with given base, of a numeric expression. |
| [`log10`][daft.expressions.expressions.Expression.log10] | The elementwise log base 10 of a numeric expression. |
| [`log1p`][daft.expressions.expressions.Expression.log1p] | The ln(self + 1) of a numeric expression. |
| [`log2`][daft.expressions.expressions.Expression.log2] | The elementwise log base 2 of a numeric expression. |
| [`negate`][daft.expressions.expressions.Expression.negate] | The negative of a numeric expression. |
| [`negative`][daft.expressions.expressions.Expression.negative] | The negative of a numeric expression. |
| [`radians`][daft.expressions.expressions.Expression.radians] | The elementwise radians of a numeric expression. |
| [`round`][daft.expressions.expressions.Expression.round] | The round of a numeric expression. |
| [`sec`][daft.expressions.expressions.Expression.sec] | The elementwise secant of a numeric expression. |
| [`sign`][daft.expressions.expressions.Expression.sign] | The sign of a numeric expression. |
| [`signum`][daft.expressions.expressions.Expression.signum] | The signum of a numeric expression. |
| [`sin`][daft.expressions.expressions.Expression.sin] | The elementwise sine of a numeric expression. |
| [`sinh`][daft.expressions.expressions.Expression.sinh] | The elementwise hyperbolic sine of a numeric expression. |
| [`sqrt`][daft.expressions.expressions.Expression.sqrt] | The square root of a numeric expression. |
| [`tan`][daft.expressions.expressions.Expression.tan] | The elementwise tangent of a numeric expression. |
| [`tanh`][daft.expressions.expressions.Expression.tanh] | The elementwise hyperbolic tangent of a numeric expression. |
<!-- END GENERATED TABLE -->

::: daft.expressions.expressions.Expression.__abs__
::: daft.expressions.expressions.Expression.__add__
::: daft.expressions.expressions.Expression.__floordiv__
::: daft.expressions.expressions.Expression.__mod__
::: daft.expressions.expressions.Expression.__mul__
::: daft.expressions.expressions.Expression.__radd__
::: daft.expressions.expressions.Expression.__rfloordiv__
::: daft.expressions.expressions.Expression.__rmod__
::: daft.expressions.expressions.Expression.__rmul__
::: daft.expressions.expressions.Expression.__rsub__
::: daft.expressions.expressions.Expression.__rtruediv__
::: daft.expressions.expressions.Expression.__sub__
::: daft.expressions.expressions.Expression.__truediv__
::: daft.expressions.expressions.Expression.abs
::: daft.expressions.expressions.Expression.arccos
::: daft.expressions.expressions.Expression.arccosh
::: daft.expressions.expressions.Expression.arcsin
::: daft.expressions.expressions.Expression.arcsinh
::: daft.expressions.expressions.Expression.arctan
::: daft.expressions.expressions.Expression.arctan2
::: daft.expressions.expressions.Expression.arctanh
::: daft.expressions.expressions.Expression.cbrt
::: daft.expressions.expressions.Expression.ceil
::: daft.expressions.expressions.Expression.clip
::: daft.expressions.expressions.Expression.cos
::: daft.expressions.expressions.Expression.cosh
::: daft.expressions.expressions.Expression.cot
::: daft.expressions.expressions.Expression.csc
::: daft.expressions.expressions.Expression.degrees
::: daft.expressions.expressions.Expression.exp
::: daft.expressions.expressions.Expression.expm1
::: daft.expressions.expressions.Expression.floor
::: daft.expressions.expressions.Expression.ln
::: daft.expressions.expressions.Expression.log
::: daft.expressions.expressions.Expression.log10
::: daft.expressions.expressions.Expression.log1p
::: daft.expressions.expressions.Expression.log2
::: daft.expressions.expressions.Expression.negate
::: daft.expressions.expressions.Expression.negative
::: daft.expressions.expressions.Expression.radians
::: daft.expressions.expressions.Expression.round
::: daft.expressions.expressions.Expression.sec
::: daft.expressions.expressions.Expression.sign
::: daft.expressions.expressions.Expression.signum
::: daft.expressions.expressions.Expression.sin
::: daft.expressions.expressions.Expression.sinh
::: daft.expressions.expressions.Expression.sqrt
::: daft.expressions.expressions.Expression.tan
::: daft.expressions.expressions.Expression.tanh

## Logical


<!-- BEGIN GENERATED TABLE -->
| Method | Description |
|--------|-------------|
| [`__and__`][daft.expressions.expressions.Expression.__and__] | Takes the logical AND of two boolean expressions, or bitwise AND of two integer expressions (``e1 & e2``). |
| [`__eq__`][daft.expressions.expressions.Expression.__eq__] | Compares if an expression is equal to another (``e1 == e2``). |
| [`__ge__`][daft.expressions.expressions.Expression.__ge__] | Compares if an expression is greater than or equal to another (``e1 >= e2``). |
| [`__gt__`][daft.expressions.expressions.Expression.__gt__] | Compares if an expression is greater than another (``e1 > e2``). |
| [`__le__`][daft.expressions.expressions.Expression.__le__] | Compares if an expression is less than or equal to another (``e1 <= e2``). |
| [`__lt__`][daft.expressions.expressions.Expression.__lt__] | Compares if an expression is less than another (``e1 < e2``). |
| [`__ne__`][daft.expressions.expressions.Expression.__ne__] | Compares if an expression is not equal to another (``e1 != e2``). |
| [`__or__`][daft.expressions.expressions.Expression.__or__] | Takes the logical OR of two boolean or integer expressions, or bitwise OR of two integer expressions (``e1 | e2``). |
| [`__rand__`][daft.expressions.expressions.Expression.__rand__] | Takes the logical reverse AND of two boolean expressions (``e1 & e2``). |
| [`__ror__`][daft.expressions.expressions.Expression.__ror__] | Takes the logical reverse OR of two boolean expressions (``e1 | e2``). |
| [`__xor__`][daft.expressions.expressions.Expression.__xor__] | Takes the logical XOR of two boolean or integer expressions, or bitwise XOR of two integer expressions (``e1 ^ e2``). |
| [`between`][daft.expressions.expressions.Expression.between] | Checks if values in the Expression are between lower and upper, inclusive. |
| [`eq_null_safe`][daft.expressions.expressions.Expression.eq_null_safe] | Performs a null-safe equality comparison between two expressions. |
| [`is_in`][daft.expressions.expressions.Expression.is_in] | Checks if values in the Expression are in the provided list. |
| [`is_null`][daft.expressions.expressions.Expression.is_null] | Checks if values in the Expression are Null (a special value indicating missing data). |
| [`not_null`][daft.expressions.expressions.Expression.not_null] | Checks if values in the Expression are not Null (a special value indicating missing data). |
<!-- END GENERATED TABLE -->

::: daft.expressions.expressions.Expression.__and__
::: daft.expressions.expressions.Expression.__eq__
::: daft.expressions.expressions.Expression.__ge__
::: daft.expressions.expressions.Expression.__gt__
::: daft.expressions.expressions.Expression.__le__
::: daft.expressions.expressions.Expression.__lt__
::: daft.expressions.expressions.Expression.__ne__
::: daft.expressions.expressions.Expression.__or__
::: daft.expressions.expressions.Expression.__rand__
::: daft.expressions.expressions.Expression.__ror__
::: daft.expressions.expressions.Expression.__xor__
::: daft.expressions.expressions.Expression.between
::: daft.expressions.expressions.Expression.eq_null_safe
::: daft.expressions.expressions.Expression.is_in
::: daft.expressions.expressions.Expression.is_null
::: daft.expressions.expressions.Expression.not_null

## Bitwise Operations

::: daft.expressions.expressions.Expression.__invert__
::: daft.expressions.expressions.Expression.__lshift__
::: daft.expressions.expressions.Expression.__rshift__
::: daft.expressions.expressions.Expression.bitwise_and
::: daft.expressions.expressions.Expression.bitwise_or
::: daft.expressions.expressions.Expression.bitwise_xor
::: daft.expressions.expressions.Expression.shift_left
::: daft.expressions.expressions.Expression.shift_right

## Aggregation


<!-- BEGIN GENERATED TABLE -->
| Method | Description |
|--------|-------------|
| [`__reduce__`][daft.expressions.expressions.Expression.__reduce__] | Helper for pickle. |
| [`agg_concat`][daft.expressions.expressions.Expression.agg_concat] | Aggregates the values in the expression into a single string by concatenating them. |
| [`agg_list`][daft.expressions.expressions.Expression.agg_list] | Aggregates the values in the expression into a list. |
| [`agg_set`][daft.expressions.expressions.Expression.agg_set] | Aggregates the values in the expression into a set (ignoring nulls). |
| [`any_value`][daft.expressions.expressions.Expression.any_value] | Returns any value in the expression. |
| [`approx_count_distinct`][daft.expressions.expressions.Expression.approx_count_distinct] | Calculates the approximate number of non-`NULL` distinct values in the expression. |
| [`approx_percentiles`][daft.expressions.expressions.Expression.approx_percentiles] | Calculates the approximate percentile(s) for a column of numeric values. |
| [`bool_and`][daft.expressions.expressions.Expression.bool_and] | Calculates the boolean AND of all values in a list. |
| [`bool_or`][daft.expressions.expressions.Expression.bool_or] | Calculates the boolean OR of all values in a list. |
| [`count`][daft.expressions.expressions.Expression.count] | Counts the number of values in the expression. |
| [`count_distinct`][daft.expressions.expressions.Expression.count_distinct] |  |
| [`max`][daft.expressions.expressions.Expression.max] | Calculates the maximum value in the expression. |
| [`mean`][daft.expressions.expressions.Expression.mean] | Calculates the mean of the values in the expression. |
| [`min`][daft.expressions.expressions.Expression.min] | Calculates the minimum value in the expression. |
| [`stddev`][daft.expressions.expressions.Expression.stddev] | Calculates the standard deviation of the values in the expression. |
| [`sum`][daft.expressions.expressions.Expression.sum] | Calculates the sum of the values in the expression. |
<!-- END GENERATED TABLE -->

::: daft.expressions.expressions.Expression.__reduce__
::: daft.expressions.expressions.Expression.agg_concat
::: daft.expressions.expressions.Expression.agg_list
::: daft.expressions.expressions.Expression.agg_set
::: daft.expressions.expressions.Expression.any_value
::: daft.expressions.expressions.Expression.approx_count_distinct
::: daft.expressions.expressions.Expression.approx_percentiles
::: daft.expressions.expressions.Expression.bool_and
::: daft.expressions.expressions.Expression.bool_or
::: daft.expressions.expressions.Expression.count
::: daft.expressions.expressions.Expression.count_distinct
::: daft.expressions.expressions.Expression.max
::: daft.expressions.expressions.Expression.mean
::: daft.expressions.expressions.Expression.min
::: daft.expressions.expressions.Expression.stddev
::: daft.expressions.expressions.Expression.sum

## Transformation

::: daft.expressions.expressions.Expression.decode
::: daft.expressions.expressions.Expression.encode
::: daft.expressions.expressions.Expression.hash
::: daft.expressions.expressions.Expression.jq
::: daft.expressions.expressions.Expression.minhash
::: daft.expressions.expressions.Expression.url_parse

## String

::: daft.expressions.expressions.ExpressionStringNamespace
    options:
        filters: ["!^_"]
        toc_label: Expression.str
        heading: Expression.str

## Binary

::: daft.expressions.expressions.ExpressionBinaryNamespace
    options:
        filters: ["!^_"]
        toc_label: Expression.binary
        heading: Expression.binary

## Float

::: daft.expressions.expressions.ExpressionFloatNamespace
    options:
        filters: ["!^_"]
        toc_label: Expression.float
        heading: Expression.float

## Temporal

::: daft.expressions.expressions.ExpressionDatetimeNamespace
    options:
        filters: ["!^_"]
        toc_label: Expression.dt
        heading: Expression.dt

## List

::: daft.expressions.expressions.ExpressionListNamespace
    options:
        filters: ["!^_", "!lengths"]
        toc_label: Expression.list
        heading: Expression.list

## Struct

::: daft.expressions.expressions.ExpressionStructNamespace
    options:
        filters: ["!^_"]
        toc_label: Expression.struct
        heading: Expression.struct

## Map

::: daft.expressions.expressions.ExpressionMapNamespace
    options:
        filters: ["!^_"]
        toc_label: Expression.map
        heading: Expression.map

## Image

::: daft.expressions.expressions.ExpressionImageNamespace
    options:
        filters: ["!^_"]
        toc_label: Expression.image
        heading: Expression.image

## Partitioning

::: daft.expressions.expressions.ExpressionPartitioningNamespace
    options:
        filters: ["!^_"]
        toc_label: Expression.partition
        heading: Expression.partition

## URL

::: daft.expressions.expressions.ExpressionUrlNamespace
    options:
        filters: ["!^_"]
        toc_label: Expression.url
        heading: Expression.url

## JSON

::: daft.expressions.expressions.ExpressionJsonNamespace
    options:
        filters: ["!^_"]
        toc_label: Expression.json
        heading: Expression.json

## Embedding

::: daft.expressions.expressions.ExpressionEmbeddingNamespace
    options:
        filters: ["!^_"]
        toc_label: Expression.embedding
        heading: Expression.embedding

## Visitor

::: daft.expressions.visitor.ExpressionVisitor
    options:
        filters: ["!^_"]
