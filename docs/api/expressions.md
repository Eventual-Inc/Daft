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

    String-specific operations through the `.str` namespace for text manipulation.

* [**Binary**](#binary)

    Binary data operations through the `.binary` namespace for raw byte processing.

* [**Float**](#float)

    Floating-point operations through the `.float` namespace for precise numeric computations.

* [**Temporal**](#temporal)

    Date, time, and timestamp operations through the `.dt` namespace.

* [**List**](#list)

    Array/list operations through the `.list` namespace for sequence manipulation.

* [**Struct**](#struct)

    Structured data with named fields through the `.struct` namespace.

* [**Map**](#map)

    Key-value pair operations through the `.map` namespace for dictionary-like data.

* [**Image**](#image)

    Image processing operations through the `.image` namespace for computer vision.

* [**Partitioning**](#partitioning)

    Data distribution control through the `.partition` namespace for distributed processing.

* [**URL**](#url)

    URL parsing and component extraction through the `.url` namespace.

* [**JSON**](#json)

    JSON parsing, querying, and manipulation through the `.json` namespace.

* [**Embedding**](#embedding)

    Vector embeddings and similarity operations through the `.embedding` namespace.

* [**Visitor**](#visitor)

    Traversal and inspection utilities for examining expression trees and AST structures.

</div>

## Constructors

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
