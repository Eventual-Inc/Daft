# Expressions

Daft Expressions allow you to express some computation that needs to happen in a DataFrame. This page provides an overview of all the functionality that is provided by Daft Expressions. Learn more about [Expressions](../core_concepts.md#expressions) in Daft User Guide.

## Constructors

::: daft.expressions.col
    options:
        heading_level: 3

::: daft.expressions.lit
    options:
        heading_level: 3

::: daft.expressions.list_
    options:
        heading_level: 3

::: daft.expressions.struct
    options:
        heading_level: 3

## SQL

::: daft.sql.sql.sql_expr
    options:
        heading_level: 3

<!--
## Generic
## Numeric
## Logical
## Aggregation
-->

::: daft.expressions.Expression
    options:
        filters: ["!^_", "!str", "!binary", "!float", "!dt", "!list", "!struct", "!map", "!image", "!partitioning", "!url", "!json", "!embedding", "!encode", "!decode"]

<!-- add more pages to filters to include them, see dataframe for example -->

<!-- ::: daft.expressions.expressions.ExpressionNamespace
    options:
        filters: ["!^_"]
        summary: false -->

## Strings

The following methods are available under the `expr.str` attribute.

<!-- todo(docs - cc): ideally we have the class? and the proper method format should be Expression.str.[method] -->

::: daft.expressions.expressions.ExpressionStringNamespace
    options:
        filters: ["!^_"]

## Binary

The following methods are available under the `expr.binary` attribute.

<!-- todo(docs - cc): ideally we have the class? and the proper method format should be Expression.binary.[method] -->

::: daft.expressions.expressions.ExpressionBinaryNamespace
    options:
        filters: ["!^_"]

<!-- ::: daft.expressions.expressions.ExpressionBinaryNamespace.concat
    options:
        heading: Expression.struct.binary.concat
        heading_level: 3

::: daft.expressions.expressions.ExpressionBinaryNamespace.length
    options:
        heading: Expression.struct.binary.length
        heading_level: 3

::: daft.expressions.expressions.ExpressionBinaryNamespace.slice
    options:
        heading: Expression.struct.binary.slice
        heading_level: 3

::: daft.expressions.expressions.Expression.encode
    options:
        heading_level: 3

::: daft.expressions.expressions.Expression.decode
    options:
        heading_level: 3 -->

## Floats

The following methods are available under the `expr.float` attribute.

<!-- todo(docs - cc): ideally we have the class? and the proper method format should be Expression.float.[method] -->

::: daft.expressions.expressions.ExpressionFloatNamespace
    options:
        filters: ["!^_"]

<!-- ::: daft.expressions.expressions.ExpressionFloatNamespace.is_inf
    options:
        heading: Expression.struct.float.is_inf
        heading_level: 3

::: daft.expressions.expressions.ExpressionFloatNamespace.is_nan
    options:
        heading: Expression.struct.float.is_nan
        heading_level: 3

::: daft.expressions.expressions.ExpressionFloatNamespace.not_nan
    options:
        heading: Expression.struct.not_nan
        heading_level: 3

::: daft.expressions.expressions.ExpressionFloatNamespace.fill_nan
    options:
        heading: Expression.struct.float.fill_nan
        heading_level: 3 -->

## Temporal

The following methods are available under the `expr.dt` attribute.

<!-- todo(docs - cc): ideally we have the class? and the proper method format should be Expression.dt.[method] -->

::: daft.expressions.expressions.ExpressionDatetimeNamespace
    options:
        filters: ["!^_"]

## List

The following methods are available under the `expr.list` attribute.

<!-- todo(docs - cc): ideally we have the class? and the proper method format should be Expression.list.[method] -->

::: daft.expressions.expressions.ExpressionListNamespace
    options:
        filters: ["!^_"]

## Struct

The following methods are available under the `expr.struct` attribute.

<!-- todo(docs - cc): ideally we have the class? and the proper method format should be Expression.struct.[method] -->

::: daft.expressions.expressions.ExpressionStructNamespace
    options:
        filters: ["!^_"]

<!-- ::: daft.expressions.expressions.ExpressionStructNamespace.get
    options:
        heading: Expression.struct.get
        heading_level: 3 -->

## Map

The following methods are available under the `expr.map` attribute.

<!-- todo(docs - cc): ideally we have the class? and the proper method format should be Expression.map.[method] -->

::: daft.expressions.expressions.ExpressionMapNamespace
    options:
        filters: ["!^_"]

<!-- ::: daft.expressions.expressions.ExpressionMapNamespace.get
    options:
        heading: Expression.map.get
        heading_level: 3 -->

## Image

The following methods are available under the `expr.image` attribute.

<!-- todo(docs - cc): ideally we have the class? and the proper method format should be Expression.image.[method] -->

::: daft.expressions.expressions.ExpressionImageNamespace
    options:
        filters: ["!^_"]

<!-- ::: daft.expressions.expressions.ExpressionImageNamespace.decode
    options:
        heading: Expression.image.decode
        heading_level: 3

::: daft.expressions.expressions.ExpressionImageNamespace.encode
    options:
        heading: Expression.image.encode
        heading_level: 3

::: daft.expressions.expressions.ExpressionImageNamespace.resize
    options:
        heading: Expression.image.resize
        heading_level: 3

::: daft.expressions.expressions.ExpressionImageNamespace.crop
    options:
        heading: Expression.image.crop
        heading_level: 3

::: daft.expressions.expressions.ExpressionImageNamespace.to_mode
    options:
        heading: Expression.image.to_mode
        heading_level: 3 -->

## Partitioning

The following methods are available under the `expr.partition` attribute.

<!-- todo(docs - cc): ideally we have the class? and the proper method format should be Expression.partition.[method] -->

::: daft.expressions.expressions.ExpressionPartitioningNamespace
    options:
        filters: ["!^_"]

<!-- ::: daft.expressions.expressions.ExpressionPartitioningNamespace.days
    options:
        heading: Expression.partition.days
        heading_level: 3

::: daft.expressions.expressions.ExpressionPartitioningNamespace.hours
    options:
        heading: Expression.partition.hours
        heading_level: 3

::: daft.expressions.expressions.ExpressionPartitioningNamespace.months
    options:
        heading: Expression.partition.months
        heading_level: 3

::: daft.expressions.expressions.ExpressionPartitioningNamespace.years
    options:
        heading: Expression.partition.years
        heading_level: 3

::: daft.expressions.expressions.ExpressionPartitioningNamespace.iceberg_bucket
    options:
        heading: Expression.partition.iceberg_truncate
        heading_level: 3 -->

## URLs

The following methods are available under the `expr.url` attribute.

<!-- todo(docs - cc): ideally we have the class? and the proper method format should be Expression.url.[method]] -->

::: daft.expressions.expressions.ExpressionUrlNamespace
    options:
        filters: ["!^_"]
        heading: "Expression.url"
        toc_label: "Expression.url"

<!-- ::: daft.expressions.expressions.ExpressionUrlNamespace.download
    options:
        heading: Expression.url.download
        heading_level: 3

::: daft.expressions.expressions.ExpressionUrlNamespace.upload
    options:
        heading: Expression.url.upload
        heading_level: 3 -->

## JSON

The following methods are available under the `expr.json` attribute.

<!-- todo(docs - cc): ideally we have the class? and the proper method format should be Expression.json.query -->

<!-- ::: daft.expressions.expressions.ExpressionJsonNamespace
    options:
        filters: ["!^_"] -->

::: daft.expressions.expressions.ExpressionJsonNamespace.query
    options:
        toc_label: fancy
        heading: module

## Embedding

The following methods are available under the `expr.embedding` attribute.

<!-- todo(docs - cc): ideally we have the class? and the proper method format should be Expression.embedding.cosine_distance -->

::: daft.expressions.expressions.ExpressionEmbeddingNamespace
    options:
        filters: ["!^_"]

<!-- ::: daft.expressions.expressions.ExpressionEmbeddingNamespace.cosine_distance
    options:
        heading: Expression.embedding.cosine_distance
        heading_level: 3 -->

<!-- todo(docs - cc): need help with flattening namespaces, the following is not on api docs
::: daft.expressions.expressions.ExpressionNamespace
::: daft.expressions.expressions.ExpressionUrlNamespace
::: daft.expressions.expressions.ExpressionFloatNamespace
::: daft.expressions.expressions.ExpressionDatetimeNamespace
::: daft.expressions.expressions.ExpressionStringNamespace
::: daft.expressions.expressions.ExpressionListNamespace
::: daft.expressions.expressions.ExpressionStructNamespace
::: daft.expressions.expressions.ExpressionMapNamespace
::: daft.expressions.expressions.ExpressionsProjection
::: daft.expressions.expressions.ExpressionImageNamespace
::: daft.expressions.expressions.ExpressionPartitioningNamespace
::: daft.expressions.expressions.ExpressionJsonNamespace
::: daft.expressions.expressions.ExpressionEmbeddingNamespace
::: daft.expressions.expressions.ExpressionBinaryNamespace
-->
