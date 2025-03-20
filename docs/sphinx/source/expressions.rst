Expressions
===========

Daft Expressions allow you to express some computation that needs to happen in a DataFrame.

This page provides an overview of all the functionality that is provided by Daft Expressions.

.. currentmodule:: daft

Constructors
############

.. autosummary::
   :nosignatures:
   :toctree: doc_gen/expression_methods

    col
    lit
    list_
    struct
    interval

Generic
#######

.. autosummary::
   :nosignatures:
   :toctree: doc_gen/expression_methods

   Expression.alias
   Expression.cast
   Expression.if_else
   Expression.is_null
   Expression.not_null
   Expression.fill_null
   Expression.hash
   Expression.apply
   Expression.__getitem__

.. _api-numeric-expression-operations:

Numeric
#######

.. autosummary::
   :nosignatures:
   :toctree: doc_gen/expression_methods

    Expression.__abs__
    Expression.__add__
    Expression.__sub__
    Expression.__mul__
    Expression.__truediv__
    Expression.__floordiv__
    Expression.__mod__
    Expression.__lshift__
    Expression.__rshift__
    Expression.abs
    Expression.ceil
    Expression.floor
    Expression.sign
    Expression.signum
    Expression.negate
    Expression.negative
    Expression.round
    Expression.clip
    Expression.sqrt
    Expression.cbrt
    Expression.sin
    Expression.cos
    Expression.tan
    Expression.csc
    Expression.sec
    Expression.cot
    Expression.sinh
    Expression.cosh
    Expression.tanh
    Expression.arcsin
    Expression.arccos
    Expression.arctan
    Expression.arctan2
    Expression.arctanh
    Expression.arccosh
    Expression.arcsinh
    Expression.radians
    Expression.degrees
    Expression.log2
    Expression.log10
    Expression.log
    Expression.ln
    Expression.log1p
    Expression.exp
    Expression.expm1
    Expression.shift_left
    Expression.shift_right
    Expression.bitwise_and
    Expression.bitwise_or
    Expression.bitwise_xor

.. _api-comparison-expression:

Logical
#######

.. autosummary::
   :nosignatures:
   :toctree: doc_gen/expression_methods

    Expression.__invert__
    Expression.__and__
    Expression.__or__
    Expression.__xor__
    Expression.__lt__
    Expression.__le__
    Expression.__eq__
    Expression.eq_null_safe
    Expression.__ne__
    Expression.__gt__
    Expression.__ge__
    Expression.between
    Expression.is_in
    Expression.minhash

.. _api=aggregation-expression:

Aggregation
###########

The following can be used with DataFrame.agg or GroupedDataFrame.agg

.. autosummary::
   :toctree: doc_gen/expression_methods

   Expression.bool_and
   Expression.bool_or
   Expression.count
   Expression.count_distinct
   Expression.sum
   Expression.mean
   Expression.stddev
   Expression.min
   Expression.max
   Expression.any_value
   Expression.agg_list
   Expression.agg_set
   Expression.agg_concat
   Expression.approx_percentiles
   Expression.approx_count_distinct

.. _expression-accessor-properties:
.. _api-string-expression-operations:

Strings
#######

The following methods are available under the ``expr.str`` attribute.

.. autosummary::
   :nosignatures:
   :toctree: doc_gen/expression_methods
   :template: autosummary/accessor_method.rst

   Expression.str.contains
   Expression.str.match
   Expression.str.startswith
   Expression.str.endswith
   Expression.str.concat
   Expression.str.split
   Expression.str.extract
   Expression.str.extract_all
   Expression.str.replace
   Expression.str.length
   Expression.str.length_bytes
   Expression.str.lower
   Expression.str.upper
   Expression.str.lstrip
   Expression.str.rstrip
   Expression.str.reverse
   Expression.str.capitalize
   Expression.str.left
   Expression.str.right
   Expression.str.find
   Expression.str.rpad
   Expression.str.lpad
   Expression.str.repeat
   Expression.str.like
   Expression.str.ilike
   Expression.str.substr
   Expression.str.to_date
   Expression.str.to_datetime
   Expression.str.normalize
   Expression.str.tokenize_encode
   Expression.str.tokenize_decode
   Expression.str.count_matches

.. _api-binary-expression-operations:

Binary
######

The following methods are available under the ``expr.binary`` attribute.

.. autosummary::
   :nosignatures:
   :toctree: doc_gen/expression_methods
   :template: autosummary/accessor_method.rst

   Expression.binary.concat
   Expression.binary.length
   Expression.binary.slice
   Expression.encode
   Expression.decode


.. _api-float-expression-operations:

Floats
#######

The following methods are available under the ``expr.float`` attribute.

.. autosummary::
   :nosignatures:
   :toctree: doc_gen/expression_methods
   :template: autosummary/accessor_method.rst

   Expression.float.is_inf
   Expression.float.is_nan
   Expression.float.not_nan
   Expression.float.fill_nan

.. _api-expressions-temporal:

Temporal
########

.. autosummary::
   :nosignatures:
   :toctree: doc_gen/expression_methods
   :template: autosummary/accessor_method.rst

   Expression.dt.date
   Expression.dt.hour
   Expression.dt.minute
   Expression.dt.second
   Expression.dt.time
   Expression.dt.day
   Expression.dt.month
   Expression.dt.year
   Expression.dt.day_of_week
   Expression.dt.truncate

List
####

.. autosummary::
   :nosignatures:
   :toctree: doc_gen/expression_methods
   :template: autosummary/accessor_method.rst

   Expression.list.bool_and
   Expression.list.bool_or
   Expression.list.chunk
   Expression.list.count
   Expression.list.get
   Expression.list.join
   Expression.list.length
   Expression.list.max
   Expression.list.mean
   Expression.list.min
   Expression.list.slice
   Expression.list.sort
   Expression.list.sum
   Expression.list.distinct
   Expression.list.value_counts

Struct
######

.. autosummary::
   :nosignatures:
   :toctree: doc_gen/expression_methods
   :template: autosummary/accessor_method.rst

   Expression.struct.get

Map
######

.. autosummary::
   :nosignatures:
   :toctree: doc_gen/expression_methods
   :template: autosummary/accessor_method.rst

   Expression.map.get

.. _api-expressions-images:

Image
#####

.. autosummary::
   :nosignatures:
   :toctree: doc_gen/expression_methods
   :template: autosummary/accessor_method.rst

   Expression.image.decode
   Expression.image.encode
   Expression.image.resize
   Expression.image.crop
   Expression.image.to_mode

Partitioning
############

.. autosummary::
   :nosignatures:
   :toctree: doc_gen/expression_methods
   :template: autosummary/accessor_method.rst

   Expression.partitioning.days
   Expression.partitioning.hours
   Expression.partitioning.months
   Expression.partitioning.years
   Expression.partitioning.iceberg_bucket
   Expression.partitioning.iceberg_truncate

URLs
####

.. autosummary::
   :nosignatures:
   :toctree: doc_gen/expression_methods
   :template: autosummary/accessor_method.rst

   Expression.url.download
   Expression.url.upload

JSON
####

.. autosummary::
   :nosignatures:
   :toctree: doc_gen/expression_methods
   :template: autosummary/accessor_method.rst

   Expression.json.query


Embedding
#########

.. autosummary::
   :nosignatures:
   :toctree: doc_gen/expression_methods
   :template: autosummary/accessor_method.rst

   Expression.embedding.cosine_distance
