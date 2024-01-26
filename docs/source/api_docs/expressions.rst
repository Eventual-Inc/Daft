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

    DataFrame.__getitem__

.. autosummary::
   :nosignatures:
   :toctree: doc_gen/expression_methods

    col

.. autosummary::
   :nosignatures:
   :toctree: doc_gen/expression_methods

    lit

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
   Expression.apply

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
    Expression.__mod__

Logical
#######

.. autosummary::
   :nosignatures:
   :toctree: doc_gen/expression_methods

    Expression.__invert__
    Expression.__and__
    Expression.__or__
    Expression.__lt__
    Expression.__le__
    Expression.__eq__
    Expression.__ne__
    Expression.__gt__
    Expression.__ge__

Strings
#######

The following methods are available under the ``expr.str`` attribute.

.. autosummary::
   :nosignatures:
   :toctree: doc_gen/expression_methods
   :template: autosummary/accessor_method.rst

   Expression.str.contains
   Expression.str.endswith
   Expression.str.startswith
   Expression.str.concat
   Expression.str.length
   Expression.str.split

Temporal
########

.. autosummary::
   :nosignatures:
   :toctree: doc_gen/expression_methods
   :template: autosummary/accessor_method.rst

   Expression.dt.date
   Expression.dt.hour
   Expression.dt.day
   Expression.dt.month
   Expression.dt.year
   Expression.dt.day_of_week

List
####

.. autosummary::
   :nosignatures:
   :toctree: doc_gen/expression_methods
   :template: autosummary/accessor_method.rst

   Expression.list.join
   Expression.list.lengths
   Expression.list.get

Struct
######

.. autosummary::
   :nosignatures:
   :toctree: doc_gen/expression_methods
   :template: autosummary/accessor_method.rst

   Expression.struct.get

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
