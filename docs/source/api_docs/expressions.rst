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
    Expression.__mod__
    Expression.ceil

.. _api-comparison-expression:

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

.. _api-membership-expression:

Membership
***********

Checking if an expression is a member of a list of values

.. autosummary::
    :toctree: doc_gen/expression_methods

    daft.expressions.Expression.is_in

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
   Expression.str.endswith
   Expression.str.startswith
   Expression.str.concat
   Expression.str.length
   Expression.str.split
   Expression.str.lower

.. _api-expressions-temporal:

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
