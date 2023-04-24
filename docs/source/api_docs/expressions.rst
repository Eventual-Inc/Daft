Expressions
===========

.. currentmodule:: daft

.. autosummary::
    :nosignatures:
    :toctree: expression_methods

    daft.expressions.Expression

Expression Constructors
#######################

.. autosummary::
    :nosignatures:
    :toctree: expression_methods

    daft.DataFrame.__getitem__
    daft.expressions.col
    daft.expressions.lit

Operators
#########

.. _api-numeric-expression-operations:

Numeric
*******

Operations on numbers (floats and integers)

.. autosummary::
    :toctree: expression_methods

    daft.expressions.Expression.__abs__
    daft.expressions.Expression.__add__
    daft.expressions.Expression.__sub__
    daft.expressions.Expression.__mul__
    daft.expressions.Expression.__truediv__
    daft.expressions.Expression.__mod__

Logical
*******

Operations on logical expressions (True/False booleans)

.. autosummary::
    :toctree: expression_methods

    daft.expressions.Expression.__invert__
    daft.expressions.Expression.__and__
    daft.expressions.Expression.__or__
    daft.expressions.Expression.if_else

.. _api-comparison-expression:

Comparisons
***********

Comparing expressions and values, returning a logical expression

.. autosummary::
    :toctree: expression_methods

    daft.expressions.Expression.__lt__
    daft.expressions.Expression.__le__
    daft.expressions.Expression.__eq__
    daft.expressions.Expression.__ne__
    daft.expressions.Expression.__gt__
    daft.expressions.Expression.__ge__
    daft.expressions.Expression.is_null

.. _expression-accessor-properties:

.. _api-string-expression-operations:

Floats
******

Operations on strings, accessible through the ``Expression.float`` method accessor.

Example: ``e1.float.is_nan()``

.. autosummary::
    :toctree: expression_methods

    daft.expressions.expressions.ExpressionFloatNamespace.is_nan

Strings
*******

Operations on strings, accessible through the ``Expression.str`` method accessor.

Example: ``e1.str.concat(e2)``

.. autosummary::
    :toctree: expression_methods

    daft.expressions.expressions.ExpressionStringNamespace.concat
    daft.expressions.expressions.ExpressionStringNamespace.contains
    daft.expressions.expressions.ExpressionStringNamespace.endswith
    daft.expressions.expressions.ExpressionStringNamespace.startswith
    daft.expressions.expressions.ExpressionStringNamespace.length


Dates
*****

Operations on datetimes, accessible through the ``Expression.dt`` method accessor:

Example: ``e.dt.day()``

.. autosummary::
    :nosignatures:
    :toctree: expression_methods

    daft.expressions.expressions.ExpressionDatetimeNamespace.day
    daft.expressions.expressions.ExpressionDatetimeNamespace.month
    daft.expressions.expressions.ExpressionDatetimeNamespace.year
    daft.expressions.expressions.ExpressionDatetimeNamespace.day_of_week


URLs
****

Operations on URLs, accessible through the ``Expression.url`` method accessor:

Example: ``e.url.download()``

.. autosummary::
    :nosignatures:
    :toctree: expression_methods

    daft.expressions.expressions.ExpressionUrlNamespace.download


Changing Column Names/Types
###########################

.. autosummary::
    :nosignatures:
    :toctree: expression_methods

    daft.expressions.Expression.alias
    daft.expressions.Expression.cast

Running Python Functions
########################

.. autosummary::
    :nosignatures:
    :toctree: expression_methods

    daft.expressions.Expression.apply
