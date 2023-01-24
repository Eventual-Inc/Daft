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

    daft.expressions.Expression.__neg__
    daft.expressions.Expression.__pos__
    daft.expressions.Expression.__abs__
    daft.expressions.Expression.__add__
    daft.expressions.Expression.__sub__
    daft.expressions.Expression.__mul__
    daft.expressions.Expression.__floordiv__
    daft.expressions.Expression.__truediv__
    daft.expressions.Expression.__pow__
    daft.expressions.Expression.__mod__
    daft.expressions.Expression.is_nan

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

Strings
*******

Operations on strings, accessible through the ``Expression.str`` method accessor.

Example: ``e1.str.concat(e2)``

.. autosummary::
    :toctree: expression_methods

    daft.expressions.StringMethodAccessor.concat
    daft.expressions.StringMethodAccessor.contains
    daft.expressions.StringMethodAccessor.endswith
    daft.expressions.StringMethodAccessor.startswith
    daft.expressions.StringMethodAccessor.length


Dates
*****

Operations on datetimes, accessible through the ``Expression.dt`` method accessor:

Example: ``e.dt.day()``

.. autosummary::
    :nosignatures:
    :toctree: expression_methods

    daft.expressions.DatetimeMethodAccessor.day
    daft.expressions.DatetimeMethodAccessor.month
    daft.expressions.DatetimeMethodAccessor.year
    daft.expressions.DatetimeMethodAccessor.day_of_week


URLs
****

Operations on URLs, accessible through the ``Expression.url`` method accessor:

Example: ``e.url.download()``

.. autosummary::
    :nosignatures:
    :toctree: expression_methods

    daft.expressions.UrlMethodAccessor.download


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

    daft.expressions.Expression.as_py
    daft.expressions.Expression.apply
