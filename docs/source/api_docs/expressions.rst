Expressions
===========

.. currentmodule:: daft

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/expression_methods

    daft.expressions.Expression

Expression Constructors
#######################

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/expression_methods

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
    :toctree: doc_gen/expression_methods

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
    :toctree: doc_gen/expression_methods

    daft.expressions.Expression.__invert__
    daft.expressions.Expression.__and__
    daft.expressions.Expression.__or__
    daft.expressions.Expression.if_else

.. _api-comparison-expression:

Comparisons
***********

Comparing expressions and values, returning a logical expression

.. autosummary::
    :toctree: doc_gen/expression_methods

    daft.expressions.Expression.__lt__
    daft.expressions.Expression.__le__
    daft.expressions.Expression.__eq__
    daft.expressions.Expression.__ne__
    daft.expressions.Expression.__gt__
    daft.expressions.Expression.__ge__
    daft.expressions.Expression.is_null

.. _expression-accessor-properties:

.. _api-float-expression-operations:

Floats
******

Operations on strings, accessible through the :meth:`Expression.float <daft.expressions.Expression.float>` method accessor.

Example: ``e1.float.is_nan()``

.. autosummary::
    :toctree: doc_gen/expression_methods

    daft.expressions.Expression.float
    daft.expressions.expressions.ExpressionFloatNamespace.is_nan

.. _api-string-expression-operations:

Strings
*******

Operations on strings, accessible through the :meth:`Expression.str <daft.expressions.Expression.str>` method accessor.

Example: ``e1.str.concat(e2)``

.. autosummary::
    :toctree: doc_gen/expression_methods

    daft.expressions.Expression.str
    daft.expressions.expressions.ExpressionStringNamespace.concat
    daft.expressions.expressions.ExpressionStringNamespace.contains
    daft.expressions.expressions.ExpressionStringNamespace.endswith
    daft.expressions.expressions.ExpressionStringNamespace.startswith
    daft.expressions.expressions.ExpressionStringNamespace.length

.. _api-expressions-temporal:

Dates
*****

Operations on datetimes, accessible through the :meth:`Expression.dt <daft.expressions.Expression.dt>` method accessor:

Example: ``e.dt.day()``

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/expression_methods

    daft.expressions.Expression.dt
    daft.expressions.expressions.ExpressionDatetimeNamespace.day
    daft.expressions.expressions.ExpressionDatetimeNamespace.month
    daft.expressions.expressions.ExpressionDatetimeNamespace.year
    daft.expressions.expressions.ExpressionDatetimeNamespace.day_of_week
    daft.expressions.expressions.ExpressionDatetimeNamespace.date

.. _api-expressions-urls:

URLs
****

Operations on URLs, accessible through the :meth:`Expression.url <daft.expressions.Expression.url>` method accessor:

Example: ``e.url.download()``

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/expression_methods

    daft.expressions.Expression.url
    daft.expressions.expressions.ExpressionUrlNamespace.download

.. _api-expressions-images:

Images
******

Operations on images, accessible through the :meth:`Expression.image <daft.expressions.Expression.image>` method accessor:

Example: ``e.image.resize()``

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/expression_methods

    daft.expressions.Expression.image
    daft.expressions.expressions.ExpressionImageNamespace.resize
    daft.expressions.expressions.ExpressionImageNamespace.decode
    daft.expressions.expressions.ExpressionImageNamespace.encode


Nested
******

Operations on nested types (such as List and FixedSizeList), accessible through the ``Expression.list`` method accessor.

Example: ``e1.list.join(e2)``

.. autosummary::
    :toctree: doc_gen/expression_methods

    daft.expressions.expressions.ExpressionListNamespace.join
    daft.expressions.expressions.ExpressionListNamespace.lengths


Changing Column Names/Types
###########################

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/expression_methods

    daft.expressions.Expression.alias
    daft.expressions.Expression.cast

Running Python Functions
########################

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/expression_methods

    daft.expressions.Expression.apply
