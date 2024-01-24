Logical
=======

Boolean Operators
*****************

Operations on logical expressions (True/False booleans)

.. autosummary::
    daft.expressions.Expression.__invert__
    daft.expressions.Expression.__and__
    daft.expressions.Expression.__or__
    daft.expressions.Expression.if_else

.. _api-comparison-expression:

Comparisons
***********

Comparing expressions and values, returning a logical expression

.. autosummary::
    daft.expressions.Expression.__lt__
    daft.expressions.Expression.__le__
    daft.expressions.Expression.__eq__
    daft.expressions.Expression.__ne__
    daft.expressions.Expression.__gt__
    daft.expressions.Expression.__ge__

Conditional Selection
*********************

.. automethod:: daft.expressions.Expression.if_else
