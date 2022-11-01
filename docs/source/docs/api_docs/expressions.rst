Expressions
===========

.. autoclass:: daft.expressions.Expression

Expression Constructors
#######################

.. autofunction:: daft.expressions.col
.. autofunction:: daft.expressions.lit

Operators
#########

Operators allow for creating new expressions from existing ones. Not all operators can be run on all types. For a list of expression types and their operations, see: https://docs.getdaft.io/daft/user-guides/daft-types-and-operations

Unary
*****

.. py:module:: daft.expressions

.. py:method:: Expression.__neg__()

    Takes the negated value of a numeric expression.

    >>> -col("x")

.. py:method:: Expression.__pos__()

    Takes the positive value of a numeric expression.

    >>> +col("x")

.. py:method:: Expression.__abs__()

    Takes the absolute value of a numeric expression.

    >>> abs(col("x"))

.. py:method:: Expression.__invert__()

    Inverts a LOGICAL expression.

    >>> ~col("x")

Binary
******

.. py:method:: Expression.__add__(other: Expression)

    Adds two numeric expressions.

    >>> col("x") + col("y")

.. py:method:: Expression.__sub__(other: Expression)

    Subtracts two numeric expressions.

    >>> col("x") - col("y")

.. py:method:: Expression.__mul__(other: Expression)

    Subtracts two numeric expressions.

    >>> col("x") * col("y")

.. py:method:: Expression.__floordiv__(other: Expression)

    Takes the floor division of two numeric expressions.

    >>> col("x") // col("y")

.. py:method:: Expression.__truediv__(other: Expression)

    Takes the true division of two numeric expressions.

    >>> col("x") / col("y")

.. py:method:: Expression.__pow__(other: Expression)

    Takes the power of two numeric expressions.

    >>> col("x") ** col("y")

.. py:method:: Expression.__mod__(other: Expression)

    Takes the mod of two numeric expressions.

    >>> col("x") % col("y")

.. py:method:: Expression.__and__(other: Expression)

    Takes the conjunction of two LOGICAL expressions.

    >>> col("x") & col("y")

.. py:method:: Expression.__or__(other: Expression)

    Takes the disjunction of two LOGICAL expressions.

    >>> col("x") | col("y")

.. py:method:: Expression.__lt__(other: Expression)

    Compares two Expressions to check if the first is less than the second.

    >>> col("x") < col("y")

.. py:method:: Expression.__le__(other: Expression)

    Compares two Expressions to check if the first is less or equal to the second.

    >>> col("x") <= col("y")

.. py:method:: Expression.__eq__(other: Expression)

    Compares two Expressions to check if they are equal.

    >>> col("x") == col("y")

.. py:method:: Expression.__ne__(other: Expression)

    Compares two Expressions to check if they are not equal.

    >>> col("x") != col("y")

.. py:method:: Expression.__gt__(other: Expression)

    Compares two Expressions to check if the first is greater than the second.

    >>> col("x") > col("y")

.. py:method:: Expression.__ge__(other: Expression)

    Compares two Expressions to check if the first is greater than or equal to the second.

    >>> col("x") >= col("y")

.. automethod:: daft.expressions.Expression.is_null
.. automethod:: daft.expressions.Expression.is_nan

Ternary
*******

.. automethod:: daft.expressions.Expression.if_else

Changing Column Names/Types
###########################
.. automethod:: daft.expressions.Expression.alias
.. automethod:: daft.expressions.Expression.cast

Running Python Functions
########################
.. automethod:: daft.expressions.Expression.as_py
.. automethod:: daft.expressions.Expression.apply

Accessors
#########

.. _expression-accessor-properties:

Expression Accessor Properties
******************************
.. autoproperty:: daft.expressions.Expression.str
.. autoproperty:: daft.expressions.Expression.url
.. autoproperty:: daft.expressions.Expression.dt

Accessor APIs
*************
.. autoclass:: daft.expressions.StringMethodAccessor
    :members:
.. autoclass:: daft.expressions.UrlMethodAccessor
    :members:
.. autoclass:: daft.expressions.DatetimeMethodAccessor
    :members:
