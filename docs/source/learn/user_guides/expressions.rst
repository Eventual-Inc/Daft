Expressions
===========

Expressions are how you define data computation in Daft. They are used in Dataframe methods to describe the computation that will be run by Daft on each column.

Introduction
------------

To refer to a column by its name, we use the ``col`` helper:

..

    ``col(name: str) -> ColumnExpression``

    Returns an expression which refers to a column with the provided name

.. code:: python

    from daft import col

    # Refers to a column named "foo"
    col("foo")

We can define computations on expressions by using operations. Operations create new Expressions from existing ones. For example, this is how to use the addition operation:

.. code:: python

    # bar is an expression that refers to a new column which is the
    # data in "foo" incremented by 1
    bar = col("foo") + 1

Operations
----------

Operations allow you to create new expressions from existing ones.

.. NOTE::

    Some operations can only work on certain data types (see: :doc:`types_and_ops`).


Unary Operations
^^^^^^^^^^^^^^^^

========= =================================================================================== ========================
Operation Description                                                                         Example
========= =================================================================================== ========================
Negate    Negates a numeric expression                                                        ``-col("foo")``
Positive  Makes a numeric expression positive                                                 ``+col("foo")``
Absolute  Gets the absolute value of a numeric expression                                     ``abs(col("foo"))``
is_null   Checks if each value is Null (a special value indicating that data is missing)      ``col("foo").is_null()``
is_nan    Checks if each value is NaN (a special float value indicating that data is invalid) ``col("foo").is_nan()``
========= =================================================================================== ========================

Binary Operations
^^^^^^^^^^^^^^^^^

=============== ================================================================================== ========================
Operation       Description                                                                        Example
=============== ================================================================================== ========================
Add             Adds two numeric expressions                                                       ``col("x") + col("y")``
Subtract        Subtracts two numeric expressions                                                  ``col("x") - col("y")``
Multiply        Multiplies two numeric expressions                                                 ``col("x") * col("y")``
Floor Divide    Divides two numeric expressions, taking the floor of the result                    ``col("x") // col("y")``
True Divide     Divides two numeric expressions                                                    ``col("x") / col("y")``
Power           Takes the power of two numeric expressions                                         ``col("x") ** col("y")``
Mod             Mods two numeric expressions                                                       ``col("x") % col("y")``
Less Than       Checks if values in an expression are less than another expression                 ``col("x") < col("y")``
Less Than Eq    Checks if values in an expression are less than or equal to another expression     ``col("x") <= col("y")``
Equal           Checks if values in an expression are equal to another expression                  ``col("x") = col("y")``
Not Equal       Checks if values in an expression are not equal to another expression              ``col("x") != col("y")``
Greater Than    Checks if values in an expression are greater than another expression              ``col("x") > col("y")``
Greater than Eq Checks if values in an expression are greater than or equal to another expression  ``col("x") >= col("y")``
=============== ================================================================================== ========================


Ternary Operations
^^^^^^^^^^^^^^^^^^

=============== ======================================================================================= ================================================
Operation       Description                                                                             Example
=============== ======================================================================================= ================================================
If Else         If the value of the expression is True, use values from x, otherwise use values from y. ``col("condition").if_else(col("x"), col("y"))``
=============== ======================================================================================= ================================================

Operation Accessors
-------------------

Some operations are housed in accessors, which help to group families of operations that work only on certain data types.

String Operation Accessor
^^^^^^^^^^^^^^^^^^^^^^^^^

Operations on strings are accessed using the ``.str`` property on an Expression.

========== ================================================================================ ====================================
Operation  Description                                                                      Example
========== ================================================================================ ====================================
contains   Check if each string value in the expression contains the provided substring     ``col("foo").str.contains("bar")``
endswith   Check if each string value in the expression ends with the provided substring    ``col("foo").str.endswith("bar")``
startswith Check if each string value in the expression starts with the provided substring  ``col("foo").str.startswith("bar")``
length     Retrieves the length of each string value in the expression                      ``col("foo").str.length()``
========== ================================================================================ ====================================

URL Operation Accessor
^^^^^^^^^^^^^^^^^^^^^^

Operations on URL strings are accessed using the ``.url`` property on an Expression.

========== ================================================================================ ====================================
Operation  Description                                                                      Example
========== ================================================================================ ====================================
download   Downloads data as bytes from each string URL value in the expression             ``col("foo").url.download()``
========== ================================================================================ ====================================

Datetime Operation Accessor
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Operations on datetimes are accessed using the ``.dt`` property on an Expression.

=========== ================================================================================ ====================================
Operation   Description                                                                      Example
=========== ================================================================================ ====================================
day         Check if each string value in the expression contains the provided substring     ``col("foo").dt.day()``
month       Check if each string value in the expression ends with the provided substring    ``col("foo").dt.month()``
year        Check if each string value in the expression starts with the provided substring  ``col("foo").dt.year()``
day of week Retrieves the length of each string value in the expression                      ``col("foo").dt.day_of_week()``
=========== ================================================================================ ====================================


User-Defined Functions
----------------------

Operations built into Daft are fast because they are vectorized and can run extremely efficiently. It is recommended that data is decomposed into Daft primitives and that operations are used where possible, but oftentimes no combination of operations can express your computation (for example, running geometric algorithms on image data).

In that case, you can use a User-Defined Function which allows you to run any arbitrary Python code on your data (see: :doc:`udf`).
