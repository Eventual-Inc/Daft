Column Types and Operations
===========================

In a Daft DataFrame's schema, every column has a type. Daft uses this to check the validity of certain expressions and operations.

For example, Daft will disallow calling ``df.where(col("foo"))`` if column ``"foo"`` was not a ``LOGICAL`` type column.

Primitive Types
---------------

Daft columns have the following primitive types:

1. STRING: a sequence of text
2. INTEGER: a number without decimal point
3. FLOAT: a number with decimal point
4. LOGICAL: a boolean true/false
5. BYTES: a sequence of bytes
6. DATE: a day/month/year non-timezone aware date

Python Types
------------

Additionally, Daft supports columns of a Python type which the "catch-all" for all types that are not primitive.

PY types are parametrized. That is to say, a PY type can have a sub-type, for example a column comprising Python dictionaries would have type ``PY[dict]`` and a column comprising of images from the PIL library would have type ``PY[PIL.Image.Image]``.

By default if DaFt does not know anything about your object it will designate it as the most generic type possible, ``PY[object]``, since ``object`` is the top level parent of all Python types.
