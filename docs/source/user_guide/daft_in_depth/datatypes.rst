Datatypes
=========

All columns in a Daft DataFrame have a DataType \(also often abbreviated as ``dtype``\).

All elements of a column are of the same dtype, or they can be the special Null value \(indicating a missing value\).

Daft provides simple DataTypes that are ubiquituous in many DataFrames such as numbers, strings and dates - all the way up to more complex types like tensors and images.

.. NOTE::

    For a full overview on all the DataTypes that Daft supports, see the :doc:`DataType API Reference <../../api_docs/datatype>`.

Numeric DataTypes
-----------------

Numeric DataTypes allows Daft to represent numbers. These numbers can differ in terms of the number of bits used to represent them (8, 16, 32 or 64 bits) and the semantic meaning of those bits
(float vs integer vs unsigned integers).

Examples:

1. :meth:`DataType.int8() <daft.DataType.int8>`: represents an 8-bit signed integer (-128 to 127)
2. :meth:`DataType.float32() <daft.DataType.float32>`: represents a 32-bit float (a float number with about 7 decimal digits of precision)

Columns/expressions with these datatypes can be operated on with many numeric expressions such as ``+`` and ``*``.

See also:

* :ref:`Numeric Expressions <userguide-numeric-expressions>`

Logical DataTypes
-----------------

The :meth:`DataType.bool() <daft.DataType.bool>` DataType represents values which are boolean values: ``True``, ``False`` or ``Null``.

Columns/expressions with this dtype can be operated on using logical expressions such as ``&`` and :meth:`.if_else() <daft.expressions.Expression.if_else>`.

See also:

* :ref:`Logical Expressions <userguide-logical-expressions>`

String Types
------------

Daft has string types, which represent a variable-length string of characters.

As a convenience method, string types also support the ``+`` Expression, which has been overloaded to support concatenation of elements between two :meth:`DataType.string() <daft.DataType.string>` columns.

1. :meth:`DataType.string() <daft.DataType.string>`: represents a string of UTF-8 characters
2. :meth:`DataType.binary() <daft.DataType.binary>`: represents a string of bytes

See also:

* :ref:`String Expressions <userguide-string-expressions>`

Temporal
--------

Temporal dtypes represent data that have to do with time.

Examples:

1. :meth:`DataType.date() <daft.DataType.date>`: represents a Date (year, month and day)
2. :meth:`DataType.timestamp() <daft.DataType.timestamp>`: represents a Timestamp (particular instance in time)

See also:

* :ref:`Temporal Expressions <api-expressions-temporal>`

Nested
------

Nested DataTypes wrap other DataTypes, allowing you to compose types into complex data structures.

Examples:

1. :meth:`DataType.list(child_dtype) <daft.DataType.list>`: represents a list where each element is of the child dtype
2. :meth:`DataType.struct({"field_name": child_dtype}) <daft.DataType.struct>`: represents a structure that has children dtypes, each mapped to a field name

Python
------

The :meth:`DataType.python() <daft.DataType.python>` dtype represent items that are Python objects.

.. WARNING::

    Daft does not impose any invariants about what *Python types* these objects are. To Daft, these are just generic Python objects!

Python is AWESOME because it's so flexible, but it's also slow and memory inefficient! Thus we recommend:

1. **Cast early!**: Casting your Python data into native Daft DataTypes if possible - this results in much more efficient downstream data serialization and computation.
2. **Use Python UDFs**: If there is no suitable Daft representation for your Python objects, use Python UDFs to process your Python data and extract the relevant data to be returned as native Daft DataTypes!

.. NOTE::

    If you work with Python classes for a generalizable use-case (e.g. documents, protobufs), it may be that these types are good candidates for "promotion" into a native Daft type!
    Please get in touch with the Daft team and we would love to work together on building your type into canonical Daft types.

Complex Types
-------------

Daft supports many more interesting complex DataTypes, for example:

* :meth:`DataType.tensor() <daft.DataType.tensor>`: Multi-dimensional (potentially uniformly-shaped) tensors of data
* :meth:`DataType.embedding() <daft.DataType.embedding>`: Lower-dimensional vector representation of data (e.g. words)
* :meth:`DataType.image() <daft.DataType.image>`: NHWC images

Daft abstracts away the in-memory representation of your data and provides kernels for many common operations on top of these data types. For supported image operations see the :ref:`image expressions API reference <api-expressions-images>`.

For more complex algorithms, you can also drop into a Python UDF to process this data using your custom Python libraries.

Please add suggestions for new DataTypes to our Github Discussions page!
