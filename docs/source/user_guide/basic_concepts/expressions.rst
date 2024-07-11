Expressions
===========

Expressions are how you can express computations that should be run over columns of data.

Creating Expressions
--------------------

Referring to a column in a DataFrame
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Most commonly you will be creating expressions by referring to a column from an existing DataFrame.

To do so, simply index a DataFrame with the string name of the column:

.. code:: python

    import daft

    df = daft.from_pydict({"A": [1, 2, 3]})

    # Refers to column "A" in `df`
    df["A"]

.. code:: none

    col(A)

When we evaluate this ``df["A"]`` Expression, it will evaluate to the column from the ``df`` DataFrame with name "A"!

Refer to a column with a certain name
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You may also find it necessary in certain situations to create an Expression with just the name of a column, without having an existing DataFrame to refer to. You can do this with the :func:`~daft.expressions.col` helper:

.. code:: python

    from daft import col

    # Refers to a column named "A"
    col("A")

When this Expression is evaluated, it will resolve to "the column named A" in whatever evaluation context it is used within!

Literals
^^^^^^^^

You may find yourself needing to hardcode a "single value" oftentimes as an expression. Daft provides a :func:`~daft.expressions.lit` helper to do so:

.. code:: python

    from daft import lit

    # Refers to an expression which always evaluates to 42
    lit(42)

This special :func:`~daft.expressions.lit` expression we just created evaluates always to the value ``42``.

.. _userguide-numeric-expressions:

Numeric Expressions
-------------------

Since column "A" is an integer, we can run numeric computation such as addition, division and checking its value. Here are some examples where we create new columns using the results of such computations:

.. code:: python

    # Add 1 to each element in column "A"
    df = df.with_column("A_add_one", df["A"] + 1)

    # Divide each element in column A by 2
    df = df.with_column("A_divide_two", df["A"] / 2.)

    # Check if each element in column A is more than 1
    df = df.with_column("A_gt_1", df["A"] > 1)

    df.collect()

.. code:: none

    +---------+-------------+----------------+-----------+
    |       A |   A_add_one |   A_divide_two | A_gt_1    |
    |   Int64 |       Int64 |        Float64 | Boolean   |
    +=========+=============+================+===========+
    |       1 |           2 |            0.5 | false     |
    +---------+-------------+----------------+-----------+
    |       2 |           3 |            1   | true      |
    +---------+-------------+----------------+-----------+
    |       3 |           4 |            1.5 | true      |
    +---------+-------------+----------------+-----------+
    (Showing first 3 of 3 rows)

Notice that the returned types of these operations are also well-typed according to their input types. For example, calling ``df["A"] > 1`` returns a column of type :meth:`Boolean <daft.DataType.boolean>`.

Both the :meth:`Float <daft.DataType.float>` and :meth:`Int <daft.DataType.int>` types are numeric types, and inherit many of the same arithmetic Expression operations. You may find the full list of numeric operations in the :ref:`Expressions API reference <api-numeric-expression-operations>`.

.. _userguide-string-expressions:

String Expressions
------------------

Daft also lets you have columns of strings in a DataFrame. Let's take a look!

.. code:: python

    df = daft.from_pydict({"B": ["foo", "bar", "baz"]})
    df.show()

.. code:: none

    +--------+
    | B      |
    | Utf8   |
    +========+
    | foo    |
    +--------+
    | bar    |
    +--------+
    | baz    |
    +--------+
    (Showing first 3 rows)

Unlike the numeric types, the string type does not support arithmetic operations such as ``*`` and ``/``. The one exception to this is the ``+`` operator, which is overridden to concatenate two string expressions as is commonly done in Python. Let's try that!

.. code:: python

    df = df.with_column("B2", df["B"] + "foo")
    df.show()

.. code:: none

    +--------+--------+
    | B      | B2     |
    | Utf8   | Utf8   |
    +========+========+
    | foo    | foofoo |
    +--------+--------+
    | bar    | barfoo |
    +--------+--------+
    | baz    | bazfoo |
    +--------+--------+
    (Showing first 3 rows)

There are also many string operators that are accessed through a separate :meth:`.str.* <daft.expressions.Expression.str>` "method namespace".

For example, to check if each element in column "B" contains the substring "a", we can use the :meth:`.str.contains <daft.expressions.expressions.ExpressionStringNamespace.contains>` method:

.. code:: python

    df = df.with_column("B2_contains_B", df["B2"].str.contains(df["B"]))
    df.show()

.. code:: none

    +--------+--------+-----------------+
    | B      | B2     | B2_contains_B   |
    | Utf8   | Utf8   | Boolean         |
    +========+========+=================+
    | foo    | foofoo | true            |
    +--------+--------+-----------------+
    | bar    | barfoo | true            |
    +--------+--------+-----------------+
    | baz    | bazfoo | true            |
    +--------+--------+-----------------+
    (Showing first 3 rows)

You may find a full list of string operations in the :ref:`Expressions API reference <api-string-expression-operations>`.

URL Expressions
^^^^^^^^^^^^^^^

One special case of a String column you may find yourself working with is a column of URL strings.

Daft provides the :meth:`.url.* <daft.expressions.Expression.url>` method namespace with functionality for working with URL strings. For example, to download data from URLs:

.. code:: python

    df = daft.from_pydict({
        "urls": [
            "https://www.google.com",
            "s3://daft-public-data/open-images/validation-images/0001eeaf4aed83f9.jpg",
        ],
    })
    df = df.with_column("data", df["urls"].url.download())
    df.collect()

.. code:: none

    +----------------------+----------------------+
    | urls                 | data                 |
    | Utf8                 | Binary               |
    +======================+======================+
    | https://www.google.c | b'<!doctype          |
    | om                   | html><html           |
    |                      | itemscope="" itemtyp |
    |                      | e="http://sche...    |
    +----------------------+----------------------+
    | s3://daft-public-    | b'\xff\xd8\xff\xe0\x |
    | data/open-           | 00\x10JFIF\x00\x01\x |
    | images/validation-   | 01\x01\x00H\x00H\... |
    | images/0001e...      |                      |
    +----------------------+----------------------+
    (Showing first 2 of 2 rows)

This works well for URLs which are HTTP paths to non-HTML files (e.g. jpeg), local filepaths or even paths to a file in an object store such as AWS S3 as well!

JSON Expressions
^^^^^^^^^^^^^^^^

If you have a column of JSON strings, Daft provides the :meth:`.json.* <daft.expressions.Expression.json>` method namespace to run `JQ-style filters <https://stedolan.github.io/jq/manual/>`_ on them. For example, to extract a value from a JSON object:

.. code:: python

    df = daft.from_pydict({
        "json": [
            '{"a": 1, "b": 2}',
            '{"a": 3, "b": 4}',
        ],
    })
    df = df.with_column("a", df["json"].json.query(".a"))
    df.collect()

.. code:: none

    ╭──────────────────┬──────╮
    │ json             ┆ a    │
    │ ---              ┆ ---  │
    │ Utf8             ┆ Utf8 │
    ╞══════════════════╪══════╡
    │ {"a": 1, "b": 2} ┆ 1    │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    │ {"a": 3, "b": 4} ┆ 3    │
    ╰──────────────────┴──────╯

    (Showing first 2 of 2 rows)

Daft uses `jaq <https://github.com/01mf02/jaq/tree/main>`_ as the underlying executor, so you can find the full list of supported filters in the `jaq documentation <https://github.com/01mf02/jaq/tree/main>`_.

.. _userguide-logical-expressions:

Logical Expressions
-------------------

Logical Expressions are an expression that refers to a column of type :meth:`Boolean <daft.DataType.boolean>`, and can only take on the values True or False.

.. code:: python

    df = daft.from_pydict({"C": [True, False, True]})
    df["C"]

Daft supports logical operations such as ``&`` (and) and ``|`` (or) between logical expressions.

Comparisons
^^^^^^^^^^^

Many of the types in Daft support comparisons between expressions that returns a Logical Expression.

For example, here we can compare if each element in column "A" is equal to elements in column "B":

.. code:: python

    df = daft.from_pydict({"A": [1, 2, 3], "B": [1, 2, 4]})

    df = df.with_column("A_eq_B", df["A"] == df["B"])

    df.collect()

.. code:: none

    +---------+---------+-----------+
    |       A |       B | A_eq_B    |
    |   Int64 |   Int64 | Boolean   |
    +=========+=========+===========+
    |       1 |       1 | true      |
    +---------+---------+-----------+
    |       2 |       2 | true      |
    +---------+---------+-----------+
    |       3 |       4 | false     |
    +---------+---------+-----------+
    (Showing first 3 of 3 rows)

Other useful comparisons can be found in the :ref:`Expressions API reference <api-comparison-expression>`.

If Else Pattern
^^^^^^^^^^^^^^^

The :meth:`.if_else() <daft.expressions.Expression.if_else>` method is a useful expression to have up your sleeve for choosing values between two other expressions based on a logical expression:

.. code:: python

    df = daft.from_pydict({"A": [1, 2, 3], "B": [0, 2, 4]})

    # Pick values from column A if the value in column A is bigger
    # than the value in column B. Otherwise, pick values from column B.
    df = df.with_column(
        "A_if_bigger_else_B",
        (df["A"] > df["B"]).if_else(df["A"], df["B"]),
    )

    df.collect()

.. code:: none

    +---------+---------+----------------------+
    |       A |       B |   A_if_bigger_else_B |
    |   Int64 |   Int64 |                Int64 |
    +=========+=========+======================+
    |       1 |       0 |                    1 |
    +---------+---------+----------------------+
    |       2 |       2 |                    2 |
    +---------+---------+----------------------+
    |       3 |       4 |                    4 |
    +---------+---------+----------------------+
    (Showing first 3 of 3 rows)

This is a useful expression for cleaning your data!



Temporal Operations
-------------------

Daft lets you work with various temporal data types such as Time, Timestamp, and Duration. Let's explore how to use these types and their interactions.

.. code:: python

    df = daft.from_pydict({"x": [
        datetime.datetime(2021, 1, 1, 0, 1, 1),
        datetime.datetime(2021, 1, 1, 0, 1, 59),
        datetime.datetime(2021, 1, 1, 0, 2, 0),
        ]
    })
    df.show()

.. code:: none

    +------------------------+
    | x                      |
    | DateTime               |
    +========================+
    | 2021-01-01T00:01:01    |
    +------------------------+
    | 2021-01-01T00:01:59    |
    +------------------------+
    | 2021-01-01T00:02:00    |
    +------------------------+
    (Showing first 3 rows)

Let's add 10 seconds to each timestamp.

.. code:: python

    df = df.with_column("x_plus_10_seconds", df["x"] + datetime.timedelta(seconds=10))
    df.show()

.. code:: none

    +------------------------+------------------------+
    | x                      | x_plus_10_seconds      |
    | DateTime               | DateTime               |
    +========================+========================+
    | 2021-01-01T00:01:01    | 2021-01-01T00:01:11    |
    +------------------------+------------------------+
    | 2021-01-01T00:01:59    | 2021-01-01T00:02:09    |
    +------------------------+------------------------+
    | 2021-01-01T00:02:00    | 2021-01-01T00:02:10    |
    +------------------------+------------------------+
    (Showing first 3 rows)

Subtracting Timestamps
^^^^^^^^^^^^^^^^^^^^^^

You can subtract one Timestamp from another to get a Duration.

.. code:: python

    df = df.with_column("duration_between_x_plus_10_and_x", df["x_plus_10_seconds"] - df["x"])
    df.show()

.. code:: none

    +------------------------+------------------------+-------------------------------+
    | x                      | x_plus_10_seconds      | duration_between_x_plus_10_and_x|
    | DateTime               | DateTime               | Duration                      |
    +========================+========================+===============================+
    | 2021-01-01T00:01:01    | 2021-01-01T00:01:11    | 0:00:10                       |
    +------------------------+------------------------+-------------------------------+
    | 2021-01-01T00:01:59    | 2021-01-01T00:02:09    | 0:00:10                       |
    +------------------------+------------------------+-------------------------------+
    | 2021-01-01T00:02:00    | 2021-01-01T00:02:10    | 0:00:10                       |
    +------------------------+------------------------+-------------------------------+
    (Showing first 3 rows)


Extracting Time Stamps
^^^^^^^^^^^^^^^^^^^^^^

You can extract parts of a Timestamp, such as the year, month, day, hour, minute, and second.

.. code:: python

    df = df.with_columns("year", df["x"].dt.year()) // with columns integration
    df = df.with_column("month", df["x"].dt.month())
    df = df.with_column("day", df["x"].dt.day())
    df = df.with_column("hour", df["x"].dt.hour())
    df = df.with_column("minute", df["x"].dt.minute())
    df = df.with_column("second", df["x"].dt.second())
    df.show()

.. code:: none

    +------------------------+------+------+-----+------+-------+--------+
    | x                      | year | month| day | hour | minute| second |
    | DateTime               | Int32| Int32|Int32| Int32| Int32 | Int32  |
    +========================+======+======+=====+======+=======+========+
    | 2021-01-01T00:01:01    | 2021 | 1    | 1   | 0    | 1     | 1      |
    +------------------------+------+------+-----+------+-------+--------+
    | 2021-01-01T00:01:59    | 2021 | 1    | 1   | 0    | 1     | 59     |
    +------------------------+------+------+-----+------+-------+--------+
    | 2021-01-01T00:02:00    | 2021 | 1    | 1   | 0    | 2     | 0      |
    +------------------------+------+------+-----+------+-------+--------+
    (Showing first 3 rows)


Converting Between Time Zones
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can convert a Timestamp to a different time zone.

.. code:: python

    df = daft.from_pydict({"x": [
        "2021-01-01 00:00:00.123 +0800",
        "2021-01-02 12:30:00.456 +0800"]
    })
    df = df.with_column("datetime", df["x"].str.to_datetime("%Y-%m-%d %H:%M:%S%.3f %z", timezone="America/New_York"))
    df.collect()

.. code:: none

    ╭───────────────────────────────┬───────────────────────────────────────────────────╮
    │ x                             ┆ datetime                                          │
    │ ---                           ┆ ---                                               │
    │ Utf8                          ┆ Timestamp(Milliseconds, Some("America/New_York")) │
    ╞═══════════════════════════════╪═══════════════════════════════════════════════════╡
    │ 2021-01-01 00:00:00.123 +0800 ┆ 2020-12-31 11:00:00.123 EST                       │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ 2021-01-02 12:30:00.456 +0800 ┆ 2021-01-01 23:30:00.456 EST                       │
    ╰───────────────────────────────┴───────────────────────────────────────────────────╯

    (Showing first 2 of 2 rows)


Using the Truncate Function
^^^^^^^^^^^^^^^^^^^^^^^^^^^


The `truncate` function can be used to truncate timestamps to a specific time unit. For example, you can use it to truncate timestamps to the nearest day
.. code:: python

    import pandas as pd
    import daft

    # Create a DataFrame with a range of dates
    df = daft.from_pydict({
             "datetime": [
                 datetime.datetime(2021, 1, 7, 0, 1, 1),
                 datetime.datetime(2021, 1, 8, 0, 1, 59),
                 datetime.datetime(2021, 1, 9, 0, 2, 0),
                 datetime.datetime(2021, 1, 10, 0, 2, 0),
             ],
         }
    )

    # Truncate dates to the start of the week
    df.with_column("truncated", df["datetime"].dt.truncate("1 week")).collect()

.. code:: none
    ╭───────────────────────────────┬───────────────────────────────╮
    │ datetime                      ┆ truncated                     │
    │ ---                           ┆ ---                           │
    │ Timestamp(Microseconds, None) ┆ Timestamp(Microseconds, None) │
    ╞═══════════════════════════════╪═══════════════════════════════╡
    │ 2021-01-07 00:01:01           ┆ 2021-01-07 00:00:00           │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ 2021-01-08 00:01:59           ┆ 2021-01-07 00:00:00           │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ 2021-01-09 00:02:00           ┆ 2021-01-07 00:00:00           │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ 2021-01-10 00:02:00           ┆ 2021-01-07 00:00:00           │
    ╰───────────────────────────────┴───────────────────────────────╯

    (Showing first 4 of 4 rows)

Explanation:
- The `dates` column contains dates from January 7, 2021, to January 10, 2021.
- The `truncated_dates` column shows the dates truncated to the start of the week.
- For dates between January 7 and January 10, the start of the week is considered as the closest preceding Sunday.

This example demonstrates the advantage of using the `truncate` function to group or round dates to a desired time unit, such as the start of the week, which can be particularly useful for summarizing or aggregating data by weeks.
