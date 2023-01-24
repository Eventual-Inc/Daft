Expressions
===========

Expressions are how you can express computations that should be run over columns of data.

.. NOTE::

    Daft Expressions do not change the number of rows of the columns that they run on.

    Operations that change the number of rows, or ordering of rows, are DataFrame-level operations since they affect other columns. Those will be covered in the next section on :doc:`dataframe-operations`.

Column Expressions
------------------

Column Expressions are an expression that refers to a column. You may think of them as "pointers" to a given dataframe's column.

To obtain a Column Expression, simply index a DataFrame with the string name of the column:

.. code:: python

    from daft import DataFrame

    df = DataFrame.from_pydict({"A": [1, 2, 3]})

    # Refers to column "A" in `df`
    df["A"]

.. code:: none

    col(A#0: INTEGER)

Note that this Column Expression points to a column with name ("A"), an ID ("#0") and a type (INTEGER)!

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

    +-----------+-------------+----------------+-----------+
    |         A |   A_add_one |   A_divide_two | A_gt_1    |
    |   INTEGER |     INTEGER |          FLOAT | LOGICAL   |
    +===========+=============+================+===========+
    |         1 |           2 |            0.5 | false     |
    +-----------+-------------+----------------+-----------+
    |         2 |           3 |            1   | true      |
    +-----------+-------------+----------------+-----------+
    |         3 |           4 |            1.5 | true      |
    +-----------+-------------+----------------+-----------+
    (Showing first 3 of 3 rows)

Notice that the returned types of these operations are also well-typed according to their input types. For example, calling ``df["A"] > 1`` returns a column of type ``LOGICAL``.

Both FLOAT and INTEGER types are numeric types, and inherit many of the same arithmetic Expression operations. You may find the full list of numeric operations in the :ref:`Expressions API reference <api-numeric-expression-operations>`.

String Expressions
------------------

String Expressions are an expression that refers to a column of type ``STRING``.

.. code:: python

    df = DataFrame.from_pydict({"B": ["foo", "bar", "baz"]})

    df["B"]

.. code:: none

    col(B#0: STRING)

Unlike the numeric types, the string type does not support arithmetic operations such as ``*`` and ``/``.

.. NOTE::

    The one exception to this is the ``+`` operator, which is overridden to concatenate two string expressions as is commonly done in Python.

Instead, many of its operations can be accessed through a "Method Accessor", ``.str.*``.

For example, to check if each element in column "B" contains the substring "a", we can use the ``.str.contains`` method:

.. code:: python

    df = df.with_column("B_contains_a", df["B"].str.contains("a"))

    df.collect()

.. code:: none

    +----------+----------------+
    | B        | B_contains_a   |
    | STRING   | LOGICAL        |
    +==========+================+
    | foo      | false          |
    +----------+----------------+
    | bar      | true           |
    +----------+----------------+
    | baz      | true           |
    +----------+----------------+
    (Showing first 3 of 3 rows)

You may find a full list of string operations in the :ref:`Expressions API reference <api-string-expression-operations>`.

URL Expressions
^^^^^^^^^^^^^^^

One special case of a STRING column you may find yourself working with is a column of URL strings.

Daft provides the ``.url.*`` method accessor with functionality for working with URL strings. For example, to download data from URLs:

.. code:: python

    df = DataFrame.from_pydict({
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
    | STRING               | BYTES                |
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

Logical Expressions
-------------------

Logical Expressions are an expression that refers to a column of type ``LOGICAL``, and can only take on the values True or False.

.. code:: python

    df = DataFrame.from_pydict({"C": [True, False, True]})

    df["C"]

.. code:: none

    col(C#0: LOGICAL)

Daft supports logical operations such as ``&`` (and) and ``|`` (or) between logical expressions.

Comparisons
^^^^^^^^^^^

Many of the types in Daft support comparisons between expressions that returns a Logical Expression.

For example, here we can compare if each element in column "A" is equal to elements in column "B":

.. code:: python

    df = DataFrame.from_pydict({"A": [1, 2, 3], "B": [1, 2, 4]})

    df = df.with_column("A_eq_B", df["A"] == df["B"])

    df.collect()

.. code:: none

    +-----------+-----------+-----------+
    |         A |         B | A_eq_B    |
    |   INTEGER |   INTEGER | LOGICAL   |
    +===========+===========+===========+
    |         1 |         1 | true      |
    +-----------+-----------+-----------+
    |         2 |         2 | true      |
    +-----------+-----------+-----------+
    |         3 |         4 | false     |
    +-----------+-----------+-----------+
    (Showing first 3 of 3 rows)

Other useful comparisons can be found in the :ref:`Expressions API reference <_api-comparison-expression>`.

If Else Pattern
^^^^^^^^^^^^^^^

The ``.if_else`` method is a useful expression to have up your sleeve for choosing values between two other expressions based on a logical expression:

.. code:: python

    df = DataFrame.from_pydict({"A": [1, 2, 3], "B": [0, 2, 4]})

    # Pick values from column A if the value in column A is bigger
    # than the value in column B. Otherwise, pick values from column B.
    df = df.with_column(
        "A_if_bigger_else_B",
        (df["A"] > df["B"]).if_else(df["A"], df["B"]),
    )

    df.collect()

.. code:: none

    +-----------+-----------+----------------------+
    |         A |         B |   A_if_bigger_else_B |
    |   INTEGER |   INTEGER |              INTEGER |
    +===========+===========+======================+
    |         1 |         0 |                    1 |
    +-----------+-----------+----------------------+
    |         2 |         2 |                    2 |
    +-----------+-----------+----------------------+
    |         3 |         4 |                    4 |
    +-----------+-----------+----------------------+
    (Showing first 3 of 3 rows)

This is a useful expression for cleaning your data!
