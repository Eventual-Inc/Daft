Expressions
===========

Expressions are how you can express computations that should be run over columns of data.

Referencing Existing Columns
----------------------------

To refer to an existing column in a DataFrame, you can index the DataFrame with a string like so:

.. code:: python

    df = DataFrame.from_pydict({"A": [1, 2, 3]})

    # Refers to column "A" in `df`
    df["A"]

If you print ``df["A"]``, you will see output that says ``col(A#0: INTEGER)``. Note that this expression has a name ("A"), an ID ("#0") and a type (it is an INTEGER)!

Creating New Columns
--------------------

Perhaps the simplest use of an Expression is to create a new DataFrame column. Here is an example of creating a new column named "B", with the same values that exist in column "A":

.. code:: python

    df = df.with_column("B", df["A"])

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
    # +-----------+-------------+----------------+-----------+
    # |         A |   A_add_one |   A_divide_two | A_gt_1    |
    # |   INTEGER |     INTEGER |          FLOAT | LOGICAL   |
    # +===========+=============+================+===========+
    # |         1 |           2 |            0.5 | false     |
    # +-----------+-------------+----------------+-----------+
    # |         2 |           3 |            1   | true      |
    # +-----------+-------------+----------------+-----------+
    # |         3 |           4 |            1.5 | true      |
    # +-----------+-------------+----------------+-----------+
    # (Showing first 3 of 3 rows)

Notice that the returned types of these operations are also well-typed according to their input types. For example, calling ``df["A"] > 1`` returns a column of type ``LOGICAL``.

There are many other operations that can be run on numeric type Expressions. You can learn more about them in the :ref:`Expressions API reference <api-numeric-expression-operations>`.
