DataFrame Operations
====================

In the previous section, we covered Expressions which are ways of expressing computation on a single column.

However, the Daft DataFrame is a table containing equal-length columns. Many operations affect the entire table at once, which in turn affects the ordering or sizes of all columns.

This section of the user guide covers these operations, and how to use them.

Selecting Columns
-----------------

Select specific columns in a DataFrame using :meth:`df.select() <daft.DataFrame.select>`, which also takes Expressions as an input.

.. code-block:: python

    import daft

    df = daft.from_pydict({"A": [1, 2, 3], "B": [4, 5, 6]})

    df.select("A").show()

.. code-block:: none

    +---------+
    |       A |
    |   Int64 |
    +=========+
    |       1 |
    +---------+
    |       2 |
    +---------+
    |       3 |
    +---------+
    (Showing first 3 rows)

A useful alias for :meth:`df.select() <daft.DataFrame.select>` is indexing a DataFrame with a list of column names or Expressions:

.. code-block:: python

    df[["A", "B"]].show()

.. code-block:: none

    +---------+---------+
    |       A |       B |
    |   Int64 |   Int64 |
    +=========+=========+
    |       1 |       4 |
    +---------+---------+
    |       2 |       5 |
    +---------+---------+
    |       3 |       6 |
    +---------+---------+
    (Showing first 3 rows)

Sometimes, it may be useful to exclude certain columns from a DataFrame. This can be done with :meth:`df.exclude() <daft.DataFrame.exclude>`:

.. code-block:: python

    df.exclude("A").show()

.. code-block:: none

    +---------+
    |       B |
    |   Int64 |
    +=========+
    |       4 |
    +---------+
    |       5 |
    +---------+
    |       6 |
    +---------+
    (Showing first 3 rows)

As we have already seen in previous guides, adding a new column can be achieved with :meth:`df.with_column() <daft.DataFrame.with_column>`:

.. code-block:: python

    df.with_column("C", df["A"] + df["B"]).show()

.. code-block:: none

    +---------+---------+---------+
    |       A |       B |       C |
    |   Int64 |   Int64 |   Int64 |
    +=========+=========+=========+
    |       1 |       4 |       5 |
    +---------+---------+---------+
    |       2 |       5 |       7 |
    +---------+---------+---------+
    |       3 |       6 |       9 |
    +---------+---------+---------+
    (Showing first 3 rows)

.. _Column Wildcards:

Selecting Columns Using Wildcards
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We can select multiple columns at once using wildcards. The expression `col("*")` selects every column in a DataFrame, and you can operate on this expression in the same way as a single column:

.. code:: python

    df = daft.from_pydict({"A": [1, 2, 3], "B": [4, 5, 6]})
    df.select(col("*") * 3).show()

.. code:: none

    ╭───────┬───────╮
    │ A     ┆ B     │
    │ ---   ┆ ---   │
    │ Int64 ┆ Int64 │
    ╞═══════╪═══════╡
    │ 3     ┆ 12    │
    ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    │ 6     ┆ 15    │
    ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    │ 9     ┆ 18    │
    ╰───────┴───────╯

We can also select multiple columns within structs using `col("struct.*")`:

.. code:: python

    df = daft.from_pydict({
        "A": [
            {"B": 1, "C": 2},
            {"B": 3, "C": 4}
        ]
    })
    df.select(col("A.*")).show()

.. code:: none

    ╭───────┬───────╮
    │ B     ┆ C     │
    │ ---   ┆ ---   │
    │ Int64 ┆ Int64 │
    ╞═══════╪═══════╡
    │ 1     ┆ 2     │
    ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    │ 3     ┆ 4     │
    ╰───────┴───────╯

Under the hood, wildcards work by finding all of the columns that match, then copying the expression several times and replacing the wildcard. This means that there are some caveats:

* Only one wildcard is allowed per expression tree. This means that `col("*") + col("*")` and similar expressions do not work.
* Be conscious about duplicated column names. Any code like `df.select(col("*"), col("*") + 3)` will not work because the wildcards expand into the same column names.

  For the same reason, `col("A") + col("*")` will not work because the name on the left-hand side is inherited, meaning all the output columns are named `A`, causing an error if there is more than one.
  However, `col("*") + col("A")` will work fine.

Selecting Rows
--------------

We can limit the rows to the first ``N`` rows using :meth:`df.limit(N) <daft.DataFrame.limit>`:

.. code-block:: python

    df = daft.from_pydict({
        "A": [1, 2, 3, 4, 5],
        "B": [6, 7, 8, 9, 10],
    })

    df.limit(3).show()

.. code-block:: none

    +---------+---------+
    |       A |       B |
    |   Int64 |   Int64 |
    +=========+=========+
    |       1 |       6 |
    +---------+---------+
    |       2 |       7 |
    +---------+---------+
    |       3 |       8 |
    +---------+---------+
    (Showing first 3 rows)


We can also filter rows using :meth:`df.where() <daft.DataFrame.where>`, which takes an input a Logical Expression predicate:

.. code-block:: python

    df.where(df["A"] > 3).show()

.. code-block:: none

    +---------+---------+
    |       A |       B |
    |   Int64 |   Int64 |
    +=========+=========+
    |       4 |       9 |
    +---------+---------+
    |       5 |      10 |
    +---------+---------+
    (Showing first 2 rows)

Combining DataFrames
--------------------

Two DataFrames can be column-wise joined using :meth:`df.join() <daft.DataFrame.join>`.

This requires a "join key", which can be supplied as the ``on`` argument if both DataFrames have the same name for their key columns, or the ``left_on`` and ``right_on`` argument if the key column has different names in each DataFrame.

Daft also supports multi-column joins if you have a join key comprising of multiple columns!

.. code-block:: python

    df1 = daft.from_pydict({"A": [1, 2, 3], "B": [4, 5, 6]})
    df2 = daft.from_pydict({"A": [1, 2, 3], "C": [7, 8, 9]})

    df1.join(df2, on="A").show()

.. code-block:: none

    +---------+---------+---------+
    |       A |       B |       C |
    |   Int64 |   Int64 |   Int64 |
    +=========+=========+=========+
    |       1 |       4 |       7 |
    +---------+---------+---------+
    |       2 |       5 |       8 |
    +---------+---------+---------+
    |       3 |       6 |       9 |
    +---------+---------+---------+
    (Showing first 3 rows)

Reordering Rows
---------------

Rows in a DataFrame can be reordered based on some column using :meth:`df.sort() <daft.DataFrame.sort>`. Daft also supports multi-column sorts for sorting on multiple columns at once.

.. code-block:: python

    df = daft.from_pydict({
        "A": [1, 2, 3],
        "B": [6, 7, 8],
    })

    df.sort("A", desc=True).show()

.. code-block:: none

    +---------+---------+
    |       A |       B |
    |   Int64 |   Int64 |
    +=========+=========+
    |       3 |       8 |
    +---------+---------+
    |       2 |       7 |
    +---------+---------+
    |       1 |       6 |
    +---------+---------+
    (Showing first 3 rows)

Exploding Columns
-----------------

The :meth:`df.explode() <daft.DataFrame.explode>` method can be used to explode a column containing a list of values into multiple rows. All other rows will be **duplicated**.

.. code:: python

    df = daft.from_pydict({
        "A": [1, 2, 3],
        "B": [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
    })

    df.explode("B").show()

.. code:: none

    +---------+---------+
    |       A |       B |
    |   Int64 |   Int64 |
    +=========+=========+
    |       1 |       1 |
    +---------+---------+
    |       1 |       2 |
    +---------+---------+
    |       1 |       3 |
    +---------+---------+
    |       2 |       4 |
    +---------+---------+
    |       2 |       5 |
    +---------+---------+
    |       2 |       6 |
    +---------+---------+
    |       3 |       7 |
    +---------+---------+
    |       3 |       8 |
    +---------+---------+
    (Showing first 8 rows)

Repartitioning
--------------

Daft is a distributed DataFrame, and the dataframe is broken into multiple "partitions" which are processed in parallel across the cores in your machine or cluster.

You may choose to increase or decrease the number of partitions with :meth:`df.repartition() <daft.DataFrame.partition>`.

1. Increasing the number of partitions to 2x the total number of CPUs could help with resource utilization
2. If each partition is potentially overly large (e.g. containing large images), causing memory issues, you may increase the number of partitions to reduce the size of each individual partition
3. If you have too many partitions, global operations such as a sort or a join may take longer to execute

A good rule of thumb is to keep the number of partitions as twice the number of CPUs available on your backend, increasing the number of partitions as necessary if they cannot be processed in memory.
