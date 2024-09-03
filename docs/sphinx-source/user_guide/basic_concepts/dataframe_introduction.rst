Dataframe
=========

Data in Daft is represented as a DataFrame, which is a collection of data organized as a **table** with **rows** and **columns**.

.. image:: /_static/daft_illustration.png
   :alt: Daft python dataframes make it easy to load any data such as PDF documents, images, protobufs, csv, parquet and audio files into a table dataframe structure for easy querying
   :width: 500
   :align: center

This document provides an introduction to the Daft Dataframe.

Creating a Dataframe
--------------------

Let's create our first Dataframe from a Python dictionary of columns.

.. code:: python

    import daft

    df = daft.from_pydict({
        "A": [1, 2, 3, 4],
        "B": [1.5, 2.5, 3.5, 4.5],
        "C": [True, True, False, False],
        "D": [None, None, None, None],
    })

Examine your Dataframe by printing it:

.. code:: python

    df

.. code:: none

    +---------+-----------+-----------+-----------+
    |       A |         B | C         | D         |
    |   Int64 |   Float64 | Boolean   | Null      |
    +=========+===========+===========+===========+
    |       1 |       1.5 | true      | None      |
    +---------+-----------+-----------+-----------+
    |       2 |       2.5 | true      | None      |
    +---------+-----------+-----------+-----------+
    |       3 |       3.5 | false     | None      |
    +---------+-----------+-----------+-----------+
    |       4 |       4.5 | false     | None      |
    +---------+-----------+-----------+-----------+
    (Showing first 4 of 4 rows)


Congratulations - you just created your first DataFrame! It has 4 columns, "A", "B", "C", and "D". Let's try to select only the "A", "B", and "C" columns:

.. code:: python

    df.select("A", "B", "C")

.. code:: none

    +---------+-----------+-----------+
    | A       | B         | C         |
    | Int64   | Float64   | Boolean   |
    +=========+===========+===========+
    +---------+-----------+-----------+
    (No data to display: Dataframe not materialized)


But wait - why is it printing the message ``(No data to display: Dataframe not materialized)`` and where are the rows of each column?

Executing our DataFrame and Viewing Data
----------------------------------------

The reason that our DataFrame currently does not display its rows is that Daft DataFrames are **lazy**. This just means that Daft DataFrames will defer all its work until you tell it to execute.

In this case, Daft is just deferring the work required to read the data and select columns, however in practice this laziness can be very useful for helping Daft optimize your queries before execution!

.. NOTE::

    When you call methods on a Daft Dataframe, it defers the work by adding to an internal "plan". You can examine the current plan of a DataFrame by calling :meth:`df.explain() <daft.DataFrame.explain>`!

    Passing the ``show_all=True`` argument will show you the plan after Daft applies its query optimizations and the physical (lower-level) plan.

We can tell Daft to execute our DataFrame and cache the results using :meth:`df.collect() <daft.DataFrame.collect>`:

.. code:: python

    df.collect()
    df

.. code:: none

    +---------+-----------+-----------+
    |       A |         B | C         |
    |   Int64 |   Float64 | Boolean   |
    +=========+===========+===========+
    |       1 |       1.5 | true      |
    +---------+-----------+-----------+
    |       2 |       2.5 | true      |
    +---------+-----------+-----------+
    |       3 |       3.5 | false     |
    +---------+-----------+-----------+
    |       4 |       4.5 | false     |
    +---------+-----------+-----------+
    (Showing first 4 of 4 rows)

Now your DataFrame object ``df`` is **materialized** - Daft has executed all the steps required to compute the results, and has cached the results in memory so that it can display this preview.

Any subsequent operations on ``df`` will avoid recomputations, and just use this materialized result!

When should I materialize my DataFrame?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you "eagerly" call :meth:`df.collect() <daft.DataFrame.collect>` immediately on every DataFrame, you may run into issues:

1. If data is too large at any step, materializing all of it may cause memory issues
2. Optimizations are not possible since we cannot "predict future operations"

However, data science is all about experimentation and trying different things on the same data. This means that materialization is crucial when working interactively with DataFrames, since it speeds up all subsequent experimentation on that DataFrame.

We suggest materializing DataFrames using :meth:`df.collect() <daft.DataFrame.collect>` when they contain expensive operations (e.g. sorts or expensive function calls) and have to be called multiple times by downstream code:

.. code:: python

    df = df.with_column("A", df["A"].apply(expensive_function))  # expensive function
    df = df.sort("A")  # expensive sort
    df.collect()  # materialize the DataFrame

    # All subsequent work on df avoids recomputing previous steps
    df.sum().show()
    df.mean().show()
    df.with_column("try_this", df["A"] + 1).show(5)

In many other cases however, there are better options than materializing your entire DataFrame with :meth:`df.collect() <daft.DataFrame.collect>`:

1. **Peeking with df.show(N)**: If you only want to "peek" at the first few rows of your data for visualization purposes, you can use :meth:`df.show(N) <daft.DataFrame.show>`, which processes and shows only the first ``N`` rows.
2. **Writing to disk**: The ``df.write_*`` methods will process and write your data to disk per-partition, avoiding materializing it all in memory at once.
3. **Pruning data**: You can materialize your DataFrame after performing a :meth:`df.limit() <daft.DataFrame.limit>`, :meth:`df.where() <daft.DataFrame.where>` or :meth:`df.select() <daft.DataFrame.select>` operation which processes your data or prune it down to a smaller size.

Schemas and Types
-----------------

Notice also that when we printed our DataFrame, Daft displayed its **schema**. Each column of your DataFrame has a **name** and a **type**, and all data in that column will adhere to that type!

Daft can display your DataFrame's schema without materializing it. Under the hood, it performs intelligent sampling of your data to determine the appropriate schema, and if you make any modifications to your DataFrame it can infer the resulting types based on the operation.

.. NOTE::

    Under the hood, Daft represents data in the `Apache Arrow <https://arrow.apache.org/>`_ format, which allows it to efficiently represent and work on data using high-performance kernels which are written in Rust.


Running Computations
--------------------

To run computations on data in our DataFrame, we use Expressions.

The following statement will :meth:`df.show() <daft.DataFrame.show>` a DataFrame that has only one column - the column ``A`` from our original DataFrame but with every row incremented by 1.

.. code:: python

    df.select(df["A"] + 1).show()

.. code:: none

    +---------+
    |       A |
    |   Int64 |
    +=========+
    |       2 |
    +---------+
    |       3 |
    +---------+
    |       4 |
    +---------+
    |       5 |
    +---------+
    (Showing first 4 rows)

.. NOTE::

    A common pattern is to create a new columns using ``DataFrame.with_column``:

    .. code:: python

        # Creates a new column named "foo" which takes on values
        # of column "A" incremented by 1
        df = df.with_column("foo", df["A"] + 1)

Congratulations, you have just written your first **Expression**: ``df["A"] + 1``!

Expressions
^^^^^^^^^^^

Expressions are how you define computations on your columns in Daft.

The world of Daft contains much more than just numbers, and you can do much more than just add numbers together. Daft's rich Expressions API allows you to do things such as:

1. Convert between different types with :meth:`df["numbers"].cast(float) <daft.DataFrame.cast>`
2. Download Bytes from a column containing String URLs using :meth:`df["urls"].url.download() <daft.expressions.expressions.ExpressionUrlNamespace.download>`
3. Run arbitrary Python functions on your data using :meth:`df["objects"].apply(my_python_function) <daft.DataFrame.apply>`

We are also constantly looking to improve Daft and add more Expression functionality. Please contribute to the project with your ideas and code if you have an Expression in mind!

The next section on :doc:`expressions` will provide a much deeper look at the Expressions that Daft provides.
