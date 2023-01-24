Introduction to DataFrames
==========================

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

    from daft import DataFrame

    df = DataFrame.from_pydict({
        "A": [1, 2, 3, 4],
        "B": [1.5, 2.5, 3.5, 4.5],
        "C": [True, True, False, False],
    })

Examine your Dataframe by printing it:

.. code:: python

    df

.. code:: none

    +-----------+---------+-----------+
    | A         | B       | C         |
    | INTEGER   | FLOAT   | LOGICAL   |
    +===========+=========+===========+
    +-----------+---------+-----------+
    (No data to display: Dataframe not materialized)


Congratulations - you just created your first DataFrame! It has 3 columns, "A", "B" and "C".

.. NOTE::

    For a deeper look at reading data, skip ahead to the section: :doc:`read-write`

But wait - why is it printing the message ``(No data to display: Dataframe not materialized)`` and where are the rows of each column?

Executing our DataFrame and Viewing Data
----------------------------------------

The reason that our DataFrame currently does not display its rows is that Daft DataFrames are **lazy**. This just means that Daft DataFrames will defer all its work until you tell it to execute.

In this case, Daft is just deferring the work required to read data from the Python dictionary, however in practice this laziness can be very useful for helping Daft optimize your queries before execution!

.. NOTE::

    When you call methods on a Daft Dataframe, it defers the work by adding to an internal "plan". You can examine the current plan of a DataFrame by calling ``DataFrame.explain()``!

    Passing the ``show_optimized=True`` argument will show you the plan after Daft applies its query optimizations.

We can tell Daft to execute our DataFrame and cache the results using ``df.collect()``:

.. code:: python

    df.collect()

.. code:: none

    +-----------+---------+-----------+
    |         A |       B | C         |
    |   INTEGER |   FLOAT | LOGICAL   |
    +===========+=========+===========+
    |         1 |     1.5 | true      |
    +-----------+---------+-----------+
    |         2 |     2.5 | true      |
    +-----------+---------+-----------+
    |         3 |     3.5 | false     |
    +-----------+---------+-----------+
    |         4 |     4.5 | false     |
    +-----------+---------+-----------+
    (Showing first 4 of 4 rows)

Now your DataFrame object ``df`` is **materialized** - Daft has executed all the steps required to compute the results, and has cached the results in memory so that it can display this preview.

Any subsequent operations on ``df`` will avoid recomputations, and just use this materialized result!

When should I materialize my DataFrame?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you "eagerly" call ``.collect()`` immediately on every DataFrame, you may run into issues:

1. If data is too large at any step, materializing all of it may cause memory issues
2. Optimizations are not possible since we cannot "predict future operations"

However, data science is all about experimentation and trying different things on the same data. This means that materialization crucial when working interactively with DataFrames, since it speeds up all subsequent experimentation on that DataFrame.

We suggest materializing DataFrames using ``.collect()`` when they contain expensive operations (e.g. sorts or expensive function calls) and have to be called multiple times by downstream code:

.. code:: python

    df = df.with_column("A", df["A"].apply(expensive_function)).sort("A")  # expensive function call followed by a sort
    df.collect()  # materialize the DataFrame

    # All subsequent work on df avoids recomputing it
    df.sum().show()
    df.mean().show()
    df.with_column("try_this", df["A"] + 1).show(5)

In many other cases however, there are better options than materializing your entire DataFrame with ``.collect()``:

1. **Peeking with df.show(N)**: If you only want to "peek" at the first few rows of your data for visualization purposes, you can use ``df.show(N)``, which processes and shows only the first ``N`` rows.
2. **Writing to disk**: The ``df.write_*`` methods will process and write your data to disk per-partition, avoiding materializing it all in memory at once.
3. **Pruning data**: You can materialize your DataFrame after performing a ``.limit``, ``.where`` or ``.select`` operation which processes your data or prune it down to a smaller size.

Schemas and Types
-----------------

Notice also that when we printed our DataFrame, Daft displayed its **schema**. Each column of your DataFrame has a **name** and a **type**, and that all data in that column will adhere to that type!

Additionally, Daft can display your DataFrame's schema without materializing it. Under the hood, it performs intelligent sampling of your data to determine the appropriate schema, and if you make any modifications to your DataFrame it can infer the resulting types based on the operation that you provided.

The following is a list of Daft types and a short explanation of the types.

+---------+------------------------------------------+
| STRING  | A sequence of text                       |
+---------+------------------------------------------+
| INTEGER | A number without decimal point           |
+---------+------------------------------------------+
| FLOAT   | A number with decimal point              |
+---------+------------------------------------------+
| LOGICAL | A boolean true/false                     |
+---------+------------------------------------------+
| BYTES   | A sequence of bytes                      |
+---------+------------------------------------------+
| DATE    | A day/month/year non-timezone aware date |
+---------+------------------------------------------+
| **PY**  | An arbitrary Python object               |
+---------+------------------------------------------+

.. NOTE::

    Under the hood, Daft represents data in the `Apache Arrow <https://arrow.apache.org/>`_ format, which allows it to efficiently represent and work on data using high-performance kernels which are written in Rust.

    Native Arrow support for more complex types such as lists, dictionaries and images are on the roadmap. Such types are supported at the moment using the **PY** type.


Running Computations
--------------------

To run computations on data in our DataFrame, we use Expressions.

The following statement will ``.show()`` a DataFrame that has only one column - the column ``A`` from our original DataFrame but with every row incremented by 1.

.. code:: python

    df.select(df["A"] + 1).show()

.. code:: none

    +-----------+
    |         A |
    |   INTEGER |
    +===========+
    |         2 |
    +-----------+
    |         3 |
    +-----------+
    |         4 |
    +-----------+
    |         5 |
    +-----------+
    (Showing first 4 of 4 rows)

.. NOTE::

    A common pattern is to create a new columns using:

    .. code:: python

        # Creates a new column named "foo" which takes on values
        # of column "A" incremented by 1
        df = df.with_column("foo", df["A"] + 1)

Congratulations, you have just written your first **Expression**: ``df["A"] + 1``!

Expressions
^^^^^^^^^^^

Expressions are how you define computations on your columns in Daft.

The world of Daft contains much more than just numbers, and you can do much more than just add numbers together. Daft's rich Expressions API allows you to do things such as:

1. Convert between different types with ``df["numbers"].cast(float)``
2. Download BYTES from a column containing STRING URLs using ``df["urls"].url.download()``
3. Run arbitrary Python functions on your data using ``df["objects"].apply(my_python_function)``

We are also constantly looking to improve Daft and add more Expression functionality. Please contribute to the project with your ideas and code if you have an Expression in mind!

For a deeper dive into Expressions, skip to: :doc:`data-processing-with-expressions`.

What now?
---------

This introduction covered the bare basics of interacting with Daft. The rest of the user guide will build on these basics and show you the features which really make Daft shine!

We suggest reading in detail the :doc:`data-processing-with-expressions` section for a good grasp on Expressions, but other sections of the guide can be read in order of necessity.
