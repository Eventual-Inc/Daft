Introduction
============

Daft is a distributed query engine with a DataFrame API. The two key concepts to Daft are:

1. :class:`DataFrame <daft.DataFrame>`: a Table-like structure that represents rows and columns of data
2. :class:`Expression <daft.expressions.Expression>`: a symbolic representation of computation that transforms columns of the DataFrame to a new one.

With Daft, you create :class:`DataFrame <daft.DataFrame>` from a variety of sources (e.g. reading data from files, data catalogs or from Python dictionaries) and use :class:`Expression <daft.expressions.Expression>` to manipulate data in that DataFrame. Let's take a closer look at these two abstractions!

DataFrame
---------

Conceptually, a DataFrame is a "table" of data, with rows and columns.

.. image:: /_static/daft_illustration.png
   :alt: Daft python dataframes make it easy to load any data such as PDF documents, images, protobufs, csv, parquet and audio files into a table dataframe structure for easy querying
   :width: 500
   :align: center

Using this abstraction of a DataFrame, you can run common tabular operations such as:

1. Filtering rows: :meth:`df.where(...) <daft.DataFrame.where>`
2. Creating new columns as a computation of existing columns: :meth:`df.with_column(...) <daft.DataFrame.with_column>`
3. Joining two tables together: :meth:`df.join(...) <daft.DataFrame.join>`
4. Sorting a table by the values in specified column(s): :meth:`df.sort(...) <daft.DataFrame.sort>`
5. Grouping and aggregations: :meth:`df.groupby(...).agg(...) <daft.DataFrame.groupby>`

Daft DataFrames are:

1. **Distributed:** your data is split into *Partitions* and can be processed in parallel/on different machines
2. **Lazy:** computations are enqueued in a query plan which is then optimized and executed only when requested
3. **Multimodal:** columns can contain complex datatypes such as tensors, images and Python objects

Since Daft is lazy, it can actually execute the query plan on a variety of different backends. By default, it will run computations locally using Python multithreading. However if you need to scale to large amounts of data that cannot be processed on a single machine, using the Ray runner allows Daft to run computations on a `Ray <https://www.ray.io/>`_ cluster instead.

Expressions
-----------

The other important concept to understand when working with Daft are **expressions**.

Because Daft is "lazy", it needs a way to represent computations that need to be performed on its data so that it can execute these computations at some later time. The answer to this is an :class:`~daft.expressions.Expression`!

The simplest Expressions are:

1. The column expression: :func:`col("a") <daft.expressions.col>` which is used to refer to "some column named 'a'"
2. Or, if you already have an existing DataFrame ``df`` with a column named "a", you can refer to its column with Python's square bracket indexing syntax: ``df["a"]``
3. The literal expression: :func:`lit(100) <daft.expressions.lit>` which represents a column that always takes on the provided value

Daft then provides an extremely rich Expressions library to allow you to compose different computations that need to happen. For example:

.. code:: python

    from daft import col, DataType

    # Take the column named "a" and add 1 to each element
    col("a") + 1

    # Take the column named "a", cast it to a string and check each element, returning True if it starts with "1"
    col("a").cast(DataType.string()).str.startswith("1")

Expressions are used in DataFrame operations, and the names of these Expressions are resolved to column names on the DataFrame that they are running on. Here is an example:

.. code:: python

    import daft

    # Create a dataframe with a column "a" that has values [1, 2, 3]
    df = daft.from_pydict({"a": [1, 2, 3]})

    # Create new columns called "a_plus_1" and "a_startswith_1" using Expressions
    df = df.select(
        col("a"),
        (col("a") + 1).alias("a_plus_1"),
        col("a").cast(DataType.string()).str.startswith("1").alias("a_startswith_1"),
    )

    df.show()

.. code:: none

    +---------+------------+------------------+
    |       a |   a_plus_1 | a_startswith_1   |
    |   Int64 |      Int64 | Boolean          |
    +=========+============+==================+
    |       1 |          2 | true             |
    +---------+------------+------------------+
    |       2 |          3 | false            |
    +---------+------------+------------------+
    |       3 |          4 | false            |
    +---------+------------+------------------+
    (Showing first 3 rows)
