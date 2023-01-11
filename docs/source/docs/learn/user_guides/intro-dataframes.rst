Introduction to DataFrames
==========================

Data in Daft is represented as a DataFrame, which is a collection of data organized as a **table** with **rows** and **columns**.

.. image:: /_static/daft_illustration.png
   :alt: Daft python dataframes make it easy to load any data such as PDF documents, images, protobufs, csv, parquet and audio files into a table dataframe structure for easy querying
   :width: 500
   :align: center

This document provides an overview on the Daft DataFrame:

1. **Schemas and Types**: DataFrame columns are typed and support arbitrary Python objects
2. **Lazy**: DataFrames execute lazily and are optimized before execution
3. **Distributed**: DataFrames can execute seamlessly both locally and on a cluster of machines

Schemas and Types
-----------------

Daft DataFrames have a schema. This merely means that columns have a **name** and a **type**, and that all data in that column will adhere to that type! The following is a list of Daft types and a short explanation of the types.

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

    More types are being added to Daft, including full support for all Arrow types, complex types such as nested lists and extension types to represent complex data such as images!

Under the hood, Daft represents data in the `Apache Arrow <https://arrow.apache.org/>`_ format, which allows it to efficiently represent and work on data using high-performance kernels which are written in Rust.

Python Types
^^^^^^^^^^^^

Additionally, Daft supports arbitrary Python objects as a type of column. This means that you can work with complex datatypes (images, documents, tensors, protobufs, Python dictionaries) in Daft with all your familiar libraries from the Python ecosystem such as NumPy, Torch, Pillow and more.

Null Values
^^^^^^^^^^^

Like Apache Arrow, Daft expresses missing data as a **Null** value. This is a special value that represents missing data in any type! This is in contrast with the popular Pandas library, where missing data is often represented as a float ``NaN`` (not-a-value).

Lazy
----

Daft DataFrames are **lazy**. This just means that Daft DataFrames will defer all its work until you tell it to execute.

In practice, this is extremely powerful as this lets Daft optimize your operations to minimize the amount of work it has to do. Here is a simple example to show off the power of laziness!

.. code:: python

    # (1): Read data from a CSV file
    df = daft.DataFrame.read_csv("iris.csv")

    # (2): Limit the dataframe to only the first 100 rows
    df = df.limit(100)

In a non-lazy DataFrame such as Pandas, the entire CSV file would be read eagerly in **Step (1)**. However because Daft defers all computation, it knows from **Step (2)** that only 100 rows are required and can optimize the CSV read task to read only the first 100 rows, saving computation time and memory!

Let's confirm that Daft is **lazy** and hasn't actually run any computations - printing our DataFrame at this point yields the following output:

.. code:: python

    df

    # Output:
    # +----------------+---------------+----------------+---------------+-----------+
    # | sepal.length   | sepal.width   | petal.length   | petal.width   | variety   |
    # | FLOAT          | FLOAT         | FLOAT          | FLOAT         | STRING    |
    # +================+===============+================+===============+===========+
    # +----------------+---------------+----------------+---------------+-----------+
    # (No data to display: Dataframe not materialized)

We see that the DataFrame's schema is printed, but also that the message ``(No data to display: Dataframe not materialized)`` is printed. This is because our current DataFrame has only a **plan** of what to do, but has not yet executed it.

.. NOTE::

    You can examine the plan of a DataFrame by calling ``DataFrame.explain()``!

    Passing the ``show_optimized=True`` argument will show you the plan after Daft applies its optimizations.

Now let's call ``DataFrame.collect()``, which will optimize the plan, execute it and materialize the results in memory:

.. code:: python

    df.collect()

    df

    # Output:
    # +----------------+---------------+----------------+---------------+-----------+
    # |   sepal.length |   sepal.width |   petal.length |   petal.width | variety   |
    # |          FLOAT |         FLOAT |          FLOAT |         FLOAT | STRING    |
    # +================+===============+================+===============+===========+
    # |            5.1 |           3.5 |            1.4 |           0.2 | Setosa    |
    # +----------------+---------------+----------------+---------------+-----------+
    # |            4.9 |           3   |            1.4 |           0.2 | Setosa    |
    # +----------------+---------------+----------------+---------------+-----------+
    # |            4.7 |           3.2 |            1.3 |           0.2 | Setosa    |
    # +----------------+---------------+----------------+---------------+-----------+
    # |            4.6 |           3.1 |            1.5 |           0.2 | Setosa    |
    # +----------------+---------------+----------------+---------------+-----------+
    # |            5   |           3.6 |            1.4 |           0.2 | Setosa    |
    # +----------------+---------------+----------------+---------------+-----------+
    # |            5.4 |           3.9 |            1.7 |           0.4 | Setosa    |
    # +----------------+---------------+----------------+---------------+-----------+
    # |            4.6 |           3.4 |            1.4 |           0.3 | Setosa    |
    # +----------------+---------------+----------------+---------------+-----------+
    # |            5   |           3.4 |            1.5 |           0.2 | Setosa    |
    # +----------------+---------------+----------------+---------------+-----------+
    # |            4.4 |           2.9 |            1.4 |           0.2 | Setosa    |
    # +----------------+---------------+----------------+---------------+-----------+
    # |            4.9 |           3.1 |            1.5 |           0.1 | Setosa    |
    # +----------------+---------------+----------------+---------------+-----------+
    # (Showing first 10 of 100 rows)

Now when the DataFrame object is printed again, Daft can also show a preview of the first 10 rows of the **materialized** DataFrame!

Moreover, any subsequent operations on this materialized DataFrame will avoid recomputing the entire DataFrame from scratch, and instead used the cached materialization of results as a starting point.

.. NOTE::

    There are some other methods that will explicitly tell Daft to execute the DataFrame:

    1. ``DataFrame.collect()``: execute computations on **all** rows and materialize the result in memory - subsequent operations on this DataFrame will be cached!
    2. ``DataFrame.show(N)``: execute the minimal amount of computation required to show the first ``N`` rows of the DataFrame
    3. ``DataFrame.write_*``: execute computations on **all** rows and write the results into a file or some other destination

Distributed
-----------

Daft DataFrames are built with an extremely modular interface to work with multiple different backends. By default, it runs on the ``PyRunner`` backend, which simply runs Daft operations on your local Python interpreter.

Additionally, Daft is also built to support distributed remote backends such as `Ray <www.ray.io>`_, allowing it to scale up to terabytes of data and thousands of cores on a cluster of machines!

.. code:: python

    # Switches Daft to use the Ray backend, connected to a remote Ray cluster
    daft.context.set_runner_ray(address="ray://my-ray-cluster.com")

    # Reads CSV in cloud storage
    df = daft.DataFrame.read_csv("s3://my-bucket/iris.csv")

    # Materializes the entire DataFrame into distributed memory on the Ray cluster
    df = df.collect()

This means that while Daft already provides an incredible local development experience, scaling up to much larger DataFrames is really easy!
