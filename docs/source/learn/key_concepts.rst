Key Concepts
============

This guide introduces all the key concepts in Daft for a new user to get up-and-running.

DataFrame
---------

Conceptually, a DataFrame is a "table" of data, with rows and columns.

.. image:: /_static/daft_illustration.png
   :alt: Daft python dataframes make it easy to load any data such as PDF documents, images, protobufs, csv, parquet and audio files into a table dataframe structure for easy querying
   :width: 500
   :align: center

Using this abstraction of a DataFrame, you can run common tabular operations such as:

1. Filters: :meth:`df.where(...) <daft.DataFrame.where>`
2. Creating new columns as a computation of existing columns: :meth:`df.with_column(...) <daft.DataFrame.with_column>`
3. Joining two tables together: :meth:`df.join(...) <daft.DataFrame.join>`
4. Sorting a table by the values in specified column(s): :meth:`df.sort(...) <daft.DataFrame.sort>`
5. Grouping and aggregations: :meth:`df.groupby(...).agg(...) <daft.DataFrame.groupby>`

Daft DataFrames are:

1. **Distributed:** your data is split into *Partitions* and can be processed in parallel
2. **Lazy:** computations are enqueued in a query plan, and only executed when requested
3. **Complex:** columns can contain complex datatypes such as tensors, images and Python objects

Distributed
^^^^^^^^^^^

Under the hood, Daft splits your DataFrame into **partitions** according to a **partitioning scheme**. This allows Daft to assign different partitions to different machines in a cluster and parallelize any work that needs to be performed across the resources on these machines!

.. NOTE::

    See the user guide: :doc:`user_guides/partitioning` for more details on working efficiently with partitions!

Daft provides different ``Runners`` that your DataFrame can use for execution. Different Runners can use different backends for running your DataFrame computations - for example, the default multithreaded Python runner will process your partitions of data using multithreading but the Ray runner can run computations on your partitions on a `Ray <https://www.ray.io/>`_ cluster instead.

.. NOTE::

    See the user guide: :doc:`user_guides/scaling-up` for more details on utilizing different Daft runners!

Lazy
^^^^

Daft does not execute the computations defined on a DataFrame until explicitly instructed to do so!

.. code:: python

    import daft

    # Create a new dataframe with one column
    df = daft.from_pydict({"a": [1, 2, 3]})

    # Create a new column which is column "a" incremented by 1
    df = df.with_column("b", df["a"] + 1)

    # Print the DataFrame
    df

.. code:: none

    +---------+---------+
    | a       | b       |
    | Int64   | Int64   |
    +=========+=========+
    +---------+---------+
    (No data to display: Dataframe not materialized)

Notice that when printing the DataFrame, Daft will say that there is "No data to display". This is because Daft enqueues all your operations into a "query plan" instead of executing it immediately when you define your operations.

To actually execute your DataFrame, you can call a method such as :meth:`df.show() <daft.DataFrame.show>`. This method will run just the necessary computation required to show the first few rows of your DataFrame:

.. code:: python

    df.show()

.. code:: none

    +---------+---------+
    |       a |       b |
    |   Int64 |   Int64 |
    +=========+=========+
    |       1 |       2 |
    +---------+---------+
    |       2 |       3 |
    +---------+---------+
    |       3 |       4 |
    +---------+---------+
    (Showing first 3 rows)

Being "lazy" allows Daft to apply really interesting query optimizations to your DataFrame when it actually executes!

.. NOTE::

    See user guide: :doc:`user_guides/intro-dataframes` for more details!

Complex
^^^^^^^

Daft defines interesting types and operations over the data in your DataFrame. For example, working with URLs is really easy with Daft:

.. code:: python

    import daft

    # Create a new dataframe with just one column of URLs
    df = daft.from_pydict({"urls": ["https://www.google.com", "https://www.yahoo.com", "https://www.bing.com"]})

    # Create a new column which contains the downloaded bytes from each URL
    df = df.with_column("url_contents", df["urls"].url.download())

    # Print the DataFrame
    df.show()

.. code:: none

    +----------------------+----------------------+
    | urls                 | url_contents         |
    | Utf8                 | Binary               |
    +======================+======================+
    | https://www.google.c | b'<!doctype          |
    | om                   | html><html           |
    |                      | itemscope="" itemtyp |
    |                      | e="http://sche...    |
    +----------------------+----------------------+
    | https://www.yahoo.co | b'<!doctype          |
    | m                    | html><html id=atomic |
    |                      | class="ltr  desktop  |
    |                      | fp-...               |
    +----------------------+----------------------+
    | https://www.bing.com | b'<!doctype          |
    |                      | html><html lang="en" |
    |                      | dir="ltr"><head><met |
    |                      | a na...              |
    +----------------------+----------------------+
    (Showing first 3 rows)

Similarly, working with complex types such as images, tensors, Python objects and more are greatly simplified when using Daft!

Expressions
-----------

The other important concept to understand when working with Daft are **expressions**.

Because Daft is "lazy", it needs a way to represent computations that need to be performed on its data so that it can execute these computations at some later time. The answer to this is an :class:`~daft.expressions.Expression`!

The simplest Expressions are:

1. The column expression: :func:`col("a") <daft.expressions.col>` which is used to refer to "some column named 'a'"
2. Or, if you already have an existing DataFrame ``df`` with a column named "a", you can refer to its column like we did before with square brackets: ``df["a"]``
3. The literal expression: :func:`lit(100) <daft.expressions.lit>` which represents a column that always takes on the provided value

Daft then provides an extremely rich Expressions library to allow you to compose different computations that need to happen. For example:

.. code:: python

    from daft import col, DataType

    # Take the column named "a" and add 1 to each element
    col("a") + 1

    # Take the column named "a", cast it to a string and check each element, returning True if it starts with "1"
    col("a").cast(DataType.string()).str.startswith("1")

Note that Expressions aren't very useful just by themselves! They are used in DataFrame operations, and the names of these Expressions are resolved to column names on the DataFrame that they are running on. Here is an example:

.. code:: python

    import daft

    df = daft.from_pydict({"a": [1, 2, 3]})

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

.. NOTE::

    See user guide: :doc:`user_guides/expressions` for more details!
