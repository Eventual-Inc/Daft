Basic Concepts
==============

Daft is a distributed data engine. The main abstraction in Daft is the :class:`DataFrame <daft.DataFrame>`, which conceptually can be thought of as a "table" of data with rows and columns.

Daft also exposes a :doc:`sql` interface which interoperates closely with the DataFrame interface, allowing you to express data transformations and queries on your tables as SQL strings.

.. image:: /_static/daft_illustration.png
   :alt: Daft python dataframes make it easy to load any data such as PDF documents, images, protobufs, csv, parquet and audio files into a table dataframe structure for easy querying
   :width: 500
   :align: center

Terminology
-----------

DataFrames
^^^^^^^^^^

The :class:`DataFrame <daft.DataFrame>` is the core concept in Daft. Think of it as a table with rows and columns, similar to a spreadsheet or a database table. It's designed to handle large amounts of data efficiently.

Daft DataFrames are lazy. This means that calling most methods on a DataFrame will not execute that operation immediately - instead, DataFrames expose explicit methods such as :meth:`daft.DataFrame.show` and :meth:`daft.DataFrame.write_parquet`
which will actually trigger computation of the DataFrame.

Expressions
^^^^^^^^^^^

An :class:`Expression <daft.expressions.Expression>` is a fundamental concept in Daft that allows you to define computations on DataFrame columns. They are the building blocks for transforming and manipulating data
within your DataFrame and will be your best friend if you are working with Daft primarily using the Python API.

Query Plan
^^^^^^^^^^

As mentioned earlier, Daft DataFrames are lazy. Under the hood, each DataFrame in Daft is represented by a plan of operations that describes how to compute that DataFrame.

This plan is called the "query plan" and calling methods on the DataFrame actually adds steps to the query plan!

When your DataFrame is executed, Daft will read this plan, optimize it to make it run faster and then execute it to compute the requested results.

Structured Query Language (SQL)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

SQL is a common query language for expressing queries over tables of data. Daft exposes a SQL API as an alternative (but often also complementary API) to the Python :class:`DataFrame <daft.DataFrame>` and
:class:`Expression <daft.expressions.Expression>` APIs for building queries.

You can use SQL in Daft via the :func:`daft.sql` function, and Daft will also convert many SQL-compatible strings into Expressions via :func:`daft.sql_expr` for easy interoperability with DataFrames.

DataFrame
---------

If you are coming from other DataFrame libraries such as Pandas or Polars, here are some key differences about Daft DataFrames:

1. **Distributed:** When running in a distributed cluster, Daft splits your data into smaller "chunks" called *Partitions*. This allows Daft to process your data in parallel across multiple machines, leveraging more resources to work with large datasets.

2. **Lazy:** When you write operations on a DataFrame, Daft doesn't execute them immediately. Instead, it creates a plan (called a query plan) of what needs to be done. This plan is optimized and only executed when you specifically request the results, which can lead to more efficient computations.

3. **Multimodal:** Unlike traditional tables that usually contain simple data types like numbers and text, Daft DataFrames can handle complex data types in its columns. This includes things like images, audio files, or even custom Python objects.

Common data operations that you would perform on DataFrames are:

1. **Filtering rows:** Use :meth:`df.where(...) <daft.DataFrame.where>` to keep only the rows that meet certain conditions.
2. **Creating new columns:** Use :meth:`df.with_column(...) <daft.DataFrame.with_column>` to add a new column based on calculations from existing ones.
3. **Joining tables:** Use :meth:`df.join(other_df, ...) <daft.DataFrame.join>` to combine two DataFrames based on common columns.
4. **Sorting:** Use :meth:`df.sort(...) <daft.DataFrame.sort>` to arrange your data based on values in one or more columns.
5. **Grouping and aggregating:** Use :meth:`df.groupby(...).agg(...) <daft.DataFrame.groupby>` to summarize your data by groups.

Creating a Dataframe
^^^^^^^^^^^^^^^^^^^^

.. seealso::

    :doc:`read-and-write` - a more in-depth guide on various options for reading/writing data to/from Daft DataFrames from in-memory data (Python, Arrow), files (Parquet, CSV, JSON), SQL Databases and Data Catalogs

Let's create our first Dataframe from a Python dictionary of columns.

.. tabs::

    .. group-tab:: 🐍 Python

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

.. code-block:: text
    :caption: Output

    ╭───────┬─────────┬─────────┬──────╮
    │ A     ┆ B       ┆ C       ┆ D    │
    │ ---   ┆ ---     ┆ ---     ┆ ---  │
    │ Int64 ┆ Float64 ┆ Boolean ┆ Null │
    ╞═══════╪═════════╪═════════╪══════╡
    │ 1     ┆ 1.5     ┆ true    ┆ None │
    ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    │ 2     ┆ 2.5     ┆ true    ┆ None │
    ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    │ 3     ┆ 3.5     ┆ false   ┆ None │
    ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    │ 4     ┆ 4.5     ┆ false   ┆ None │
    ╰───────┴─────────┴─────────┴──────╯

    (Showing first 4 of 4 rows)


Congratulations - you just created your first DataFrame! It has 4 columns, "A", "B", "C", and "D". Let's try to select only the "A", "B", and "C" columns:

.. tabs::

    .. group-tab:: 🐍 Python

        .. code:: python

            df = df.select("A", "B", "C")
            df

    .. group-tab:: ⚙️ SQL

        .. code:: python

            df = daft.sql("SELECT A, B, C FROM df")
            df

.. code-block:: text
    :caption: Output

    ╭───────┬─────────┬─────────╮
    │ A     ┆ B       ┆ C       │
    │ ---   ┆ ---     ┆ ---     │
    │ Int64 ┆ Float64 ┆ Boolean │
    ╰───────┴─────────┴─────────╯

    (No data to display: Dataframe not materialized)


But wait - why is it printing the message ``(No data to display: Dataframe not materialized)`` and where are the rows of each column?

Executing our DataFrame and Viewing Data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The reason that our DataFrame currently does not display its rows is that Daft DataFrames are **lazy**. This just means that Daft DataFrames will defer all its work until you tell it to execute.

In this case, Daft is just deferring the work required to read the data and select columns, however in practice this laziness can be very useful for helping Daft optimize your queries before execution!

.. NOTE::

    When you call methods on a Daft Dataframe, it defers the work by adding to an internal "plan". You can examine the current plan of a DataFrame by calling :meth:`df.explain() <daft.DataFrame.explain>`!

    Passing the ``show_all=True`` argument will show you the plan after Daft applies its query optimizations and the physical (lower-level) plan.

    .. code-block:: text
        :caption: Plan Output

        == Unoptimized Logical Plan ==

        * Project: col(A), col(B), col(C)
        |
        * Source:
        |   Number of partitions = 1
        |   Output schema = A#Int64, B#Float64, C#Boolean, D#Null


        == Optimized Logical Plan ==

        * Project: col(A), col(B), col(C)
        |
        * Source:
        |   Number of partitions = 1
        |   Output schema = A#Int64, B#Float64, C#Boolean, D#Null


        == Physical Plan ==

        * Project: col(A), col(B), col(C)
        |   Clustering spec = { Num partitions = 1 }
        |
        * InMemoryScan:
        |   Schema = A#Int64, B#Float64, C#Boolean, D#Null,
        |   Size bytes = 65,
        |   Clustering spec = { Num partitions = 1 }

We can tell Daft to execute our DataFrame and store the results in-memory using :meth:`df.collect() <daft.DataFrame.collect>`:

.. tabs::

    .. group-tab:: 🐍 Python

        .. code:: python

            df.collect()
            df

.. code-block:: text
    :caption: Output

    ╭───────┬─────────┬─────────┬──────╮
    │ A     ┆ B       ┆ C       ┆ D    │
    │ ---   ┆ ---     ┆ ---     ┆ ---  │
    │ Int64 ┆ Float64 ┆ Boolean ┆ Null │
    ╞═══════╪═════════╪═════════╪══════╡
    │ 1     ┆ 1.5     ┆ true    ┆ None │
    ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    │ 2     ┆ 2.5     ┆ true    ┆ None │
    ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    │ 3     ┆ 3.5     ┆ false   ┆ None │
    ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    │ 4     ┆ 4.5     ┆ false   ┆ None │
    ╰───────┴─────────┴─────────┴──────╯

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

.. tabs::

    .. group-tab:: 🐍 Python

        .. code:: python

            df = df.sort("A")  # expensive sort
            df.collect()  # materialize the DataFrame

            # All subsequent work on df avoids recomputing previous steps
            df.sum("B").show()
            df.mean("B").show()
            df.with_column("try_this", df["A"] + 1).show(5)

    .. group-tab:: ⚙️ SQL

        .. code:: python

            df = daft.sql("SELECT * FROM df ORDER BY A")
            df.collect()

            # All subsequent work on df avoids recomputing previous steps
            daft.sql("SELECT sum(B) FROM df").show()
            daft.sql("SELECT mean(B) FROM df").show()
            daft.sql("SELECT *, (A + 1) AS try_this FROM df").show(5)

.. code-block:: text
    :caption: Output

    ╭─────────╮
    │ B       │
    │ ---     │
    │ Float64 │
    ╞═════════╡
    │ 12      │
    ╰─────────╯

    (Showing first 1 of 1 rows)

    ╭─────────╮
    │ B       │
    │ ---     │
    │ Float64 │
    ╞═════════╡
    │ 3       │
    ╰─────────╯

    (Showing first 1 of 1 rows)

    ╭───────┬─────────┬─────────┬──────────╮
    │ A     ┆ B       ┆ C       ┆ try_this │
    │ ---   ┆ ---     ┆ ---     ┆ ---      │
    │ Int64 ┆ Float64 ┆ Boolean ┆ Int64    │
    ╞═══════╪═════════╪═════════╪══════════╡
    │ 1     ┆ 1.5     ┆ true    ┆ 2        │
    ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
    │ 2     ┆ 2.5     ┆ true    ┆ 3        │
    ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
    │ 3     ┆ 3.5     ┆ false   ┆ 4        │
    ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
    │ 4     ┆ 4.5     ┆ false   ┆ 5        │
    ╰───────┴─────────┴─────────┴──────────╯

    (Showing first 4 of 4 rows)


In many other cases however, there are better options than materializing your entire DataFrame with :meth:`df.collect() <daft.DataFrame.collect>`:

1. **Peeking with df.show(N)**: If you only want to "peek" at the first few rows of your data for visualization purposes, you can use :meth:`df.show(N) <daft.DataFrame.show>`, which processes and shows only the first ``N`` rows.
2. **Writing to disk**: The ``df.write_*`` methods will process and write your data to disk per-partition, avoiding materializing it all in memory at once.
3. **Pruning data**: You can materialize your DataFrame after performing a :meth:`df.limit() <daft.DataFrame.limit>`, :meth:`df.where() <daft.DataFrame.where>` or :meth:`df.select() <daft.DataFrame.select>` operation which processes your data or prune it down to a smaller size.

Schemas and Types
^^^^^^^^^^^^^^^^^

Notice also that when we printed our DataFrame, Daft displayed its **schema**. Each column of your DataFrame has a **name** and a **type**, and all data in that column will adhere to that type!

Daft can display your DataFrame's schema without materializing it. Under the hood, it performs intelligent sampling of your data to determine the appropriate schema, and if you make any modifications to your DataFrame it can infer the resulting types based on the operation.

.. NOTE::

    Under the hood, Daft represents data in the `Apache Arrow <https://arrow.apache.org/>`_ format, which allows it to efficiently represent and work on data using high-performance kernels which are written in Rust.


Running Computation with Expressions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To run computations on data in our DataFrame, we use Expressions.

The following statement will :meth:`df.show() <daft.DataFrame.show>` a DataFrame that has only one column - the column ``A`` from our original DataFrame but with every row incremented by 1.

.. tabs::

    .. group-tab:: 🐍 Python

        .. code:: python

            df.select(df["A"] + 1).show()

    .. group-tab:: ⚙️ SQL

        .. code:: python

            daft.sql("SELECT A + 1 FROM df").show()

.. code-block:: text
    :caption: Output

    ╭───────╮
    │ A     │
    │ ---   │
    │ Int64 │
    ╞═══════╡
    │ 2     │
    ├╌╌╌╌╌╌╌┤
    │ 3     │
    ├╌╌╌╌╌╌╌┤
    │ 4     │
    ├╌╌╌╌╌╌╌┤
    │ 5     │
    ╰───────╯

    (Showing first 4 of 4 rows)

.. NOTE::

    A common pattern is to create a new columns using ``DataFrame.with_column``:

    .. tabs::

        .. group-tab:: 🐍 Python

            .. code:: python

                # Creates a new column named "foo" which takes on values
                # of column "A" incremented by 1
                df = df.with_column("foo", df["A"] + 1)
                df.show()

        .. group-tab:: ⚙️ SQL

            .. code:: python

                # Creates a new column named "foo" which takes on values
                # of column "A" incremented by 1
                df = daft.sql("SELECT *, A + 1 AS foo FROM df")
                df.show()

.. code-block:: text
    :caption: Output

    ╭───────┬─────────┬─────────┬───────╮
    │ A     ┆ B       ┆ C       ┆ foo   │
    │ ---   ┆ ---     ┆ ---     ┆ ---   │
    │ Int64 ┆ Float64 ┆ Boolean ┆ Int64 │
    ╞═══════╪═════════╪═════════╪═══════╡
    │ 1     ┆ 1.5     ┆ true    ┆ 2     │
    ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    │ 2     ┆ 2.5     ┆ true    ┆ 3     │
    ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    │ 3     ┆ 3.5     ┆ false   ┆ 4     │
    ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    │ 4     ┆ 4.5     ┆ false   ┆ 5     │
    ╰───────┴─────────┴─────────┴───────╯

    (Showing first 4 of 4 rows)

Congratulations, you have just written your first **Expression**: ``df["A"] + 1``!

Expressions are a powerful way of describing computation on columns. For more details, check out the next section on :doc:`expressions`
