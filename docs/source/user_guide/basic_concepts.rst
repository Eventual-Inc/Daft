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

.. code-block:: text
    :caption: Output

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

.. code-block:: text
    :caption: Output

    +---------+-----------+-----------+
    | A       | B         | C         |
    | Int64   | Float64   | Boolean   |
    +=========+===========+===========+
    +---------+-----------+-----------+
    (No data to display: Dataframe not materialized)


But wait - why is it printing the message ``(No data to display: Dataframe not materialized)`` and where are the rows of each column?

Executing our DataFrame and Viewing Data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The reason that our DataFrame currently does not display its rows is that Daft DataFrames are **lazy**. This just means that Daft DataFrames will defer all its work until you tell it to execute.

In this case, Daft is just deferring the work required to read the data and select columns, however in practice this laziness can be very useful for helping Daft optimize your queries before execution!

.. NOTE::

    When you call methods on a Daft Dataframe, it defers the work by adding to an internal "plan". You can examine the current plan of a DataFrame by calling :meth:`df.explain() <daft.DataFrame.explain>`!

    Passing the ``show_all=True`` argument will show you the plan after Daft applies its query optimizations and the physical (lower-level) plan.

We can tell Daft to execute our DataFrame and cache the results using :meth:`df.collect() <daft.DataFrame.collect>`:

.. code:: python

    df.collect()
    df

.. code-block:: text
    :caption: Output

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
^^^^^^^^^^^^^^^^^

Notice also that when we printed our DataFrame, Daft displayed its **schema**. Each column of your DataFrame has a **name** and a **type**, and all data in that column will adhere to that type!

Daft can display your DataFrame's schema without materializing it. Under the hood, it performs intelligent sampling of your data to determine the appropriate schema, and if you make any modifications to your DataFrame it can infer the resulting types based on the operation.

.. NOTE::

    Under the hood, Daft represents data in the `Apache Arrow <https://arrow.apache.org/>`_ format, which allows it to efficiently represent and work on data using high-performance kernels which are written in Rust.


Running Computations
^^^^^^^^^^^^^^^^^^^^

To run computations on data in our DataFrame, we use Expressions.

The following statement will :meth:`df.show() <daft.DataFrame.show>` a DataFrame that has only one column - the column ``A`` from our original DataFrame but with every row incremented by 1.

.. code:: python

    df.select(df["A"] + 1).show()

.. code-block:: text
    :caption: Output

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
-----------

Expressions are how you can express computations that should be run over columns of data.

Creating Expressions
^^^^^^^^^^^^^^^^^^^^

Referring to a column in a DataFrame
####################################

Most commonly you will be creating expressions by using the :func:`daft.col` function.

.. code:: python

    # Refers to column "A"
    daft.col("A")

.. code-block:: text
    :caption: Output

    col(A)

The above code creates an Expression that refers to a column named ``"A"``.

Using SQL
#########

Daft can also parse valid SQL as expressions.

.. code:: python

    from daft import sql_expr

    sql_expr("A + 1")

.. code-block:: text
    :caption: Output

    col(A) + lit(1)

The above code will create an expression representing "the column named 'x' incremented by 1".

Literals
########

You may find yourself needing to hardcode a "single value" oftentimes as an expression. Daft provides a :func:`~daft.expressions.lit` helper to do so:

.. code:: python

    from daft import lit

    # Refers to an expression which always evaluates to 42
    lit(42)

.. code-block:: text
    :caption: Output

    lit(42)

This special :func:`~daft.expressions.lit` expression we just created evaluates always to the value ``42``.

Wildcard Expressions
####################

You can create expressions on multiple columns at once using a wildcard. The expression `col("*")` selects every column in a DataFrame, and you can operate on this expression in the same way as a single column:

.. code:: python

    import daft
    from daft import col

    df = daft.from_pydict({"A": [1, 2, 3], "B": [4, 5, 6]})
    df.select(col("*") * 3).show()

.. code-block:: text
    :caption: Output

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

Composing Expressions
^^^^^^^^^^^^^^^^^^^^^

.. _userguide-numeric-expressions:

Numeric Expressions
###################

Since column "A" is an integer, we can run numeric computation such as addition, division and checking its value. Here are some examples where we create new columns using the results of such computations:

.. code:: python

    # Add 1 to each element in column "A"
    df = df.with_column("A_add_one", df["A"] + 1)

    # Divide each element in column A by 2
    df = df.with_column("A_divide_two", df["A"] / 2.)

    # Check if each element in column A is more than 1
    df = df.with_column("A_gt_1", df["A"] > 1)

    df.collect()

.. code-block:: text
    :caption: Output

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
##################

Daft also lets you have columns of strings in a DataFrame. Let's take a look!

.. code:: python

    df = daft.from_pydict({"B": ["foo", "bar", "baz"]})
    df.show()

.. code-block:: text
    :caption: Output

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

.. code-block:: text
    :caption: Output

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

.. code-block:: text
    :caption: Output

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
###############

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

.. code-block:: text
    :caption: Output

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
################

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

.. code-block:: text
    :caption: Output

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
###################

Logical Expressions are an expression that refers to a column of type :meth:`Boolean <daft.DataType.boolean>`, and can only take on the values True or False.

.. code:: python

    df = daft.from_pydict({"C": [True, False, True]})
    df["C"]

Daft supports logical operations such as ``&`` (and) and ``|`` (or) between logical expressions.

Comparisons
###########

Many of the types in Daft support comparisons between expressions that returns a Logical Expression.

For example, here we can compare if each element in column "A" is equal to elements in column "B":

.. code:: python

    df = daft.from_pydict({"A": [1, 2, 3], "B": [1, 2, 4]})

    df = df.with_column("A_eq_B", df["A"] == df["B"])

    df.collect()

.. code-block:: text
    :caption: Output

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
###############

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

.. code-block:: text
    :caption: Output

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
