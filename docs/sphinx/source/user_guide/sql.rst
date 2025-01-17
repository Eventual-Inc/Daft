SQL
===

Daft supports Structured Query Language (SQL) as a way of constructing query plans (represented in Python as a :class:`daft.DataFrame`) and expressions (:class:`daft.Expression`).

SQL is a human-readable way of constructing these query plans, and can often be more ergonomic than using DataFrames for writing queries.

.. NOTE::
    Daft's SQL support is new and is constantly being improved on! Please give us feedback and we'd love to hear more about what you would like.

Running SQL on DataFrames
-------------------------

Daft's :func:`daft.sql` function will automatically detect any :class:`daft.DataFrame` objects in your current Python environment to let you query them easily by name.

.. tabs::

    .. group-tab:: ⚙️ SQL

        .. code:: python

            # Note the variable name `my_special_df`
            my_special_df = daft.from_pydict({"A": [1, 2, 3], "B": [1, 2, 3]})

            # Use the SQL table name "my_special_df" to refer to the above DataFrame!
            sql_df = daft.sql("SELECT A, B FROM my_special_df")

            sql_df.show()

.. code-block:: text
    :caption: Output

    ╭───────┬───────╮
    │ A     ┆ B     │
    │ ---   ┆ ---   │
    │ Int64 ┆ Int64 │
    ╞═══════╪═══════╡
    │ 1     ┆ 1     │
    ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    │ 2     ┆ 2     │
    ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    │ 3     ┆ 3     │
    ╰───────┴───────╯

    (Showing first 3 of 3 rows)

In the above example, we query the DataFrame called `"my_special_df"` by simply referring to it in the SQL command. This produces a new DataFrame `sql_df` which can
natively integrate with the rest of your Daft query.

Reading data from SQL
---------------------

.. WARNING::

    This feature is a WIP and will be coming soon! We will support reading common datasources directly from SQL:

    .. code-block:: python

        daft.sql("SELECT * FROM read_parquet('s3://...')")
        daft.sql("SELECT * FROM read_delta_lake('s3://...')")

    Today, a workaround for this is to construct your dataframe in Python first and use it from SQL instead:

    .. code-block:: python

        df = daft.read_parquet("s3://...")
        daft.sql("SELECT * FROM df")

    We appreciate your patience with us and hope to deliver this crucial feature soon!

SQL Expressions
---------------

SQL has the concept of expressions as well. Here is an example of a simple addition expression, adding columns "a" and "b" in SQL to produce a new column C.

We also present here the equivalent query for SQL and DataFrame. Notice how similar the concepts are!

.. tabs::

    .. group-tab:: ⚙️ SQL

        .. code:: python

            df = daft.from_pydict({"A": [1, 2, 3], "B": [1, 2, 3]})
            df = daft.sql("SELECT A + B as C FROM df")
            df.show()

    .. group-tab:: 🐍 Python

        .. code:: python

            expr = (daft.col("A") + daft.col("B")).alias("C")

            df = daft.from_pydict({"A": [1, 2, 3], "B": [1, 2, 3]})
            df = df.select(expr)
            df.show()

.. code-block:: text
    :caption: Output

    ╭───────╮
    │ C     │
    │ ---   │
    │ Int64 │
    ╞═══════╡
    │ 2     │
    ├╌╌╌╌╌╌╌┤
    │ 4     │
    ├╌╌╌╌╌╌╌┤
    │ 6     │
    ╰───────╯

    (Showing first 3 of 3 rows)

In the above query, both the SQL version of the query and the DataFrame version of the query produce the same result.

Under the hood, they run the same Expression ``col("A") + col("B")``!

One really cool trick you can do is to use the :func:`daft.sql_expr` function as a helper to easily create Expressions. The following are equivalent:

.. tabs::

    .. group-tab:: ⚙️ SQL

        .. code:: python

            sql_expr = daft.sql_expr("A + B as C")
            print("SQL expression:", sql_expr)

    .. group-tab:: 🐍 Python

        .. code:: python

            py_expr = (daft.col("A") + daft.col("B")).alias("C")
            print("Python expression:", py_expr)


.. code-block:: text
    :caption: Output

    SQL expression: col(A) + col(B) as C
    Python expression: col(A) + col(B) as C

This means that you can pretty much use SQL anywhere you use Python expressions, making Daft extremely versatile at mixing workflows which leverage both SQL and Python.

As an example, consider the filter query below and compare the two equivalent Python and SQL queries:

.. tabs::

    .. group-tab:: ⚙️ SQL

        .. code:: python

            df = daft.from_pydict({"A": [1, 2, 3], "B": [1, 2, 3]})

            # Daft automatically converts this string using `daft.sql_expr`
            df = df.where("A < 2")

            df.show()

    .. group-tab:: 🐍 Python

        .. code:: python

            df = daft.from_pydict({"A": [1, 2, 3], "B": [1, 2, 3]})

            # Using Daft's Python Expression API
            df = df.where(df["A"] < 2)

            df.show()

.. code-block:: text
    :caption: Output

    ╭───────┬───────╮
    │ A     ┆ B     │
    │ ---   ┆ ---   │
    │ Int64 ┆ Int64 │
    ╞═══════╪═══════╡
    │ 1     ┆ 1     │
    ╰───────┴───────╯

    (Showing first 1 of 1 rows)

Pretty sweet! Of course, this support for running Expressions on your columns extends well beyond arithmetic as we'll see in the next section on SQL Functions.

SQL Functions
-------------

SQL also has access to all of Daft's powerful :class:`daft.Expression` functionality through SQL functions.

However, unlike the Python Expression API which encourages method-chaining (e.g. ``col("a").url.download().image.decode()``), in SQL you have to do function nesting instead (e.g. ``"image_decode(url_download(a))""``).

.. NOTE::

    A full catalog of the available SQL Functions in Daft is available in the :doc:`../api_docs/sql`.

    Note that it closely mirrors the Python API, with some function naming differences vs the available Python methods.
    We also have some aliased functions for ANSI SQL-compliance or familiarity to users coming from other common SQL dialects such as PostgreSQL and SparkSQL to easily find their functionality.

Here is an example of an equivalent function call in SQL vs Python:

.. tabs::

    .. group-tab:: ⚙️ SQL

        .. code:: python

            df = daft.from_pydict({"urls": [
                "https://user-images.githubusercontent.com/17691182/190476440-28f29e87-8e3b-41c4-9c28-e112e595f558.png",
                "https://user-images.githubusercontent.com/17691182/190476440-28f29e87-8e3b-41c4-9c28-e112e595f558.png",
                "https://user-images.githubusercontent.com/17691182/190476440-28f29e87-8e3b-41c4-9c28-e112e595f558.png",
            ]})
            df = daft.sql("SELECT image_decode(url_download(urls)) FROM df")
            df.show()

    .. group-tab:: 🐍 Python

        .. code:: python

            df = daft.from_pydict({"urls": [
                "https://user-images.githubusercontent.com/17691182/190476440-28f29e87-8e3b-41c4-9c28-e112e595f558.png",
                "https://user-images.githubusercontent.com/17691182/190476440-28f29e87-8e3b-41c4-9c28-e112e595f558.png",
                "https://user-images.githubusercontent.com/17691182/190476440-28f29e87-8e3b-41c4-9c28-e112e595f558.png",
            ]})
            df = df.select(daft.col("urls").url.download().image.decode())
            df.show()

.. code-block:: text
    :caption: Output

    ╭──────────────╮
    │ urls         │
    │ ---          │
    │ Image[MIXED] │
    ╞══════════════╡
    │ <Image>      │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ <Image>      │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ <Image>      │
    ╰──────────────╯

    (Showing first 3 of 3 rows)
