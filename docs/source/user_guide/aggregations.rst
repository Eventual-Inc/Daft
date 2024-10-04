Aggregations and Grouping
=========================

Some operations such as the sum or the average of a column are called **aggregations**. Aggregations are operations that reduce the number of rows in a column.

Global Aggregations
-------------------

An aggregation can be applied on an entire DataFrame, for example to get the mean on a specific column:

.. code:: python

    import daft

    df = daft.from_pydict({
        "class": ["a", "a", "b", "b"],
        "score": [10, 20., 30., 40],
    })

    df.mean("score").show()

.. code:: none

    ╭─────────╮
    │ score   │
    │ ---     │
    │ Float64 │
    ╞═════════╡
    │ 25      │
    ╰─────────╯

    (Showing first 1 of 1 rows)

For a full list of available Dataframe aggregations, see: :ref:`df-aggregations`.

Aggregations can also be mixed and matched across columns, via the `agg` method:

.. code:: python

    df.agg(
        df["score"].mean().alias("mean_score"),
        df["score"].max().alias("max_score"),
        df["class"].count().alias("class_count"),
    ).show()

.. code:: none

    ╭────────────┬───────────┬─────────────╮
    │ mean_score ┆ max_score ┆ class_count │
    │ ---        ┆ ---       ┆ ---         │
    │ Float64    ┆ Float64   ┆ UInt64      │
    ╞════════════╪═══════════╪═════════════╡
    │ 25         ┆ 40        ┆ 4           │
    ╰────────────┴───────────┴─────────────╯

    (Showing first 1 of 1 rows)

For a full list of available aggregation expressions, see: :ref:`Aggregation Expressions <api=aggregation-expression>`

Grouped Aggregations
--------------------

Aggregations can also be called on a "Grouped DataFrame". For the above example, perhaps we want to get the mean "score" not for the entire DataFrame, but for each "class".

Let's run the mean of column "score" again, but this time grouped by "class":

.. code:: python

    df.groupby("class").mean("score").show()

.. code:: none

    ╭───────┬─────────╮
    │ class ┆ score   │
    │ ---   ┆ ---     │
    │ Utf8  ┆ Float64 │
    ╞═══════╪═════════╡
    │ a     ┆ 15      │
    ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
    │ b     ┆ 35      │
    ╰───────┴─────────╯

    (Showing first 2 of 2 rows)

To run multiple aggregations on a Grouped DataFrame, you can use the `agg` method:

.. code:: python

    df.groupby("class").agg(
        df["score"].mean().alias("mean_score"),
        df["score"].max().alias("max_score"),
    ).show()

.. code:: none

    ╭───────┬────────────┬───────────╮
    │ class ┆ mean_score ┆ max_score │
    │ ---   ┆ ---        ┆ ---       │
    │ Utf8  ┆ Float64    ┆ Float64   │
    ╞═══════╪════════════╪═══════════╡
    │ a     ┆ 15         ┆ 20        │
    ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
    │ b     ┆ 35         ┆ 40        │
    ╰───────┴────────────┴───────────╯

    (Showing first 2 of 2 rows)
