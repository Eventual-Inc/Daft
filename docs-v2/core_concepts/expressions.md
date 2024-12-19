# Expressions

Expressions are how you can express computations that should be run over columns of data.

## Creating Expressions

### Referring to a column in a DataFrame

Most commonly you will be creating expressions by using the [`daft.col`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/expression_methods/daft.col.html#daft.col) function.

=== "🐍 Python"
    ``` python
    # Refers to column "A"
    daft.col("A")
    ```

=== "⚙️ SQL"
    ```python
    daft.sql_expr("A")
    ```

``` {title="Output"}

col(A)
```

The above code creates an Expression that refers to a column named `"A"`.

### Using SQL

Daft can also parse valid SQL as expressions.

=== "⚙️ SQL"
    ```python
    daft.sql_expr("A + 1")
    ```
``` {title="Output"}

col(A) + lit(1)
```

The above code will create an expression representing "the column named 'x' incremented by 1". For many APIs, [`sql_expr`](https://www.getdaft.io/projects/docs/en/stable/api_docs/sql.html#daft.sql_expr) will actually be applied for you as syntactic sugar!

### Literals

You may find yourself needing to hardcode a "single value" oftentimes as an expression. Daft provides a [`lit()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/expression_methods/daft.lit.html) helper to do so:

=== "🐍 Python"
    ``` python
    from daft import lit

    # Refers to an expression which always evaluates to 42
    lit(42)
    ```

=== "⚙️ SQL"
    ```python
    # Refers to an expression which always evaluates to 42
    daft.sql_expr("42")
    ```

```{title="Output"}

lit(42)
```
This special :func:`~daft.expressions.lit` expression we just created evaluates always to the value ``42``.

### Wildcard Expressions

You can create expressions on multiple columns at once using a wildcard. The expression [`col("*")`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/expression_methods/daft.col.html#daft.col)) selects every column in a DataFrame, and you can operate on this expression in the same way as a single column:

=== "🐍 Python"
    ``` python
    import daft
    from daft import col

    df = daft.from_pydict({"A": [1, 2, 3], "B": [4, 5, 6]})
    df.select(col("*") * 3).show()
    ```

``` {title="Output"}

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
```

Wildcards also work very well for accessing all members of a struct column:

=== "🐍 Python"
    ``` python

    import daft
    from daft import col

    df = daft.from_pydict({
        "person": [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "Charlie", "age": 35}
        ]
    })

    # Access all fields of the 'person' struct
    df.select(col("person.*")).show()
    ```

=== "⚙️ SQL"
    ```python
    import daft

    df = daft.from_pydict({
        "person": [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "Charlie", "age": 35}
        ]
    })

    # Access all fields of the 'person' struct using SQL
    daft.sql("SELECT person.* FROM df").show()
    ```

``` {title="Output"}

╭──────────┬───────╮
│ name     ┆ age   │
│ ---      ┆ ---   │
│ String   ┆ Int64 │
╞══════════╪═══════╡
│ Alice    ┆ 30    │
├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ Bob      ┆ 25    │
├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ Charlie  ┆ 35    │
╰──────────┴───────╯
```

In this example, we use the wildcard `*` to access all fields of the `person` struct column. This is equivalent to selecting each field individually (`person.name`, `person.age`), but is more concise and flexible, especially when dealing with structs that have many fields.



## Composing Expressions

### Numeric Expressions

Since column "A" is an integer, we can run numeric computation such as addition, division and checking its value. Here are some examples where we create new columns using the results of such computations:

=== "🐍 Python"
    ``` python
    # Add 1 to each element in column "A"
    df = df.with_column("A_add_one", df["A"] + 1)

    # Divide each element in column A by 2
    df = df.with_column("A_divide_two", df["A"] / 2.)

    # Check if each element in column A is more than 1
    df = df.with_column("A_gt_1", df["A"] > 1)

    df.collect()
    ```

=== "⚙️ SQL"
    ```python
    df = daft.sql("""
        SELECT
            *,
            A + 1 AS A_add_one,
            A / 2.0 AS A_divide_two,
            A > 1 AS A_gt_1
        FROM df
    """)
    df.collect()
    ```

```{title="Output"}

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
```

Notice that the returned types of these operations are also well-typed according to their input types. For example, calling ``df["A"] > 1`` returns a column of type [`Boolean`](https://www.getdaft.io/projects/docs/en/stable/api_docs/datatype.html#daft.DataType.bool).

Both the [`Float`](https://www.getdaft.io/projects/docs/en/stable/api_docs/datatype.html#daft.DataType.float32) and [`Int`](https://www.getdaft.io/projects/docs/en/stable/api_docs/datatype.html#daft.DataType.int16) types are numeric types, and inherit many of the same arithmetic Expression operations. You may find the full list of numeric operations in the [Expressions API Reference](https://www.getdaft.io/projects/docs/en/stable/api_docs/expressions.html#numeric).

### String Expressions

Daft also lets you have columns of strings in a DataFrame. Let's take a look!

=== "🐍 Python"
    ``` python
    df = daft.from_pydict({"B": ["foo", "bar", "baz"]})
    df.show()
    ```

``` {title="Output"}

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
```

Unlike the numeric types, the string type does not support arithmetic operations such as `*` and `/`. The one exception to this is the `+` operator, which is overridden to concatenate two string expressions as is commonly done in Python. Let's try that!

=== "🐍 Python"
    ``` python
    df = df.with_column("B2", df["B"] + "foo")
    df.show()
    ```

=== "⚙️ SQL"
    ```python
    df = daft.sql("SELECT *, B + 'foo' AS B2 FROM df")
    df.show()
    ```

``` {title="Output"}

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
```

There are also many string operators that are accessed through a separate [`.str.*`](https://www.getdaft.io/projects/docs/en/stable/api_docs/expressions.html#strings) "method namespace".

For example, to check if each element in column "B" contains the substring "a", we can use the [`.str.contains`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/expression_methods/daft.Expression.str.contains.html#daft.Expression.str.contains) method:

=== "🐍 Python"
    ``` python
    df = df.with_column("B2_contains_B", df["B2"].str.contains(df["B"]))
    df.show()
    ```

=== "⚙️ SQL"
    ```python
    df = daft.sql("SELECT *, contains(B2, B) AS B2_contains_B FROM df")
    df.show()
    ```

``` {title="Output"}

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
```

You may find a full list of string operations in the [Expressions API Reference](https://www.getdaft.io/projects/docs/en/stable/api_docs/expressions.html).

### URL Expressions

One special case of a String column you may find yourself working with is a column of URL strings.

Daft provides the [`.url.*`](https://www.getdaft.io/projects/docs/en/stable/api_docs/expressions.html) method namespace with functionality for working with URL strings. For example, to download data from URLs:

=== "🐍 Python"
    ``` python
    df = daft.from_pydict({
        "urls": [
            "https://www.google.com",
            "s3://daft-public-data/open-images/validation-images/0001eeaf4aed83f9.jpg",
        ],
    })
    df = df.with_column("data", df["urls"].url.download())
    df.collect()
    ```

=== "⚙️ SQL"
    ```python
    df = daft.from_pydict({
        "urls": [
            "https://www.google.com",
            "s3://daft-public-data/open-images/validation-images/0001eeaf4aed83f9.jpg",
        ],
    })
    df = daft.sql("""
        SELECT
            urls,
            url_download(urls) AS data
        FROM df
    """)
    df.collect()
    ```

``` {title="Output"}

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
```

This works well for URLs which are HTTP paths to non-HTML files (e.g. jpeg), local filepaths or even paths to a file in an object store such as AWS S3 as well!

### JSON Expressions

If you have a column of JSON strings, Daft provides the [`.json.*`](https://www.getdaft.io/projects/docs/en/stable/api_docs/expressions.html#json) method namespace to run [JQ-style filters](https://stedolan.github.io/jq/manual/) on them. For example, to extract a value from a JSON object:

=== "🐍 Python"
    ``` python
    df = daft.from_pydict({
        "json": [
            '{"a": 1, "b": 2}',
            '{"a": 3, "b": 4}',
        ],
    })
    df = df.with_column("a", df["json"].json.query(".a"))
    df.collect()
    ```

=== "⚙️ SQL"
    ```python
    df = daft.from_pydict({
        "json": [
            '{"a": 1, "b": 2}',
            '{"a": 3, "b": 4}',
        ],
    })
    df = daft.sql("""
        SELECT
            json,
            json_query(json, '.a') AS a
        FROM df
    """)
    df.collect()
    ```

``` {title="Output"}

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
```

Daft uses [jaq](https://github.com/01mf02/jaq/tree/main) as the underlying executor, so you can find the full list of supported filters in the [jaq documentation](https://github.com/01mf02/jaq/tree/main).

### Logical Expressions

Logical Expressions are an expression that refers to a column of type [`Boolean`](https://www.getdaft.io/projects/docs/en/stable/api_docs/datatype.html#daft.DataType.bool), and can only take on the values True or False.

=== "🐍 Python"
    ``` python
    df = daft.from_pydict({"C": [True, False, True]})
    ```

Daft supports logical operations such as `&` (and) and `|` (or) between logical expressions.

### Comparisons

Many of the types in Daft support comparisons between expressions that returns a Logical Expression.

For example, here we can compare if each element in column "A" is equal to elements in column "B":

=== "🐍 Python"
    ``` python
    df = daft.from_pydict({"A": [1, 2, 3], "B": [1, 2, 4]})

    df = df.with_column("A_eq_B", df["A"] == df["B"])

    df.collect()
    ```

=== "⚙️ SQL"
    ```python
    df = daft.from_pydict({"A": [1, 2, 3], "B": [1, 2, 4]})

    df = daft.sql("""
        SELECT
            A,
            B,
            A = B AS A_eq_B
        FROM df
    """)

    df.collect()
    ```

```{title="Output"}

╭───────┬───────┬─────────╮
│ A     ┆ B     ┆ A_eq_B  │
│ ---   ┆ ---   ┆ ---     │
│ Int64 ┆ Int64 ┆ Boolean │
╞═══════╪═══════╪═════════╡
│ 1     ┆ 1     ┆ true    │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ 2     ┆ 2     ┆ true    │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ 3     ┆ 4     ┆ false   │
╰───────┴───────┴─────────╯

(Showing first 3 of 3 rows)
```

Other useful comparisons can be found in the [Expressions API Reference](https://www.getdaft.io/projects/docs/en/stable/api_docs/expressions.html#numeric).

### If Else Pattern

The [`.if_else()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/expression_methods/daft.Expression.if_else.html#daft.Expression.if_else) method is a useful expression to have up your sleeve for choosing values between two other expressions based on a logical expression:

=== "🐍 Python"
    ``` python
    df = daft.from_pydict({"A": [1, 2, 3], "B": [0, 2, 4]})

    # Pick values from column A if the value in column A is bigger
    # than the value in column B. Otherwise, pick values from column B.
    df = df.with_column(
        "A_if_bigger_else_B",
        (df["A"] > df["B"]).if_else(df["A"], df["B"]),
    )

    df.collect()
    ```

=== "⚙️ SQL"
    ```python
    df = daft.from_pydict({"A": [1, 2, 3], "B": [0, 2, 4]})

    df = daft.sql("""
        SELECT
            A,
            B,
            CASE
                WHEN A > B THEN A
                ELSE B
            END AS A_if_bigger_else_B
        FROM df
    """)

    df.collect()
    ```

```{title="Output"}

╭───────┬───────┬────────────────────╮
│ A     ┆ B     ┆ A_if_bigger_else_B │
│ ---   ┆ ---   ┆ ---                │
│ Int64 ┆ Int64 ┆ Int64              │
╞═══════╪═══════╪════════════════════╡
│ 1     ┆ 0     ┆ 1                  │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 2     ┆ 2     ┆ 2                  │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 3     ┆ 4     ┆ 4                  │
╰───────┴───────┴────────────────────╯

(Showing first 3 of 3 rows)
```

This is a useful expression for cleaning your data!


### Temporal Expressions

Daft provides rich support for working with temporal data types like Timestamp and Duration. Let's explore some common temporal operations:

#### Basic Temporal Operations

You can perform arithmetic operations with timestamps and durations, such as adding a duration to a timestamp or calculating the duration between two timestamps:

=== "🐍 Python"
    ``` python
    import datetime

    df = daft.from_pydict({
        "timestamp": [
            datetime.datetime(2021, 1, 1, 0, 1, 1),
            datetime.datetime(2021, 1, 1, 0, 1, 59),
            datetime.datetime(2021, 1, 1, 0, 2, 0),
        ]
    })

    # Add 10 seconds to each timestamp
    df = df.with_column(
        "plus_10_seconds",
        df["timestamp"] + datetime.timedelta(seconds=10)
    )

    df.show()
    ```

=== "⚙️ SQL"
    ```python
    import datetime

    df = daft.from_pydict({
        "timestamp": [
            datetime.datetime(2021, 1, 1, 0, 1, 1),
            datetime.datetime(2021, 1, 1, 0, 1, 59),
            datetime.datetime(2021, 1, 1, 0, 2, 0),
        ]
    })

    # Add 10 seconds to each timestamp and calculate duration between timestamps
    df = daft.sql("""
        SELECT
            timestamp,
            timestamp + INTERVAL '10 seconds' as plus_10_seconds,
        FROM df
    """)

    df.show()
    ```

``` {title="Output"}

╭───────────────────────────────┬───────────────────────────────╮
│ timestamp                     ┆ plus_10_seconds               │
│ ---                           ┆ ---                           │
│ Timestamp(Microseconds, None) ┆ Timestamp(Microseconds, None) │
╞═══════════════════════════════╪═══════════════════════════════╡
│ 2021-01-01 00:01:01           ┆ 2021-01-01 00:01:11           │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 2021-01-01 00:01:59           ┆ 2021-01-01 00:02:09           │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 2021-01-01 00:02:00           ┆ 2021-01-01 00:02:10           │
╰───────────────────────────────┴───────────────────────────────╯
```

#### Temporal Component Extraction

The [`.dt.*`](https://www.getdaft.io/projects/docs/en/stable/api_docs/expressions.html#temporal) method namespace provides extraction methods for the components of a timestamp, such as year, month, day, hour, minute, and second:

=== "🐍 Python"
    ``` python
    df = daft.from_pydict({
        "timestamp": [
            datetime.datetime(2021, 1, 1, 0, 1, 1),
            datetime.datetime(2021, 1, 1, 0, 1, 59),
            datetime.datetime(2021, 1, 1, 0, 2, 0),
        ]
    })

    # Extract year, month, day, hour, minute, and second from the timestamp
    df = df.with_columns({
        "year": df["timestamp"].dt.year(),
        "month": df["timestamp"].dt.month(),
        "day": df["timestamp"].dt.day(),
        "hour": df["timestamp"].dt.hour(),
        "minute": df["timestamp"].dt.minute(),
        "second": df["timestamp"].dt.second()
    })

    df.show()
    ```

=== "⚙️ SQL"
    ```python
    df = daft.from_pydict({
        "timestamp": [
            datetime.datetime(2021, 1, 1, 0, 1, 1),
            datetime.datetime(2021, 1, 1, 0, 1, 59),
            datetime.datetime(2021, 1, 1, 0, 2, 0),
        ]
    })

    # Extract year, month, day, hour, minute, and second from the timestamp
    df = daft.sql("""
        SELECT
            timestamp,
            year(timestamp) as year,
            month(timestamp) as month,
            day(timestamp) as day,
            hour(timestamp) as hour,
            minute(timestamp) as minute,
            second(timestamp) as second
        FROM df
    """)

    df.show()
    ```

``` {title="Output"}

╭───────────────────────────────┬───────┬────────┬────────┬────────┬────────┬────────╮
│ timestamp                     ┆ year  ┆ month  ┆ day    ┆ hour   ┆ minute ┆ second │
│ ---                           ┆ ---   ┆ ---    ┆ ---    ┆ ---    ┆ ---    ┆ ---    │
│ Timestamp(Microseconds, None) ┆ Int32 ┆ UInt32 ┆ UInt32 ┆ UInt32 ┆ UInt32 ┆ UInt32 │
╞═══════════════════════════════╪═══════╪════════╪════════╪════════╪════════╪════════╡
│ 2021-01-01 00:01:01           ┆ 2021  ┆ 1      ┆ 1      ┆ 0      ┆ 1      ┆ 1      │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ 2021-01-01 00:01:59           ┆ 2021  ┆ 1      ┆ 1      ┆ 0      ┆ 1      ┆ 59     │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ 2021-01-01 00:02:00           ┆ 2021  ┆ 1      ┆ 1      ┆ 0      ┆ 2      ┆ 0      │
╰───────────────────────────────┴───────┴────────┴────────┴────────┴────────┴────────╯
```

#### Time Zone Operations

You can parse strings as timestamps with time zones and convert between different time zones:

=== "🐍 Python"
    ``` python
    df = daft.from_pydict({
        "timestamp_str": [
            "2021-01-01 00:00:00.123 +0800",
            "2021-01-02 12:30:00.456 +0800"
        ]
    })

    # Parse the timestamp string with time zone and convert to New York time
    df = df.with_column(
        "ny_time",
        df["timestamp_str"].str.to_datetime(
            "%Y-%m-%d %H:%M:%S%.3f %z",
            timezone="America/New_York"
        )
    )

    df.show()
    ```

=== "⚙️ SQL"
    ```python
    df = daft.from_pydict({
        "timestamp_str": [
            "2021-01-01 00:00:00.123 +0800",
            "2021-01-02 12:30:00.456 +0800"
        ]
    })

    # Parse the timestamp string with time zone and convert to New York time
    df = daft.sql("""
        SELECT
            timestamp_str,
            to_datetime(timestamp_str, '%Y-%m-%d %H:%M:%S%.3f %z', 'America/New_York') as ny_time
        FROM df
    """)

    df.show()
    ```

``` {title="Output"}

╭───────────────────────────────┬───────────────────────────────────────────────────╮
│ timestamp_str                 ┆ ny_time                                           │
│ ---                           ┆ ---                                               │
│ Utf8                          ┆ Timestamp(Milliseconds, Some("America/New_York")) │
╞═══════════════════════════════╪═══════════════════════════════════════════════════╡
│ 2021-01-01 00:00:00.123 +0800 ┆ 2020-12-31 11:00:00.123 EST                       │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 2021-01-02 12:30:00.456 +0800 ┆ 2021-01-01 23:30:00.456 EST                       │
╰───────────────────────────────┴───────────────────────────────────────────────────╯
```

#### Temporal Truncation

The [`.dt.truncate()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/expression_methods/daft.Expression.dt.truncate.html#daft.Expression.dt.truncate) method allows you to truncate timestamps to specific time units. This can be useful for grouping data by time periods. For example, to truncate timestamps to the nearest hour:

=== "🐍 Python"
    ``` python
    df = daft.from_pydict({
        "timestamp": [
            datetime.datetime(2021, 1, 7, 0, 1, 1),
            datetime.datetime(2021, 1, 8, 0, 1, 59),
            datetime.datetime(2021, 1, 9, 0, 30, 0),
            datetime.datetime(2021, 1, 10, 1, 59, 59),
        ]
    })

    # Truncate timestamps to the nearest hour
    df = df.with_column(
        "hour_start",
        df["timestamp"].dt.truncate("1 hour")
    )

    df.show()
    ```

``` {title="Output"}

╭───────────────────────────────┬───────────────────────────────╮
│ timestamp                     ┆ hour_start                    │
│ ---                           ┆ ---                           │
│ Timestamp(Microseconds, None) ┆ Timestamp(Microseconds, None) │
╞═══════════════════════════════╪═══════════════════════════════╡
│ 2021-01-07 00:01:01           ┆ 2021-01-07 00:00:00           │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 2021-01-08 00:01:59           ┆ 2021-01-08 00:00:00           │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 2021-01-09 00:30:00           ┆ 2021-01-09 00:00:00           │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 2021-01-10 01:59:59           ┆ 2021-01-10 01:00:00           │
╰───────────────────────────────┴───────────────────────────────╯
```
