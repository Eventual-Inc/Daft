# Working with JSON and Nested Data

Daft provides powerful capabilities for working with JSON data and nested data structures. Whether you're processing API responses, log files, or complex hierarchical data, Daft's JSON modality makes it easy to parse, query, and manipulate structured data.

## JSON

If you have a column of JSON strings, Daft provides the [`.json.*`](../api/expressions.md#daft.expressions.expressions.ExpressionJsonNamespace) method namespace to run [JQ-style filters](https://stedolan.github.io/jq/manual/) on them. For example, to extract a value from a JSON object:

<!-- todo(docs - cc): add relative path to .json after figure out json namespace-->

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

<!-- ### Deserializing JSON and extracting multiple fields -->

## Extracting and Flattening Nested Data

When working with nested data---like log files, metadata, deserialized JSON---we often need to extract specific fields or flatten the entire structure into individual columns. Daft provides two main approaches for this:

1. **Extracting specific fields**: Using the `[]` operator to access nested fields
2. **Flattening all fields**: Using `.unnest()` or the `*` wildcard to expand all nested fields into separate columns

Consider the following example reading from the [nebius/SWE-rebench](https://huggingface.co/datasets/nebius/SWE-rebench) dataset.

=== "🐍 Python"
    ``` python

    import daft
    from daft import col

    swe_rebench_metadata = daft.read_parquet("hf://datasets/nebius/SWE-rebench/data/*.parquet").select("meta")
    swe_rebench_metadata.schema()
    ```

``` {title="Output"}

╭─────────────┬─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ column_name ┆ type                                                                                                                                                                                                                        │
╞═════════════╪═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╡
│ meta        ┆ Struct[commit_name: Utf8, failed_lite_validators: List[Utf8], has_test_patch: Boolean, is_lite: Boolean, llm_score: Struct[difficulty_score: Int64, issue_text_score: Int64, test_score: Int64], num_modified_files: Int64] │
╰─────────────┴─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

We could extract a specific field from the struct by using the `[]` operator. For example, to extract the `difficulty_score` from the `llm_score` struct:

=== "🐍 Python"
    ```python
    swe_rebench_metadata.select(col("meta")["llm_score"]["difficulty_score"]).show()
    ```

``` {title="Output"}

╭──────────────────╮
│ difficulty_score │
│ ---              │
│ Int64            │
╞══════════════════╡
│ 2                │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 1                │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 2                │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 2                │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 0                │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 0                │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 1                │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 0                │
╰──────────────────╯

(Showing first 8 rows)
```

If we want to extract all the nested columns, we can use the [`.unnest()`][daft.expressions.Expression.unnest] expression or the wildcard `*` to access all fields of the `meta` struct column.

=== "🐍 Python"
    ``` python

    swe_rebench_metadata.select(daft.col("meta").unnest()).show()
    # Alternatively:
    #   swe_rebench_metadata.select(daft.col("meta")["*"]).show()
    ```

``` {title="Output"}

╭─────────────┬────────────────────────────────┬────────────────┬─────────┬─────────────────────────────────────────────────────────────────────────────┬────────────────────╮
│ commit_name ┆ failed_lite_validators         ┆ has_test_patch ┆ is_lite ┆ llm_score                                                                   ┆ num_modified_files │
│ ---         ┆ ---                            ┆ ---            ┆ ---     ┆ ---                                                                         ┆ ---                │
│ Utf8        ┆ List[Utf8]                     ┆ Boolean        ┆ Boolean ┆ Struct[difficulty_score: Int64, issue_text_score: Int64, test_score: Int64] ┆ Int64              │
╞═════════════╪════════════════════════════════╪════════════════╪═════════╪═════════════════════════════════════════════════════════════════════════════╪════════════════════╡
│ head_commit ┆ [has_short_problem_statement,… ┆ true           ┆ false   ┆ {difficulty_score: 2,                                                       ┆ 5                  │
│             ┆                                ┆                ┆         ┆ issue_t…                                                                    ┆                    │
├╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ head_commit ┆ [has_many_modified_files, has… ┆ true           ┆ false   ┆ {difficulty_score: 1,                                                       ┆ 5                  │
│             ┆                                ┆                ┆         ┆ issue_t…                                                                    ┆                    │
├╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ head_commit ┆ [has_removed_files, has_many_… ┆ true           ┆ false   ┆ {difficulty_score: 2,                                                       ┆ 6                  │
│             ┆                                ┆                ┆         ┆ issue_t…                                                                    ┆                    │
├╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ head_commit ┆ []                             ┆ true           ┆ true    ┆ {difficulty_score: 2,                                                       ┆ 1                  │
│             ┆                                ┆                ┆         ┆ issue_t…                                                                    ┆                    │
├╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ head_commit ┆ []                             ┆ true           ┆ true    ┆ {difficulty_score: 0,                                                       ┆ 1                  │
│             ┆                                ┆                ┆         ┆ issue_t…                                                                    ┆                    │
├╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ head_commit ┆ []                             ┆ true           ┆ true    ┆ {difficulty_score: 0,                                                       ┆ 1                  │
│             ┆                                ┆                ┆         ┆ issue_t…                                                                    ┆                    │
├╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ head_commit ┆ []                             ┆ true           ┆ true    ┆ {difficulty_score: 1,                                                       ┆ 1                  │
│             ┆                                ┆                ┆         ┆ issue_t…                                                                    ┆                    │
├╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ head_commit ┆ [has_hyperlinks, has_issue_re… ┆ true           ┆ false   ┆ {difficulty_score: 0,                                                       ┆ 3                  │
│             ┆                                ┆                ┆         ┆ issue_t…                                                                    ┆                    │
╰─────────────┴────────────────────────────────┴────────────────┴─────────┴─────────────────────────────────────────────────────────────────────────────┴────────────────────╯

(Showing first 8 rows)
```
