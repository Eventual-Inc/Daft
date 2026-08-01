# functions list

## chunk

```python
chunk(list_expr: Expression, size: int) -> Expression
```

Splits each list into chunks of the given size.

Args:
    list_expr (List Expression): expression to chunk
    size (int): size of chunks to split the list into. Must be greater than 0

Returns:
    Expression (List[FixedSizedList] Expression):
        expression with lists of fixed size lists of the type of the list values

## explode

```python
explode(list_expr: Expression, ignore_empty_and_null: bool=False) -> Expression
```

Explode a list expression.

A row is created for each item in the lists, and the other non-exploded output columns are broadcasted to match.

If exploding multiple columns at once, all list lengths must match.

Note:
    Since this changes the cardinality of the dataframe, We only allow a single explode per projection (`select`, `with_columns`)
    If you need to do multiple explodes, each one must be done separately.

Args:
    list_expr (List Expression): expression to explode.
    ignore_empty_and_null: If True, drops rows where the list is empty or null.
        If False (default), empty lists and null values each produce a single row with a null value.

Returns:
    Expression: Expression representing the exploded list.

Tip: See also
    [`DataFrame.explode`](https://docs.daft.ai/en/stable/api/dataframe/#daft.DataFrame.explode)

## list_append

```python
list_append(list_expr: Expression, other: Expression) -> Expression
```

Appends a value to each list in the column.

Args:
    list_expr (List Expression): expression to append to
    other (Expression): A value or column of values to append to each list

Returns:
    Expression (List Expression): an expression with the updated lists

## list_bool_and

```python
list_bool_and(list_expr: Expression) -> Expression
```

Calculates the boolean AND of all values in a list.

For each list:
- Returns True if all non-null values are True
- Returns False if any non-null value is False
- Returns null if the list is empty or contains only null values

Args:
    list_expr (List Expression): expression to calculate the boolean AND of.

Returns:
    Expression (Boolean Expression): an expression with the result of the boolean AND operation.

## list_bool_or

```python
list_bool_or(list_expr: Expression) -> Expression
```

Calculates the boolean OR of all values in a list.

For each list:
- Returns True if any non-null value is True
- Returns False if all non-null values are False
- Returns null if the list is empty or contains only null values

Args:
    list_expr (List Expression): expression to calculate the boolean OR of.

Returns:
     Expression (Boolean Expression): an expression with the result of the boolean OR operation.

## list_contains

```python
list_contains(list_expr: Expression, item: Expression) -> Expression
```

Checks if each list contains the specified item.

Args:
    list_expr: expression to search in
    item: value or column of values to search for

Returns:
    Boolean expression indicating whether each list contains the item

## list_count

```python
list_count(list_expr: Expression, mode: Literal['all', 'valid', 'null'] | CountMode=CountMode.Valid) -> Expression
```

Counts the number of elements in each list.

Args:
    list_expr (List Expression): The list expression to count elements of.
    mode (str | CountMode, default=CountMode.Valid):
        A string ("all", "valid", or "null") that represents whether to count all values, non-null (valid) values, or null values. Defaults to "valid".

Returns:
    Expression (UInt64 Expression): an expression which is the length of each list

## list_distinct

```python
list_distinct(list_expr: Expression) -> Expression
```

Returns a list of unique elements in each list, preserving order of first occurrence and ignoring nulls.

Args:
    list_expr (List Expression): The input list expression

Returns:
    Expression (List Expression): an expression with lists containing only unique elements

## list_filter

```python
list_filter(list_expr: Expression, predicate: Expression) -> Expression
```

Filters elements in a list using a boolean predicate expression.

Elements where the predicate evaluates to `False` or `null` are removed.
Null list rows remain null. Empty list rows remain empty.

Args:
    list_expr (List Expression): expression to filter.
    predicate: Boolean expression to evaluate on each element. Use `daft.element()` to reference the current element.

Returns:
    Expression (List Expression): an expression representing the filtered list.

## list_flatten

```python
list_flatten(list_expr: Expression) -> Expression
```

Flattens one level of nesting in each list.

Outer null rows are preserved as null. Null inner lists are skipped while flattening,
and null leaf values are preserved in the output.

Args:
    list_expr (List Expression): expression to flatten one level.

Returns:
    Expression (List Expression): an expression with one fewer level of list nesting.

## list_join

```python
list_join(list_expr: Expression, delimiter: str | Expression) -> Expression
```

Joins every element of a list using the specified string delimiter.

Args:
    list_expr (List Expression): expression to join
    delimiter (str | String Expression): the delimiter to use to join lists with

Returns:
    Expression (String Expression): an expression which is every element of the list joined on the delimiter

## list_map

```python
list_map(list_expr: Expression, mapper: Expression) -> Expression
```

Evaluates an expression on all elements in the list.

Args:
    list_expr (List Expression): expression to map over.
    mapper: Expression to run. You can select the element with `daft.element()`

Returns:
    Expression (List Expression): an expression representing the mapped list.

## list_max

```python
list_max(list_expr: Expression) -> Expression
```

Calculates the maximum of each list. If no non-null values in a list, the result is null.

Args:
    list_expr (List Expression): expression to calculate the maximum of.

Returns:
    Expression:
        an expression with the type of the list values representing the maximum value in the list

## list_mean

```python
list_mean(list_expr: Expression) -> Expression
```

Calculates the mean of each list. If no non-null values in a list, the result is null.

Args:
    list_expr (List Expression): expression to calculate the mean of.

Returns:
    Expression (Float64 Expression): an expression with the calculated mean of the list values

## list_min

```python
list_min(list_expr: Expression) -> Expression
```

Calculates the minimum of each list. If no non-null values in a list, the result is null.

Args:
    list_expr (List Expression): expression to calculate the minimum of.

Returns:
    Expression:
        an expression with the type of the list values representing the minimum value in the list

## list_sort

```python
list_sort(list_expr: Expression, desc: bool | Expression | None=None, nulls_first: bool | Expression | None=None) -> Expression
```

Sorts the inner lists of a list column.

Args:
    list_expr (List Expression): expression to sort
    desc: (bool | Boolean Expression) Whether to sort in descending order. Defaults to false. Pass in a boolean column to control for each row.
    nulls_first: (bool | Boolean Expression) Whether to put nulls first. Defaults to false (nulls last). Pass in a boolean column to control for each row.

Returns:
    Expression (List Expression): an expression with the sorted lists

## list_sum

```python
list_sum(list_expr: Expression) -> Expression
```

Sums each list. Empty lists and lists with all nulls yield null.

Args:
    list_expr (List Expression): expression to sum elements of.

Returns:
    Expression: an expression with the type of the list values

## seq

```python
seq(n: Expression) -> Expression
```

Generates a list of sequential integers [0, 1, 2, ..., n-1] for each row.

Args:
    n (Expression): An integer expression specifying the length of the sequence.

Returns:
    Expression (List[UInt64] Expression): An expression with lists of sequential integers.

## to_list

```python
to_list(*items: Expression) -> Expression
```

Constructs a list from the item expressions.

Args:
    *items: item expressions to construct the list

Returns:
    Expression (List Expression): expression representing the constructed list

## value_counts

```python
value_counts(list_expr: Expression) -> Expression
```

Counts the occurrences of each distinct value in the list.

Args:
    list_expr (List Expression): expression to count the occurrences of each distinct value in.

Returns:
    Expression (Map Expression):
        A Map<X, UInt64> expression where the keys are distinct elements from the
        original list of type X, and the values are UInt64 counts representing
        the number of times each element appears in the list.

Note:
    This function does not work for nested types. For example, it will not produce a map
    with lists as keys.
